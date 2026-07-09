import { describe, expect, it } from "vitest";
import { DuplicateAggregateError, OptimisticLockError, StoreError } from "../src/index.js";
import {
  type PgPoolClientLike,
  type PgPoolLike,
  type PgQueryResultLike,
  PostgresEventStore,
  defaultPostgresStoreConfig,
} from "../src/postgres/index.js";
import type { Counter, CounterCommand, CounterEvent } from "./fixtures.js";
import {
  IncrementedEvent,
  counterAggregateSerializer,
  counterEventSerializer,
  counterId,
} from "./fixtures.js";

const MAX_UINT64 = 0xffffffffffffffffn;

interface RecordedQuery {
  sql: string;
  values: unknown[] | undefined;
}

class FakePool implements PgPoolLike {
  readonly queries: RecordedQuery[] = [];
  readonly txQueries: RecordedQuery[] = [];
  released = false;
  private readonly responses: (PgQueryResultLike | Error)[] = [];
  private readonly txResponses: (PgQueryResultLike | Error)[] = [];

  enqueue(response: PgQueryResultLike | Error): void {
    this.responses.push(response);
  }

  enqueueTx(response: PgQueryResultLike | Error): void {
    this.txResponses.push(response);
  }

  async query(sql: string, values?: unknown[]): Promise<PgQueryResultLike> {
    this.queries.push({ sql, values });
    const response = this.responses.shift() ?? { rows: [], rowCount: 0 };
    if (response instanceof Error) throw response;
    return response;
  }

  async connect(): Promise<PgPoolClientLike> {
    return {
      query: async (sql: string, values?: unknown[]): Promise<PgQueryResultLike> => {
        this.txQueries.push({ sql, values });
        const response = this.txResponses.shift() ?? { rows: [], rowCount: 0 };
        if (response instanceof Error) throw response;
        return response;
      },
      release: () => {
        this.released = true;
      },
    };
  }
}

function recorded(list: RecordedQuery[], index: number): RecordedQuery {
  const query = list[index];
  if (query === undefined) throw new Error(`no query recorded at index ${index}`);
  return query;
}

function newStore(pool: FakePool) {
  return new PostgresEventStore<Counter, CounterCommand, CounterEvent>({
    pool,
    aggregateSerializer: counterAggregateSerializer,
    eventSerializer: counterEventSerializer,
  });
}

function storedEvent(seqNr: bigint, overrides?: { traceParent?: string; traceState?: string }) {
  return {
    event: new IncrementedEvent(counterId("c1"), 2),
    eventId: "a".repeat(32),
    seqNr,
    isCreated: seqNr === 1n,
    occurredAt: new Date(1720000000000),
    traceParent: overrides?.traceParent ?? "",
    traceState: overrides?.traceState ?? "",
  };
}

describe("defaultPostgresStoreConfig", () => {
  it("uses the Go default table names", () => {
    expect(defaultPostgresStoreConfig()).toEqual({
      journalTable: "event_journal",
      snapshotTable: "event_snapshot",
    });
  });
});

describe("PostgresEventStore.getLatestSnapshot", () => {
  it("returns found:false when no row exists", async () => {
    const pool = new FakePool();
    pool.enqueue({ rows: [], rowCount: 0 });
    const result = await newStore(pool).getLatestSnapshot(counterId("c1"));
    expect(result._unsafeUnwrap()).toEqual({ found: false });
    expect(recorded(pool.queries, 0).sql).toContain("FROM event_snapshot");
    expect(recorded(pool.queries, 0).values).toEqual(["Counter-c1"]);
  });

  it("unmarshals a snapshot row (bigint columns arrive as strings)", async () => {
    const pool = new FakePool();
    const payload = counterAggregateSerializer
      .serialize({ aggregateId: () => counterId("c1"), count: 6 } as Counter)
      ._unsafeUnwrap();
    pool.enqueue({
      rows: [
        {
          seq_nr: "5",
          version: "1",
          payload: Buffer.from(payload),
          occurred_at: new Date(1720000000000),
        },
      ],
      rowCount: 1,
    });
    const result = (await newStore(pool).getLatestSnapshot(counterId("c1")))._unsafeUnwrap();
    expect(result.found).toBe(true);
    if (result.found) {
      expect(result.snapshot.seqNr).toBe(5n);
      expect(result.snapshot.version).toBe(1n);
      expect(result.snapshot.aggregate.count).toBe(6);
      expect(result.snapshot.occurredAt.getTime()).toBe(1720000000000);
    }
  });

  it("wraps query failures in StoreError", async () => {
    const pool = new FakePool();
    pool.enqueue(new Error("connection refused"));
    const result = await newStore(pool).getLatestSnapshot(counterId("c1"));
    expect(result._unsafeUnwrapErr()).toBeInstanceOf(StoreError);
  });
});

describe("PostgresEventStore.getEventsSince", () => {
  it("returns [] for the max-uint64 sentinel without querying", async () => {
    const pool = new FakePool();
    const result = await newStore(pool).getEventsSince(counterId("c1"), MAX_UINT64);
    expect(result._unsafeUnwrap()).toEqual([]);
    expect(pool.queries).toHaveLength(0);
  });

  it("queries with an exclusive lower bound and unmarshals rows", async () => {
    const pool = new FakePool();
    const payload = counterEventSerializer
      .serialize(new IncrementedEvent(counterId("c1"), 2))
      ._unsafeUnwrap();
    pool.enqueue({
      rows: [
        {
          event_id: "e".repeat(32),
          seq_nr: "6",
          type_name: "Incremented",
          payload: Buffer.from(payload),
          is_created: false,
          occurred_at: new Date(1720000000000),
          traceparent: "",
          tracestate: "",
        },
      ],
      rowCount: 1,
    });
    const events = (await newStore(pool).getEventsSince(counterId("c1"), 5n))._unsafeUnwrap();
    expect(events).toHaveLength(1);
    const first = events[0];
    if (first === undefined) throw new Error("missing event");
    expect(first.seqNr).toBe(6n);
    expect(first.eventId).toBe("e".repeat(32));
    expect(first.isCreated).toBe(false);
    expect(first.traceParent).toBe("");
    expect((first.event as IncrementedEvent).amount).toBe(2);
    const q = recorded(pool.queries, 0);
    expect(q.sql).toContain("FROM event_journal");
    expect(q.sql).toContain("seq_nr > $2");
    expect(q.sql).toContain("ORDER BY seq_nr ASC");
    expect(q.values).toEqual(["Counter-c1", "5"]);
  });
});

describe("PostgresEventStore.persistEvent", () => {
  it("inserts an event with ON CONFLICT DO NOTHING", async () => {
    const pool = new FakePool();
    pool.enqueue({ rows: [], rowCount: 1 });
    const result = await newStore(pool).persistEvent(storedEvent(1n), 0n);
    expect(result.isOk()).toBe(true);
    const q = recorded(pool.queries, 0);
    expect(q.sql).toContain("INSERT INTO event_journal");
    expect(q.sql).toContain("ON CONFLICT (aggregate_id, seq_nr) DO NOTHING");
    expect(q.values?.[0]).toBe("Counter-c1");
    expect(q.values?.[1]).toBe("1"); // seq_nr as string (pg cannot bind bigint)
    expect(q.values?.[2]).toBe("a".repeat(32));
    expect(q.values?.[3]).toBe("Incremented");
    expect(q.values?.[5]).toBe(true); // is_created
    expect(q.values?.[6]).toEqual(new Date(1720000000000));
    expect(q.values?.[7]).toBeNull(); // empty traceparent -> NULL
    expect(q.values?.[8]).toBeNull();
  });

  it("writes trace columns when the stored event carries them", async () => {
    const pool = new FakePool();
    pool.enqueue({ rows: [], rowCount: 1 });
    await newStore(pool).persistEvent(
      storedEvent(1n, { traceParent: "00-abc-def-01", traceState: "vendor=1" }),
      0n,
    );
    expect(recorded(pool.queries, 0).values?.[7]).toBe("00-abc-def-01");
    expect(recorded(pool.queries, 0).values?.[8]).toBe("vendor=1");
  });

  it("maps a conflicting insert (0 rows) to DuplicateAggregateError", async () => {
    const pool = new FakePool();
    pool.enqueue({ rows: [], rowCount: 0 });
    const result = await newStore(pool).persistEvent(storedEvent(1n), 0n);
    expect(result._unsafeUnwrapErr()).toBeInstanceOf(DuplicateAggregateError);
  });
});

describe("PostgresEventStore.persistEventAndSnapshot", () => {
  function snapshot(version: bigint) {
    return {
      aggregate: {
        aggregateId: () => counterId("c1"),
        count: 5,
      } as Counter,
      seqNr: 5n,
      version,
      occurredAt: new Date(1720000000000),
    };
  }

  it("wraps event insert + snapshot upsert in one transaction", async () => {
    const pool = new FakePool();
    pool.enqueueTx({ rows: [], rowCount: 0 }); // BEGIN
    pool.enqueueTx({ rows: [], rowCount: 1 }); // insert event
    pool.enqueueTx({ rows: [], rowCount: 1 }); // upsert snapshot
    pool.enqueueTx({ rows: [], rowCount: 0 }); // COMMIT
    const result = await newStore(pool).persistEventAndSnapshot(storedEvent(5n), snapshot(2n));
    expect(result.isOk()).toBe(true);
    const sqls = pool.txQueries.map((q) => q.sql);
    expect(sqls[0]).toBe("BEGIN");
    expect(sqls[1]).toContain("INSERT INTO event_journal");
    expect(sqls[2]).toContain("INSERT INTO event_snapshot");
    expect(sqls[2]).toContain("ON CONFLICT (aggregate_id) DO UPDATE");
    expect(sqls[3]).toBe("COMMIT");
    // upsert params: aid, seq_nr, version, payload, occurred_at, expected version
    expect(recorded(pool.txQueries, 2).values?.[2]).toBe("2");
    expect(recorded(pool.txQueries, 2).values?.[5]).toBe("1"); // expected = version - 1
    expect(pool.released).toBe(true);
  });

  it("maps an event seq_nr conflict to OptimisticLockError and rolls back", async () => {
    const pool = new FakePool();
    pool.enqueueTx({ rows: [], rowCount: 0 }); // BEGIN
    pool.enqueueTx({ rows: [], rowCount: 0 }); // insert event conflict
    const result = await newStore(pool).persistEventAndSnapshot(storedEvent(5n), snapshot(2n));
    expect(result._unsafeUnwrapErr()).toBeInstanceOf(OptimisticLockError);
    expect(pool.txQueries.at(-1)?.sql).toBe("ROLLBACK");
    expect(pool.released).toBe(true);
  });

  it("maps a snapshot version mismatch to OptimisticLockError and rolls back", async () => {
    const pool = new FakePool();
    pool.enqueueTx({ rows: [], rowCount: 0 }); // BEGIN
    pool.enqueueTx({ rows: [], rowCount: 1 }); // insert event
    pool.enqueueTx({ rows: [], rowCount: 0 }); // upsert misses (version mismatch)
    const result = await newStore(pool).persistEventAndSnapshot(storedEvent(5n), snapshot(2n));
    expect(result._unsafeUnwrapErr()).toBeInstanceOf(OptimisticLockError);
    expect(pool.txQueries.at(-1)?.sql).toBe("ROLLBACK");
    expect(pool.released).toBe(true);
  });

  it("wraps unexpected tx failures in StoreError and releases the client", async () => {
    const pool = new FakePool();
    pool.enqueueTx({ rows: [], rowCount: 0 }); // BEGIN
    pool.enqueueTx(new Error("deadlock"));
    const result = await newStore(pool).persistEventAndSnapshot(storedEvent(5n), snapshot(2n));
    expect(result._unsafeUnwrapErr()).toBeInstanceOf(StoreError);
    expect(pool.released).toBe(true);
  });
});

describe("PostgresEventStore schema management", () => {
  it("creates journal and snapshot tables idempotently", async () => {
    const pool = new FakePool();
    pool.enqueue({ rows: [], rowCount: 0 });
    pool.enqueue({ rows: [], rowCount: 0 });
    const result = await newStore(pool).createTables();
    expect(result.isOk()).toBe(true);
    expect(recorded(pool.queries, 0).sql).toContain("CREATE TABLE IF NOT EXISTS event_journal");
    expect(recorded(pool.queries, 0).sql).toContain("PRIMARY KEY (aggregate_id, seq_nr)");
    expect(recorded(pool.queries, 1).sql).toContain("CREATE TABLE IF NOT EXISTS event_snapshot");
    expect(recorded(pool.queries, 1).sql).toContain("aggregate_id text   PRIMARY KEY");
  });

  it("drops both tables", async () => {
    const pool = new FakePool();
    pool.enqueue({ rows: [], rowCount: 0 });
    pool.enqueue({ rows: [], rowCount: 0 });
    const result = await newStore(pool).dropTables();
    expect(result.isOk()).toBe(true);
    expect(recorded(pool.queries, 0).sql).toContain("DROP TABLE IF EXISTS event_journal");
    expect(recorded(pool.queries, 1).sql).toContain("DROP TABLE IF EXISTS event_snapshot");
  });
});
