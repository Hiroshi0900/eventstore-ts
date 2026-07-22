import { ResultAsync, errAsync, okAsync } from "neverthrow";
import {
  DuplicateAggregateError,
  type EventStoreError,
  OptimisticLockError,
  StoreError,
} from "../errors.js";
import type { EventStore } from "../event-store.js";
import { currentTraceContext } from "../otel.js";
import type { AggregateSerializer, EventSerializer } from "../serializer.js";
import type {
  Aggregate,
  AggregateId,
  Command,
  Event,
  StoredEvent,
  StoredSnapshot,
} from "../types.js";

const MAX_UINT64 = 0xffffffffffffffffn;

export interface PostgresStoreConfig {
  readonly journalTable: string;
  readonly snapshotTable: string;
}

export function defaultPostgresStoreConfig(): PostgresStoreConfig {
  return {
    journalTable: "event_journal",
    snapshotTable: "event_snapshot",
  };
}

export interface PgQueryResultLike {
  rows: Record<string, unknown>[];
  rowCount: number | null;
}

/** Minimal query surface shared by pg.Pool and pg.PoolClient. */
export interface PgClientLike {
  query(sql: string, values?: unknown[]): Promise<PgQueryResultLike>;
}

export interface PgPoolClientLike extends PgClientLike {
  release(): void;
}

/** Minimal pool surface (satisfied by pg.Pool); fakeable in tests. */
export interface PgPoolLike extends PgClientLike {
  connect(): Promise<PgPoolClientLike>;
}

export interface PostgresStoreOptions<A, E> {
  pool: PgPoolLike;
  aggregateSerializer: AggregateSerializer<A>;
  eventSerializer: EventSerializer<E>;
  config?: PostgresStoreConfig;
}

/**
 * PostgreSQL-backed EventStore mirroring the Go postgres adapter: same table
 * layout (journal PK = (aggregate_id, seq_nr), snapshot PK = aggregate_id)
 * and the same optimistic-lock semantics — DynamoDB's conditional writes map
 * to ON CONFLICT DO NOTHING / version-conditioned UPSERT in one transaction.
 */
export class PostgresEventStore<
  A extends Aggregate<C, E, unknown>,
  C extends Command,
  E extends Event,
> implements EventStore<A, C, E>
{
  private readonly pool: PgPoolLike;
  private readonly aggSer: AggregateSerializer<A>;
  private readonly eventSer: EventSerializer<E>;
  private readonly config: PostgresStoreConfig;

  private readonly selectSnapshotSql: string;
  private readonly selectEventsSql: string;
  private readonly insertEventSql: string;
  private readonly upsertSnapshotSql: string;

  constructor(options: PostgresStoreOptions<A, E>) {
    this.pool = options.pool;
    this.aggSer = options.aggregateSerializer;
    this.eventSer = options.eventSerializer;
    this.config = options.config ?? defaultPostgresStoreConfig();

    const journal = this.config.journalTable;
    const snapshot = this.config.snapshotTable;
    this.selectSnapshotSql = `SELECT seq_nr, version, payload, occurred_at FROM ${snapshot} WHERE aggregate_id = $1`;
    this.selectEventsSql = `SELECT event_id, seq_nr, type_name, payload, is_created, occurred_at,
       COALESCE(traceparent, '') AS traceparent, COALESCE(tracestate, '') AS tracestate
     FROM ${journal} WHERE aggregate_id = $1 AND seq_nr > $2 ORDER BY seq_nr ASC`;
    this.insertEventSql = `INSERT INTO ${journal}
       (aggregate_id, seq_nr, event_id, type_name, payload, is_created, occurred_at, traceparent, tracestate)
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
     ON CONFLICT (aggregate_id, seq_nr) DO NOTHING`;
    // Version-conditioned UPSERT, equivalent to DynamoDB's
    // attribute_not_exists(version) OR version = :expected — insert when the
    // row is absent, update only when the stored version matches, otherwise
    // affect 0 rows (optimistic-lock failure).
    this.upsertSnapshotSql = `INSERT INTO ${snapshot} (aggregate_id, seq_nr, version, payload, occurred_at)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (aggregate_id) DO UPDATE
       SET seq_nr = EXCLUDED.seq_nr, version = EXCLUDED.version,
           payload = EXCLUDED.payload, occurred_at = EXCLUDED.occurred_at
     WHERE ${snapshot}.version = $6`;
  }

  getLatestSnapshot(
    id: AggregateId,
  ): ResultAsync<{ snapshot: StoredSnapshot<A>; found: true } | { found: false }, EventStoreError> {
    return ResultAsync.fromPromise(
      this.pool.query(this.selectSnapshotSql, [id.asString()]),
      (cause) => new StoreError("failed to get latest snapshot", cause),
    ).andThen(
      (
        out,
      ): ResultAsync<
        { snapshot: StoredSnapshot<A>; found: true } | { found: false },
        EventStoreError
      > => {
        const row = out.rows[0];
        if (row === undefined) {
          return okAsync({ found: false as const });
        }
        const aggregate = this.aggSer.deserialize(toBytes(row.payload));
        if (aggregate.isErr()) return errAsync(aggregate.error);
        return okAsync({
          snapshot: {
            aggregate: aggregate.value,
            seqNr: toBigInt(row.seq_nr),
            version: toBigInt(row.version),
            occurredAt: toDate(row.occurred_at),
          },
          found: true as const,
        });
      },
    );
  }

  getEventsSince(id: AggregateId, seqNr: bigint): ResultAsync<StoredEvent<E>[], EventStoreError> {
    if (seqNr === MAX_UINT64) {
      return okAsync([]);
    }
    return ResultAsync.fromPromise(
      this.pool.query(this.selectEventsSql, [id.asString(), seqNr.toString(10)]),
      (cause) => new StoreError("failed to query events", cause),
    ).andThen((out) => {
      const events: StoredEvent<E>[] = [];
      for (const row of out.rows) {
        const event = this.eventSer.deserialize(String(row.type_name), toBytes(row.payload));
        if (event.isErr()) return errAsync<StoredEvent<E>[], EventStoreError>(event.error);
        events.push({
          event: event.value,
          eventId: String(row.event_id),
          seqNr: toBigInt(row.seq_nr),
          isCreated: Boolean(row.is_created),
          occurredAt: toDate(row.occurred_at),
          traceParent: String(row.traceparent ?? ""),
          traceState: String(row.tracestate ?? ""),
        });
      }
      return okAsync(events);
    });
  }

  persistEvent(
    event: StoredEvent<E>,
    _expectedVersion: bigint,
  ): ResultAsync<void, EventStoreError> {
    const aggregateId = event.event.aggregateId();
    const params = this.eventParams(event);
    if (params.isErr()) return errAsync(params.error);

    return ResultAsync.fromPromise(
      this.pool.query(this.insertEventSql, params.value),
      (cause) => new StoreError("failed to persist event", cause),
    ).andThen((out) =>
      (out.rowCount ?? 0) === 0
        ? errAsync<void, EventStoreError>(new DuplicateAggregateError(aggregateId.asString()))
        : okAsync(undefined),
    );
  }

  persistEventAndSnapshot(
    event: StoredEvent<E>,
    snapshot: StoredSnapshot<A>,
  ): ResultAsync<void, EventStoreError> {
    const aggregateId = snapshot.aggregate.aggregateId();
    const eventParams = this.eventParams(event);
    if (eventParams.isErr()) return errAsync(eventParams.error);
    const snapshotPayload = this.aggSer.serialize(snapshot.aggregate);
    if (snapshotPayload.isErr()) return errAsync(snapshotPayload.error);

    const expected = snapshot.version - 1n;
    const snapshotParams = [
      aggregateId.asString(),
      snapshot.seqNr.toString(10),
      snapshot.version.toString(10),
      snapshotPayload.value,
      snapshot.occurredAt,
      expected.toString(10),
    ];

    const run = async (): Promise<void> => {
      const client = await this.pool.connect();
      try {
        await client.query("BEGIN");
        const eventResult = await client.query(this.insertEventSql, eventParams.value);
        if ((eventResult.rowCount ?? 0) === 0) {
          // Existing (aggregate_id, seq_nr) row means a concurrent update.
          throw new OptimisticLockError(aggregateId.asString(), expected, 0n);
        }
        const snapshotResult = await client.query(this.upsertSnapshotSql, snapshotParams);
        if ((snapshotResult.rowCount ?? 0) === 0) {
          // Stored snapshot version != expected means a concurrent update.
          throw new OptimisticLockError(aggregateId.asString(), expected, 0n);
        }
        await client.query("COMMIT");
      } catch (cause) {
        await client.query("ROLLBACK").catch(() => undefined);
        throw cause;
      } finally {
        client.release();
      }
    };

    return ResultAsync.fromPromise(run(), (cause) =>
      cause instanceof OptimisticLockError
        ? cause
        : new StoreError("failed to persist event and snapshot", cause),
    );
  }

  /** Creates the journal / snapshot tables (idempotent: IF NOT EXISTS). */
  createTables(): ResultAsync<void, StoreError> {
    const journalDdl = `CREATE TABLE IF NOT EXISTS ${this.config.journalTable} (
    aggregate_id text   NOT NULL,
    seq_nr       bigint NOT NULL,
    event_id     text   NOT NULL,
    type_name    text   NOT NULL,
    payload      bytea  NOT NULL,
    is_created   boolean NOT NULL,
    occurred_at  timestamptz NOT NULL,
    traceparent  text,
    tracestate   text,
    PRIMARY KEY (aggregate_id, seq_nr)
)`;
    const snapshotDdl = `CREATE TABLE IF NOT EXISTS ${this.config.snapshotTable} (
    aggregate_id text   PRIMARY KEY,
    seq_nr       bigint NOT NULL,
    version      bigint NOT NULL,
    payload      bytea  NOT NULL,
    occurred_at  timestamptz NOT NULL
)`;
    return ResultAsync.fromPromise(
      this.pool.query(journalDdl).then(() => this.pool.query(snapshotDdl)),
      (cause) => new StoreError("failed to create tables", cause),
    ).map(() => undefined);
  }

  /** Drops the journal / snapshot tables (for tests). */
  dropTables(): ResultAsync<void, StoreError> {
    return ResultAsync.fromPromise(
      this.pool
        .query(`DROP TABLE IF EXISTS ${this.config.journalTable}`)
        .then(() => this.pool.query(`DROP TABLE IF EXISTS ${this.config.snapshotTable}`)),
      (cause) => new StoreError("failed to drop tables", cause),
    ).map(() => undefined);
  }

  private eventParams(stored: StoredEvent<E>) {
    return this.eventSer.serialize(stored.event).map((payload) => {
      const trace = resolveTrace(stored);
      return [
        stored.event.aggregateId().asString(),
        stored.seqNr.toString(10),
        stored.eventId,
        stored.event.eventTypeName(),
        payload,
        stored.isCreated,
        stored.occurredAt,
        nullIfEmpty(trace.traceParent),
        nullIfEmpty(trace.traceState),
      ];
    });
  }
}

function resolveTrace(stored: StoredEvent<Event>): { traceParent: string; traceState: string } {
  if (stored.traceParent !== "" || stored.traceState !== "") {
    return { traceParent: stored.traceParent, traceState: stored.traceState };
  }
  return currentTraceContext();
}

function nullIfEmpty(value: string): string | null {
  return value === "" ? null : value;
}

function toBigInt(value: unknown): bigint {
  if (typeof value === "bigint") return value;
  if (typeof value === "number" || typeof value === "string") return BigInt(value);
  throw new StoreError(`cannot convert column value to bigint: ${String(value)}`);
}

function toDate(value: unknown): Date {
  if (value instanceof Date) return value;
  if (typeof value === "string" || typeof value === "number") return new Date(value);
  throw new StoreError(`cannot convert column value to Date: ${String(value)}`);
}

function toBytes(value: unknown): Uint8Array {
  if (value instanceof Uint8Array) return value;
  throw new StoreError("payload column is not binary");
}
