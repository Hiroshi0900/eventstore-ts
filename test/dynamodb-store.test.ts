import type {
  AttributeValue,
  GetItemCommand,
  GetItemCommandInput,
  PutItemCommand,
  PutItemCommandInput,
  QueryCommand,
  QueryCommandInput,
  TransactWriteItemsCommand,
  TransactWriteItemsCommandInput,
} from "@aws-sdk/client-dynamodb";
import { describe, expect, it } from "vitest";
import {
  type DynamoDBClientLike,
  DynamoDBEventStore,
  defaultDynamoDBStoreConfig,
} from "../src/dynamodb/index.js";
import {
  DuplicateAggregateError,
  OptimisticLockError,
  type StoredEvent,
  type StoredSnapshot,
} from "../src/index.js";
import {
  Counter,
  type CounterCommand,
  type CounterEvent,
  IncrementedEvent,
  counterAggregateSerializer,
  counterEventSerializer,
  counterId,
} from "./fixtures.js";

type Item = Record<string, AttributeValue>;
type SentCommand = GetItemCommand | QueryCommand | PutItemCommand | TransactWriteItemsCommand;

class FakeClient implements DynamoDBClientLike {
  readonly sent: SentCommand[] = [];
  private readonly responses: {
    Item?: Item;
    Items?: Item[];
    LastEvaluatedKey?: Item;
  }[];
  private error: Error | undefined;

  constructor(responses: { Item?: Item; Items?: Item[]; LastEvaluatedKey?: Item }[] = [{}]) {
    this.responses = responses;
  }

  failWith(error: Error): this {
    this.error = error;
    return this;
  }

  send(command: SentCommand): Promise<{ Item?: Item; Items?: Item[]; LastEvaluatedKey?: Item }> {
    this.sent.push(command);
    if (this.error) return Promise.reject(this.error);
    return Promise.resolve(
      this.responses[Math.min(this.sent.length, this.responses.length) - 1] ?? {},
    );
  }
}

function newStore(client: DynamoDBClientLike) {
  return new DynamoDBEventStore<Counter, CounterCommand, CounterEvent>({
    client,
    aggregateSerializer: counterAggregateSerializer,
    eventSerializer: counterEventSerializer,
  });
}

function storedEvent(seqNr: bigint, traceParent = ""): StoredEvent<CounterEvent> {
  return {
    event: new IncrementedEvent(counterId("c1"), 1),
    eventId: "a".repeat(32),
    seqNr,
    isCreated: seqNr === 1n,
    occurredAt: new Date(1750000000000),
    traceParent,
    traceState: "",
  };
}

function storedSnapshot(seqNr: bigint, version: bigint): StoredSnapshot<Counter> {
  return {
    aggregate: new Counter(counterId("c1"), Number(seqNr)),
    seqNr,
    version,
    occurredAt: new Date(1750000000000),
  };
}

function eventItem(seqNr: bigint): Item {
  const payload = counterEventSerializer
    .serialize(new IncrementedEvent(counterId("c1"), 1))
    ._unsafeUnwrap();
  return {
    pkey: { S: "Counter-0" },
    skey: { S: `Counter-c1-${seqNr.toString().padStart(20, "0")}` },
    aid: { S: "Counter-c1" },
    seq_nr: { N: seqNr.toString() },
    payload: { B: payload },
    occurred_at: { N: "1750000000000" },
    type_name: { S: "Incremented" },
    event_id: { S: "a".repeat(32) },
    is_created: { BOOL: seqNr === 1n },
  };
}

describe("DynamoDBEventStore.getLatestSnapshot", () => {
  it("reads the snapshot -0 slot with GetItem and unmarshals it", async () => {
    const payload = counterAggregateSerializer
      .serialize(new Counter(counterId("c1"), 5))
      ._unsafeUnwrap();
    const client = new FakeClient([
      {
        Item: {
          pkey: { S: "Counter-0" },
          skey: { S: "Counter-c1-0" },
          aid: { S: "Counter-c1" },
          seq_nr: { N: "5" },
          version: { N: "1" },
          payload: { B: payload },
          occurred_at: { N: "1750000000000" },
        },
      },
    ]);
    const result = await newStore(client).getLatestSnapshot(counterId("c1"));
    const value = result._unsafeUnwrap();
    expect(value.found).toBe(true);
    if (value.found) {
      expect(value.snapshot.seqNr).toBe(5n);
      expect(value.snapshot.version).toBe(1n);
      expect(value.snapshot.aggregate.count).toBe(5);
      expect(value.snapshot.occurredAt.getTime()).toBe(1750000000000);
    }
    const command = client.sent[0] as GetItemCommand;
    const input = command.input as GetItemCommandInput;
    expect(input.TableName).toBe("snapshot");
    expect(input.Key).toEqual({
      pkey: { S: "Counter-0" },
      skey: { S: "Counter-c1-0" },
    });
    expect(input.ConsistentRead).toBeUndefined();
  });

  it("returns found=false when the item is absent", async () => {
    const result = await newStore(new FakeClient([{}])).getLatestSnapshot(counterId("c1"));
    expect(result._unsafeUnwrap()).toEqual({ found: false });
  });
});

describe("DynamoDBEventStore.getEventsSince", () => {
  it("queries the journal GSI with an exclusive seq_nr lower bound, ascending", async () => {
    const client = new FakeClient([{ Items: [eventItem(2n), eventItem(3n)] }]);
    const events = (await newStore(client).getEventsSince(counterId("c1"), 1n))._unsafeUnwrap();
    expect(events.map((e) => e.seqNr)).toEqual([2n, 3n]);
    expect(events[0]?.occurredAt.getTime()).toBe(1750000000000);

    const input = (client.sent[0] as QueryCommand).input as QueryCommandInput;
    expect(input.TableName).toBe("journal");
    expect(input.IndexName).toBe("journal-aid-index");
    expect(input.KeyConditionExpression).toBe("#aid = :aid AND #seq_nr > :seqNr");
    expect(input.ExpressionAttributeNames).toEqual({ "#aid": "aid", "#seq_nr": "seq_nr" });
    expect(input.ExpressionAttributeValues).toEqual({
      ":aid": { S: "Counter-c1" },
      ":seqNr": { N: "1" },
    });
    expect(input.ScanIndexForward).toBe(true);
  });

  it("paginates with ExclusiveStartKey until LastEvaluatedKey is absent", async () => {
    const lastKey: Item = { pkey: { S: "Counter-0" } };
    const client = new FakeClient([
      { Items: [eventItem(1n)], LastEvaluatedKey: lastKey },
      { Items: [eventItem(2n)] },
    ]);
    const events = (await newStore(client).getEventsSince(counterId("c1"), 0n))._unsafeUnwrap();
    expect(events.map((e) => e.seqNr)).toEqual([1n, 2n]);
    expect(client.sent).toHaveLength(2);
    const second = (client.sent[1] as QueryCommand).input as QueryCommandInput;
    expect(second.ExclusiveStartKey).toEqual(lastKey);
  });

  it("returns [] immediately for max uint64 without querying", async () => {
    const client = new FakeClient();
    const events = (
      await newStore(client).getEventsSince(counterId("c1"), 0xffffffffffffffffn)
    )._unsafeUnwrap();
    expect(events).toEqual([]);
    expect(client.sent).toHaveLength(0);
  });
});

describe("DynamoDBEventStore.persistEvent", () => {
  it("writes the wire-compatible item with attribute_not_exists(pkey)", async () => {
    const client = new FakeClient();
    const result = await newStore(client).persistEvent(storedEvent(1n), 0n);
    expect(result.isOk()).toBe(true);

    const input = (client.sent[0] as PutItemCommand).input as PutItemCommandInput;
    expect(input.TableName).toBe("journal");
    expect(input.ConditionExpression).toBe("attribute_not_exists(#pk)");
    expect(input.ExpressionAttributeNames).toEqual({ "#pk": "pkey" });
    expect(input.Item?.pkey).toEqual({ S: "Counter-0" });
    expect(input.Item?.skey).toEqual({ S: "Counter-c1-00000000000000000001" });
    expect(input.Item?.aid).toEqual({ S: "Counter-c1" });
    expect(input.Item?.seq_nr).toEqual({ N: "1" });
    expect(input.Item?.occurred_at).toEqual({ N: "1750000000000" });
    expect(input.Item?.type_name).toEqual({ S: "Incremented" });
    expect(input.Item?.event_id).toEqual({ S: "a".repeat(32) });
    expect(input.Item?.is_created).toEqual({ BOOL: true });
    expect(input.Item?.payload?.B).toBeInstanceOf(Uint8Array);
    expect(input.Item?.traceparent).toBeUndefined();
    expect(input.Item?.tracestate).toBeUndefined();
  });

  it("writes traceparent only when non-empty", async () => {
    const client = new FakeClient();
    const traceParent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
    await newStore(client).persistEvent(storedEvent(1n, traceParent), 0n);
    const input = (client.sent[0] as PutItemCommand).input as PutItemCommandInput;
    expect(input.Item?.traceparent).toEqual({ S: traceParent });
    expect(input.Item?.tracestate).toBeUndefined();
  });

  it("maps ConditionalCheckFailedException to DuplicateAggregateError", async () => {
    const error = Object.assign(new Error("conditional check failed"), {
      name: "ConditionalCheckFailedException",
    });
    const client = new FakeClient().failWith(error);
    const result = await newStore(client).persistEvent(storedEvent(1n), 0n);
    expect(result._unsafeUnwrapErr()).toBeInstanceOf(DuplicateAggregateError);
  });
});

describe("DynamoDBEventStore.persistEventAndSnapshot", () => {
  it("uses TransactWriteItems with attribute_not_exists(version) on first snapshot", async () => {
    const client = new FakeClient();
    const result = await newStore(client).persistEventAndSnapshot(
      storedEvent(5n),
      storedSnapshot(5n, 1n),
    );
    expect(result.isOk()).toBe(true);

    const input = (client.sent[0] as TransactWriteItemsCommand)
      .input as TransactWriteItemsCommandInput;
    const [eventPut, snapshotPut] = input.TransactItems ?? [];
    expect(eventPut?.Put?.TableName).toBe("journal");
    expect(eventPut?.Put?.ConditionExpression).toBe("attribute_not_exists(#pk)");
    expect(snapshotPut?.Put?.TableName).toBe("snapshot");
    expect(snapshotPut?.Put?.ConditionExpression).toBe("attribute_not_exists(#version)");
    expect(snapshotPut?.Put?.ExpressionAttributeValues).toBeUndefined();
    expect(snapshotPut?.Put?.Item?.skey).toEqual({ S: "Counter-c1-0" });
    expect(snapshotPut?.Put?.Item?.seq_nr).toEqual({ N: "5" });
    expect(snapshotPut?.Put?.Item?.version).toEqual({ N: "1" });
    expect(snapshotPut?.Put?.Item?.type_name).toBeUndefined();
    expect(snapshotPut?.Put?.Item?.event_id).toBeUndefined();
    expect(snapshotPut?.Put?.Item?.is_created).toBeUndefined();
  });

  it("binds :expected for subsequent snapshots", async () => {
    const client = new FakeClient();
    await newStore(client).persistEventAndSnapshot(storedEvent(10n), storedSnapshot(10n, 2n));
    const input = (client.sent[0] as TransactWriteItemsCommand)
      .input as TransactWriteItemsCommandInput;
    const snapshotPut = input.TransactItems?.[1];
    expect(snapshotPut?.Put?.ConditionExpression).toBe(
      "attribute_not_exists(#version) OR #version = :expected",
    );
    expect(snapshotPut?.Put?.ExpressionAttributeValues).toEqual({ ":expected": { N: "1" } });
  });

  it("maps transactional ConditionalCheckFailed to OptimisticLockError", async () => {
    const error = Object.assign(new Error("transaction canceled"), {
      name: "TransactionCanceledException",
      CancellationReasons: [{ Code: "None" }, { Code: "ConditionalCheckFailed" }],
    });
    const client = new FakeClient().failWith(error);
    const result = await newStore(client).persistEventAndSnapshot(
      storedEvent(10n),
      storedSnapshot(10n, 2n),
    );
    const err = result._unsafeUnwrapErr();
    expect(err).toBeInstanceOf(OptimisticLockError);
    expect((err as OptimisticLockError).expectedVersion).toBe(1n);
  });
});

describe("config defaults", () => {
  it("matches the Go defaults", () => {
    expect(defaultDynamoDBStoreConfig()).toEqual({
      journalTableName: "journal",
      snapshotTableName: "snapshot",
      journalGsiName: "journal-aid-index",
      snapshotGsiName: "snapshot-aid-index",
      shardCount: 1,
    });
  });
});
