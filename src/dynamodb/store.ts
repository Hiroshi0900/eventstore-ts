import {
  type AttributeValue,
  GetItemCommand,
  PutItemCommand,
  QueryCommand,
  TransactWriteItemsCommand,
  TransactionCanceledException,
} from "@aws-sdk/client-dynamodb";
import { ResultAsync, err, errAsync, okAsync } from "neverthrow";
import {
  DuplicateAggregateError,
  type EventStoreError,
  OptimisticLockError,
  SerializationError,
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
import { KeyResolver } from "./key-resolver.js";

// Attribute names shared with the Go implementation (wire contract).
const ATTR = {
  pkey: "pkey",
  skey: "skey",
  aid: "aid",
  seqNr: "seq_nr",
  payload: "payload",
  occurredAt: "occurred_at",
  typeName: "type_name",
  eventId: "event_id",
  isCreated: "is_created",
  version: "version",
  traceParent: "traceparent",
  traceState: "tracestate",
} as const;

const MAX_UINT64 = 0xffffffffffffffffn;

export interface DynamoDBStoreConfig {
  readonly journalTableName: string;
  readonly snapshotTableName: string;
  readonly journalGsiName: string;
  readonly snapshotGsiName: string;
  readonly shardCount: number;
}

export function defaultDynamoDBStoreConfig(): DynamoDBStoreConfig {
  return {
    journalTableName: "journal",
    snapshotTableName: "snapshot",
    journalGsiName: "journal-aid-index",
    snapshotGsiName: "snapshot-aid-index",
    shardCount: 1,
  };
}

/** Minimal client surface (satisfied by DynamoDBClient); fakeable in tests. */
export interface DynamoDBClientLike {
  send(
    command: GetItemCommand | QueryCommand | PutItemCommand | TransactWriteItemsCommand,
  ): Promise<{
    Item?: Record<string, AttributeValue>;
    Items?: Record<string, AttributeValue>[];
    LastEvaluatedKey?: Record<string, AttributeValue>;
  }>;
}

type Item = Record<string, AttributeValue>;

export interface DynamoDBStoreOptions<A, E> {
  client: DynamoDBClientLike;
  aggregateSerializer: AggregateSerializer<A>;
  eventSerializer: EventSerializer<E>;
  config?: DynamoDBStoreConfig;
}

/**
 * DynamoDB-backed EventStore, wire-compatible with the Go dynamodb.Store:
 * same table/GSI layout, key formats, attribute names and condition
 * expressions, so both languages can share the same tables.
 */
export class DynamoDBEventStore<A extends Aggregate<C, E>, C extends Command, E extends Event>
  implements EventStore<A, C, E>
{
  private readonly client: DynamoDBClientLike;
  private readonly aggSer: AggregateSerializer<A>;
  private readonly eventSer: EventSerializer<E>;
  private readonly config: DynamoDBStoreConfig;
  private readonly keys: KeyResolver;

  constructor(options: DynamoDBStoreOptions<A, E>) {
    this.client = options.client;
    this.aggSer = options.aggregateSerializer;
    this.eventSer = options.eventSerializer;
    this.config = options.config ?? defaultDynamoDBStoreConfig();
    this.keys = new KeyResolver(this.config.shardCount);
  }

  getLatestSnapshot(
    id: AggregateId,
  ): ResultAsync<{ snapshot: StoredSnapshot<A>; found: true } | { found: false }, EventStoreError> {
    const command = new GetItemCommand({
      TableName: this.config.snapshotTableName,
      Key: {
        [ATTR.pkey]: { S: this.keys.resolvePartitionKey(id) },
        [ATTR.skey]: { S: this.keys.resolveSnapshotSortKey(id) },
      },
    });
    return ResultAsync.fromPromise(
      this.client.send(command),
      (cause) => new StoreError("failed to get latest snapshot", cause),
    ).andThen(
      (
        out,
      ): ResultAsync<
        { snapshot: StoredSnapshot<A>; found: true } | { found: false },
        EventStoreError
      > => {
        if (out.Item === undefined) {
          return okAsync({ found: false as const });
        }
        const snapshot = this.unmarshalSnapshot(out.Item);
        if (snapshot.isErr()) return errAsync(snapshot.error);
        return okAsync({ snapshot: snapshot.value, found: true as const });
      },
    );
  }

  getEventsSince(id: AggregateId, seqNr: bigint): ResultAsync<StoredEvent<E>[], EventStoreError> {
    if (seqNr === MAX_UINT64) {
      return ResultAsync.fromSafePromise(Promise.resolve([]));
    }
    return ResultAsync.fromPromise(this.queryAllEventsSince(id, seqNr), (cause) =>
      cause instanceof SerializationError ? cause : new StoreError("failed to query events", cause),
    );
  }

  private async queryAllEventsSince(id: AggregateId, seqNr: bigint): Promise<StoredEvent<E>[]> {
    const events: StoredEvent<E>[] = [];
    let exclusiveStartKey: Item | undefined;
    do {
      const out = await this.client.send(
        new QueryCommand({
          TableName: this.config.journalTableName,
          IndexName: this.config.journalGsiName,
          KeyConditionExpression: "#aid = :aid AND #seq_nr > :seqNr",
          ExpressionAttributeNames: { "#aid": ATTR.aid, "#seq_nr": ATTR.seqNr },
          ExpressionAttributeValues: {
            ":aid": { S: id.asString() },
            ":seqNr": { N: seqNr.toString(10) },
          },
          ScanIndexForward: true,
          ...(exclusiveStartKey !== undefined ? { ExclusiveStartKey: exclusiveStartKey } : {}),
        }),
      );
      for (const item of out.Items ?? []) {
        const event = this.unmarshalEvent(id, item);
        if (event.isErr()) throw event.error;
        events.push(event.value);
      }
      exclusiveStartKey = out.LastEvaluatedKey;
    } while (exclusiveStartKey !== undefined);
    return events;
  }

  persistEvent(
    event: StoredEvent<E>,
    _expectedVersion: bigint,
  ): ResultAsync<void, EventStoreError> {
    const aggregateId = event.event.aggregateId();
    const item = this.marshalEvent(event);
    if (item.isErr()) return errAsync(item.error);

    const command = new PutItemCommand({
      TableName: this.config.journalTableName,
      Item: item.value,
      ConditionExpression: "attribute_not_exists(#pk)",
      ExpressionAttributeNames: { "#pk": ATTR.pkey },
    });
    return ResultAsync.fromPromise(this.client.send(command), (cause) =>
      isConditionalCheckFailed(cause)
        ? new DuplicateAggregateError(aggregateId.asString())
        : new StoreError("failed to persist event", cause),
    ).map(() => undefined);
  }

  persistEventAndSnapshot(
    event: StoredEvent<E>,
    snapshot: StoredSnapshot<A>,
  ): ResultAsync<void, EventStoreError> {
    const aggregateId = event.event.aggregateId();
    const eventItem = this.marshalEvent(event);
    if (eventItem.isErr()) return errAsync(eventItem.error);
    const snapshotItem = this.marshalSnapshot(aggregateId, snapshot);
    if (snapshotItem.isErr()) return errAsync(snapshotItem.error);

    const expected = snapshot.version - 1n;
    const condition =
      expected === 0n
        ? "attribute_not_exists(#version)"
        : "attribute_not_exists(#version) OR #version = :expected";
    const conditionValues: Record<string, AttributeValue> | undefined =
      expected === 0n ? undefined : { ":expected": { N: expected.toString(10) } };

    const command = new TransactWriteItemsCommand({
      TransactItems: [
        {
          Put: {
            TableName: this.config.journalTableName,
            Item: eventItem.value,
            ConditionExpression: "attribute_not_exists(#pk)",
            ExpressionAttributeNames: { "#pk": ATTR.pkey },
          },
        },
        {
          Put: {
            TableName: this.config.snapshotTableName,
            Item: snapshotItem.value,
            ConditionExpression: condition,
            ExpressionAttributeNames: { "#version": ATTR.version },
            ...(conditionValues !== undefined
              ? { ExpressionAttributeValues: conditionValues }
              : {}),
          },
        },
      ],
    });
    return ResultAsync.fromPromise(this.client.send(command), (cause) =>
      isConditionalCheckFailedInTransaction(cause)
        ? new OptimisticLockError(aggregateId.asString(), expected, 0n)
        : new StoreError("failed to persist event and snapshot", cause),
    ).map(() => undefined);
  }

  private marshalEvent(stored: StoredEvent<E>) {
    const id = stored.event.aggregateId();
    return this.eventSer.serialize(stored.event).map((payload) => {
      const trace = resolveTrace(stored);
      const item: Item = {
        [ATTR.pkey]: { S: this.keys.resolvePartitionKey(id) },
        [ATTR.skey]: { S: this.keys.resolveEventSortKey(id, stored.seqNr) },
        [ATTR.aid]: { S: id.asString() },
        [ATTR.seqNr]: { N: stored.seqNr.toString(10) },
        [ATTR.payload]: { B: payload },
        [ATTR.occurredAt]: { N: String(stored.occurredAt.getTime()) },
        [ATTR.typeName]: { S: stored.event.eventTypeName() },
        [ATTR.eventId]: { S: stored.eventId },
        [ATTR.isCreated]: { BOOL: stored.isCreated },
      };
      if (trace.traceParent !== "") item[ATTR.traceParent] = { S: trace.traceParent };
      if (trace.traceState !== "") item[ATTR.traceState] = { S: trace.traceState };
      return item;
    });
  }

  private marshalSnapshot(id: AggregateId, snapshot: StoredSnapshot<A>) {
    return this.aggSer.serialize(snapshot.aggregate).map(
      (payload): Item => ({
        [ATTR.pkey]: { S: this.keys.resolvePartitionKey(id) },
        [ATTR.skey]: { S: this.keys.resolveSnapshotSortKey(id) },
        [ATTR.aid]: { S: id.asString() },
        [ATTR.seqNr]: { N: snapshot.seqNr.toString(10) },
        [ATTR.version]: { N: snapshot.version.toString(10) },
        [ATTR.payload]: { B: payload },
        [ATTR.occurredAt]: { N: String(snapshot.occurredAt.getTime()) },
      }),
    );
  }

  private unmarshalEvent(id: AggregateId, item: Item) {
    const typeName = item[ATTR.typeName]?.S ?? "";
    const payload = item[ATTR.payload]?.B;
    if (payload === undefined) {
      return err<never, EventStoreError>(
        new StoreError(`event item for ${id.asString()} is missing payload`),
      );
    }
    return this.eventSer.deserialize(typeName, payload).map(
      (event): StoredEvent<E> => ({
        event,
        eventId: item[ATTR.eventId]?.S ?? "",
        seqNr: requireN(item, ATTR.seqNr),
        isCreated: item[ATTR.isCreated]?.BOOL ?? false,
        occurredAt: new Date(Number(requireN(item, ATTR.occurredAt))),
        traceParent: item[ATTR.traceParent]?.S ?? "",
        traceState: item[ATTR.traceState]?.S ?? "",
      }),
    );
  }

  private unmarshalSnapshot(item: Item) {
    const payload = item[ATTR.payload]?.B;
    if (payload === undefined) {
      return err<never, EventStoreError>(new StoreError("snapshot item is missing payload"));
    }
    return this.aggSer.deserialize(payload).map(
      (aggregate): StoredSnapshot<A> => ({
        aggregate,
        seqNr: requireN(item, ATTR.seqNr),
        version: requireN(item, ATTR.version),
        occurredAt: new Date(Number(requireN(item, ATTR.occurredAt))),
      }),
    );
  }
}

function resolveTrace(stored: StoredEvent<Event>): { traceParent: string; traceState: string } {
  if (stored.traceParent !== "" || stored.traceState !== "") {
    return { traceParent: stored.traceParent, traceState: stored.traceState };
  }
  return currentTraceContext();
}

function requireN(item: Item, attr: string): bigint {
  const value = item[attr]?.N;
  if (value === undefined) {
    throw new StoreError(`item is missing numeric attribute ${attr}`);
  }
  return BigInt(value);
}

function isConditionalCheckFailed(cause: unknown): boolean {
  return (
    typeof cause === "object" &&
    cause !== null &&
    (cause as { name?: string }).name === "ConditionalCheckFailedException"
  );
}

function isConditionalCheckFailedInTransaction(cause: unknown): boolean {
  if (cause instanceof TransactionCanceledException) {
    return (cause.CancellationReasons ?? []).some((r) => r.Code === "ConditionalCheckFailed");
  }
  return (
    typeof cause === "object" &&
    cause !== null &&
    (cause as { name?: string }).name === "TransactionCanceledException" &&
    ((cause as { CancellationReasons?: { Code?: string }[] }).CancellationReasons ?? []).some(
      (r) => r.Code === "ConditionalCheckFailed",
    )
  );
}
