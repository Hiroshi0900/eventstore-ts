# @hiroshi0900/eventstore

Event sourcing library for TypeScript, **wire-compatible** with
[github.com/Hiroshi0900/eventstore](https://github.com/Hiroshi0900/eventstore) (Go):
both languages can read and write the same DynamoDB tables.

## Install

```sh
pnpm add @hiroshi0900/eventstore neverthrow
# for the DynamoDB store:
pnpm add @aws-sdk/client-dynamodb
```

## Architecture

```
Consumer code
    │
    ▼
Repository<A, C, E>              ← high-level: load/save aggregates
    │   uses
    ▼
EventStore (interface)           ← low-level: persist/fetch raw events & snapshots
    │
    ├── DynamoDBEventStore        ← @hiroshi0900/eventstore/dynamodb (production)
    └── MemoryEventStore          ← @hiroshi0900/eventstore/memory (tests)
```

- `seqNr` / `version` are `bigint` throughout (Go `uint64` equivalent).
- Errors are returned as `neverthrow` `Result` / `ResultAsync`, never thrown.
- Aggregates are immutable: `applyEvent` must return a **new** instance.

## Implementing an aggregate

```ts
import { ok, err, type Result } from "neverthrow";
import {
  createRepository,
  newAggregateId,
  type Aggregate, type AggregateId, type Command, type Event,
} from "@hiroshi0900/eventstore";
import { MemoryEventStore } from "@hiroshi0900/eventstore/memory";

class Increment implements Command {
  constructor(readonly amount: number) {}
  commandTypeName() { return "Increment"; }
}

class Incremented implements Event {
  constructor(private readonly id: AggregateId, readonly amount: number) {}
  eventTypeName() { return "Incremented"; }
  aggregateId() { return this.id; }
}

class Counter implements Aggregate<Increment, Incremented> {
  constructor(private readonly id: AggregateId, readonly count: number) {}
  aggregateId() { return this.id; }
  applyCommand(cmd: Increment): Result<Incremented, Error> {
    return ok(new Incremented(this.id, cmd.amount));
  }
  applyEvent(event: Incremented): Counter {
    return new Counter(this.id, this.count + event.amount); // new instance
  }
}

const repo = createRepository({
  store: new MemoryEventStore<Counter, Increment, Incremented>(),
  createBlank: (id) => new Counter(id, 0),
});

const id = newAggregateId("Counter", "c1");
const created = await repo.save(repo.newAggregate(id), new Increment(3));
const loaded = await repo.load(id); // Result<LoadedAggregate<Counter>, ...>
```

## DynamoDB store

```ts
import { DynamoDBClient } from "@aws-sdk/client-dynamodb";
import {
  DynamoDBEventStore,
  defaultDynamoDBStoreConfig,
} from "@hiroshi0900/eventstore/dynamodb";
import { jsonAggregateSerializer, jsonEventSerializer } from "@hiroshi0900/eventstore";

const store = new DynamoDBEventStore({
  client: new DynamoDBClient({}),
  aggregateSerializer: jsonAggregateSerializer(encodeCounter, decodeCounter),
  eventSerializer: jsonEventSerializer(encodeEvent, decodeEvent),
  config: defaultDynamoDBStoreConfig(), // journal / snapshot tables, shardCount 1
});
```

### Snapshot strategy

`snapshotInterval` (default 5) controls when snapshots are taken: every save
whose new `seqNr % interval === 0n` writes event + snapshot atomically via
`TransactWriteItems` with optimistic locking on the snapshot `version`; all
other saves write only the event. `load` reads the latest snapshot first and
replays only newer events.

### Wire format (shared with Go)

- Journal partition key `pkey`: `"{TypeName}-{shardID}"`, shard = FNV-1a 64(value) % shardCount (0 when shardCount ≤ 1)
- Event sort key `skey`: `"{TypeName}-{value}-{seqNr zero-padded to 20 digits}"`
- Snapshot sort key: `"{TypeName}-{value}-0"`
- `payload` is DynamoDB Binary; `occurred_at` is Unix **milliseconds** (N)
- W3C `traceparent` / `tracestate` are injected into event items from the
  active OpenTelemetry context (only when non-empty)

> ⚠️ For cross-language data sharing, your serializers must produce the same
> payload bytes as the Go serializers, and `AggregateId.asString()` must match
> the Go `AsString()` for the same aggregate.

## Development

```sh
pnpm install
pnpm test        # vitest
pnpm typecheck   # tsc --noEmit
pnpm lint        # biome
pnpm build       # tsup (ESM + CJS + d.ts)
```
