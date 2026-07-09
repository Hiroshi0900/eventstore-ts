import { type ResultAsync, errAsync, okAsync } from "neverthrow";
import { DuplicateAggregateError, type EventStoreError, OptimisticLockError } from "../errors.js";
import type { EventStore } from "../event-store.js";
import type {
  Aggregate,
  AggregateId,
  Command,
  Event,
  StoredEvent,
  StoredSnapshot,
} from "../types.js";

/**
 * In-memory EventStore for tests. Holds StoredEvent/StoredSnapshot values
 * directly (no serialization), keyed by `id.asString()`. Mirrors the Go
 * memory store's semantics including its locking rules.
 */
export class MemoryEventStore<A extends Aggregate<C, E>, C extends Command, E extends Event>
  implements EventStore<A, C, E>
{
  private readonly events = new Map<string, StoredEvent<E>[]>();
  private readonly snapshots = new Map<string, StoredSnapshot<A>>();

  getLatestSnapshot(
    id: AggregateId,
  ): ResultAsync<{ snapshot: StoredSnapshot<A>; found: true } | { found: false }, EventStoreError> {
    const snapshot = this.snapshots.get(id.asString());
    return okAsync(snapshot === undefined ? { found: false } : { snapshot, found: true });
  }

  getEventsSince(id: AggregateId, seqNr: bigint): ResultAsync<StoredEvent<E>[], EventStoreError> {
    const all = this.events.get(id.asString()) ?? [];
    const filtered = all
      .filter((ev) => ev.seqNr > seqNr)
      .sort((a, b) => (a.seqNr < b.seqNr ? -1 : a.seqNr > b.seqNr ? 1 : 0));
    return okAsync(filtered);
  }

  persistEvent(event: StoredEvent<E>, expectedVersion: bigint): ResultAsync<void, EventStoreError> {
    const key = event.event.aggregateId().asString();
    const existing = this.events.get(key) ?? [];
    if (expectedVersion === 0n && existing.length > 0) {
      return errAsync(new DuplicateAggregateError(key));
    }
    if (existing.some((ev) => ev.seqNr === event.seqNr)) {
      return errAsync(new DuplicateAggregateError(key));
    }
    this.events.set(key, [...existing, event]);
    return okAsync(undefined);
  }

  persistEventAndSnapshot(
    event: StoredEvent<E>,
    snapshot: StoredSnapshot<A>,
  ): ResultAsync<void, EventStoreError> {
    const key = event.event.aggregateId().asString();
    const expected = snapshot.version - 1n;
    const current = this.snapshots.get(key)?.version ?? 0n;
    if (current !== expected) {
      return errAsync(new OptimisticLockError(key, expected, current));
    }
    const existing = this.events.get(key) ?? [];
    if (existing.some((ev) => ev.seqNr === event.seqNr)) {
      return errAsync(new DuplicateAggregateError(key));
    }
    this.events.set(key, [...existing, event]);
    this.snapshots.set(key, snapshot);
    return okAsync(undefined);
  }
}
