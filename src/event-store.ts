import type { ResultAsync } from "neverthrow";
import type { EventStoreError } from "./errors.js";
import type {
  Aggregate,
  AggregateId,
  Command,
  Event,
  StoredEvent,
  StoredSnapshot,
} from "./types.js";

/**
 * Low-level storage abstraction: latest snapshot + event stream reads, plus
 * event / event+snapshot writes. Mirrors the Go EventStore interface.
 */
export interface EventStore<A extends Aggregate<C, E>, C extends Command, E extends Event> {
  getLatestSnapshot(
    id: AggregateId,
  ): ResultAsync<{ snapshot: StoredSnapshot<A>; found: true } | { found: false }, EventStoreError>;

  /** Returns events with seqNr strictly greater than `seqNr`, ascending. */
  getEventsSince(id: AggregateId, seqNr: bigint): ResultAsync<StoredEvent<E>[], EventStoreError>;

  persistEvent(event: StoredEvent<E>, expectedVersion: bigint): ResultAsync<void, EventStoreError>;

  persistEventAndSnapshot(
    event: StoredEvent<E>,
    snapshot: StoredSnapshot<A>,
  ): ResultAsync<void, EventStoreError>;
}
