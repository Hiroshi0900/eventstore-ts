import type { Result } from "neverthrow";

/**
 * Identity of an aggregate. `asString()` is used as the `aid` GSI key and
 * must match the Go implementation for the same aggregate type
 * (typically `"{typeName}-{value}"`).
 */
export interface AggregateId {
  typeName(): string;
  value(): string;
  asString(): string;
}

/** Creates a simple AggregateId whose asString() is `"{typeName}-{value}"`. */
export function newAggregateId(typeName: string, value: string): AggregateId {
  return {
    typeName: () => typeName,
    value: () => value,
    asString: () => `${typeName}-${value}`,
  };
}

export interface Command {
  commandTypeName(): string;
}

export interface Event {
  eventTypeName(): string;
  aggregateId(): AggregateId;
}

/**
 * Aggregate root. `applyEvent` MUST return a new instance (immutability by
 * convention); `applyCommand` decides the next event without mutating state.
 */
export interface Aggregate<C extends Command, E extends Event> {
  aggregateId(): AggregateId;
  applyCommand(cmd: C): Result<E, Error>;
  applyEvent(event: E): Aggregate<C, E>;
}

/** Event plus persistence metadata, wire-compatible with the Go StoredEvent. */
export interface StoredEvent<E extends Event> {
  readonly event: E;
  readonly eventId: string;
  readonly seqNr: bigint;
  readonly isCreated: boolean;
  readonly occurredAt: Date;
  readonly traceParent: string;
  readonly traceState: string;
}

/** Aggregate snapshot plus metadata, wire-compatible with the Go StoredSnapshot. */
export interface StoredSnapshot<A> {
  readonly aggregate: A;
  readonly seqNr: bigint;
  readonly version: bigint;
  readonly occurredAt: Date;
}
