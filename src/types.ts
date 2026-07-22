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
 *
 * `DE` is the caller-defined domain error `applyCommand` rejects commands
 * with (defaults to `Error` for backward compatibility). `applyEvent` is
 * total: replaying a persisted event must not fail — corrupted data is a
 * defect (throw), not a domain error.
 */
export interface Aggregate<C extends Command, E extends Event, DE = Error> {
  aggregateId(): AggregateId;
  applyCommand(cmd: C): Result<E, DE>;
  applyEvent(event: E): Aggregate<C, E, DE>;
}

/** Result of applying a command to an aggregate: the decided event and the next state. */
export interface Decided<A, E> {
  readonly event: E;
  readonly next: A;
}

/**
 * Pure core of `Repository.save`: decides the next event and advances the
 * aggregate, without any persistence. Usable standalone for tests and
 * what-if evaluation.
 */
export function decideNext<A extends Aggregate<C, E, DE>, C extends Command, E extends Event, DE>(
  aggregate: A,
  cmd: C,
): Result<Decided<A, E>, DE> {
  return aggregate.applyCommand(cmd).map((event) => ({
    event,
    next: aggregate.applyEvent(event) as A,
  }));
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
