export class AggregateNotFoundError extends Error {
  readonly kind = "AggregateNotFound" as const;
  constructor(readonly aggregateId: string) {
    super(`aggregate not found: ${aggregateId}`);
    this.name = "AggregateNotFoundError";
  }
}

export class OptimisticLockError extends Error {
  readonly kind = "OptimisticLock" as const;
  constructor(
    readonly aggregateId: string,
    readonly expectedVersion: bigint,
    readonly actualVersion: bigint,
  ) {
    super(
      `optimistic lock failure for ${aggregateId}: expected version ${expectedVersion}, actual ${actualVersion}`,
    );
    this.name = "OptimisticLockError";
  }
}

export class DuplicateAggregateError extends Error {
  readonly kind = "DuplicateAggregate" as const;
  constructor(readonly aggregateId: string) {
    super(`aggregate already exists: ${aggregateId}`);
    this.name = "DuplicateAggregateError";
  }
}

export class InvalidAggregateError extends Error {
  readonly kind = "InvalidAggregate" as const;
  constructor(reason: string) {
    super(`invalid aggregate: ${reason}`);
    this.name = "InvalidAggregateError";
  }
}

export class SerializationError extends Error {
  readonly kind = "Serialization" as const;
  constructor(reason: string, cause?: unknown) {
    super(`serialization failure: ${reason}`, { cause });
    this.name = "SerializationError";
  }
}

export class StoreError extends Error {
  readonly kind = "Store" as const;
  constructor(reason: string, cause?: unknown) {
    super(`store failure: ${reason}`, { cause });
    this.name = "StoreError";
  }
}

export type EventStoreError =
  | OptimisticLockError
  | DuplicateAggregateError
  | SerializationError
  | StoreError;

/** Failures `Repository.load` can produce. `load` never runs domain logic. */
export type LoadError = EventStoreError | AggregateNotFoundError;

/**
 * Failures `Repository.save` can produce: storage failures, handle misuse
 * (`InvalidAggregateError`), or the aggregate's own domain rejection `DE`.
 */
export type SaveError<DE> = EventStoreError | InvalidAggregateError | DE;

/** @deprecated Use `LoadError` / `SaveError<DE>` — this loses the per-operation split. */
export type RepositoryError =
  | EventStoreError
  | AggregateNotFoundError
  | InvalidAggregateError
  | Error;
