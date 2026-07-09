export type {
  AggregateId,
  Aggregate,
  Command,
  Event,
  StoredEvent,
  StoredSnapshot,
} from "./types.js";
export { newAggregateId } from "./types.js";
export {
  AggregateNotFoundError,
  DuplicateAggregateError,
  InvalidAggregateError,
  OptimisticLockError,
  SerializationError,
  StoreError,
  type EventStoreError,
  type RepositoryError,
} from "./errors.js";
export { generateEventId } from "./event-id.js";
export {
  defaultEventStoreConfig,
  shouldSnapshot,
  type EventStoreConfig,
} from "./config.js";
export {
  jsonAggregateSerializer,
  jsonEventSerializer,
  type AggregateSerializer,
  type EventSerializer,
} from "./serializer.js";
export type { EventStore } from "./event-store.js";
export {
  createRepository,
  type LoadedAggregate,
  type Repository,
  type RepositoryOptions,
} from "./repository.js";
