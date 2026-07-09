import { type ResultAsync, errAsync, okAsync } from "neverthrow";
import { type EventStoreConfig, defaultEventStoreConfig, shouldSnapshot } from "./config.js";
import { AggregateNotFoundError, InvalidAggregateError, type RepositoryError } from "./errors.js";
import { generateEventId } from "./event-id.js";
import type { EventStore } from "./event-store.js";
import type {
  Aggregate,
  AggregateId,
  Command,
  Event,
  StoredEvent,
  StoredSnapshot,
} from "./types.js";

const OWNER = Symbol("eventstore.repository.owner");

/**
 * Opaque handle to a loaded aggregate. Only the repository that created it
 * can save it; consumers can only read the aggregate itself.
 */
export interface LoadedAggregate<A> {
  aggregate(): A;
}

interface InternalLoaded<A> extends LoadedAggregate<A> {
  readonly [OWNER]: object;
  readonly seqNr: bigint;
  readonly version: bigint;
}

export interface Repository<A extends Aggregate<C, E>, C extends Command, E extends Event> {
  /** Returns a blank handle at seqNr 0 / version 0 without touching storage. */
  newAggregate(id: AggregateId): LoadedAggregate<A>;
  /** Loads the latest snapshot (if any) and replays subsequent events. */
  load(id: AggregateId): ResultAsync<LoadedAggregate<A>, RepositoryError>;
  /** Applies the command, persists the resulting event (and snapshot when due). */
  save(loaded: LoadedAggregate<A>, cmd: C): ResultAsync<LoadedAggregate<A>, RepositoryError>;
}

export interface RepositoryOptions<A extends Aggregate<C, E>, C extends Command, E extends Event> {
  store: EventStore<A, C, E>;
  createBlank: (id: AggregateId) => A;
  config?: EventStoreConfig;
}

export function createRepository<A extends Aggregate<C, E>, C extends Command, E extends Event>(
  options: RepositoryOptions<A, C, E>,
): Repository<A, C, E> {
  const { store, createBlank } = options;
  const config = options.config ?? defaultEventStoreConfig();
  const ownerToken = {};

  function wrap(aggregate: A, seqNr: bigint, version: bigint): InternalLoaded<A> {
    return {
      [OWNER]: ownerToken,
      seqNr,
      version,
      aggregate: () => aggregate,
    };
  }

  function unwrap(loaded: LoadedAggregate<A>): InternalLoaded<A> | undefined {
    const candidate = loaded as Partial<InternalLoaded<A>>;
    if (candidate[OWNER] !== ownerToken) return undefined;
    return loaded as InternalLoaded<A>;
  }

  return {
    newAggregate(id) {
      return wrap(createBlank(id), 0n, 0n);
    },

    load(id) {
      return store.getLatestSnapshot(id).andThen((result) => {
        const base = result.found
          ? {
              aggregate: result.snapshot.aggregate,
              seqNr: result.snapshot.seqNr,
              version: result.snapshot.version,
            }
          : { aggregate: createBlank(id), seqNr: 0n, version: 0n };

        return store.getEventsSince(id, base.seqNr).andThen((events) => {
          if (!result.found && events.length === 0) {
            return errAsync<LoadedAggregate<A>, RepositoryError>(
              new AggregateNotFoundError(id.asString()),
            );
          }
          let aggregate = base.aggregate;
          let seqNr = base.seqNr;
          for (const stored of events) {
            aggregate = aggregate.applyEvent(stored.event) as A;
            seqNr = stored.seqNr;
          }
          return okAsync(wrap(aggregate, seqNr, base.version));
        });
      });
    },

    save(loaded, cmd) {
      const current = unwrap(loaded);
      if (current === undefined) {
        return errAsync(new InvalidAggregateError("aggregate was not loaded from this repository"));
      }

      const commandResult = current.aggregate().applyCommand(cmd);
      if (commandResult.isErr()) {
        return errAsync(commandResult.error);
      }
      const event = commandResult.value;
      const next = current.aggregate().applyEvent(event) as A;
      const nextSeqNr = current.seqNr + 1n;
      const now = new Date();

      const storedEvent: StoredEvent<E> = {
        event,
        eventId: generateEventId(),
        seqNr: nextSeqNr,
        isCreated: current.seqNr === 0n,
        occurredAt: now,
        traceParent: "",
        traceState: "",
      };

      if (shouldSnapshot(config, nextSeqNr)) {
        const nextVersion = current.version + 1n;
        const snapshot: StoredSnapshot<A> = {
          aggregate: next,
          seqNr: nextSeqNr,
          version: nextVersion,
          occurredAt: now,
        };
        return store
          .persistEventAndSnapshot(storedEvent, snapshot)
          .map(() => wrap(next, nextSeqNr, nextVersion));
      }

      // A first write is detected by expectedVersion === 0; an aggregate that
      // has events but no snapshot yet must not be mistaken for one.
      let expectedVersion = current.version;
      if (current.seqNr > 0n && expectedVersion === 0n) {
        expectedVersion = current.seqNr;
      }
      return store
        .persistEvent(storedEvent, expectedVersion)
        .map(() => wrap(next, nextSeqNr, current.version));
    },
  };
}
