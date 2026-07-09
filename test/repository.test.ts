import { describe, expect, it } from "vitest";
import {
  AggregateNotFoundError,
  InvalidAggregateError,
  OptimisticLockError,
  type Repository,
  createRepository,
} from "../src/index.js";
import { MemoryEventStore } from "../src/memory/index.js";
import {
  type Counter,
  type CounterCommand,
  type CounterEvent,
  FailingCommand,
  IncrementCommand,
  counterId,
  createBlankCounter,
} from "./fixtures.js";

type CounterRepo = Repository<Counter, CounterCommand, CounterEvent>;

function newRepo(snapshotInterval = 5n): {
  repo: CounterRepo;
  store: MemoryEventStore<Counter, CounterCommand, CounterEvent>;
} {
  const store = new MemoryEventStore<Counter, CounterCommand, CounterEvent>();
  const repo = createRepository({
    store,
    createBlank: createBlankCounter,
    config: { snapshotInterval },
  });
  return { repo, store };
}

describe("Repository", () => {
  it("saves a first event from a new aggregate handle", async () => {
    const { repo } = newRepo();
    const loaded = repo.newAggregate(counterId("c1"));
    const saved = await repo.save(loaded, new IncrementCommand(3));
    expect(saved._unsafeUnwrap().aggregate().count).toBe(3);
  });

  it("supports chained saves reusing the returned handle without reloads", async () => {
    const { repo } = newRepo();
    let loaded = repo.newAggregate(counterId("c1"));
    for (let i = 0; i < 7; i++) {
      loaded = (await repo.save(loaded, new IncrementCommand(1)))._unsafeUnwrap();
    }
    expect(loaded.aggregate().count).toBe(7);
  });

  it("load replays events into the same state", async () => {
    const { repo } = newRepo();
    let loaded = repo.newAggregate(counterId("c1"));
    for (let i = 0; i < 3; i++) {
      loaded = (await repo.save(loaded, new IncrementCommand(2)))._unsafeUnwrap();
    }
    const reloaded = (await repo.load(counterId("c1")))._unsafeUnwrap();
    expect(reloaded.aggregate().count).toBe(6);
  });

  it("returns AggregateNotFoundError for unknown aggregates", async () => {
    const { repo } = newRepo();
    const result = await repo.load(counterId("missing"));
    expect(result._unsafeUnwrapErr()).toBeInstanceOf(AggregateNotFoundError);
  });

  it("takes a snapshot at the interval and loads from it", async () => {
    const { repo, store } = newRepo(5n);
    let loaded = repo.newAggregate(counterId("c1"));
    for (let i = 0; i < 6; i++) {
      loaded = (await repo.save(loaded, new IncrementCommand(1)))._unsafeUnwrap();
    }
    const snapshot = await store.getLatestSnapshot(counterId("c1"));
    const unwrapped = snapshot._unsafeUnwrap();
    expect(unwrapped.found).toBe(true);
    if (unwrapped.found) {
      expect(unwrapped.snapshot.seqNr).toBe(5n);
      expect(unwrapped.snapshot.version).toBe(1n);
    }
    const reloaded = (await repo.load(counterId("c1")))._unsafeUnwrap();
    expect(reloaded.aggregate().count).toBe(6);
  });

  it("continues saving correctly after loading from a snapshot", async () => {
    const { repo } = newRepo(5n);
    let loaded = repo.newAggregate(counterId("c1"));
    for (let i = 0; i < 5; i++) {
      loaded = (await repo.save(loaded, new IncrementCommand(1)))._unsafeUnwrap();
    }
    let reloaded = (await repo.load(counterId("c1")))._unsafeUnwrap();
    for (let i = 0; i < 5; i++) {
      reloaded = (await repo.save(reloaded, new IncrementCommand(1)))._unsafeUnwrap();
    }
    expect(reloaded.aggregate().count).toBe(10);
    const final = (await repo.load(counterId("c1")))._unsafeUnwrap();
    expect(final.aggregate().count).toBe(10);
  });

  it("rejects a handle created by another repository instance", async () => {
    const { repo } = newRepo();
    const { repo: other } = newRepo();
    const foreign = other.newAggregate(counterId("c1"));
    const result = await repo.save(foreign, new IncrementCommand(1));
    expect(result._unsafeUnwrapErr()).toBeInstanceOf(InvalidAggregateError);
  });

  it("rejects a plain object masquerading as a handle", async () => {
    const { repo } = newRepo();
    const fake = { aggregate: () => createBlankCounter(counterId("c1")) };
    const result = await repo.save(fake, new IncrementCommand(1));
    expect(result._unsafeUnwrapErr()).toBeInstanceOf(InvalidAggregateError);
  });

  it("propagates applyCommand errors", async () => {
    const { repo } = newRepo();
    const loaded = repo.newAggregate(counterId("c1"));
    const result = await repo.save(loaded, new FailingCommand());
    expect(result._unsafeUnwrapErr().message).toBe("command rejected");
  });

  it("detects concurrent snapshot writes via optimistic locking", async () => {
    const { repo } = newRepo(1n); // snapshot on every save
    const seed = await repo.save(repo.newAggregate(counterId("c1")), new IncrementCommand(1));
    const stale = seed._unsafeUnwrap();
    // Two writers save from the same loaded state; the second must fail.
    (await repo.save(stale, new IncrementCommand(1)))._unsafeUnwrap();
    const result = await repo.save(stale, new IncrementCommand(1));
    expect(result._unsafeUnwrapErr()).toBeInstanceOf(OptimisticLockError);
  });

  it("does not mutate the previous handle's aggregate on save", async () => {
    const { repo } = newRepo();
    const first = repo.newAggregate(counterId("c1"));
    const saved = (await repo.save(first, new IncrementCommand(5)))._unsafeUnwrap();
    expect(first.aggregate().count).toBe(0);
    expect(saved.aggregate().count).toBe(5);
  });
});
