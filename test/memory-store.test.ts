import { describe, expect, it } from "vitest";
import {
  DuplicateAggregateError,
  OptimisticLockError,
  type StoredEvent,
  type StoredSnapshot,
} from "../src/index.js";
import { MemoryEventStore } from "../src/memory/index.js";
import {
  Counter,
  type CounterCommand,
  type CounterEvent,
  IncrementedEvent,
  counterId,
} from "./fixtures.js";

type Store = MemoryEventStore<Counter, CounterCommand, CounterEvent>;

function storedEvent(value: string, seqNr: bigint): StoredEvent<CounterEvent> {
  return {
    event: new IncrementedEvent(counterId(value), 1),
    eventId: "0".repeat(32),
    seqNr,
    isCreated: seqNr === 1n,
    occurredAt: new Date(),
    traceParent: "",
    traceState: "",
  };
}

function storedSnapshot(value: string, seqNr: bigint, version: bigint): StoredSnapshot<Counter> {
  return {
    aggregate: new Counter(counterId(value), Number(seqNr)),
    seqNr,
    version,
    occurredAt: new Date(),
  };
}

describe("MemoryEventStore", () => {
  it("returns found=false when no snapshot exists", async () => {
    const store: Store = new MemoryEventStore();
    const result = await store.getLatestSnapshot(counterId("c1"));
    expect(result._unsafeUnwrap()).toEqual({ found: false });
  });

  it("returns the stored snapshot", async () => {
    const store: Store = new MemoryEventStore();
    const snapshot = storedSnapshot("c1", 5n, 1n);
    await store.persistEventAndSnapshot(storedEvent("c1", 5n), snapshot);
    const result = await store.getLatestSnapshot(counterId("c1"));
    expect(result._unsafeUnwrap()).toEqual({ snapshot, found: true });
  });

  it("filters and sorts events by seqNr ascending, exclusive lower bound", async () => {
    const store: Store = new MemoryEventStore();
    await store.persistEvent(storedEvent("c1", 1n), 0n);
    await store.persistEvent(storedEvent("c1", 2n), 1n);
    await store.persistEvent(storedEvent("c1", 3n), 1n);
    const events = (await store.getEventsSince(counterId("c1"), 1n))._unsafeUnwrap();
    expect(events.map((e) => e.seqNr)).toEqual([2n, 3n]);
  });

  it("rejects a first write (expectedVersion 0) when events already exist", async () => {
    const store: Store = new MemoryEventStore();
    await store.persistEvent(storedEvent("c1", 1n), 0n);
    const result = await store.persistEvent(storedEvent("c1", 2n), 0n);
    expect(result._unsafeUnwrapErr()).toBeInstanceOf(DuplicateAggregateError);
  });

  it("rejects duplicate seqNr", async () => {
    const store: Store = new MemoryEventStore();
    await store.persistEvent(storedEvent("c1", 1n), 0n);
    const result = await store.persistEvent(storedEvent("c1", 1n), 1n);
    expect(result._unsafeUnwrapErr()).toBeInstanceOf(DuplicateAggregateError);
  });

  it("rejects snapshot write when current version does not match expected", async () => {
    const store: Store = new MemoryEventStore();
    await store.persistEventAndSnapshot(storedEvent("c1", 5n), storedSnapshot("c1", 5n, 1n));
    const result = await store.persistEventAndSnapshot(
      storedEvent("c1", 10n),
      storedSnapshot("c1", 10n, 3n), // expected 2, current 1
    );
    const error = result._unsafeUnwrapErr();
    expect(error).toBeInstanceOf(OptimisticLockError);
    expect((error as OptimisticLockError).expectedVersion).toBe(2n);
    expect((error as OptimisticLockError).actualVersion).toBe(1n);
  });

  it("accepts sequential snapshot versions", async () => {
    const store: Store = new MemoryEventStore();
    const first = await store.persistEventAndSnapshot(
      storedEvent("c1", 5n),
      storedSnapshot("c1", 5n, 1n),
    );
    expect(first.isOk()).toBe(true);
    const second = await store.persistEventAndSnapshot(
      storedEvent("c1", 10n),
      storedSnapshot("c1", 10n, 2n),
    );
    expect(second.isOk()).toBe(true);
  });

  it("isolates aggregates by id", async () => {
    const store: Store = new MemoryEventStore();
    await store.persistEvent(storedEvent("c1", 1n), 0n);
    const events = (await store.getEventsSince(counterId("c2"), 0n))._unsafeUnwrap();
    expect(events).toEqual([]);
  });
});
