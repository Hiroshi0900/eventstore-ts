import { describe, expect, it } from "vitest";
import { KeyResolver, fnv1a64 } from "../src/dynamodb/index.js";
import { newAggregateId } from "../src/index.js";

// Expected hashes generated with Go's hash/fnv New64a (bit-compat fixture).
const FNV_FIXTURES: [string, bigint][] = [
  ["", 14695981039346656037n],
  ["a", 12638187200555641996n],
  ["hello", 11831194018420276491n],
  ["user-12345", 3394814444776315280n],
  ["MemorialSetting", 2159334222414353849n],
  ["日本語", 17194429697725099911n],
];

describe("fnv1a64", () => {
  for (const [input, want] of FNV_FIXTURES) {
    it(`hashes ${JSON.stringify(input)} identically to Go`, () => {
      expect(fnv1a64(input)).toBe(want);
    });
  }
});

describe("KeyResolver", () => {
  const id = newAggregateId("MemorialSetting", "user-12345");

  it("resolves partition key with shard 0 when shardCount is 1", () => {
    expect(new KeyResolver(1).resolvePartitionKey(id)).toBe("MemorialSetting-0");
  });

  it("normalizes shardCount 0 to 1", () => {
    expect(new KeyResolver(0).resolvePartitionKey(id)).toBe("MemorialSetting-0");
  });

  it("shards by FNV-1a(value) % shardCount when shardCount > 1", () => {
    // Go fixture: fnv1a64("user-12345") % 4 == 0, fnv1a64("abc") % 4 == 3, fnv1a64("日本語") % 4 == 3
    const resolver = new KeyResolver(4);
    expect(resolver.resolvePartitionKey(id)).toBe("MemorialSetting-0");
    expect(resolver.resolvePartitionKey(newAggregateId("T", "abc"))).toBe("T-3");
    expect(resolver.resolvePartitionKey(newAggregateId("T", "日本語"))).toBe("T-3");
  });

  it("resolves event sort key with 20-digit zero-padded seqNr", () => {
    expect(new KeyResolver(1).resolveEventSortKey(id, 42n)).toBe(
      "MemorialSetting-user-12345-00000000000000000042",
    );
  });

  it("resolves snapshot sort key with literal 0 suffix", () => {
    expect(new KeyResolver(1).resolveSnapshotSortKey(id)).toBe("MemorialSetting-user-12345-0");
  });
});
