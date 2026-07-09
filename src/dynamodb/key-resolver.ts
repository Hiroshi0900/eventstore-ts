import type { AggregateId } from "../types.js";

const FNV_OFFSET_BASIS = 0xcbf29ce484222325n;
const FNV_PRIME = 0x100000001b3n;
const MASK_64 = 0xffffffffffffffffn;

const textEncoder = new TextEncoder();

/** FNV-1a 64-bit hash, bit-compatible with Go's hash/fnv New64a. */
export function fnv1a64(input: string): bigint {
  let hash = FNV_OFFSET_BASIS;
  for (const byte of textEncoder.encode(input)) {
    hash ^= BigInt(byte);
    hash = (hash * FNV_PRIME) & MASK_64;
  }
  return hash;
}

/**
 * DynamoDB key generation identical to the Go internal/keyresolver:
 * shard by FNV-1a(value) % shardCount (skipped when shardCount <= 1).
 */
export class KeyResolver {
  private readonly shardCount: bigint;

  constructor(shardCount: number) {
    this.shardCount = BigInt(shardCount <= 0 ? 1 : shardCount);
  }

  resolvePartitionKey(id: AggregateId): string {
    return `${id.typeName()}-${this.computeShardId(id)}`;
  }

  resolveEventSortKey(id: AggregateId, seqNr: bigint): string {
    return `${id.typeName()}-${id.value()}-${seqNr.toString(10).padStart(20, "0")}`;
  }

  resolveSnapshotSortKey(id: AggregateId): string {
    return `${id.typeName()}-${id.value()}-0`;
  }

  private computeShardId(id: AggregateId): bigint {
    if (this.shardCount <= 1n) return 0n;
    return fnv1a64(id.value()) % this.shardCount;
  }
}
