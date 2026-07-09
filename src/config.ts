/** Snapshot policy. Interval 0 disables snapshots entirely. */
export interface EventStoreConfig {
  readonly snapshotInterval: bigint;
}

export function defaultEventStoreConfig(): EventStoreConfig {
  return { snapshotInterval: 5n };
}

export function shouldSnapshot(config: EventStoreConfig, seqNr: bigint): boolean {
  if (config.snapshotInterval === 0n) return false;
  return seqNr % config.snapshotInterval === 0n;
}
