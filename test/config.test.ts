import { describe, expect, it } from "vitest";
import { defaultEventStoreConfig, shouldSnapshot } from "../src/index.js";

describe("defaultEventStoreConfig", () => {
  it("uses snapshot interval 5", () => {
    expect(defaultEventStoreConfig().snapshotInterval).toBe(5n);
  });
});

describe("shouldSnapshot", () => {
  const cases: { interval: bigint; seqNr: bigint; want: boolean }[] = [
    { interval: 5n, seqNr: 1n, want: false },
    { interval: 5n, seqNr: 4n, want: false },
    { interval: 5n, seqNr: 5n, want: true },
    { interval: 5n, seqNr: 10n, want: true },
    { interval: 5n, seqNr: 11n, want: false },
    { interval: 1n, seqNr: 3n, want: true },
    { interval: 0n, seqNr: 5n, want: false },
  ];

  for (const { interval, seqNr, want } of cases) {
    it(`interval=${interval} seqNr=${seqNr} -> ${want}`, () => {
      expect(shouldSnapshot({ snapshotInterval: interval }, seqNr)).toBe(want);
    });
  }
});
