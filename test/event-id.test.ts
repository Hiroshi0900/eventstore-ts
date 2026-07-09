import { describe, expect, it } from "vitest";
import { generateEventId } from "../src/index.js";

describe("generateEventId", () => {
  it("returns 32 lowercase hex chars", () => {
    expect(generateEventId()).toMatch(/^[0-9a-f]{32}$/);
  });

  it("generates unique ids", () => {
    const ids = new Set(Array.from({ length: 1000 }, () => generateEventId()));
    expect(ids.size).toBe(1000);
  });
});
