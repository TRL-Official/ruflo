/**
 * GHSA-mrhx-c7xc-m3w5: zod before 4.6.6 validates EVERY element of an array before it applies the array's `.max()`,
 * so an oversized input to a capped schema costs time and memory in proportion to its length (3,000,000 items: 576 ms,
 * +197 MB on zod 3.25.76; 0 ms, +0 MB on zod 4.6.7). @claude-flow/cli depended on zod ^3.22.0, the only line with no fix.
 *
 * These tests count how many elements the CLI's own schemas read from an oversized array. With a fixed zod the count is
 * bounded by the cap, not by the input; with zod 3 it equals the input length, so the first two tests fail there.
 */
import { describe, expect, it } from 'vitest';
import { commandDefinitionSchema, LIMITS } from '../src/mods/command-registry/schema.js';
import { taskSpecSchema, MISSION_LIMITS } from '../src/missions/schemas.js';

const N = 100_000;

/** An array of `n` valid-looking strings that counts every element read. */
function countingArray(n: number, item: (i: number) => string) {
  const target = Array.from({ length: n }, (_, i) => item(i));
  const seen = { reads: 0 };
  const proxy = new Proxy(target, {
    get(t, key, receiver) {
      if (typeof key === 'string' && /^\d+$/.test(key)) seen.reads++;
      return Reflect.get(t, key, receiver);
    },
  });
  return { proxy, seen };
}

describe('oversized collections are rejected without reading every element (GHSA-mrhx-c7xc-m3w5)', () => {
  it('command registry: legacyNames over its cap', () => {
    const { proxy, seen } = countingArray(N, (i) => `name-${i}`);
    const r = commandDefinitionSchema.safeParse({ legacyNames: proxy });
    expect(r.success).toBe(false);
    expect(seen.reads).toBeLessThanOrEqual(LIMITS.names);
  });

  it('missions: task dependsOn over its cap', () => {
    const { proxy, seen } = countingArray(N, (i) => `t${i}`);
    const r = taskSpecSchema.safeParse({ dependsOn: proxy });
    expect(r.success).toBe(false);
    expect(seen.reads).toBeLessThanOrEqual(MISSION_LIMITS.deps);
  });

  it('control: an array within its cap is still validated element by element', () => {
    const { proxy, seen } = countingArray(2, (i) => `name-${i}`);
    commandDefinitionSchema.safeParse({ legacyNames: proxy });
    expect(seen.reads).toBeGreaterThanOrEqual(2);
  });
});
