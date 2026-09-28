/**
 * How much one question may ask a store for, decided once for both.
 *
 * Each backend kept its own copy of these, and both copies had the same
 * hole: `Math.min(children, 30)` lets -1 through, and the row query read
 * -1 as "no limit given" and fetched its default of 50 (#31). The
 * endpoint refuses such a value now; these hold for any other caller.
 */
import { describe, expect, it } from 'vitest';

import {
  boundedLimit,
  boundedOffset,
  DEFAULT_LIMIT,
  MAX_LIMIT,
  MAX_OFFSET,
  treeShape,
} from '../src/dataset/shape';

describe('boundedLimit', () => {
  it('is the default when none is given, or none that can be used', () => {
    for (const limit of [undefined, 0, -1, Number.NaN, Number.POSITIVE_INFINITY]) {
      expect(boundedLimit(limit)).toBe(DEFAULT_LIMIT);
    }
  });

  it('is whole, and no more than the cap', () => {
    expect(boundedLimit(2.7)).toBe(2);
    expect(boundedLimit(10_000)).toBe(MAX_LIMIT);
  });
});

describe('boundedOffset', () => {
  it('starts at the beginning when none is given, or none that can be used', () => {
    for (const offset of [undefined, -5, Number.NaN, Number.NEGATIVE_INFINITY]) {
      expect(boundedOffset(offset)).toBe(0);
    }
  });

  it('is whole, and no more than a UInt32 parameter holds', () => {
    // ClickHouse binds the offset as `{offset:UInt32}`; 1e12 failed the
    // statement rather than returning the empty page it describes.
    expect(boundedOffset(2.5)).toBe(2);
    expect(boundedOffset(1e12)).toBe(MAX_OFFSET);
    expect(MAX_OFFSET).toBe(2 ** 32 - 1);
  });
});

describe('treeShape', () => {
  it('is the default shape when none is asked for', () => {
    expect(treeShape({})).toEqual({ children: 14, branch: 4 });
  });

  it('clamps nothing, or less, to the smallest tree', () => {
    for (const value of [0, -1, -10_000]) {
      expect(treeShape({ children: value, branch: value })).toEqual({
        children: 1,
        branch: 1,
      });
    }
  });

  it('clamps a wide tree to the bounds', () => {
    expect(treeShape({ children: 1e6, branch: 1e6 })).toEqual({
      children: 30,
      branch: 12,
    });
  });

  it('falls back to the default for a value that is not a number at all', () => {
    expect(treeShape({ children: Number.NaN, branch: Number.NaN })).toEqual({
      children: 14,
      branch: 4,
    });
  });
});
