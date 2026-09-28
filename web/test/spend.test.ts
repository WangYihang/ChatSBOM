/**
 * The daily spend cap's counter (#33).
 *
 * The cap was a running total in KV, checked before a call and added
 * to after it. KV reads can be a minute stale and concurrent writes to
 * one key keep one of them, so the check admitted whatever arrived
 * together and the total lost whatever was added together: 20
 * questions at once against a $5 cap were all admitted, about $44 of
 * calls, of which $2.20 was recorded. The counter is a Durable Object
 * now, which holds a call's worst case before the call is made.
 */
import { describe, expect, it } from 'vitest';

import { counterOver } from './counters';

/** A moment, so that calls made together arrive interleaved. */
const pause = () => new Promise((resolve) => setTimeout(resolve, Math.random() * 3));

describe('a reservation', () => {
  it('is held while it fits under the cap, and refused once it would not', async () => {
    const counter = await counterOver();
    expect(counter.reserve('a', 0.6, 1)).toBe(true);
    expect(counter.reserve('b', 0.4, 1)).toBe(true);
    expect(counter.reserve('c', 0.01, 1)).toBe(false);
    expect(counter.usage()).toEqual({ spent: 0, held: 1 });
  });

  it('can never take concurrent calls past the cap', async () => {
    const counter = await counterOver();
    const held = await Promise.all(
      Array.from({ length: 100 }, async (_, index) => {
        await pause();
        return counter.reserve(`call-${index}`, 0.3, 5);
      }),
    );
    // 16 × $0.30 is $4.80; a 17th would be $5.10.
    expect(held.filter(Boolean)).toHaveLength(16);
    expect(counter.usage().held).toBeCloseTo(4.8, 10);
  });

  it('is settled at what the call cost, which frees the rest', async () => {
    const counter = await counterOver();
    counter.reserve('a', 0.9, 1);
    counter.settle('a', 0.05);
    expect(counter.usage()).toEqual({ spent: 0.05, held: 0 });
    expect(counter.reserve('b', 0.95, 1)).toBe(true);
    expect(counter.reserve('c', 0.01, 1)).toBe(false);
  });

  it('is refunded when its call was never answered', async () => {
    const counter = await counterOver();
    counter.reserve('a', 1, 1);
    counter.refund('a');
    expect(counter.usage()).toEqual({ spent: 0, held: 0 });
    expect(counter.reserve('b', 1, 1)).toBe(true);
  });

  it('is held once, however often it is asked for', async () => {
    const counter = await counterOver();
    expect(counter.reserve('a', 0.6, 1)).toBe(true);
    expect(counter.reserve('a', 0.6, 1)).toBe(true);
    expect(counter.usage()).toEqual({ spent: 0, held: 0.6 });
  });

  it('is settled or refunded once, however often it is asked', async () => {
    const counter = await counterOver();
    counter.reserve('a', 0.5, 1);
    counter.settle('a', 0.2);
    counter.settle('a', 0.2);
    counter.refund('a');
    counter.settle('never-held', 0.3);
    expect(counter.usage()).toEqual({ spent: 0.2, held: 0 });
  });

  it('keeps its worst case when what the call cost cannot be read', async () => {
    // A usage the counter cannot add would otherwise make every later
    // comparison false, and a cap that compares false admits anything.
    const counter = await counterOver();
    counter.reserve('a', 0.5, 1);
    counter.settle('a', Number.NaN);
    expect(counter.usage()).toEqual({ spent: 0.5, held: 0 });
  });

  it.each([
    ['a cost that is not a number', Number.NaN, 1],
    ['a negative cost', -1, 1],
    ['a cap that is not a number', 0.1, Number.NaN],
  ])('refuses %s', async (_, usd, cap) => {
    const counter = await counterOver();
    expect(counter.reserve('a', usd, cap)).toBe(false);
    expect(counter.usage()).toEqual({ spent: 0, held: 0 });
  });
});

describe('the counter', () => {
  it('keeps its reservations and its spend across a restart', async () => {
    const stored = new Map<string, unknown>();
    const before = await counterOver(stored);
    before.reserve('a', 0.4, 1);
    before.reserve('b', 0.3, 1);
    before.settle('b', 0.1);
    // Each write lands a moment after it was made.
    await new Promise((resolve) => setTimeout(resolve, 5));

    const after = await counterOver(stored);
    expect(after.usage()).toEqual({ spent: 0.1, held: 0.4 });
    after.settle('a', 0.2);
    expect(after.usage().spent).toBeCloseTo(0.3, 10);
    expect(after.usage().held).toBe(0);
  });
});
