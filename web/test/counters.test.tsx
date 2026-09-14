/**
 * The header tiles' labels.
 *
 * Untested until now, which is how one of them came to name the wrong
 * thing: `repositories` for a value that counts only the repositories
 * carrying dependency data. The footer had the same gap and the same
 * kind of bug, so these assert the label text, not just the numbers.
 */
import { describe as group, expect, it } from 'vitest';
import { counterTiles } from '../src/app';
import type { Totals } from '../src/d1/queries';

/** The live corpus, so a wrong label reads as the real page's would. */
const LIVE: Totals = {
  repositories: 24_339,
  dependencies: 19_502_430,
  packages: 225_400,
  classified: 19_492_961,
};

const labelFor = (t: Totals, value: number): string | undefined =>
  counterTiles(t).find(([v]) => v === value)?.[1];

group('counterTiles', () => {
  it('does not call the analysed subset the corpus', () => {
    // 24,339 of 28,075 have dependency data. A bare `repositories`
    // names all of them, and the coverage panel's denominators sum to
    // the other number.
    expect(labelFor(LIVE, 24_339)).not.toBe('repositories');
  });

  it('says what the first number is counted over', () => {
    expect(labelFor(LIVE, 24_339)).toContain('dependency data');
  });

  it('matches the coverage panel wording', () => {
    // `Overview.tsx` writes "N with dependency data (P%)" in its bar
    // titles. Same fact, same words, so the two panels reconcile.
    expect(labelFor(LIVE, 24_339)).toBe('repositories with dependency data');
  });

  it('floors the classified percentage rather than rounding it', () => {
    // 99.951% — `Math.round` would claim 100 while 9,469 records are
    // unclassified.
    expect(counterTiles(LIVE)[3]).toEqual([99, '% classified']);
  });

  it('reports 0% rather than dividing by zero on an empty dataset', () => {
    const empty: Totals = {
      repositories: 0, dependencies: 0, packages: 0, classified: 0,
    };
    expect(counterTiles(empty)[3]).toEqual([0, '% classified']);
  });

  it('keeps the other three labels', () => {
    const labels = counterTiles(LIVE).map(([, l]) => l);
    expect(labels.slice(1)).toEqual([
      'dependency records', 'distinct packages', '% classified',
    ]);
  });
});
