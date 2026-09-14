/**
 * The caveat on both edge panels.
 *
 * It used to be a fixed sentence with four measurements pasted into it,
 * taken before the dependency-graph ingest and never revisited. By the
 * time anyone re-read them the page was claiming 2,508 ambiguous names
 * of 141,938 carrying 107,974 of 455,281 edges — 23.7% — while the true
 * figures were 39,186 of 225,400 carrying 316,546 of 614,221, or 51.5%.
 * It told the reader a quarter of the edges might be merged when most
 * of them are.
 */
import { describe as group, expect, it } from 'vitest';
import { edgeCaveat } from '../src/components/QueryView';
import type { EdgeAmbiguity } from '../src/d1/queries';

/** The live values, so a regression reads as the real page's would. */
const LIVE: EdgeAmbiguity = {
  names: 225_400,
  ambiguousNames: 39_186,
  edges: 614_221,
  ambiguousEdges: 316_546,
  largestRepository: 5_388,
};

group('edgeCaveat', () => {
  it('reports the measured share rather than a pasted one', () => {
    const text = edgeCaveat(LIVE);
    expect(text).toContain('39,186 of 225,400 names');
    expect(text).toContain('316,546 of 614,221 edges');
    expect(text).toContain('52%');
  });

  it('never carries the stale figures again', () => {
    const text = edgeCaveat(LIVE);
    for (const stale of ['2,508', '141,938', '107,974', '455,281', '23.7']) {
      expect(text).not.toContain(stale);
    }
  });

  it('keeps the warning when the store cannot count collisions', () => {
    // D1 answers null — its `artifacts` is four integers with no
    // ecosystem column. The warning must survive without figures
    // rather than disappear or print zeroes.
    const text = edgeCaveat(null);
    expect(text).toContain('not unique across ecosystems');
    expect(text).toContain('The filters above do not reach this panel.');
    expect(text).not.toMatch(/\d/);
  });

  it('does not divide by zero on an empty edge table', () => {
    const text = edgeCaveat({ ...LIVE, edges: 0, ambiguousEdges: 0 });
    expect(text).not.toContain('NaN');
    expect(text).not.toMatch(/\d/);
  });

  it('rounds the share rather than truncating it', () => {
    // 3 of 4 is 75%, not 75.0 or 74.
    const text = edgeCaveat({
      ...LIVE, edges: 4, ambiguousEdges: 3, names: 10, ambiguousNames: 2,
    });
    expect(text).toContain('75%');
  });
});
