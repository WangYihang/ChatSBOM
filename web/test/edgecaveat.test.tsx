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
import { DICTIONARIES } from '../src/i18n/strings';

const EN = DICTIONARIES.en;
const ZH = DICTIONARIES.zh;
import type { EdgeAmbiguity } from '../src/d1/queries';

/** The live values, so a regression reads as the real page's would. */
/**
 * Canonical figures. The earlier 39,186 and 316,546 counted `cargo`
 * apart from `rust-crate` and `composer` apart from `php-composer`, so
 * they overstated the ambiguity fivefold — the fixture said what the
 * page said, and both were wrong.
 */
const LIVE: EdgeAmbiguity = {
  names: 225_582,
  ambiguousNames: 2_730,
  edges: 614_221,
  ambiguousEdges: 63_384,
  largestRepository: 5_388,
};

group('edgeCaveat', () => {
  it('reports the measured share rather than a pasted one', () => {
    const text = edgeCaveat(LIVE, EN, 'en');
    expect(text).toContain('2,730 of 225,582 names');
    expect(text).toContain('63,384 of 614,221 edges');
    expect(text).toContain('10%');
  });

  it('never carries the stale figures again', () => {
    const text = edgeCaveat(LIVE, EN, 'en');
    for (const stale of [
      '2,508', '141,938', '107,974', '455,281', '23.7',
      // And the inflated pair that replaced them.
      '39,186', '316,546', '51.5',
    ]) {
      expect(text).not.toContain(stale);
    }
  });

  it('keeps the warning when the store cannot count collisions', () => {
    // D1 answers null — its `artifacts` is four integers with no
    // ecosystem column. The warning must survive without figures
    // rather than disappear or print zeroes.
    const text = edgeCaveat(null, EN, 'en');
    expect(text).toContain('not unique across ecosystems');
    expect(text).toContain('The filters above do not reach this panel.');
    expect(text).not.toMatch(/\d/);
  });

  it('does not divide by zero on an empty edge table', () => {
    const text = edgeCaveat({ ...LIVE, edges: 0, ambiguousEdges: 0 }, EN, 'en');
    expect(text).not.toContain('NaN');
    expect(text).not.toMatch(/\d/);
  });

  it('rounds the share rather than truncating it', () => {
    // 3 of 4 is 75%, not 75.0 or 74.
    const text = edgeCaveat({
      ...LIVE, edges: 4, ambiguousEdges: 3, names: 10, ambiguousNames: 2,
    }, EN, 'en');
    expect(text).toContain('75%');
  });

  it('translates the caveat and keeps the figures', () => {
    const text = edgeCaveat(LIVE, ZH, 'zh');
    expect(text).toContain('2,730');
    expect(text).toContain('10%');
    expect(text).toContain('上方的过滤器不作用于这个面板');
    expect(text).not.toContain('Edges are aggregated');
  });

  it('keeps the warning in Chinese when the store cannot count', () => {
    const text = edgeCaveat(null, ZH, 'zh');
    expect(text).toContain('跨生态并不唯一');
    expect(text).not.toMatch(/\d/);
  });
});
