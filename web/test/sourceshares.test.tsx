/**
 * The source panel's encoding.
 *
 * The previous form drew both collectors on one shared scale, and row
 * counts span four orders of magnitude — TypeScript 3,549,474 rows next
 * to Ruby 30,879. Every language but the largest was an invisible
 * sliver, and the dependency-graph series was invisible everywhere, so
 * the panel could not answer the question it posed. These tests pin the
 * properties that make per-row shares readable instead.
 */
// @vitest-environment jsdom
import { render } from '@testing-library/react';
import { describe, expect, it } from 'vitest';

import { SourceShares } from '../src/charts/SourceShares';
import { DICTIONARIES } from '../src/i18n/strings';

const EN = DICTIONARIES.en;

// Keyed by ecosystem since #55 §4.12; the row labels keep the old
// names so the magnitudes below read as they were measured.
const ROWS = [
  { label: 'typescript', syft: 3549474, depgraph: 0, manifest: 0 },
  { label: 'java', syft: 9648, depgraph: 47329, manifest: 0 },
  { label: 'ruby', syft: 30879, depgraph: 0, manifest: 0 },
];

const marks = (host: HTMLElement) => host.querySelectorAll('path');

describe('SourceShares', () => {
  it('gives every row the same width, whatever its magnitude', () => {
    const { container } = render(<SourceShares words={EN} locale="en" label={EN.sourcesChartLabel} rows={ROWS} />);
    const widthOf = (row: string) => {
      const g = container.querySelector(`[data-row="${row}"]`)!;
      return [...g.querySelectorAll('path')].reduce((sum, p) => {
        const d = p.getAttribute('d') ?? '';
        const xs = [...d.matchAll(/[MH]\s*([-\d.]+)/g)].map((m) => Number(m[1]));
        return sum + (Math.max(...xs) - Math.min(...xs));
      }, 0);
    };
    // TypeScript has 115x Ruby's rows; the bars must not differ by that.
    const ts = widthOf('typescript');
    const ruby = widthOf('ruby');
    expect(Math.abs(ts - ruby)).toBeLessThan(4);
  });

  it('draws a visible segment for a minority collector', () => {
    const { container } = render(<SourceShares words={EN} locale="en" label={EN.sourcesChartLabel} rows={ROWS} />);
    // Java is 83% dependency graph. On the old shared scale its syft
    // segment was a sliver; here both segments are a real share.
    const java = container.querySelector('[data-row="java"]')!;
    expect(java.querySelectorAll('path')).toHaveLength(2);
  });

  it('omits a collector that contributed nothing rather than drawing zero', () => {
    const { container } = render(<SourceShares words={EN} locale="en" label={EN.sourcesChartLabel} rows={ROWS} />);
    const ts = container.querySelector('[data-row="typescript"]')!;
    expect(ts.querySelectorAll('path')).toHaveLength(1);
  });

  it('keeps the absolute total on the page, since the bars no longer carry it', () => {
    const { container } = render(<SourceShares words={EN} locale="en" label={EN.sourcesChartLabel} rows={ROWS} />);
    expect(container.textContent).toContain('3,549,474');
    expect(container.textContent).toContain('56,977');
  });

  it('names both collectors, so identity is never colour alone', () => {
    const { container } = render(<SourceShares words={EN} locale="en" label={EN.sourcesChartLabel} rows={ROWS} />);
    const legend = container.querySelector('.chart-legend')!;
    expect(legend.textContent).toContain('Syft');
    expect(legend.textContent).toContain('Dependency graph');
  });

  it('says why it is blank rather than rendering an empty frame', () => {
    const { container } = render(<SourceShares words={EN} locale="en" label={EN.sourcesChartLabel} rows={[]} />);
    expect(container.querySelector('.chart-empty')).not.toBeNull();
    expect(marks(container as HTMLElement)).toHaveLength(0);
  });

  it('draws the Gradle declarations as a third collector', () => {
    // The `manifest` source (#55 D1): what Gradle build files declare,
    // which neither Syft nor the graph reads.
    const { container } = render(
      <SourceShares words={EN} locale="en" label={EN.sourcesChartLabel}
        rows={[{ label: 'maven', syft: 10, depgraph: 20, manifest: 5 }]}
      />,
    );
    const maven = container.querySelector('[data-row="maven"]')!;
    expect(maven.querySelectorAll('path')).toHaveLength(3);
    expect(container.textContent).toContain('35');
    expect(container.textContent).toContain('Gradle build files');
  });
});
