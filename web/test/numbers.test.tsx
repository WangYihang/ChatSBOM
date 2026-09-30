/**
 * The page's numbers, in the language the reader chose (#43).
 *
 * About fifteen call sites wrote a number with `toLocaleString()` and no
 * argument, which is the browser's locale rather than the page's: a
 * reader who chose English on a German machine read 24.339 in a chart
 * beside 24,339 in the panel over it. Each of these runs under a German
 * default (`locales.ts`), which is how that shows.
 */
// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { DependencyTree } from '../src/charts/DependencyTree';
import { Histogram, StackedShare } from '../src/charts/Plots';
import { RankedBars } from '../src/charts/RankedBars';
import { SourceShares } from '../src/charts/SourceShares';
import { TimeSeries } from '../src/charts/TimeSeries';
import { QueryView } from '../src/components/QueryView';
import type { DatasetClient } from '../src/dataset/client';
import { DICTIONARIES } from '../src/i18n/strings';
import { germanDefault } from './locales';

const EN = DICTIONARIES.en;
const ZH = DICTIONARIES.zh;

/** `n` as `Intl` writes it for a locale tag. */
const intl = (tag: string, n: number) => new Intl.NumberFormat(tag).format(n);

beforeEach(() => {
  cleanup();
  germanDefault();
});

afterEach(() => {
  cleanup();
  vi.restoreAllMocks();
});

const svgText = (host: HTMLElement) =>
  [...host.querySelectorAll('svg text')].map((node) => node.textContent ?? '');

/** The tooltip a mark shows when the pointer is on it. */
function tooltipOf(mark: Element): string {
  fireEvent.mouseEnter(mark);
  const text = document.querySelector('.chart-tooltip')?.textContent ?? '';
  fireEvent.mouseLeave(mark);
  return text;
}

describe('the charts, under a German browser', () => {
  it('ranks in the chosen language', () => {
    const { container } = render(
      <RankedBars words={EN} locale="en" bars={[{ label: 'serde', value: 24_339 }]} label="x" />,
    );
    expect(svgText(container)).toContain('24,339');
    expect(tooltipOf(container.querySelector('path')!)).toContain('24,339');
  });

  it('totals each ecosystem in the chosen language', () => {
    const { container } = render(
      <SourceShares
        words={EN}
        locale="en"
        label="x"
        rows={[{ label: 'maven', syft: 9_648, depgraph: 47_329, manifest: 0 }]}
      />,
    );
    expect(svgText(container)).toContain('56,977');
    expect(tooltipOf(container.querySelector('path')!)).toContain('9,648');
  });

  it('writes the share bar, the histogram and the series in the chosen language', () => {
    const share = render(
      <StackedShare
        words={EN}
        locale="en"
        label="x"
        valueLabel="records"
        slices={[
          { series: 'direct', label: 'declared', value: 463_150 },
          { series: 'transitive', label: 'inherited', value: 5_590_319 },
        ]}
      />,
    );
    expect(tooltipOf(share.container.querySelector('path')!)).toContain('463,150');
    cleanup();

    const histogram = render(
      <Histogram
        words={EN}
        locale="en"
        label="x"
        valueLabel="repositories"
        buckets={[{ label: '1-9', value: 11_840 }]}
      />,
    );
    // The y axis's top tick, and the bar's own tooltip.
    expect(svgText(histogram.container)).toContain('11,840');
    expect(tooltipOf(histogram.container.querySelector('path')!)).toContain('11,840');
    cleanup();

    const series = render(
      <TimeSeries
        words={EN}
        locale="en"
        label="x"
        snapshotNote={EN.adoptionSnapshot}
        series={[{ source: 'syft', points: [{ label: '2026-02', total: 1_124, direct: 1_030 }] }]}
      />,
    );
    const tip = tooltipOf(series.container.querySelector('circle')!);
    expect(tip).toContain('1,124');
    expect(tip).toContain('1,030');
  });

  it('counts the tree’s repositories in the chosen language', () => {
    const { container } = render(
      <DependencyTree
        words={EN}
        locale="en"
        width={720}
        tree={{
          root: 'mail',
          children: [{ name: 'mini_mime', repositories: 3_580 }],
          grandchildren: [{ parent: 'mini_mime', child: 'net-imap', repositories: 1_200 }],
        }}
      />,
    );
    expect(svgText(container)).toEqual(expect.arrayContaining(['3,580', '1,200']));
    expect(tooltipOf(container.querySelector('path[data-edge="mini_mime>net-imap"]')!))
      .toContain('1,200');
  });

  it('writes Chinese numbers as Intl does for zh-CN', () => {
    const { container } = render(
      <RankedBars words={ZH} locale="zh" bars={[{ label: 'serde', value: 24_339 }]} label="x" />,
    );
    expect(svgText(container)).toContain(intl('zh-CN', 24_339));
  });
});

describe('the query view, under a German browser', () => {
  /** Answers for one package with four-figure counts everywhere. */
  const dataset = new Proxy(
    {},
    {
      get(_target, key: string) {
        const answers: Record<string, unknown> = {
          ecosystemsFor: [
            { type: 'gem', repositoryCount: 1_167, directCount: 30 },
            { type: 'maven', repositoryCount: 6, directCount: 2 },
          ],
          countDependents: 1_234,
          countDependentRows: 1_234,
          dependentsOf: [
            {
              owner: 'rails', repo: 'rails', stars: 58_182, version: '2.8.1',
              url: 'https://github.com/rails/rails', relationship: 'transitive',
              observedAt: '2026-09-13', ecosystem: 'gem', language: 'ruby', manifests: 1,
            },
          ],
          versionSpread: { versions: [], constrained: 0, unversioned: 0 },
          adoptionOverTime: [],
          edgeAmbiguity: {
            names: 225_582, ambiguousNames: 2_730, edges: 614_221,
            ambiguousEdges: 63_384, largestRepository: 5_388,
          },
          pulledInBy: [],
          dependencyTree: { root: 'mail', children: [], grandchildren: [] },
          searchPackages: [],
        };
        return () => Promise.resolve(answers[key]);
      },
    },
  ) as DatasetClient;

  it('writes stars, counts and the largest repository in the chosen language', async () => {
    render(
      <QueryView
        words={EN}
        locale="en"
        dataset={dataset}
        languages={[]}
        route={{ view: 'query', package: 'mail' }}
        go={vi.fn()}
      />,
    );
    // The status line's count, the table's stars, the tree's bound.
    await waitFor(() => expect(screen.getByText(/^1,234 dependants on mail/)).toBeTruthy());
    expect(screen.getByText('58,182')).toBeTruthy();
    expect(screen.getByText(/the largest repository here has 5,388 dependencies/)).toBeTruthy();
    // And the ecosystem filter's counts, which were not formatted at all.
    expect(screen.getByRole('option', { name: 'gem · 1,167' })).toBeTruthy();
  });
});
