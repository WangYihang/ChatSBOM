/**
 * Invariants for the share bar, the histogram and the time series,
 * carried over from the imperative versions so the port to visx cannot
 * quietly drop a tested behaviour.
 */
// @vitest-environment jsdom
import { cleanup, fireEvent, render } from '@testing-library/react';
import { beforeEach, describe, expect, it } from 'vitest';

import {
  groupBySource,
  Histogram,
  StackedShare,
  TimeSeries,
} from '../src/charts/Plots';

beforeEach(() => cleanup());

const paths = (h: HTMLElement) => [...h.querySelectorAll('path')];
const textOf = (h: HTMLElement) => h.textContent ?? '';

describe('StackedShare', () => {
  const SLICES = [
    { series: 'direct' as const, label: 'declared', value: 463150 },
    { series: 'transitive' as const, label: 'inherited', value: 5590319 },
    { series: 'unknown' as const, label: 'undetermined', value: 9427 },
  ];

  it('draws one bar in parts, not one bar per part', () => {
    const { container } = render(<StackedShare slices={SLICES} label="x" />);
    expect(container.querySelectorAll('svg')).toHaveLength(1);
    expect(paths(container)).toHaveLength(3);
  });

  it('labels every slice with its share in the legend', () => {
    const { container } = render(<StackedShare slices={SLICES} label="x" />);
    const legend = container.querySelector('.chart-legend')!;
    expect(legend.textContent).toContain('inherited · 92.2%');
    expect(legend.textContent).toContain('declared · 7.6%');
  });

  it('direct-labels only segments wide enough to hold the text', () => {
    const { container } = render(<StackedShare slices={SLICES} label="x" />);
    // 92.2% is wide enough; 0.2% is not.
    expect(textOf(container)).toContain('92.2%');
    const inSvg = [...container.querySelectorAll('svg text')].map((t) => t.textContent);
    expect(inSvg).not.toContain('0.2%');
  });

  it('says why it is blank rather than drawing a zero-width bar', () => {
    const { container } = render(
      <StackedShare
        slices={[{ series: 'direct', label: 'a', value: 0 }]}
        label="x"
      />,
    );
    expect(container.querySelector('.chart-empty')).not.toBeNull();
  });
});

describe('Histogram', () => {
  const BUCKETS = [
    { label: 'none', value: 11840 },
    { label: '1-9', value: 4228 },
    { label: '10-24', value: 1589 },
  ];

  it('draws one bar per bucket', () => {
    const { container } = render(<Histogram buckets={BUCKETS} label="x" />);
    expect(paths(container)).toHaveLength(3);
  });

  it('keeps one y-axis worth of tick labels, never two', () => {
    const { container } = render(<Histogram buckets={BUCKETS} label="x" />);
    const numeric = [...container.querySelectorAll('svg text')]
      .map((t) => t.textContent ?? '')
      .filter((s) => /^[\d,]+$/.test(s));
    expect(numeric.length).toBeLessThanOrEqual(3);
  });

  it('names the x dimension when given one', () => {
    const { container } = render(
      <Histogram buckets={BUCKETS} label="x" xLabel="dependencies" />,
    );
    expect(textOf(container)).toContain('dependencies');
  });

  it('offers a tooltip per bar', () => {
    const { container } = render(<Histogram buckets={BUCKETS} label="x" />);
    fireEvent.mouseEnter(paths(container)[0]!);
    const tip = container.querySelector('.chart-tooltip')!;
    expect(tip.textContent).toContain('none');
    expect(tip.textContent).toContain('11,840');
  });

  it('says why it is blank', () => {
    const { container } = render(<Histogram buckets={[]} label="x" />);
    expect(container.querySelector('.chart-empty')).not.toBeNull();
  });
});

describe('TimeSeries', () => {
  const SYFT = {
    source: 'syft',
    points: [
      { label: '2026-01', total: 90, direct: 12 },
      { label: '2026-02', total: 105, direct: 19 },
      { label: '2026-03', total: 118, direct: 17 },
    ],
  };
  const DEPGRAPH = {
    source: 'github-depgraph',
    points: [{ label: '2026-09', total: 149, direct: 149 }],
  };

  it('draws one line per source', () => {
    const { container } = render(
      <TimeSeries series={[SYFT, DEPGRAPH]} label="x" />,
    );
    // Only syft has more than one point, so only syft has a line —
    // and the two are separate groups either way.
    expect(container.querySelectorAll('g[data-series]')).toHaveLength(2);
    expect(
      container.querySelector('g[data-series="syft"] polyline'),
    ).not.toBeNull();
  });

  it('never merges two sources into one line', () => {
    /**
     * The defect this shape exists to remove. Merged, the adoption
     * series drew `mail` from February's 124 to September's 149 and
     * read as adoption growing — when February was syft's lockfile
     * closure and September was GitHub's manifest parse, seven months
     * apart. A rising line is a claim neither measurement makes.
     */
    const { container } = render(
      <TimeSeries
        series={[
          { source: 'syft', points: [{ label: '2026-02', total: 124, direct: 30 }] },
          DEPGRAPH,
        ]}
        label="x"
      />,
    );
    // Two points, and no line joining them.
    expect(container.querySelectorAll('circle')).toHaveLength(2);
    expect(container.querySelectorAll('polyline')).toHaveLength(0);
  });

  it('puts both sources on one timeline', () => {
    // Scaling each series to its own months would place February and
    // September at the same x and make two collections seven months
    // apart look simultaneous.
    const { container } = render(
      <TimeSeries series={[SYFT, DEPGRAPH]} label="x" />,
    );
    const at = (source: string) =>
      [...container.querySelectorAll(`g[data-series="${source}"] circle`)]
        .map((c) => Number(c.getAttribute('cx')));
    const syftX = at('syft');
    const depX = at('github-depgraph');
    expect(Math.max(...syftX)).toBeLessThan(depX[0]!);
  });

  it('gives every point a marker, ringed in the surface colour', () => {
    const { container } = render(
      <TimeSeries series={[SYFT, DEPGRAPH]} label="x" />,
    );
    const dots = container.querySelectorAll('circle');
    expect(dots).toHaveLength(4);
    expect(dots[0]!.getAttribute('stroke-width')).toBe('2');
  });

  it('draws no line from a single observation', () => {
    // The line would run from one point to nowhere, or the area from
    // the origin — a ramp that reads as "grew from zero", which one
    // measurement cannot support.
    const { container } = render(
      <TimeSeries series={[DEPGRAPH]} label="x" />,
    );
    expect(container.querySelectorAll('polyline')).toHaveLength(0);
    expect(container.querySelectorAll('circle')).toHaveLength(1);
  });

  it('says so when every source has only one observation', () => {
    const { container } = render(
      <TimeSeries series={[DEPGRAPH]} label="x" />,
    );
    const note = container.querySelector('.chart-note')!.textContent!;
    expect(note).toContain('One observation per source');
    // And says what the gap between them is not.
    expect(note).toMatch(/not a change in adoption/);
  });

  it('adds no such caveat once a source has two observations', () => {
    const { container } = render(<TimeSeries series={[SYFT]} label="x" />);
    expect(container.querySelector('.chart-note')).toBeNull();
  });

  it('names each source in a legend, so identity is not colour alone', () => {
    const { container } = render(
      <TimeSeries series={[SYFT, DEPGRAPH]} label="x" />,
    );
    const legend = container.querySelector('.chart-legend')!.textContent!;
    expect(legend).toContain('syft');
    expect(legend).toContain('github-depgraph');
  });

  it('gives an unknown source a neutral rather than someone else\'s hue', () => {
    // A third collector must not repaint syft's line or crash the panel.
    const { container } = render(
      <TimeSeries
        series={[SYFT, { source: 'some-new-tool', points: DEPGRAPH.points }]}
        label="x"
      />,
    );
    const colourOf = (source: string) =>
      container
        .querySelector(`g[data-series="${source}"] circle`)!
        .getAttribute('fill');
    expect(colourOf('some-new-tool')).not.toBe(colourOf('syft'));
  });

  it('keeps the end month labels inside the frame', () => {
    /**
     * A centred label at the last month sits at `width - pad.right`,
     * so half of it falls outside the frame and is clipped — measured
     * 8.8 px of `2026-09` past a 579.7 px frame. `pad.right` is 10 px
     * and cannot absorb a 38 px label, so the anchor moves rather than
     * the padding.
     */
    const { container } = render(
      <TimeSeries series={[SYFT, DEPGRAPH]} label="x" />,
    );
    const months = [...container.querySelectorAll('text')].filter((node) =>
      /^\d{4}-\d{2}$/.test(node.textContent ?? ''),
    );
    expect(months.length).toBeGreaterThan(1);
    expect(months[0]!.getAttribute('text-anchor')).toBe('start');
    expect(months[months.length - 1]!.getAttribute('text-anchor')).toBe('end');
  });

  it('centres a lone month, which has no edge to fall off', () => {
    const { container } = render(<TimeSeries series={[DEPGRAPH]} label="x" />);
    const month = [...container.querySelectorAll('text')].find((node) =>
      /^\d{4}-\d{2}$/.test(node.textContent ?? ''),
    )!;
    expect(month.getAttribute('text-anchor')).toBe('middle');
  });

  it('says why it is empty rather than drawing nothing', () => {
    const { container } = render(<TimeSeries series={[]} label="x" />);
    expect(container.querySelector('.chart-empty')!.textContent).toContain(
      'accumulates',
    );
  });

  it('treats a source with no points as absent', () => {
    const { container } = render(
      <TimeSeries series={[{ source: 'syft', points: [] }]} label="x" />,
    );
    expect(container.querySelector('.chart-empty')).not.toBeNull();
  });
});

describe('groupBySource', () => {
  it('splits rows into one series per source', () => {
    const series = groupBySource([
      { source: 'syft', month: '2026-02', repositoryCount: 124, directCount: 30 },
      { source: 'github-depgraph', month: '2026-09', repositoryCount: 149, directCount: 149 },
      { source: 'syft', month: '2026-01', repositoryCount: 110, directCount: 28 },
    ]);
    expect(series.map((s) => s.source)).toEqual(['github-depgraph', 'syft']);
    expect(series[1]!.points).toHaveLength(2);
  });

  it('orders months within a series, not by arrival', () => {
    // A line drawn in arrival order zigzags.
    const series = groupBySource([
      { source: 'syft', month: '2026-03', repositoryCount: 3, directCount: 1 },
      { source: 'syft', month: '2026-01', repositoryCount: 1, directCount: 1 },
      { source: 'syft', month: '2026-02', repositoryCount: 2, directCount: 1 },
    ]);
    expect(series[0]!.points.map((p) => p.label)).toEqual([
      '2026-01', '2026-02', '2026-03',
    ]);
  });

  it('orders sources stably, so a colour does not move between renders', () => {
    const one = groupBySource([
      { source: 'syft', month: '2026-01', repositoryCount: 1, directCount: 1 },
      { source: 'github-depgraph', month: '2026-01', repositoryCount: 1, directCount: 1 },
    ]);
    const other = groupBySource([
      { source: 'github-depgraph', month: '2026-01', repositoryCount: 1, directCount: 1 },
      { source: 'syft', month: '2026-01', repositoryCount: 1, directCount: 1 },
    ]);
    expect(one.map((s) => s.source)).toEqual(other.map((s) => s.source));
  });
});
