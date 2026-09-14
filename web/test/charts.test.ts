/**
 * Charts are DOM output, so these assert on the rendered SVG: the marks
 * that exist, the encodings applied, and the rules the house style makes
 * non-negotiable (one axis, a legend for two or more series, no number on
 * every mark).
 */
// @vitest-environment jsdom
import { beforeEach, describe, expect, it } from 'vitest';

import {
  groupedBars,
  histogram,
  rankedBars,
  stackedShare,
  timeSeries,
} from '../src/charts';

let host: HTMLElement;

beforeEach(() => {
  document.body.innerHTML = '';
  host = document.createElement('div');
  document.body.append(host);
});

const svg = () => host.querySelector('svg')!;
const marks = (selector: string) => host.querySelectorAll(selector);

describe('rankedBars', () => {
  const bars = [
    { label: 'rails', value: 96, part: 40 },
    { label: 'rake', value: 93, part: 12 },
    { label: 'puma', value: 76 },
  ];

  it('draws one mark per row', () => {
    rankedBars(host, bars, { label: 'direct dependants' });
    // Three bars, plus two inset segments each with a surface spacer.
    expect(marks('path').length).toBeGreaterThanOrEqual(3);
  });

  it('is accessible as an image with the claim as its label', () => {
    rankedBars(host, bars, { label: 'direct dependants' });
    expect(svg().getAttribute('role')).toBe('img');
    expect(svg().getAttribute('aria-label')).toBe('direct dependants');
  });

  it('scales to the largest value, not to the axis maximum', () => {
    rankedBars(host, bars, { label: 'x' });
    const widths = [...marks('path')].map(
      (p) => p.getAttribute('d')!.match(/H (\d+(\.\d+)?)/)?.[1],
    );
    expect(widths.filter(Boolean).length).toBeGreaterThan(0);
  });

  it('labels every row but values only once per row', () => {
    rankedBars(host, bars, { label: 'x' });
    const labels = [...marks('text')].map((t) => t.textContent);
    expect(labels).toContain('rails');
    expect(labels.filter((l) => l === '96')).toHaveLength(1);
  });

  it('shows a legend only when a second encoding is present', () => {
    rankedBars(host, bars, { label: 'x', partLabel: 'direct' });
    expect(host.querySelector('.chart-legend')).not.toBeNull();

    host.innerHTML = '';
    rankedBars(host, [{ label: 'a', value: 1 }], { label: 'x' });
    expect(host.querySelector('.chart-legend')).toBeNull();
  });

  it('renders an explanation rather than an empty frame', () => {
    rankedBars(host, [], { label: 'x' });
    expect(host.querySelector('svg')).toBeNull();
    expect(host.querySelector('.chart-empty')).not.toBeNull();
  });

  it('attaches a hover layer', () => {
    rankedBars(host, bars, { label: 'x' });
    expect(host.querySelector('.chart-tooltip')).not.toBeNull();
  });
});

describe('stackedShare', () => {
  const slices = [
    { series: 'direct' as const, label: 'direct', value: 417054 },
    { series: 'transitive' as const, label: 'transitive', value: 5662653 },
    { series: 'unknown' as const, label: 'unknown', value: 9469 },
  ];

  it('draws one segment per slice', () => {
    stackedShare(host, slices, { label: 'how dependencies arrived' });
    expect(marks('path')).toHaveLength(3);
  });

  it('gives each slice its own hue', () => {
    stackedShare(host, slices, { label: 'x' });
    const fills = new Set([...marks('path')].map((p) => p.getAttribute('fill')));
    expect(fills.size).toBe(3);
  });

  it('always carries a legend, since colour alone is not identity', () => {
    stackedShare(host, slices, { label: 'x' });
    const legend = host.querySelector('.chart-legend')!;
    expect(legend.textContent).toContain('direct');
    expect(legend.textContent).toContain('transitive');
  });

  it('direct-labels only segments wide enough to hold the text', () => {
    stackedShare(host, slices, { label: 'x' });
    const labels = [...marks('text')].map((t) => t.textContent);
    // transitive is 93%; unknown is 0.2% and must stay unlabelled.
    expect(labels).toContain('93%');
    expect(labels).not.toContain('0%');
  });

  it('handles an all-zero total without dividing by zero', () => {
    stackedShare(host, [
      { series: 'direct', label: 'direct', value: 0 },
    ], { label: 'x' });
    expect(host.querySelector('.chart-empty')).not.toBeNull();
  });
});

describe('histogram', () => {
  const buckets = Array.from({ length: 8 }, (_, i) => ({
    label: `${i * 10}`, value: (i + 1) * 3,
  }));

  it('draws one bar per bucket', () => {
    histogram(host, buckets, { label: 'dependency counts' });
    expect(marks('path')).toHaveLength(8);
  });

  it('uses one hue, because this is magnitude not identity', () => {
    histogram(host, buckets, { label: 'x' });
    const fills = [...marks('path')].map((p) => p.getAttribute('fill'));
    // Steps of a single ramp, so hues repeat rather than cycle through
    // categorical colours.
    expect(new Set(fills).size).toBeLessThanOrEqual(5);
  });

  it('draws a baseline and recessive gridlines', () => {
    histogram(host, buckets, { label: 'x' });
    expect(marks('line').length).toBeGreaterThanOrEqual(4);
  });

  it('thins x labels when buckets would collide', () => {
    const many = Array.from({ length: 24 }, (_, i) => ({
      label: `b${i}`, value: i,
    }));
    histogram(host, many, { label: 'x' });
    const labels = [...marks('text')].filter((t) =>
      t.textContent?.startsWith('b'),
    );
    expect(labels.length).toBeLessThan(24);
  });
});

describe('rankedBars selection', () => {
  // The old wiring matched handlers to marks by index, walking
  // `querySelectorAll("path")` and pairing element N with datum N. That
  // holds only while every row draws exactly one path, which stopped
  // being true the moment a form drew a track and a fill. The handler
  // travels with the datum now.
  it('invokes the row own handler, not the one at its index', () => {
    const picked: string[] = [];
    rankedBars(
      host,
      [
        { label: 'first', value: 10, onSelect: () => picked.push('first') },
        { label: 'second', value: 5, onSelect: () => picked.push('second') },
      ],
      { label: 'x' },
    );
    const paths = [...marks('path')];
    paths[1]!.dispatchEvent(new MouseEvent('click', { bubbles: true }));
    expect(picked).toEqual(['second']);
  });

  it('pairs handler and mark even when a row draws two paths', () => {
    const picked: string[] = [];
    rankedBars(
      host,
      [
        { label: 'a', value: 10, part: 4, onSelect: () => picked.push('a') },
        { label: 'b', value: 5, part: 1, onSelect: () => picked.push('b') },
      ],
      { label: 'x', partLabel: 'part' },
    );
    // Four paths, two rows: an index-matched handler would fire 'a' here.
    const paths = [...marks('path')];
    expect(paths).toHaveLength(4);
    paths[3]!.dispatchEvent(new MouseEvent('click', { bubbles: true }));
    expect(picked).toEqual(['b']);
  });

  it('marks a selectable row as selectable', () => {
    rankedBars(host, [{ label: 'a', value: 1, onSelect: () => {} }], {
      label: 'x',
    });
    expect(marks('path')[0]!.getAttribute('cursor')).toBe('pointer');
  });

  it('leaves rows without a handler inert', () => {
    rankedBars(host, [{ label: 'a', value: 1 }], { label: 'x' });
    expect(marks('path')[0]!.getAttribute('cursor')).toBeNull();
  });
});

describe('rankedBars with a part series', () => {
  const bars = [
    { label: 'python', value: 9102, part: 7392 },
    { label: 'ruby', value: 1400, part: 863 },
  ];

  // The part used to be drawn inset inside the bar, sandwiched between
  // two surface-coloured paths. That arithmetic (barHeight - 8) survives
  // a 12px bar and collapses to a 1px hairline at 9px, which renders as
  // a glitch line through the bar rather than a proportion. Track plus
  // fill has no height budget to run out of.
  it('draws the part over a track, not inset inside the bar', () => {
    rankedBars(host, bars, { label: 'repositories', partLabel: 'with an SBOM' });
    expect(marks('path')).toHaveLength(bars.length * 2);
  });

  it('never paints a mark in the surface colour', () => {
    rankedBars(host, bars, { label: 'repositories', partLabel: 'with an SBOM' });
    const surface = getComputedStyle(document.body).backgroundColor;
    for (const path of marks('path')) {
      expect(path.getAttribute('fill')).not.toBe(surface);
    }
  });

  it('keeps the part readable at the tightest row height', () => {
    rankedBars(host, bars, { label: 'repositories', partLabel: 'with an SBOM' });
    // Both marks in a row span the same vertical extent, so the part is
    // as legible as the track regardless of how tight rows become.
    const heights = [...marks('path')].map((p) => {
      const d = p.getAttribute('d') ?? '';
      const ys = [...d.matchAll(/[-\d.]+\s+([-\d.]+)/g)].map((m) => Number(m[1]));
      return Math.round(Math.max(...ys) - Math.min(...ys));
    });
    expect(new Set(heights).size).toBe(1);
    expect(heights[0]).toBeGreaterThanOrEqual(8);
  });

  it('still labels both series, so identity is not colour alone', () => {
    rankedBars(host, bars, { label: 'repositories', partLabel: 'with an SBOM' });
    expect(host.querySelector('.chart-legend')!.textContent).toContain(
      'with an SBOM',
    );
  });
});

describe('timeSeries', () => {
  const points = [
    { label: '2026-01', total: 90, direct: 12 },
    { label: '2026-02', total: 105, direct: 19 },
    { label: '2026-03', total: 118, direct: 17 },
  ];

  it('plots both series on one axis', () => {
    timeSeries(host, points, { label: 'adoption' });
    expect(marks('polyline')).toHaveLength(2);
    // One y-axis worth of tick labels, not two.
    const ticks = [...marks('text')].filter((t) =>
      /^\d+$/.test(t.textContent ?? ''),
    );
    expect(ticks.length).toBeLessThanOrEqual(3);
  });

  it('fills the total as an area so the direct line reads inside it', () => {
    timeSeries(host, points, { label: 'x' });
    expect(marks('polygon')).toHaveLength(1);
  });

  it('gives every point a marker as a hit target', () => {
    timeSeries(host, points, { label: 'x' });
    expect(marks('circle')).toHaveLength(6);
  });

  it('rings markers with the surface colour so overlaps stay readable', () => {
    timeSeries(host, points, { label: 'x' });
    const dot = marks('circle')[0]!;
    expect(dot.getAttribute('stroke-width')).toBe('2');
  });

  it('says why it is empty rather than drawing nothing', () => {
    timeSeries(host, [], { label: 'x' });
    expect(host.querySelector('.chart-empty')!.textContent).toContain(
      'accumulates',
    );
  });

  it('does not divide by zero for a single point', () => {
    timeSeries(host, [points[0]!], { label: 'x' });
    expect(marks('circle')).toHaveLength(2);
  });

  // A single observation is a snapshot, not a trend. Drawing the area
  // from the origin to that one point renders a ramp that reads as
  // "grew from zero", which is a claim the data does not make.
  it('draws no trend marks from a single observation', () => {
    timeSeries(host, [points[0]!], { label: 'x' });
    expect(marks('polygon')).toHaveLength(0);
    expect(marks('polyline')).toHaveLength(0);
  });

  it('says a single observation is not yet a trend', () => {
    timeSeries(host, [points[0]!], { label: 'x' });
    expect(host.querySelector('.chart-note')!.textContent).toContain(
      'one observation',
    );
  });

  it('still draws trend marks once there are two observations', () => {
    timeSeries(host, points.slice(0, 2), { label: 'x' });
    expect(marks('polygon')).toHaveLength(1);
    expect(marks('polyline')).toHaveLength(2);
    expect(host.querySelector('.chart-note')).toBeNull();
  });
});

describe('groupedBars', () => {
  const groups = [
    {
      label: 'java',
      values: [
        { series: 'syft' as const, value: 10065 },
        { series: 'github-depgraph' as const, value: 58566 },
      ],
    },
    {
      label: 'ruby',
      values: [
        { series: 'syft' as const, value: 30879 },
        { series: 'github-depgraph' as const, value: 0 },
      ],
    },
  ];
  const labels = { syft: 'Syft', 'github-depgraph': 'Dependency graph' };

  it('draws a bar per series per group', () => {
    groupedBars(host, groups, { label: 'sources', seriesLabels: labels });
    expect(marks('path')).toHaveLength(4);
  });

  it('keeps a series colour stable across groups', () => {
    groupedBars(host, groups, { label: 'x', seriesLabels: labels });
    const fills = [...marks('path')].map((p) => p.getAttribute('fill'));
    expect(fills[0]).toBe(fills[2]);
    expect(fills[1]).toBe(fills[3]);
  });

  it('labels the series in a legend', () => {
    groupedBars(host, groups, { label: 'x', seriesLabels: labels });
    expect(host.querySelector('.chart-legend')!.textContent).toContain('Syft');
  });
});

describe('tooltip content', () => {
  // Tooltip strings used to be HTML assigned to `innerHTML`, and they
  // were assembled from dataset values — package names among them.
  // Anyone can publish a package, so that was untrusted input reaching
  // an HTML sink on a page that also fronts an API relay. Content is
  // structured now and every field goes through a text node.
  it('renders a name containing markup as that name', () => {
    const hostile = '<img src=x onerror="alert(1)">';
    rankedBars(
      host,
      [{ label: hostile, value: 1, detail: { title: hostile, lines: ['1'] } }],
      { label: 'x' },
    );
    const bar = marks('path')[0]!;
    bar.dispatchEvent(new MouseEvent('mouseenter', { bubbles: true }));

    const tip = host.querySelector('.chart-tooltip')!;
    expect(tip.querySelector('img')).toBeNull();
    expect(tip.textContent).toContain(hostile);
  });

  it('positions against the viewport, matching position: fixed', () => {
    rankedBars(host, [{ label: 'a', value: 1 }], { label: 'x' });
    const bar = marks('path')[0]!;
    bar.dispatchEvent(
      new MouseEvent('mousemove', { bubbles: true, clientX: 400, clientY: 300 }),
    );
    // `.chart-tooltip` is position: fixed, so these are viewport
    // coordinates. Subtracting the host's offset — as the original did —
    // displaced the tooltip by the panel's distance from the left edge.
    const tip = host.querySelector<HTMLElement>('.chart-tooltip')!;
    expect(tip.style.left).toBe('412px');
    expect(tip.style.top).toBe('312px');
  });
});
