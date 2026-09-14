/**
 * Invariants for the share bar, the histogram and the time series,
 * carried over from the imperative versions so the port to visx cannot
 * quietly drop a tested behaviour.
 */
// @vitest-environment jsdom
import { cleanup, fireEvent, render } from '@testing-library/react';
import { beforeEach, describe, expect, it } from 'vitest';

import { Histogram, StackedShare, TimeSeries } from '../src/charts/Plots';

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
  const POINTS = [
    { label: '2026-01', total: 90, direct: 12 },
    { label: '2026-02', total: 105, direct: 19 },
    { label: '2026-03', total: 118, direct: 17 },
  ];

  it('plots both series on one axis', () => {
    const { container } = render(<TimeSeries points={POINTS} label="x" />);
    expect(container.querySelectorAll('polyline')).toHaveLength(2);
    const numeric = [...container.querySelectorAll('svg text')]
      .map((t) => t.textContent ?? '')
      .filter((s) => /^\d+$/.test(s));
    expect(numeric.length).toBeLessThanOrEqual(3);
  });

  it('fills the total as an area so the direct line reads inside it', () => {
    const { container } = render(<TimeSeries points={POINTS} label="x" />);
    expect(container.querySelectorAll('polygon')).toHaveLength(1);
  });

  it('gives every point a marker, ringed in the surface colour', () => {
    const { container } = render(<TimeSeries points={POINTS} label="x" />);
    const dots = container.querySelectorAll('circle');
    expect(dots).toHaveLength(6);
    expect(dots[0]!.getAttribute('stroke-width')).toBe('2');
  });

  // A single observation is a snapshot. The area would run from the
  // origin to that one point, drawing a ramp that reads as "grew from
  // zero" — a claim one measurement cannot support.
  it('draws no trend marks from a single observation', () => {
    const { container } = render(
      <TimeSeries points={[POINTS[0]!]} label="x" />,
    );
    expect(container.querySelectorAll('polygon')).toHaveLength(0);
    expect(container.querySelectorAll('polyline')).toHaveLength(0);
    expect(container.querySelectorAll('circle')).toHaveLength(2);
  });

  it('says a single observation is not yet a trend', () => {
    const { container } = render(
      <TimeSeries points={[POINTS[0]!]} label="x" />,
    );
    expect(container.querySelector('.chart-note')!.textContent).toContain(
      'one observation',
    );
  });

  it('adds no such caveat once there are two observations', () => {
    const { container } = render(
      <TimeSeries points={POINTS.slice(0, 2)} label="x" />,
    );
    expect(container.querySelector('.chart-note')).toBeNull();
  });

  it('says why it is empty rather than drawing nothing', () => {
    const { container } = render(<TimeSeries points={[]} label="x" />);
    expect(container.querySelector('.chart-empty')!.textContent).toContain(
      'accumulates',
    );
  });
});
