/**
 * The ranked-bar invariants, carried over from the imperative version.
 *
 * The assertions are the point of the port: the house rules and the
 * defects already fixed have to survive the move to visx unchanged, or
 * the migration has quietly traded tested behaviour for new code.
 */
// @vitest-environment jsdom
import { cleanup, fireEvent, render } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { RankedBars } from '../src/charts/RankedBars';

beforeEach(() => cleanup());

const paths = (host: HTMLElement) => [...host.querySelectorAll('path')];
const texts = (host: HTMLElement) =>
  [...host.querySelectorAll('text')].map((t) => t.textContent ?? '');

const BARS = [
  { label: 'python', value: 9102 },
  { label: 'ruby', value: 1400 },
];

const WITH_PART = [
  { label: 'python', value: 7392, part: 3256 },
  { label: 'ruby', value: 863, part: 274 },
];

describe('RankedBars', () => {
  it('draws one mark per row', () => {
    const { container } = render(<RankedBars bars={BARS} label="repositories" />);
    expect(paths(container)).toHaveLength(2);
  });

  it('labels every row and its value', () => {
    const { container } = render(<RankedBars bars={BARS} label="repositories" />);
    const all = texts(container);
    expect(all).toContain('python');
    expect(all).toContain('9,102');
  });

  it('scales bars against the largest value, not the sum', () => {
    const { container } = render(<RankedBars bars={BARS} label="x" />);
    const width = (index: number) => {
      const d = paths(container)[index]!.getAttribute('d') ?? '';
      const xs = [...d.matchAll(/[MH]\s*([-\d.]+)/g)].map((m) => Number(m[1]));
      return Math.max(...xs) - Math.min(...xs);
    };
    // ruby/python is 1400/9102; the bar ratio should match within a
    // pixel or two of the rounded cap.
    expect(width(1) / width(0)).toBeCloseTo(1400 / 9102, 1);
  });

  it('says why it is blank rather than rendering an empty frame', () => {
    const { container } = render(<RankedBars bars={[]} label="x" />);
    expect(container.querySelector('.chart-empty')).not.toBeNull();
    expect(paths(container)).toHaveLength(0);
  });

  it('carries a role and a label, for readers who cannot see it', () => {
    const { container } = render(
      <RankedBars bars={BARS} label="repositories per language" />,
    );
    const svg = container.querySelector('svg')!;
    expect(svg.getAttribute('role')).toBe('img');
    expect(svg.getAttribute('aria-label')).toBe('repositories per language');
  });
});

describe('RankedBars proportions', () => {
  // The inset encoding was sized `barHeight - 8`, which survives a 12px
  // bar and collapses to 1px at 9px — a glitch line through the bar
  // rather than a proportion. Track plus fill has no height budget.
  it('draws the part over a track, not inset inside the bar', () => {
    const { container } = render(
      <RankedBars bars={WITH_PART} label="repositories" partLabel="with an SBOM" />,
    );
    expect(paths(container)).toHaveLength(WITH_PART.length * 2);
  });

  it('gives both marks in a row the same vertical extent', () => {
    const { container } = render(
      <RankedBars bars={WITH_PART} label="x" partLabel="part" />,
    );
    const heights = paths(container).map((p) => {
      const d = p.getAttribute('d') ?? '';
      const ys = [...d.matchAll(/[-\d.]+\s+([-\d.]+)/g)].map((m) => Number(m[1]));
      return Math.round(Math.max(...ys) - Math.min(...ys));
    });
    expect(new Set(heights).size).toBe(1);
    expect(heights[0]).toBeGreaterThanOrEqual(8);
  });

  it('never clips the part beyond its own track', () => {
    const { container } = render(
      <RankedBars bars={[{ label: 'a', value: 10, part: 99 }]} label="x" />,
    );
    const extent = (index: number) => {
      const d = paths(container)[index]!.getAttribute('d') ?? '';
      const xs = [...d.matchAll(/[MH]\s*([-\d.]+)/g)].map((m) => Number(m[1]));
      return Math.max(...xs);
    };
    expect(extent(1)).toBeLessThanOrEqual(extent(0));
  });

  it('names both series in a legend', () => {
    const { container } = render(
      <RankedBars bars={WITH_PART} label="repositories" partLabel="with an SBOM" />,
    );
    const legend = container.querySelector('.chart-legend')!;
    expect(legend.textContent).toContain('repositories');
    expect(legend.textContent).toContain('with an SBOM');
  });

  it('needs no legend for a single series — the title names it', () => {
    const { container } = render(<RankedBars bars={BARS} label="repositories" />);
    expect(container.querySelector('.chart-legend')).toBeNull();
  });

  it('shows part and total together, so the ratio is readable', () => {
    const { container } = render(
      <RankedBars bars={WITH_PART} label="x" partLabel="part" />,
    );
    expect(texts(container)).toContain('3,256 / 7,392');
  });
});

describe('RankedBars selection', () => {
  it("invokes the row's own handler, not the one at its index", () => {
    const picked: string[] = [];
    const { container } = render(
      <RankedBars
        bars={[
          { label: 'first', value: 10, onSelect: () => picked.push('first') },
          { label: 'second', value: 5, onSelect: () => picked.push('second') },
        ]}
        label="x"
      />,
    );
    fireEvent.click(paths(container)[1]!);
    expect(picked).toEqual(['second']);
  });

  it('pairs handler and mark even when a row draws two paths', () => {
    const picked: string[] = [];
    const { container } = render(
      <RankedBars
        bars={[
          { label: 'a', value: 10, part: 4, onSelect: () => picked.push('a') },
          { label: 'b', value: 5, part: 1, onSelect: () => picked.push('b') },
        ]}
        label="x"
        partLabel="part"
      />,
    );
    // Four paths, two rows: an index-matched handler would fire 'a'.
    expect(paths(container)).toHaveLength(4);
    fireEvent.click(paths(container)[3]!);
    expect(picked).toEqual(['b']);
  });

  it('marks a selectable row as selectable, and others not', () => {
    const { container } = render(
      <RankedBars
        bars={[
          { label: 'a', value: 1, onSelect: vi.fn() },
          { label: 'b', value: 1 },
        ]}
        label="x"
      />,
    );
    expect(paths(container)[0]!.getAttribute('cursor')).toBe('pointer');
    expect(paths(container)[1]!.getAttribute('cursor')).toBeNull();
  });
});

describe('RankedBars tooltip', () => {
  // Tooltip bodies used to be HTML assigned to innerHTML, assembled from
  // dataset values — package names among them. Anyone can publish a
  // package, so that was untrusted input reaching an HTML sink.
  it('renders a name containing markup as that name', () => {
    const hostile = '<img src=x onerror="alert(1)">';
    const { container } = render(
      <RankedBars bars={[{ label: hostile, value: 1 }]} label="x" />,
    );
    // fireEvent, not dispatchEvent: React derives onMouseEnter from its
    // own mouseover delegation, so a native 'mouseenter' never reaches it.
    fireEvent.mouseEnter(paths(container)[0]!);
    const tip = container.querySelector('.chart-tooltip');
    expect(tip).not.toBeNull();
    expect(tip!.querySelector('img')).toBeNull();
    expect(tip!.textContent).toContain(hostile);
  });

  it('closes on mouse leave', () => {
    const { container } = render(<RankedBars bars={BARS} label="x" />);
    const bar = paths(container)[0]!;
    fireEvent.mouseEnter(bar);
    expect(container.querySelector('.chart-tooltip')).not.toBeNull();
    fireEvent.mouseLeave(bar);
    expect(container.querySelector('.chart-tooltip')).toBeNull();
  });
});
