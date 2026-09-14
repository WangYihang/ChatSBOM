/**
 * Type size must not depend on panel width.
 *
 * A chart drawn into a fixed `viewBox` and stretched to its container
 * scales its text with the container: the full-width panel rendered its
 * labels at 2.5x while the narrow rail rendered the same labels at
 * 1.4x, so the page had two type scales and neither matched the CSS.
 * Laying out in measured pixels is what keeps 11.5px meaning 11.5px.
 */
// @vitest-environment jsdom
import { cleanup, render } from '@testing-library/react';
import { beforeEach, describe, expect, it } from 'vitest';

import { Measured } from '../src/charts/Frame';
import { RankedBars } from '../src/charts/RankedBars';
import { SourceShares } from '../src/charts/SourceShares';

beforeEach(() => cleanup());

const viewBox = (host: HTMLElement) =>
  host.querySelector('svg')!.getAttribute('viewBox') ?? '';
const boxWidth = (host: HTMLElement) => Number(viewBox(host).split(' ')[2]);

describe('chart width', () => {
  it('lays a ranking out at the width it is given', () => {
    const { container } = render(
      <RankedBars bars={[{ label: 'a', value: 1 }]} label="x" width={1400} />,
    );
    expect(boxWidth(container)).toBe(1400);
  });

  it('lays the source panel out at the width it is given', () => {
    const { container } = render(
      <SourceShares rows={[{ language: 'java', syft: 1, depgraph: 2 }]} width={1400} />,
    );
    expect(boxWidth(container)).toBe(1400);
  });

  it('keeps label type size constant across widths', () => {
    const size = (width: number) => {
      cleanup();
      const { container } = render(
        <RankedBars bars={[{ label: 'a', value: 1 }]} label="x" width={width} />,
      );
      return container.querySelector('text')!.getAttribute('font-size');
    };
    expect(size(600)).toBe(size(1600));
  });

  it('widens only the plot, never the label or value gutters', () => {
    const barEnd = (width: number) => {
      cleanup();
      const { container } = render(
        <RankedBars bars={[{ label: 'a', value: 10 }]} label="x" width={width} />,
      );
      const d = container.querySelector('path')!.getAttribute('d') ?? '';
      const xs = [...d.matchAll(/[MH]\s*([-\d.]+)/g)].map((m) => Number(m[1]));
      return { start: Math.min(...xs), end: Math.max(...xs) };
    };
    const narrow = barEnd(600);
    const wide = barEnd(1600);
    // Same left gutter; the bar grows by exactly the extra width.
    expect(wide.start).toBe(narrow.start);
    expect(wide.end - narrow.end).toBe(1000);
  });

  it('leaves room for the widest value label it will draw', () => {
    const { container } = render(
      <RankedBars
        bars={[{ label: 'python', value: 7392, part: 3256 }]}
        label="repositories"
        partLabel="with an SBOM"
        width={720}
      />,
    );
    const value = [...container.querySelectorAll('text')].find((t) =>
      (t.textContent ?? '').includes('/'),
    )!;
    const bar = container.querySelector('path')!;
    const barEnd = Math.max(
      ...[...(bar.getAttribute('d') ?? '').matchAll(/[MH]\s*([-\d.]+)/g)].map(
        (m) => Number(m[1]),
      ),
    );
    // The label is right-aligned at the frame edge; its text is about
    // 7px per character at 11px mono, so the gutter has to clear it.
    const labelWidth = (value.textContent ?? '').length * 7;
    const labelStart = Number(value.getAttribute('x')) - labelWidth;
    expect(labelStart).toBeGreaterThan(barEnd);
  });
});

describe('Measured', () => {
  // ParentSize needs ResizeObserver. Its absence makes measuring
  // impossible but not drawing: falling back gives a page that looks
  // slightly off where throwing gives a blank one. Found because jsdom
  // has no ResizeObserver and the entire query view rendered empty —
  // the same thing an old browser would have seen.
  it('draws at a fallback width when the container cannot be measured', () => {
    const saved = globalThis.ResizeObserver;
    // @ts-expect-error deleting an optional global for the test
    delete globalThis.ResizeObserver;
    try {
      const { container } = render(
        <Measured fallbackWidth={900}>
          {(width) => <RankedBars bars={[{ label: 'a', value: 1 }]} label="x" width={width} />}
        </Measured>,
      );
      expect(boxWidth(container)).toBe(900);
    } finally {
      globalThis.ResizeObserver = saved;
    }
  });
});

describe('chart frame sizing', () => {
  // The `.plot` wrapper went away with the imperative bridge, and the
  // stylesheet's `.plot svg { width: 100% }` stopped matching — so the
  // SVG fell back to its viewBox as an intrinsic size, overflowed its
  // panel and overlapped every panel below it. A measured chart should
  // not depend on a stylesheet rule to be the right size at all.
  it('carries its measured size as attributes, not just a viewBox', () => {
    const { container } = render(
      <RankedBars bars={[{ label: 'a', value: 1 }]} label="x" width={640} />,
    );
    const svg = container.querySelector('svg')!;
    expect(svg.getAttribute('width')).toBe('640');
    expect(Number(svg.getAttribute('height'))).toBeGreaterThan(0);
    expect(svg.getAttribute('viewBox')).toBe(`0 0 640 ${svg.getAttribute('height')}`);
  });

  it('keeps the plot wrapper the stylesheet targets', () => {
    const { container } = render(
      <Measured fallbackWidth={640}>
        {(width) => <RankedBars bars={[{ label: 'a', value: 1 }]} label="x" width={width} />}
      </Measured>,
    );
    expect(container.querySelector('.plot')).not.toBeNull();
    expect(container.querySelector('.plot > svg')).not.toBeNull();
  });
});

describe('Measured wrapper height', () => {
  // ParentSize's default wrapper style is `height: 100%`, and these
  // panels are content-height, so 100% resolved to 0: the wrapper
  // measured 910x0 with a 38px SVG inside it, every panel collapsed,
  // and the page overlapped itself. The wrapper must take its height
  // from the chart, not from a parent that has none to give.
  it('does not force a percentage height on the wrapper', () => {
    const { container } = render(
      <Measured fallbackWidth={640}>
        {(width) => <RankedBars bars={[{ label: 'a', value: 1 }]} label="x" width={width} />}
      </Measured>,
    );
    for (const div of container.querySelectorAll('div')) {
      expect(div.style.height).not.toBe('100%');
    }
  });
});

describe('Measured stays in flow', () => {
  // ParentSize put its children in an inner `position: absolute;
  // inset: 0` div, which takes the chart out of flow, so the wrapper
  // measured 910x0 around a 38px chart and every panel collapsed. The
  // chart has to stay in normal flow, because the panel's height comes
  // from it.
  it('puts the chart in normal flow, not absolutely positioned', () => {
    const { container } = render(
      <Measured fallbackWidth={640}>
        {(width) => <RankedBars bars={[{ label: 'a', value: 1 }]} label="x" width={width} />}
      </Measured>,
    );
    for (const node of container.querySelectorAll('div, svg')) {
      expect(['', 'static', 'relative']).toContain(
        (node as HTMLElement).style.position || '',
      );
    }
  });

  it('wraps the chart in exactly one element', () => {
    const { container } = render(
      <Measured fallbackWidth={640}>
        {(width) => <RankedBars bars={[{ label: 'a', value: 1 }]} label="x" width={width} />}
      </Measured>,
    );
    // A single .plot, and the svg is its direct child — which is what
    // the stylesheet's `.plot > svg` guard targets.
    expect(container.querySelectorAll('.plot')).toHaveLength(1);
    expect(container.querySelector('.plot > svg')).not.toBeNull();
  });
});
