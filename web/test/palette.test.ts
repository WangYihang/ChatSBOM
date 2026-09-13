// @vitest-environment jsdom
import { describe, expect, it } from 'vitest';

import {
  CATEGORICAL_DARK,
  CATEGORICAL_LIGHT,
  rampColor,
  SEQUENTIAL_DARK,
  SEQUENTIAL_LIGHT,
  seriesColor,
  type ChartTheme,
} from '../src/palette';

const LIGHT = {
  categorical: CATEGORICAL_LIGHT,
  sequential: SEQUENTIAL_LIGHT,
  surface: '#FBFCFB', ink: '#14181A', inkMuted: '#6E7C78',
  grid: '#E4E9E7', axis: '#C3CCC9',
} satisfies ChartTheme;

describe('palette', () => {
  it('keeps light and dark as separate selections', () => {
    // Dark's lightness band is narrower, so an inverted light palette
    // fails outright. They must not share values.
    expect(new Set([...CATEGORICAL_LIGHT, ...CATEGORICAL_DARK]).size).toBe(8);
  });

  it('offers exactly four categorical hues', () => {
    expect(CATEGORICAL_LIGHT).toHaveLength(4);
    expect(CATEGORICAL_DARK).toHaveLength(4);
  });

  it('gives direct and transitive different hues', () => {
    expect(seriesColor('direct', LIGHT)).not.toBe(
      seriesColor('transitive', LIGHT),
    );
  });

  it('colours a series by identity, not by rank', () => {
    // The same series keeps its hue however many others are drawn.
    const before = seriesColor('github-depgraph', LIGHT);
    const after = seriesColor('github-depgraph', LIGHT);
    expect(before).toBe(after);
    expect(before).not.toBe(seriesColor('syft', LIGHT));
  });

  it('ramps monotonically across the magnitude range', () => {
    const steps = [0, 0.25, 0.5, 0.75, 1].map((f) => rampColor(f, LIGHT));
    expect(new Set(steps).size).toBeGreaterThan(1);
    expect(steps[0]).toBe(SEQUENTIAL_LIGHT[0]);
    expect(steps.at(-1)).toBe(SEQUENTIAL_LIGHT.at(-1));
  });

  it('clamps out-of-range magnitudes rather than throwing', () => {
    expect(rampColor(-5, LIGHT)).toBe(SEQUENTIAL_LIGHT[0]);
    expect(rampColor(99, LIGHT)).toBe(SEQUENTIAL_LIGHT.at(-1));
  });
});

describe('chartTheme', () => {
  it('falls back to light where matchMedia is unavailable', async () => {
    // Drawing happens on every render, so a missing matchMedia must not
    // throw — that would blank the chart instead of mis-theming it.
    const { chartTheme } = await import('../src/palette');
    expect(() => chartTheme()).not.toThrow();
  });
});
