/**
 * The chart palette, and the record of it having been validated.
 *
 * Every value below was produced by iterating against the palette
 * validator rather than chosen by eye. The first attempt — the project's
 * own accent `#0F6B57` plus a violet-blue — failed two checks: the accent
 * sits at chroma 0.086 and reads as grey in a fill, and the green/blue
 * pair separates by only ΔE 5.1 under tritanopia. Both are invisible
 * problems to a normal-vision reader looking at the chart.
 *
 * Validator results, recorded so a future change can be re-checked
 * against the same bar:
 *
 *   categorical light   #00805F,#A35C00,#94286F,#2F5FD0
 *     lightness band    all 4 inside L 0.48–0.67          PASS
 *     chroma floor      all 4 >= 0.1                      PASS
 *     CVD separation    worst adjacent ΔE 9.3 protan      PASS
 *     normal vision     worst adjacent ΔE 18.8            PASS
 *     contrast          all 4 >= 3:1 vs #FBFCFB           PASS
 *
 *   categorical dark    #22A681,#C2851F,#C25494,#5E86E0
 *     lightness band    all 4 inside L 0.48–0.67          PASS
 *     CVD separation    worst adjacent ΔE 10.9 protan     PASS
 *     contrast          all 4 >= 3:1 vs #161C1A           PASS
 *
 *   sequential light    5 steps, hue spread 4°, ΔL >= 0.06, pale end 2.03:1
 *   sequential dark     5 steps, hue spread 7°, ΔL >= 0.06, pale end 2.41:1
 *
 * Dark is a separate selection, not an inversion: the dark lightness band
 * is L 0.48–0.67 against light's 0.43–0.77, so the light steps sit
 * outside it and fail outright.
 */

/** Categorical hues, in fixed order. Never cycled, never reordered. */
export const CATEGORICAL_LIGHT = [
  '#00805F', // 1 — verdigris
  '#A35C00', // 2 — amber
  '#94286F', // 3 — plum
  '#2F5FD0', // 4 — blue
] as const;

export const CATEGORICAL_DARK = [
  '#22A681',
  '#C2851F',
  '#C25494',
  '#5E86E0',
] as const;

/** One hue, light to dark. For magnitude, never for identity. */
export const SEQUENTIAL_LIGHT = [
  '#7BC1AC', '#4CA890', '#1F8D73', '#00755A', '#045340',
] as const;

export const SEQUENTIAL_DARK = [
  '#2A6152', '#237866', '#209074', '#22AA84', '#59C9A7',
] as const;

/**
 * Named series, so a filter that changes which series are present cannot
 * repaint the survivors — colour follows the entity, not its rank.
 */
export const SERIES = {
  direct: 0,
  transitive: 1,
  unknown: 2,
  syft: 0,
  'github-depgraph': 3,
} as const;

export type SeriesName = keyof typeof SERIES;

/** Chart chrome. Kept out of the CSS tokens so the SVG can read it directly. */
export interface ChartTheme {
  categorical: readonly string[];
  sequential: readonly string[];
  surface: string;
  ink: string;
  inkMuted: string;
  grid: string;
  axis: string;
}

const LIGHT: ChartTheme = {
  categorical: CATEGORICAL_LIGHT,
  sequential: SEQUENTIAL_LIGHT,
  surface: '#FBFCFB',
  ink: '#14181A',
  inkMuted: '#6E7C78',
  grid: '#E4E9E7',
  axis: '#C3CCC9',
};

const DARK: ChartTheme = {
  categorical: CATEGORICAL_DARK,
  sequential: SEQUENTIAL_DARK,
  surface: '#161C1A',
  ink: '#E5EAE8',
  inkMuted: '#8C9894',
  grid: '#242D2A',
  axis: '#333F3B',
};

/**
 * The theme the page is actually rendering in.
 *
 * Read at draw time rather than cached: the OS theme can change while the
 * page is open, and a cached palette would leave one chart in the other
 * theme's colours.
 */
export function chartTheme(): ChartTheme {
  const explicit = document.documentElement.dataset['theme'];
  if (explicit === 'dark') return DARK;
  if (explicit === 'light') return LIGHT;

  // Called on every draw, so it must not throw: an environment without
  // matchMedia (a test renderer, an old embed) would otherwise blank
  // every chart rather than render one in the wrong theme.
  const query = typeof window !== 'undefined' && window.matchMedia
    ? window.matchMedia('(prefers-color-scheme: dark)')
    : null;
  return query?.matches ? DARK : LIGHT;
}

/** Colour for a named series, stable regardless of how many are shown. */
export function seriesColor(name: SeriesName, theme = chartTheme()): string {
  const index = SERIES[name];
  return theme.categorical[index % theme.categorical.length]!;
}

/** Step of the sequential ramp for a 0–1 magnitude. */
export function rampColor(fraction: number, theme = chartTheme()): string {
  const steps = theme.sequential;
  const clamped = Math.min(Math.max(fraction, 0), 1);
  const index = Math.min(Math.floor(clamped * steps.length), steps.length - 1);
  return steps[index]!;
}
