/**
 * Mark geometry, as pure functions.
 *
 * Extracted so the imperative drawing code and the React components
 * share one definition while the migration to visx is half done. Two
 * copies of `barPath` would drift, and the drift would show up as marks
 * that look subtly different between panels rather than as a failure.
 *
 * These are the house rules expressed as numbers: a 2px surface gap
 * between adjacent fills so segments read as separate, and a 4px radius
 * on the data end only — the baseline end stays square, because a bar
 * rounded at both ends no longer reads as anchored to its axis.
 */

/** Gap between adjacent fills, so two segments never touch. */
export const SPACER = 2;

/** Radius on the data end of a bar. */
export const CAP = 4;

/** A bar whose data end is rounded and whose baseline end is square. */
export function barPath(
  x: number,
  y: number,
  width: number,
  height: number,
  horizontal: boolean,
): string {
  const r = Math.min(CAP, horizontal ? width : height);
  if (horizontal) {
    return [
      `M ${x} ${y}`,
      `H ${x + width - r}`,
      `Q ${x + width} ${y} ${x + width} ${y + r}`,
      `V ${y + height - r}`,
      `Q ${x + width} ${y + height} ${x + width - r} ${y + height}`,
      `H ${x}`,
      'Z',
    ].join(' ');
  }
  return [
    `M ${x} ${y + height}`,
    `V ${y + r}`,
    `Q ${x} ${y} ${x + r} ${y}`,
    `H ${x + width - r}`,
    `Q ${x + width} ${y} ${x + width} ${y + r}`,
    `V ${y + height}`,
    'Z',
  ].join(' ');
}

/**
 * Ranked-bar row metrics.
 *
 * A 26px row with a 12px bar spent more than half its height on air;
 * 18/9 keeps the surface gap the house style requires while fitting half
 * again as many rows into a panel. Kept here rather than inline because
 * the part encoding depends on the bar height, and the last time those
 * two numbers lived apart, tightening the row collapsed the part mark to
 * a 1px hairline.
 */
export const ROW = {
  height: 18,
  bar: 9,
  labelWidth: 150,
  valueWidth: 62,
  width: 720,
  top: 6,
} as const;
