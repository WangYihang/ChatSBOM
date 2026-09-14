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
  /**
   * Room for the widest value a row will draw.
   *
   * A proportion prints "3,256 / 7,392", which at 11px mono is about
   * 90px — the previous 62px was sized for a single number and the
   * label overlapped the end of the longest bar.
   */
  valueWidth: 104,
  /** Fallback when nothing has measured the container yet. */
  width: 720,
  top: 6,
} as const;


/**
 * Advance width per character, by face and size, measured in the
 * browser rather than estimated.
 *
 * Rendered at deviceScaleFactor 2 and divided by the character count,
 * the observed worst cases were:
 *
 *   mono 11.5px    7.20   `body-parser`
 *   mono 10.5px    6.30   `raw-body`
 *   sans 11.5px    6.14   `@compodoc/compodoc`
 *
 * Rounded up, never down. Under-estimating draws a label past its
 * column; over-estimating trims one that would have fitted, and the
 * full name is in the tooltip either way. Only the first is a defect.
 */
export const ADVANCE = {
  mono115: 7.2,
  mono105: 6.6,
  sans115: 6.4,
} as const;

/**
 * Trim a label to the room it has, marking that it was trimmed.
 *
 * The failure this exists for is specific and was found by rendering,
 * not by reading: a right-anchored label longer than its gutter runs
 * off the left edge of the SVG and is cut at the *start*, so
 * `@react-native-community/cli-server-api` drew as
 * `t-native-community/cli-server-api` — a package that does not exist.
 * A trailing ellipsis says a name was shortened; a silent head-cut
 * invents one.
 */
export function clipLabel(text: string, px: number, perChar: number): string {
  const room = Math.floor(px / perChar);
  if (room >= text.length) return text;
  if (room < 2) return '…';
  return `${text.slice(0, room - 1)}…`;
}
