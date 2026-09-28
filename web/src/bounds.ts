/**
 * How much one question may ask a store for, decided once for both.
 *
 * Each backend used to keep its own copy of these, and both copies had
 * the same hole: `Math.min(children, TREE_CHILDREN_MAX)` lets a negative
 * through, and the row query read -1 as "no limit given" and fetched its
 * default of 50, past the 30 the tree is bounded at (#31). A bound kept
 * in two places is a bound that can be fixed in one.
 *
 * The endpoint refuses a value that is not a whole number, or is below
 * its minimum, before any store sees it (`d1/api.ts`). These hold for
 * every other caller, and they are where the ceilings live: past them a
 * value is not wrong, only more than anyone gets.
 */

export const DEFAULT_LIMIT = 50;
export const MAX_LIMIT = 500;

/**
 * The furthest a page may start.
 *
 * `{offset:UInt32}` is how ClickHouse binds it, and 1e12 failed the
 * statement rather than returning the empty page it describes. No
 * package has four billion dependant rows, so the clamp changes no page
 * that exists.
 */
export const MAX_OFFSET = 2 ** 32 - 1;

/**
 * How wide a drawn tree may get.
 *
 * These are display bounds, not data bounds: the tree is a diagram, and
 * past roughly this many marks it stops being one. `express` pulls in
 * 31 packages directly and each of those pulls in more, so without a
 * cap the second hop alone runs to hundreds of rows.
 */
const TREE_CHILDREN = 14;
const TREE_CHILDREN_MAX = 30;
const TREE_BRANCH = 4;
const TREE_BRANCH_MAX = 12;

/** A whole number within [min, max], or the fallback for no number at all. */
function clamp(
  value: number | undefined,
  min: number,
  max: number,
  fallback: number,
): number {
  if (value === undefined || !Number.isFinite(value)) return fallback;
  return Math.min(Math.max(Math.floor(value), min), max);
}

/** Rows to return: the default when none is given or none can be used. */
export function boundedLimit(limit: number | undefined): number {
  if (limit === undefined || !Number.isFinite(limit) || limit < 1) {
    return DEFAULT_LIMIT;
  }
  return Math.min(Math.floor(limit), MAX_LIMIT);
}

/** Rows to skip, for paging: never negative, never past `MAX_OFFSET`. */
export function boundedOffset(offset: number | undefined): number {
  return clamp(offset, 0, MAX_OFFSET, 0);
}

/**
 * A tree's first hop and its per-parent second hop.
 *
 * Nothing, or less, is the smallest tree rather than the default one,
 * the same as `branch` always was: one child is what was asked for, as
 * near as the diagram can come to it.
 */
export function treeShape(options: { children?: number; branch?: number }): {
  children: number;
  branch: number;
} {
  return {
    children: clamp(options.children, 1, TREE_CHILDREN_MAX, TREE_CHILDREN),
    branch: clamp(options.branch, 1, TREE_BRANCH_MAX, TREE_BRANCH),
  };
}
