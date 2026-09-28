/**
 * What both stores do to a question before asking it, and to an answer
 * after: how much may be asked for, and how a row becomes a result.
 *
 * Each backend used to keep its own copy of all of this, and the copies
 * drifted. The bounds had the same hole in both (#31); the ecosystem
 * names were merged in one and not the other; `Number(row.x)` was
 * written out once per field per store. One copy now, and the contract
 * suite (`test/contract.test.ts`) holds both stores to what it returns.
 */
import { type Relationship, RELATIONSHIPS } from '../schema';
import type {
  Dependent,
  DependencyTree,
  PackageEdge,
  TreeEdge,
  VersionShare,
  VersionSpread,
} from './types';

/* ---------------- bounds ---------------- */

/*
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

/* ---------------- rows ---------------- */

/** A row as a store returns it, keyed by the names the statement gave. */
export type Row = Record<string, unknown>;

/**
 * A count, however the store spelled it.
 *
 * ClickHouse's JSON quotes every 64-bit integer — `"198"`, not `198` —
 * so that no reader rounds one, and SQLite's are numbers. Absent is
 * none.
 */
export function num(value: unknown): number {
  return Number(value ?? 0);
}

/** A relationship, or `unknown` for anything the contract does not name. */
export function relationshipOf(value: unknown): Relationship {
  return (RELATIONSHIPS as readonly unknown[]).includes(value)
    ? (value as Relationship)
    : 'unknown';
}

/**
 * A dependants row, from the columns both stores' statements name:
 * `owner`, `repo`, `stars`, `version`, `url`, `language`, `ecosystem`,
 * `relationship`, `observed_on` and `manifests`.
 */
export function shapeDependant(row: Row): Dependent {
  return {
    owner: String(row['owner'] ?? ''),
    repo: String(row['repo'] ?? ''),
    stars: num(row['stars']),
    version: String(row['version'] ?? ''),
    url: String(row['url'] ?? ''),
    language: String(row['language'] ?? ''),
    ecosystem: String(row['ecosystem'] ?? ''),
    relationship: relationshipOf(row['relationship']),
    observedAt: String(row['observed_on'] ?? ''),
    manifests: num(row['manifests'] ?? 1),
  };
}

/**
 * Split version rows into the resolved list and what was set aside.
 *
 * Shared by both backends, because the shape a panel needs is the same
 * whichever store answered — and because the split is the part that is
 * easy to get subtly wrong. A backend that filtered the constraints out
 * in SQL would return a list the panel cannot caveat.
 *
 * `rows` are the resolved versions, widest first, and every other row
 * of every kind: `constrained` and `unversioned` are their sums,
 * whether a store sends one row per constraint or one per kind.
 */
export function shapeSpread(
  rows: readonly VersionShare[],
  limit: number,
): VersionSpread {
  const resolved = rows.filter((row) => row.kind === 'resolved');
  const sum = (kind: string) =>
    rows
      .filter((row) => row.kind === kind)
      .reduce((total, row) => total + row.repositoryCount, 0);
  return {
    versions: resolved.slice(0, limit),
    constrained: sum('constraint'),
    unversioned: sum('unversioned'),
  };
}

/** An edge from the named end: `name` is the package at the other end. */
export function shapeEdge(row: Row): PackageEdge {
  return { name: String(row['name'] ?? ''), repositories: num(row['repositories']) };
}

/** A second-hop edge, naming the first-hop package it hangs from. */
export function shapeTreeEdge(row: Row): TreeEdge {
  return {
    parent: String(row['parent'] ?? ''),
    child: String(row['child'] ?? ''),
    repositories: num(row['repositories']),
  };
}

/**
 * Two hops out from one package, bounded at both: the first hop, then
 * a second statement per store for the hop after it.
 *
 * The walk is the same in both stores, so it is written once; how the
 * second hop is asked is not (`secondHop` in each backend). `branch` is
 * per parent, not a global cap: a global `LIMIT 40` would be spent
 * almost entirely on whichever child happens to have the widest edges,
 * and the other parents would draw as leaves that have no children — a
 * claim the data does not make.
 */
export async function walkTree(
  root: string,
  options: { children?: number; branch?: number },
  firstHop: (name: string, limit: number) => Promise<PackageEdge[]>,
  secondHop: (
    root: string,
    children: readonly PackageEdge[],
    branch: number,
  ) => Promise<Row[]>,
): Promise<DependencyTree> {
  const shape = treeShape(options);
  const children = await firstHop(root, shape.children);
  if (children.length === 0) {
    return { root, children: [], grandchildren: [] };
  }
  const rows = await secondHop(root, children, shape.branch);
  return { root, children, grandchildren: rows.map(shapeTreeEdge) };
}
