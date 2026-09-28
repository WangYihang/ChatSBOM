/**
 * What the model is told about its tools, and what it is handed back.
 *
 * Each part of this has put a wrong number in an answer:
 *
 * - `dependents_of` returned `count: rows.length`. The rows stop at a
 *   limit — 50 unless the model asks for more — so asked about `react`
 *   the model reported 50 dependants, where the store counts 5,095.
 * - The descriptions described other tools: a "substring" search that
 *   matches prefixes, so a name the model began wrongly "did not
 *   exist", and defaults of 50 where the stores return 20 and 10. They
 *   are checked here against what both stores do, rather than against a
 *   copy of the numbers that could go stale with them.
 * - Results had no size. Two `dependents_of` calls at limit 500 were
 *   more text than the Worker accepts in a whole conversation.
 */
import { describe, expect, it } from 'vitest';

import type { DatasetQueries } from '../src/backend';
import type { ClickHouse, Param } from '../src/clickhouse/client';
import { ClickHouseDataset } from '../src/clickhouse/queries';
import type { DatasetClient } from '../src/d1/client';
import { D1Dataset, type D1Queryable } from '../src/d1/queries';
import type { Dependent, DependentQuery } from '../src/dataset/types';
import {
  executeTool,
  MAX_TOOL_RESULT_CHARS,
  RESULT_CHARS,
  RESULT_ROWS,
  TOOL_DEFINITIONS,
  type ToolName,
} from '../src/tools';

/**
 * A dependant as the stores return one.
 *
 * About as wide as a real row by default, a little over 200 characters
 * of JSON. GitHub allows a repository name of 100 characters, and a
 * long `repo` is how a test asks for the widest rows there can be.
 */
function dependant(index: number, repo = `project-${index}`): Dependent {
  return {
    owner: `owner-${index}`,
    repo,
    stars: 250_000 - index,
    version: '18.3.1',
    url: `https://github.com/owner-${index}/${repo}`,
    relationship: index % 3 === 0 ? 'direct' : 'transitive',
    observedAt: '2026-09-13',
    ecosystem: 'npm',
    language: 'typescript',
    manifests: 1,
  };
}

/**
 * A dataset that answers as the store would, and records what it was
 * asked.
 *
 * `dependentsOf` honours the limit, 50 when none is given, as both
 * stores do. `searchPackages` and `topPackages` return what they were
 * handed whatever the limit: on ClickHouse a search is a row per
 * ecosystem, so its limit bounds names rather than rows.
 */
function fakeDataset(
  data: {
    dependents?: Dependent[];
    total?: number;
    directTotal?: number;
    matches?: unknown[];
    top?: unknown[];
  } = {},
) {
  const asked = {
    dependentsOf: [] as DependentQuery[],
    countDependents: [] as DependentQuery[],
    searchPackages: [] as unknown[][],
    topPackages: [] as unknown[],
    versionSpread: [] as unknown[][],
  };
  const dataset = {
    dependentsOf: async (query: DependentQuery) => {
      asked.dependentsOf.push(query);
      return (data.dependents ?? []).slice(0, query.limit ?? 50);
    },
    countDependents: async (query: DependentQuery) => {
      asked.countDependents.push(query);
      return query.directOnly ? (data.directTotal ?? 0) : (data.total ?? 0);
    },
    searchPackages: async (term: string, limit?: number) => {
      asked.searchPackages.push([term, limit]);
      return data.matches ?? [];
    },
    topPackages: async (options: unknown) => {
      asked.topPackages.push(options);
      return data.top ?? [];
    },
    versionSpread: async (name: string, limit?: number) => {
      asked.versionSpread.push([name, limit]);
      return { versions: [], constrained: 0, unversioned: 0 };
    },
  } as unknown as DatasetClient;
  return { dataset, asked };
}

/** What `rows` is for a tool, when a test needs its length. */
function rowsOf(result: unknown): unknown[] {
  return (result as { rows: unknown[] }).rows;
}

describe('dependents_of', () => {
  const REACT = Array.from({ length: 200 }, (_, index) => dependant(index));

  it('reports how many repositories depend on it, not how many rows it shows', async () => {
    const { dataset } = fakeDataset({
      dependents: REACT,
      total: 5_095,
      directTotal: 1_207,
    });

    const result = await executeTool(dataset, 'dependents_of', { name: 'react' });

    expect(result).toMatchObject({
      total: 5_095,
      direct_total: 1_207,
      rows_shown: 50,
    });
    expect(rowsOf(result)).toHaveLength(50);
    expect(result).not.toHaveProperty('truncated');
  });

  it('counts with the filters the rows were chosen by', async () => {
    // A count over other filters than the rows beside it is worse than
    // none: it looks authoritative and disagrees with what is shown.
    const { dataset, asked } = fakeDataset({ dependents: REACT, total: 118 });

    await executeTool(dataset, 'dependents_of', {
      name: 'mail',
      type: 'gem',
      language: 'ruby',
      limit: 20,
    });

    const filters = { name: 'mail', type: 'gem', language: 'ruby' };
    expect(asked.dependentsOf).toEqual([
      { ...filters, directOnly: false, limit: 20 },
    ]);
    expect(asked.countDependents).toEqual([
      { ...filters, directOnly: false },
      { ...filters, directOnly: true },
    ]);
  });

  it('with direct_only, counts only the declared, and once', async () => {
    const { dataset, asked } = fakeDataset({
      dependents: REACT,
      total: 118,
      directTotal: 17,
    });

    const result = await executeTool(dataset, 'dependents_of', {
      name: 'mail',
      direct_only: true,
    });

    expect(result).toMatchObject({ total: 17 });
    // `total` already is the declared count; a second would repeat it.
    expect(result).not.toHaveProperty('direct_total');
    expect(asked.countDependents).toEqual([{ name: 'mail', directOnly: true }]);
  });
});

describe('every result is bounded', () => {
  it('cuts a result too long for the cap, says so, and stays under what the Worker accepts', async () => {
    // The widest rows there can be. At the old ceiling of 500 rows this
    // was over 200,000 characters: one result, larger than the Worker
    // accepts for a whole conversation.
    const wide = Array.from({ length: 500 }, (_, index) =>
      dependant(index, `a-repository-name-as-long-as-github-allows-${index}`.padEnd(100, '-')),
    );
    const { dataset } = fakeDataset({
      dependents: wide,
      total: 5_095,
      directTotal: 1_207,
    });

    const result = await executeTool(dataset, 'dependents_of', {
      name: 'react',
      limit: 500,
    });
    const sent = JSON.stringify(result);

    expect(sent.length).toBeLessThanOrEqual(RESULT_CHARS);
    expect(sent.length).toBeLessThan(MAX_TOOL_RESULT_CHARS);
    expect(result).toMatchObject({ total: 5_095, truncated: true });
    const { rows_shown: shown, rows_dropped: dropped } = result as {
      rows_shown: number;
      rows_dropped: number;
    };
    expect(shown).toBeGreaterThan(0);
    expect(dropped).toBeGreaterThan(0);
    // What was fetched is what was shown plus what was dropped, and the
    // rows kept are the head of the ranking: the most starred.
    expect(shown + dropped).toBe(RESULT_ROWS);
    expect(rowsOf(result)).toEqual(wide.slice(0, shown));
  });

  it('cuts rows past the row cap, whichever tool returned them', async () => {
    // 75 names in two ecosystems each: 150 rows for a limit of 75.
    const matches = Array.from({ length: 150 }, (_, index) => ({
      name: `eslint-plugin-${Math.floor(index / 2)}`,
      ecosystem: index % 2 === 0 ? 'npm' : 'pypi',
      repositoryCount: 150 - index,
      nameTotal: 150 - index,
    }));
    const { dataset } = fakeDataset({ matches });

    const result = await executeTool(dataset, 'search_packages', {
      fragment: 'eslint-plugin-',
      limit: 75,
    });

    expect(result).toMatchObject({
      rows_shown: RESULT_ROWS,
      truncated: true,
      rows_dropped: 150 - RESULT_ROWS,
    });
    expect(rowsOf(result)).toEqual(matches.slice(0, RESULT_ROWS));
  });

  it('leaves a result that fits as it came', async () => {
    const top = Array.from({ length: 30 }, (_, index) => ({
      name: `package-${index}`,
      repositoryCount: 1_000 - index,
      directCount: 100 - index,
    }));
    const { dataset } = fakeDataset({ top });

    const result = await executeTool(dataset, 'top_packages', { direct_only: true });

    expect(result).toEqual({ rows_shown: 30, rows: top });
  });

  it('ranks within an ecosystem, never a language (#55 §4.13)', async () => {
    const { dataset, asked } = fakeDataset();

    await executeTool(dataset, 'top_packages', { ecosystem: 'maven' });

    expect(asked.topPackages[0]).toMatchObject({ ecosystem: 'maven' });
    expect(asked.topPackages[0]).not.toHaveProperty('language');
  });

  it('asks the store for no more rows than a result may carry', async () => {
    const { dataset, asked } = fakeDataset();

    await executeTool(dataset, 'dependents_of', { name: 'react', limit: 500 });
    await executeTool(dataset, 'search_packages', { fragment: 'react', limit: 500 });
    await executeTool(dataset, 'top_packages', { limit: 500 });
    await executeTool(dataset, 'version_spread', { name: 'react', limit: 500 });

    expect(asked.dependentsOf[0]).toMatchObject({ limit: RESULT_ROWS });
    expect(asked.searchPackages[0]).toEqual(['react', RESULT_ROWS]);
    expect(asked.topPackages[0]).toMatchObject({ limit: RESULT_ROWS });
    expect(asked.versionSpread[0]).toEqual(['react', RESULT_ROWS]);
  });

  it('lists a version spread as rows, beside what it set aside', async () => {
    // Rows like every other tool's, so the same cap applies to them.
    const versions = [
      { version: '2.8.1', repositoryCount: 40, kind: 'resolved' },
      { version: '2.7.0', repositoryCount: 12, kind: 'resolved' },
    ];
    const dataset = {
      versionSpread: async () => ({ versions, constrained: 9, unversioned: 3 }),
    } as unknown as DatasetClient;

    const result = await executeTool(dataset, 'version_spread', { name: 'mail' });

    expect(result).toEqual({
      constrained: 9,
      unversioned: 3,
      rows_shown: 2,
      rows: versions,
    });
  });
});

/* ---- the descriptions, against what the stores do ---- */

/** Rows carrying every column any statement below reads. */
const PLENTY = Array.from({ length: 600 }, (_, index) => ({
  owner: `owner-${index}`,
  repo: `project-${index}`,
  stars: 1_000 - index,
  url: `https://github.com/owner-${index}/project-${index}`,
  language: 'ruby',
  relationship: 'direct',
  observed_on: '2026-09-13',
  manifests: 1,
  type: 'gem',
  ecosystem: 'gem',
  name: `package-${index}`,
  version: `1.0.${index}`,
  listed: `1.0.${index}`,
  version_kind: 'resolved',
  repository_count: 1_000 - index,
  repositoryCount: 1_000 - index,
  direct_count: 1,
  directCount: 1,
  name_total: 1_000 - index,
}));

/**
 * D1 holding more of everything than any limit, applying a statement's
 * limit as SQLite would, and recording what it ran.
 */
class PlentifulD1 implements D1Queryable {
  calls: { sql: string; params: unknown[] }[] = [];

  async all<T>(sql: string, params: unknown[] = []): Promise<T[]> {
    this.calls.push({ sql, params });
    // The placeholders before the first `LIMIT ?` or `rank <= ?` say
    // which bound value it is.
    const at = sql.search(/LIMIT \?|rank <= \?/);
    if (at < 0) return PLENTY as T[];
    const index = sql.slice(0, at).split('?').length - 1;
    return PLENTY.slice(0, Number(params[index])) as T[];
  }
}

/** The same for ClickHouse, whose parameters are named. */
class PlentifulClickHouse {
  calls: { sql: string; params: Record<string, Param> }[] = [];

  async rows<T>(sql: string, params: Record<string, Param> = {}): Promise<T[]> {
    this.calls.push({ sql, params });
    const limit = params['limit'];
    return (limit === undefined ? PLENTY : PLENTY.slice(0, Number(limit))) as T[];
  }

  async row<T>(sql: string, params: Record<string, Param> = {}): Promise<T | undefined> {
    return (await this.rows<T>(sql, params))[0];
  }
}

const STORES: Record<string, () => DatasetQueries> = {
  D1: () => new D1Dataset(new PlentifulD1()),
  ClickHouse: () =>
    new ClickHouseDataset(new PlentifulClickHouse() as unknown as ClickHouse),
};

/**
 * How many rows a store gives each tool that names no limit — its own
 * default, since the tools pass none (asserted below).
 */
const STORE_DEFAULT: Record<string, (store: DatasetQueries) => Promise<number>> = {
  dependents_of: async (store) =>
    (await store.dependentsOf({ name: 'react' })).length,
  search_packages: async (store) =>
    (await store.searchPackages('package-')).length,
  top_packages: async (store) => (await store.topPackages({})).length,
  version_spread: async (store) =>
    (await store.versionSpread('react')).versions.length,
};

interface Definition {
  name: string;
  description: string;
  input_schema: { properties: Record<string, { description?: string }> };
}

function definition(name: ToolName): Definition {
  return (TOOL_DEFINITIONS as unknown as Definition[]).find(
    (tool) => tool.name === name,
  )!;
}

function describedLimit(name: ToolName): string {
  return definition(name).input_schema.properties['limit']?.description ?? '';
}

describe('the descriptions', () => {
  const LIMITED = Object.keys(STORE_DEFAULT) as ToolName[];

  it('cover every tool that takes a limit', () => {
    const takingLimit = (TOOL_DEFINITIONS as unknown as Definition[])
      .filter((tool) => 'limit' in tool.input_schema.properties)
      .map((tool) => tool.name);
    expect(takingLimit.sort()).toEqual([...LIMITED].sort());
  });

  it.each(LIMITED)('%s states the default both stores apply', async (tool) => {
    const stated = /default (\d+)/.exec(describedLimit(tool))?.[1];
    expect(stated, describedLimit(tool)).toBeDefined();
    for (const [store, open] of Object.entries(STORES)) {
      expect(String(await STORE_DEFAULT[tool]!(open())), store).toBe(stated);
    }
  });

  it.each(LIMITED)('%s states the cap its rows are held to', (tool) => {
    expect(describedLimit(tool)).toContain(`at most ${RESULT_ROWS}`);
  });

  it('hold only because the tools pass no limit of their own', async () => {
    const { dataset, asked } = fakeDataset();

    await executeTool(dataset, 'dependents_of', { name: 'react' });
    await executeTool(dataset, 'search_packages', { fragment: 'react' });
    await executeTool(dataset, 'top_packages', {});
    await executeTool(dataset, 'version_spread', { name: 'react' });

    expect(asked.dependentsOf[0]).not.toHaveProperty('limit');
    expect(asked.searchPackages[0]).toEqual(['react', undefined]);
    expect(asked.topPackages[0]).not.toHaveProperty('limit');
    expect(asked.versionSpread[0]).toEqual(['react', undefined]);
  });

  it('say search_packages matches a prefix, which is what both stores do', async () => {
    const d1 = new PlentifulD1();
    await new D1Dataset(d1).searchPackages('mail');
    // Anchored at the start: no leading wildcard.
    expect(d1.calls[0]!.params[0]).toBe('mail%');

    const clickhouse = new PlentifulClickHouse();
    await new ClickHouseDataset(
      clickhouse as unknown as ClickHouse,
    ).searchPackages('mail');
    expect(clickhouse.calls[0]!.sql).toContain('startsWith(name, {term:String})');

    const search = definition('search_packages');
    expect(search.description).toMatch(/prefix/);
    expect(search.input_schema.properties['fragment']!.description).toMatch(
      /prefix/,
    );
    expect(JSON.stringify(search)).not.toMatch(/substring to match/i);
  });

  it('say which dependents_of number is the count, and that the rows are a sample', () => {
    const { description } = definition('dependents_of');
    expect(description).toMatch(/`total`[^.]*the real count/);
    expect(description).toMatch(/`direct_total`/);
    expect(description).toMatch(/`rows` is a sample/);
  });
});
