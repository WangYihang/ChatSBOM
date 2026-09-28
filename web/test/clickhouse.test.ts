/**
 * The ClickHouse backend.
 *
 * The assertion that matters most is the one about interpolation. This
 * page is public, and a backend that builds SQL by concatenation is one
 * hostile package name away from being a SQL console. ClickHouse binds
 * parameters out of band — `{name:Type}` in the statement,
 * `param_name=` in the query string — and every method here has to use
 * it. That is checked by passing a hostile value through every method
 * and asserting it never appears in a statement.
 *
 * The rest pin the strategy, which differs from D1's by design: point
 * lookups read the fact table because the sort key makes them cheap,
 * and the overview reads rollups the server maintains rather than
 * tables an export writes.
 *
 * What the answers are is not here. `contract.test.ts` asks this store
 * and D1 the same questions about one corpus and expects one answer;
 * the tests that fed this backend a row by hand and checked what came
 * back became that suite (#41).
 */
import { afterEach, describe, expect, it, vi } from 'vitest';

import { ClickHouse, ClickHouseError } from '../src/clickhouse/client';
import { ClickHouseDataset } from '../src/clickhouse/queries';
import type { DatasetQueries } from '../src/backend';

/** Records what was sent, and replies with whatever the test needs. */
class Spy {
  calls: { sql: string; params: Record<string, unknown> }[] = [];
  constructor(private readonly replies: unknown[][] = []) {}

  async rows<T>(
    sql: string,
    params: Record<string, unknown> = {},
  ): Promise<T[]> {
    this.calls.push({ sql, params });
    return (this.replies[this.calls.length - 1] ?? []) as T[];
  }

  async row<T>(
    sql: string,
    params: Record<string, unknown> = {},
  ): Promise<T | undefined> {
    return (await this.rows<T>(sql, params))[0];
  }

  get last() {
    return this.calls[this.calls.length - 1]!;
  }
}

const spy = (...replies: unknown[][]) =>
  new Spy(replies) as unknown as ClickHouse;

describe('the contract', () => {
  it('is satisfied by the ClickHouse implementation', () => {
    // A compile-time check made runtime-visible. `DatasetQueries` was
    // written before this class existed; the compiler is what says the
    // interface was written correctly rather than around D1.
    const backend: DatasetQueries = new ClickHouseDataset(spy());
    expect(backend).toBeInstanceOf(ClickHouseDataset);
  });
});

describe('values are bound, never interpolated', () => {
  const HOSTILE = "x'; DROP TABLE artifacts; SELECT 1 AS '";

  it('keeps a hostile package name out of every statement', async () => {
    const dataset = new ClickHouseDataset(
      spy(
        [{ name: 'a', repositories: 1 }],
        [],
        [],
      ),
    );
    const db = (dataset as unknown as { db: Spy }).db;

    await dataset.dependentsOf({ name: HOSTILE });
    await dataset.countDependents({ name: HOSTILE });
    await dataset.ecosystemsFor(HOSTILE);
    await dataset.versionSpread(HOSTILE);
    await dataset.adoptionOverTime(HOSTILE);
    await dataset.searchPackages(HOSTILE);
    await dataset.dependenciesOf(HOSTILE);
    await dataset.pulledInBy(HOSTILE);
    await dataset.dependencyTree(HOSTILE);
    await dataset.relationshipSplit(HOSTILE);
    await dataset.topPackages({ ecosystem: HOSTILE });

    expect(db.calls.length).toBeGreaterThan(10);
    for (const call of db.calls) {
      expect(call.sql).not.toContain('DROP TABLE');
      expect(call.sql).not.toContain(HOSTILE);
    }
    // And it did travel — as a parameter, so the query is still about
    // the thing the reader asked for.
    const bound = db.calls.flatMap((c) => Object.values(c.params));
    expect(bound.some((v) => String(v).includes('DROP TABLE'))).toBe(true);
  });

  it('binds the limit rather than splicing it into LIMIT', async () => {
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.versionSpread('mail', 7);
    expect(db.last.sql).toContain('LIMIT {limit:UInt32}');
    expect(db.last.params['limit']).toBe(7);
  });

  it('caps a limit a caller asks for', async () => {
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.pulledInBy('ms', 10_000);
    expect(db.last.params['limit']).toBe(500);
  });

  it('passes no array literal it had to escape itself', async () => {
    /**
     * The first version built the second hop's `IN` list by quoting
     * names into a bracketed array literal, which means hand-escaping
     * quotes — and a package really can be called `o'reilly`. The
     * subquery form has nothing to escape.
     */
    const dataset = new ClickHouseDataset(
      spy([{ name: "o'reilly", repositories: 3 }], []),
    );
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.dependencyTree('body-parser');
    for (const call of db.calls) {
      expect(call.sql).not.toContain('Array(String)');
      for (const value of Object.values(call.params)) {
        expect(String(value)).not.toMatch(/^\[/);
      }
    }
  });
});

describe('point lookups read the fact table', () => {
  it('reads repository metadata from a dictionary, not a join', async () => {
    // `repositories` is 28,075 rows — a dimension table — and hashed
    // in memory the join becomes a lookup: 13.6 ms to 4.3 ms for `ms`.
    // This is the page's slowest query and the one the rollups cannot
    // touch, since the package name is arbitrary.
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.dependentsOf({ name: 'mail' });
    expect(db.last.sql).toContain("dictGet('dict_repositories', 'stars'");
    expect(db.last.sql).not.toMatch(/JOIN\s+repositories/);
  });

  it('guards the dictionary lookup with dictHas, as the join did', async () => {
    /**
     * The difference between `dictGet` and a join, and the reason this
     * is asserted rather than assumed: a key the dictionary does not
     * hold yields the type's default, so a dependency row pointing at a
     * repository absent from `repositories` would render as a blank
     * owner with zero stars instead of being dropped. `INNER JOIN` drops
     * it. Measured today: zero rows fail `dictHas`, which is why the
     * guard belongs here rather than in whatever change first creates
     * one.
     */
    const rows = new ClickHouseDataset(spy([]));
    const count = new ClickHouseDataset(spy([]));
    await rows.dependentsOf({ name: 'mail' });
    // Filtered, because the unfiltered count reads a rollup and never
    // touches the dictionary.
    await count.countDependents({ name: 'mail', directOnly: true });
    for (const dataset of [rows, count]) {
      expect((dataset as unknown as { db: Spy }).db.last.sql).toContain(
        "dictHas('dict_repositories', a.repository_id)",
      );
    }
  });

  it('counts the current observations only, as the CLI and the rollups do', async () => {
    /**
     * `artifacts` is append-only, so a repository scanned twice keeps
     * both scans' rows, and "who depends on X" is a question about the
     * newer one. The CLI and the exports join on what each repository
     * records now; these queries used to read every row ever appended,
     * so a repository that moved from mail 2.7.1 to 2.9.1 was listed at
     * both versions here and at one in the CLI.
     *
     * Two observations, two keys (#22): a Syft row belongs to the
     * commit its repository records, a dependency-graph row to the
     * graph document it records, by the date the document states. A
     * repository whose row predates that record (0) keeps the commit
     * rule until it is indexed again. The 0 is compared as seconds:
     * `dictGet(...) != 0` is rewritten into a scan of the whole
     * dictionary on every request.
     *
     * Asserted on all three, because the page shows them together: rows
     * filtered one way beside a count filtered another is the
     * disagreement `dependentFilters` exists to prevent.
     */
    const current =
      "if(a.source = 'github-depgraph'"
      + " AND toUnixTimestamp(dictGet('dict_repositories', 'depgraph_observed_at', a.repository_id)) != 0,"
      + " a.observed_at = dictGet('dict_repositories', 'depgraph_observed_at', a.repository_id),"
      + " a.sbom_commit_sha = dictGet('dict_repositories', 'sbom_commit_sha', a.repository_id))";
    const rows = new ClickHouseDataset(spy([]));
    const count = new ClickHouseDataset(spy([{ total: 1 }]));
    const pages = new ClickHouseDataset(spy([{ total: 1 }]));
    await rows.dependentsOf({ name: 'mail' });
    // Filtered, because the unfiltered count reads `mv_packages`, a
    // rollup that is itself built on the current scan.
    await count.countDependents({ name: 'mail', directOnly: true });
    await pages.countDependentRows({ name: 'mail' });
    for (const dataset of [rows, count, pages]) {
      expect((dataset as unknown as { db: Spy }).db.last.sql).toContain(current);
    }
  });

  it('reads a graph row at the document its repository records', async () => {
    /**
     * Not at the Syft commit, which every graph fetched while the Syft
     * target stood still was stamped with: a package the newer graph
     * dropped stayed listed. The date is compared as stored, a
     * `DateTime` on both sides, so no zone or format can move it.
     */
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.dependentsOf({ name: 'sidekiq' });
    const where = db.last.sql.split('WHERE')[1]!.split('GROUP BY')[0]!;
    expect(where).toContain(
      "a.observed_at = dictGet('dict_repositories', 'depgraph_observed_at', a.repository_id)",
    );
  });

  it('asks the dictionary which scan is current, not a view', async () => {
    /**
     * `current_artifacts` answers the same question for the rollups and
     * the exports, by joining `repositories FINAL`. Here that join would
     * be rebuilt on every page load; the dictionary already holds the
     * table hashed in memory, so the check is a lookup per row.
     */
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.dependentsOf({ name: 'mail' });
    expect(db.last.sql).not.toContain('current_artifacts');
    expect(db.last.sql).not.toMatch(/\bJOIN\b/);
  });

  it('matches the package name directly, with no join through a lookup', async () => {
    // The opposite of the D1 backend, which must join `packages`
    // because it stores integer references. Here `artifacts` is sorted
    // by `name`, so this is a sparse-index read.
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.dependentsOf({ name: 'mail' });
    expect(db.last.sql).toContain('a.name = {name:String}');
    expect(db.last.sql).not.toMatch(/JOIN\s+packages/);
  });

  it('counts distinct repositories exactly, not approximately', async () => {
    /**
     * `uniq` is HyperLogLog — roughly half a percent out. The page
     * prints this number as "198 dependants" beside the rows it
     * describes, so an approximation would be a number nobody can
     * reconcile with what they can see.
     */
    const dataset = new ClickHouseDataset(spy([{ total: 198 }]));
    const db = (dataset as unknown as { db: Spy }).db;
    // The filtered path, which is the one that still counts. The
    // unfiltered path reads a stored count that was itself computed
    // with `uniqExact` at refresh time.
    const total = await dataset.countDependents({
      name: 'laravel/framework',
      directOnly: true,
    });
    expect(total).toBe(198);
    expect(db.last.sql).toContain('uniqExact(a.repository_id)');
    expect(db.last.sql).not.toMatch(/\buniq\(/);
    expect(db.last.sql).not.toContain('uniqCombined');
  });

  it('counts an unfiltered package from the by-name rollup', async () => {
    // The default page load. One stored row against 49,152 read from
    // the fact table.
    const dataset = new ClickHouseDataset(spy([{ total: 198 }]));
    const db = (dataset as unknown as { db: Spy }).db;
    expect(await dataset.countDependents({ name: 'laravel/framework' })).toBe(
      198,
    );
    expect(db.last.sql).toContain('FROM mv_packages');
  });

  it('counts a filtered package from the fact table', async () => {
    /**
     * Deliberately not from a rollup. Covering the filtered cases would
     * mean choosing a rollup per filter combination — and `type` with
     * `language` together has none — so the branch would have to know
     * which combinations it can serve. A branch that picks wrong
     * returns a confident wrong number under the rows a reader can see.
     */
    for (const query of [
      { name: 'mail', type: 'gem' },
      { name: 'mail', language: 'ruby' },
      { name: 'mail', directOnly: true },
      { name: 'mail', type: 'gem', language: 'ruby' },
    ]) {
      const dataset = new ClickHouseDataset(spy([{ total: 1 }]));
      const db = (dataset as unknown as { db: Spy }).db;
      await dataset.countDependents(query);
      expect(db.last.sql).toContain('FROM artifacts');
      expect(db.last.sql).toContain('uniqExact');
    }
  });

  it('asks the rows and the count over identical filters', async () => {
    // A count computed over different filters than the rows beside it
    // is worse than no count: it looks authoritative and disagrees
    // with what the reader can see.
    // Filtered, because the unfiltered count now reads a rollup and
    // has no WHERE clause to compare. The property being pinned is that
    // where they *do* share a path, they share it exactly.
    const query = {
      name: 'mail',
      type: 'gem',
      language: 'Ruby',
      directOnly: true,
    };
    const rows = new ClickHouseDataset(spy([]));
    const count = new ClickHouseDataset(spy([]));
    await rows.dependentsOf(query);
    await count.countDependents(query);

    // Stops at whichever clause follows. The row query gained a
    // GROUP BY — it collapses the dependency graph's per-manifest rows
    // — and splitting only on ORDER BY swept that into the predicates,
    // so the comparison failed on a difference that is not a filter.
    const where = (sql: string) =>
      sql
        .split('WHERE')[1]!
        .split(/GROUP BY|ORDER BY/)[0]!
        .replace(/\s+/g, ' ')
        .trim();
    expect(where((rows as unknown as { db: Spy }).db.last.sql)).toBe(
      where((count as unknown as { db: Spy }).db.last.sql),
    );
  });

  it('collapses the per-manifest rows the dependency graph reports',
    async () => {
      /**
       * GitHub reports each manifest separately, so a repository
       * declaring one package in several of them produced several rows
       * identical in every column the table shows — distinguishable
       * only by an opaque `SPDXRef-pypi-requests-4205b9` that is not
       * displayed. Searching `requests` showed
       * `affaan-m/everything-claude-code` four times, and one
       * repository declares it in 80: eighty of the hundred rows.
       *
       * It also made the table disagree with its own heading, which
       * counts repositories.
       */
      const dataset = new ClickHouseDataset(spy([]));
      const db = (dataset as unknown as { db: Spy }).db;
      await dataset.dependentsOf({ name: 'requests' });
      expect(db.last.sql).toMatch(/GROUP BY/);
      expect(db.last.sql).toMatch(/count\(\) AS manifests/);
      // Never grouped on it: that is the per-manifest discriminator,
      // and grouping by it would collapse nothing.
      expect(db.last.sql).not.toMatch(/GROUP BY[^]*artifact_id/);
    });

  it('filters on the folded language the coverage panel lists', async () => {
    // The filter's values are the coverage panel's rows: the twelve
    // most common GitHub languages, lowercased, `other` and `none`
    // (#55 D7). The dictionary holds each repository's bucket, folded
    // by the rollup's own expression, so the filter matches what the
    // panel offered rather than GitHub's spelling.
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.dependentsOf({ name: 'mail', language: 'Ruby' });
    expect(db.last.sql).toContain(
      "dictGet('dict_repositories', 'language_bucket', a.repository_id)",
    );
    expect(db.last.params['language']).toBe('ruby');
  });

  it('searches by prefix with no pattern to inject a wildcard into', async () => {
    // `startsWith`, not `LIKE 'term%'`: there is no pattern, so a `%`
    // or `_` a reader typed is just a character. The D1 backend has to
    // escape those.
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.searchPackages('100%');
    expect(db.last.sql).toContain('startsWith(name, {term:String})');
    expect(db.last.sql).not.toContain('LIKE');
    expect(db.last.params['term']).toBe('100%');
  });

  it('ranks the search by popularity, not alphabetically', async () => {
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.searchPackages('laravel');
    // Names by how many repositories depend on them, then each name's
    // ecosystems by the same measure — so `mail · gem` (167) sits
    // above `mail · pypi` (1).
    expect(db.last.sql).toMatch(/ORDER BY h\.repositories DESC/);
    expect(db.last.sql).toMatch(/t\.repositories DESC/);
  });

  it('bounds names, not rows, so every ecosystem of a name is offered',
    async () => {
      /**
       * The limit is inside the `hits` CTE. Applying it to the joined
       * result would cut a name's ecosystems off mid-list, and which
       * ones survived would depend on how many ecosystems the names
       * above happened to have.
       */
      const dataset = new ClickHouseDataset(spy([]));
      const db = (dataset as unknown as { db: Spy }).db;
      await dataset.searchPackages('mail');
      const sql = db.last.sql;
      // The invariant is the order: bound first, join second. Slicing
      // the CTE out by hand was a brittle way to say that.
      expect(sql.indexOf('LIMIT {limit:UInt32}')).toBeGreaterThan(-1);
      expect(sql.indexOf('LIMIT {limit:UInt32}'))
        .toBeLessThan(sql.indexOf('LEFT JOIN'));
    });

  it('returns nothing for an empty term without asking', async () => {
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    expect(await dataset.searchPackages('')).toEqual([]);
    expect(db.calls).toHaveLength(0);
  });
});

describe('the overview reads rollups', () => {
  it.each([
    // Not the fact table: that was 77.8 ms.
    ['totals', (d: ClickHouseDataset) => d.totals(), 'mv_totals'],
    // Fourteen stored rows. The rollup behind it reads the corpus
    // rather than `artifacts`, because the repositories with no
    // dependency row are the finding the panel exists to show.
    ['languageCoverage', (d: ClickHouseDataset) => d.languageCoverage(), 'mv_language_coverage'],
    ['ecosystemCoverage', (d: ClickHouseDataset) => d.ecosystemCoverage(), 'mv_ecosystem_coverage'],
    ['sourceComparison', (d: ClickHouseDataset) => d.sourceComparison(), 'mv_ecosystem_totals'],
    ['licenseShares', (d: ClickHouseDataset) => d.licenseShares(), 'mv_licenses'],
    ['edgeAmbiguity', (d: ClickHouseDataset) => d.edgeAmbiguity(), 'mv_edge_ambiguity'],
    ['relationshipByEcosystem', (d: ClickHouseDataset) => d.relationshipByEcosystem(), 'mv_ecosystem_totals'],
  ] as const)('%s reads its rollup, not the fact table', async (_, ask, rollup) => {
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await ask(dataset);
    expect(db.last.sql).toMatch(new RegExp(`\\bFROM ${rollup}\\b`));
    expect(db.last.sql).not.toContain('artifacts');
    expect(db.last.sql).not.toMatch(/uniq|count\(\)/);
  });

  it('compares all three collectors, by the columns the rollup keeps', async () => {
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.sourceComparison();
    expect(db.last.sql).toContain('manifest_records');
    // The empty ecosystem is a record with no type, not an ecosystem.
    expect(db.last.sql).toContain("WHERE ecosystem <> ''");
  });

  it('takes the ranking from the stored ranks', async () => {
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.topPackages({ directOnly: true, ecosystem: 'Composer', limit: 30 });
    expect(db.last.sql).toContain('FROM mv_top_packages');
    expect(db.last.sql).toContain('ecosystem = {ecosystem:String}');
    expect(db.last.params).toMatchObject({
      ecosystem: 'composer',
      direct: 1,
      limit: 30,
    });
  });

  it('asks for the whole corpus as the empty ecosystem', async () => {
    // The same convention the D1 aggregates use, so a reader comparing
    // the two stores is not also comparing two conventions.
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.topPackages({});
    expect(db.last.params['ecosystem']).toBe('');
  });

  it('splits relationships by ecosystem, summing records only', async () => {
    // Records partition by ecosystem, so the unfiltered split is the
    // sum of the ecosystem rows. A repository count would not be.
    const dataset = new ClickHouseDataset(
      spy([{ direct: 1, transitive: 2, unknown: 3 }]),
    );
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.relationshipSplit();
    expect(db.last.sql).toContain('FROM mv_ecosystem_totals');
    expect(db.last.sql).not.toContain('repositories');
    await dataset.relationshipSplit('Maven');
    expect(db.last.params['ecosystem']).toBe('maven');
  });

  it('reads the histogram already bucketed', async () => {
    // Bucketing moved into the rollup after measuring: the boundaries
    // were in the query so changing them needed no refresh, and a
    // refresh costs 0.3 s — not a reason to bucket 24,339 rows on
    // every page load. Six stored rows instead.
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.dependencyDistribution();
    expect(db.last.sql).toContain('FROM mv_dependency_buckets');
    expect(db.last.sql).not.toContain('multiIf');
  });

  it('orders the histogram by position, not by label', async () => {
    // '1000+' sorts between '10-24' and '100-249' as a string.
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.dependencyDistribution();
    expect(db.last.sql).toMatch(/ORDER BY position/);
  });
});

describe('per-package rollups', () => {
  it('reads the ecosystem split as a point lookup', async () => {
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.ecosystemsFor('mail');
    expect(db.last.sql).toContain('FROM mv_package_ecosystem');
    expect(db.last.sql).toContain('WHERE name = {name:String}');
    expect(db.last.sql).not.toContain('uniqExact');
  });

  it('reads the version spread as a point lookup', async () => {
    // Grouped, but only the rollup's own rows for one name: what is
    // set aside is summed to a row per kind. Never the fact table.
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.versionSpread('laravel/framework');
    expect(db.last.sql).toContain('FROM mv_package_version');
    expect(db.last.sql).toContain('WHERE name = {name:String}');
    expect(db.last.sql).not.toContain('artifacts');
    expect(db.last.sql).not.toContain('uniqExact');
  });

  it('reads the adoption series as a point lookup', async () => {
    // The D1 export builds a `history` table for the same reason.
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.adoptionOverTime('express');
    expect(db.last.sql).toContain('FROM mv_package_month');
    expect(db.last.sql).not.toContain('formatDateTime');
  });
});

describe('the edge table', () => {
  it('looks up the reverse direction by the child', async () => {
    // `edges` is ORDER BY (child, parent), so this direction gets the
    // primary-key prefix: 2.6 ms against 4.8 ms forward.
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.pulledInBy('ms');
    expect(db.last.sql).toContain('WHERE child = {name:String}');
    expect(db.last.sql).toContain('parent AS name');
  });

  it('looks up the forward direction by the parent', async () => {
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.dependenciesOf('body-parser');
    expect(db.last.sql).toContain('WHERE parent = {name:String}');
    expect(db.last.sql).toContain('child AS name');
  });

  it('reads the forward direction from its own ordering', async () => {
    // `edges` is ordered child-first, so the forward direction had no
    // prefix and scanned all 614,221 rows. ClickHouse refuses a
    // projection on a SummingMergeTree unless projection upkeep joins
    // every merge, so the second ordering is a rollup.
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.dependenciesOf('body-parser');
    expect(db.last.sql).toContain('FROM mv_edges_forward');
    expect(db.last.sql).not.toContain('GROUP BY');
  });

  it('sums, because the table is a SummingMergeTree', async () => {
    // A pair can sit in more than one unmerged part, so reading the
    // column without summing reports whichever part answered first.
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.pulledInBy('ms');
    expect(db.last.sql).toContain('sum(repositories)');
  });

  it('asks nothing about the second hop when there is no first', async () => {
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    const tree = await dataset.dependencyTree('left-pad');
    expect(db.calls).toHaveLength(1);
    expect(tree).toEqual({ root: 'left-pad', children: [], grandchildren: [] });
  });

  it('excludes the root from the second hop, before ranking', async () => {
    /**
     * The edges run both ways: `bytes -> body-parser` is recorded in
     * one repository as well as `body-parser -> bytes` in 3,589. Drawn
     * as a second hop that reads as the path
     * `body-parser -> bytes -> body-parser`, which the data does not
     * claim. Filtered outside the window, the row is dropped but its
     * rank is spent and a parent shows two children where three were
     * asked for.
     */
    const dataset = new ClickHouseDataset(
      spy([{ name: 'bytes', repositories: 3589 }], []),
    );
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.dependencyTree('body-parser', { branch: 3 });
    const sql = db.last.sql;
    expect(sql).toContain('child != {root:String}');
    expect(sql.indexOf('child != {root:String}')).toBeLessThan(
      sql.indexOf('branch_rank <='),
    );
    expect(db.last.params['root']).toBe('body-parser');
  });

  it('bounds the second hop per parent', async () => {
    const dataset = new ClickHouseDataset(
      spy([{ name: 'bytes', repositories: 3589 }], []),
    );
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.dependencyTree('body-parser', { branch: 3 });
    expect(db.last.sql).toContain('PARTITION BY parent');
    expect(db.last.params['branch']).toBe(3);
  });

  it('clamps the shape a caller asks for', async () => {
    const dataset = new ClickHouseDataset(
      spy([{ name: 'bytes', repositories: 1 }], []),
    );
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.dependencyTree('body-parser', {
      children: 10_000,
      branch: 10_000,
    });
    expect(db.calls[0]!.params['limit']).toBe(30);
    expect(db.last.params['branch']).toBe(12);
  });

  it('clamps a tree of no children, or fewer, to the smallest tree', async () => {
    // #31: -1 got through `Math.min(children, 30)` and was read
    // downstream as "no limit", which fetched 50.
    for (const children of [0, -1, -10_000]) {
      const dataset = new ClickHouseDataset(spy([{ name: 'bytes', repositories: 1 }], []));
      const db = (dataset as unknown as { db: Spy }).db;
      await dataset.dependencyTree('body-parser', { children });
      expect([children, db.calls[0]!.params['limit']]).toEqual([children, 1]);
    }
  });
});

describe('bounds the other store holds as well (#31)', () => {
  it('slices the versions by the limit it bound, so none is dropped', async () => {
    // The statement took the bounded limit and the slice the raw one:
    // with -1, `slice(0, -1)` dropped the last version silently.
    const resolved = ['3.0.0', '2.9.1', '2.8.0'].map((listed, index) => ({
      version_kind: 'resolved',
      listed,
      repository_count: 30 - index,
    }));
    const spread = await new ClickHouseDataset(spy(resolved)).versionSpread('mail', -1);
    expect(spread.versions.map((v) => v.version)).toEqual(['3.0.0', '2.9.1', '2.8.0']);

    // And a limit it can use is the same one in both places.
    const dataset = new ClickHouseDataset(spy(resolved));
    const db = (dataset as unknown as { db: Spy }).db;
    const two = await dataset.versionSpread('mail', 2);
    expect(db.last.params['limit']).toBe(2);
    expect(two.versions.map((v) => v.version)).toEqual(['3.0.0', '2.9.1']);
  });

  it('binds an offset its UInt32 parameter can hold', async () => {
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.dependentsOf({ name: 'mail', offset: 1e12 });
    expect(db.last.params['offset']).toBe(2 ** 32 - 1);
  });

  it('looks up an ecosystem named after a JavaScript built-in as itself', async () => {
    // `MEMBERS['toString']` is a function, and `.map` on it was a
    // TypeError: a 500 for a type no registry has.
    for (const type of ['toString', '__proto__', 'constructor']) {
      const dataset = new ClickHouseDataset(spy([]));
      const db = (dataset as unknown as { db: Spy }).db;
      await dataset.dependentsOf({ name: 'mail', type });
      expect(db.last.sql).toContain('a.type = {type:String}');
      expect(db.last.params['type']).toBe(type);
    }
  });
});

describe('the HTTP client', () => {
  const config = {
    url: 'http://ch.test:8123',
    database: 'chatsbom',
    user: 'guest',
    password: 'guest',
  };

  function stub(reply: unknown, init: { ok?: boolean; status?: number } = {}) {
    const seen: { url: string; body: string; headers: HeadersInit }[] = [];
    vi.stubGlobal('fetch', (url: string, options: RequestInit) => {
      seen.push({
        url,
        body: String(options.body),
        headers: options.headers as HeadersInit,
      });
      return Promise.resolve({
        ok: init.ok ?? true,
        status: init.status ?? 200,
        text: () =>
          Promise.resolve(
            typeof reply === 'string' ? reply : JSON.stringify(reply),
          ),
      } as Response);
    });
    return seen;
  }

  it('sends parameters in the query string, not in the statement', async () => {
    const seen = stub({ data: [{ n: 1 }] });
    await new ClickHouse(config).rows('SELECT {p:String} AS n', { p: 'mail' });
    expect(seen[0]!.url).toContain('param_p=mail');
    expect(seen[0]!.body).toContain('{p:String}');
    expect(seen[0]!.body).not.toContain('mail');
    vi.unstubAllGlobals();
  });

  it('asks for read-only, behind the read-only account', async () => {
    // Defence in depth: a statement that somehow mutated is refused
    // twice. The account's `readonly=1` is the one that matters.
    const seen = stub({ data: [] });
    await new ClickHouse(config).rows('SELECT 1');
    expect(seen[0]!.url).toContain('readonly=1');
    vi.unstubAllGlobals();
  });

  it('sends no setting a read-only account may not change', async () => {
    /**
     * Found by running it: `max_execution_time=25` in the query string
     * came back as `Cannot modify 'max_execution_time' setting in
     * readonly mode` and every query 500'd. The ceilings live on the
     * `guest_readonly` profile — 30 s, 100,000 result rows, 2e9 rows
     * read — where a caller cannot raise them either.
     */
    const seen = stub({ data: [] });
    await new ClickHouse(config).rows('SELECT 1');
    for (const setting of [
      'max_execution_time',
      'max_result_rows',
      'max_rows_to_read',
      'max_memory_usage',
    ]) {
      expect(seen[0]!.url).not.toContain(setting);
    }
    vi.unstubAllGlobals();
  });

  it('chooses the response format itself', async () => {
    // So no query can pick a format that changes the shape `rows`
    // promises to return.
    const seen = stub({ data: [] });
    await new ClickHouse(config).rows('SELECT 1');
    expect(seen[0]!.body).toMatch(/FORMAT JSON$/);
    vi.unstubAllGlobals();
  });

  it('fails on an exception delivered inside a 200', async () => {
    /**
     * ClickHouse streams results, so a limit tripped mid-scan arrives
     * after the headers: HTTP 200 with an `exception` field and a
     * partial `data`. Treating that as success returns a truncated
     * answer as a complete one.
     */
    stub({ data: [{ n: 1 }], exception: 'Code: 160, TOO_SLOW' });
    await expect(new ClickHouse(config).rows('SELECT 1')).rejects.toThrow(
      /TOO_SLOW/,
    );
    vi.unstubAllGlobals();
  });

  it('fails on a non-JSON body rather than returning nothing', async () => {
    stub('<html>proxy error</html>');
    await expect(new ClickHouse(config).rows('SELECT 1')).rejects.toBeInstanceOf(
      ClickHouseError,
    );
    vi.unstubAllGlobals();
  });

  it('reports an unreachable server as such', async () => {
    vi.stubGlobal('fetch', () => Promise.reject(new Error('ECONNREFUSED')));
    await expect(new ClickHouse(config).rows('SELECT 1')).rejects.toThrow(
      /Could not reach ClickHouse/,
    );
    vi.unstubAllGlobals();
  });

  it('gives up before the server does', async () => {
    // The server's 30 s cap bounds cost; this bounds latency. A Worker
    // holding a request open for thirty seconds has already lost the
    // reader.
    vi.stubGlobal('fetch', (_url: string, options: RequestInit) =>
      new Promise((_resolve, reject) => {
        (options.signal as AbortSignal).addEventListener('abort', () =>
          reject(new Error('aborted')),
        );
      }),
    );
    await expect(
      new ClickHouse({ ...config, timeoutMs: 10 }).rows('SELECT 1'),
    ).rejects.toThrow(/exceeded 10ms/);
    vi.unstubAllGlobals();
  });
});

describe('the span, which costs a scan (#41)', () => {
  /**
   * The span the stores agree on is each repository's newest current
   * observation, which no rollup keeps: on 2,000,000 synthetic rows it
   * read every one of them, 92 ms, where the minimum of every row ever
   * appended came out of part metadata in 2 ms. The healthcheck asks
   * for it every 15 s and the watchdog every 30 s.
   */
  const SPAN = [{ observed_from: '2026-02-11', observed_to: '2026-09-14', repositories: 11 }];
  let targets = 0;

  /** A spy that says where it points, as the client does. */
  function pointed(db: Spy): ClickHouse {
    targets += 1;
    return Object.assign(db, { target: `http://ch.test/${targets}` }) as unknown as ClickHouse;
  }

  afterEach(() => vi.restoreAllMocks());

  it('is kept a few minutes per server and database', async () => {
    const now = vi.spyOn(Date, 'now').mockReturnValue(1_000_000);
    const db = new Spy([SPAN, SPAN]);
    const server = pointed(db);
    // A dataset per request, as the endpoint builds them, and one server.
    const first = await new ClickHouseDataset(server).meta();
    expect(await new ClickHouseDataset(server).meta()).toEqual(first);
    expect(db.calls).toHaveLength(1);
    expect(first).toMatchObject({ observedFrom: '2026-02-11', observedTo: '2026-09-14' });

    now.mockReturnValue(1_000_000 + 5 * 60 * 1000 + 1);
    await new ClickHouseDataset(server).meta();
    expect(db.calls).toHaveLength(2);
  });

  it('keeps nothing it could not get', async () => {
    class Down extends Spy {
      failures = 1;
      override async rows<T>(sql: string, params: Record<string, unknown> = {}): Promise<T[]> {
        if (this.failures-- > 0) {
          this.calls.push({ sql, params });
          throw new Error('Could not reach ClickHouse');
        }
        return super.rows<T>(sql, params);
      }
    }
    const db = new Down([[], SPAN]);
    const server = pointed(db);
    await expect(new ClickHouseDataset(server).meta()).rejects.toThrow(/reach/);
    expect((await new ClickHouseDataset(server).meta()).observedTo).toBe('2026-09-14');
    expect(db.calls).toHaveLength(2);
  });

  it('says an empty store has no span, rather than the epoch', async () => {
    const db = new Spy([[{ observed_from: '1970-01-01', observed_to: '1970-01-01', repositories: 0 }]]);
    const meta = await new ClickHouseDataset(pointed(db)).meta();
    expect([meta.observedFrom, meta.observedTo]).toEqual(['', '']);
  });
});

describe('what the contract needs of the statements (#41)', () => {
  /** A clause's comma-separated terms, each without its direction. */
  function terms(sql: string, clause: 'GROUP BY' | 'ORDER BY'): string[] {
    const flat = sql.replace(/\s+/g, ' ');
    const after = flat.slice(flat.lastIndexOf(clause) + clause.length);
    const body = after.split(/ GROUP BY | ORDER BY | LIMIT | HAVING /)[0]!;
    return body.split(',').map((term) => term.replace(/ (ASC|DESC)$/i, '').trim());
  }

  it('orders the dependants by every key it groups them on', async () => {
    // ClickHouse sorts ties in whatever order its threads finish, so
    // an order that stops at the version can repeat or skip a row
    // between one page's statement and the next.
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.dependentsOf({ name: 'mail' });
    const ordered = terms(db.last.sql, 'ORDER BY');
    for (const key of terms(db.last.sql, 'GROUP BY')) {
      expect(ordered).toContain(key);
    }
  });

  it('groups the dependants under canonical ecosystems', async () => {
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.dependentsOf({ name: 'laravel/framework' });
    expect(db.last.sql).toContain("transform(a.type, ['rust-crate'");
  });

  it('expands a type from its canonical name, whichever spelling it came in', async () => {
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.dependentsOf({ name: 'laravel/framework', type: 'php-composer' });
    expect(Object.values(db.last.params)).toEqual(
      expect.arrayContaining(['composer', 'php-composer']),
    );
  });

  it('makes its dates in UTC, by name, as the export does', async () => {
    // Without a zone `formatDateTime` takes the server's: a scan after
    // 16:00 UTC is the next day on a server in UTC+8.
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.dependentsOf({ name: 'mail' });
    await dataset.meta();
    for (const call of db.calls) {
      const made = call.sql.match(/formatDateTime\([^()]*(\([^()]*\)[^()]*)*\)/g) ?? [];
      expect(made.length).toBeGreaterThan(0);
      for (const date of made) {
        expect(date).toMatch(/, 'UTC'\)$/);
      }
    }
  });

  it('spans the current observations, not every row ever appended', async () => {
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.meta();
    expect(db.last.sql).toContain("dictHas('dict_repositories', a.repository_id)");
    expect(db.last.sql).toContain(
      "a.observed_at = dictGet('dict_repositories', 'depgraph_observed_at', a.repository_id)",
    );
    expect(db.last.sql).toMatch(/GROUP BY a\.repository_id/);
  });

  it('reads the ecosystem split under canonical names, counted once', async () => {
    // `mv_package_type` is keyed by each collector's spelling, and the
    // larger of two distinct counts is not the count of their union.
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.ecosystemsFor('laravel/framework');
    expect(db.last.sql).toContain('FROM mv_package_ecosystem');
    await dataset.searchPackages('laravel');
    expect(db.last.sql).toContain('mv_package_ecosystem');
    expect(db.last.sql).not.toContain('mv_package_type');
  });

  it('sums what a version spread sets aside before the limit, not after', async () => {
    // `LIMIT n BY version_kind` kept the n widest constraint strings,
    // so `constrained` summed only those.
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.versionSpread('laravel/framework', 2);
    expect(db.last.sql).toContain("if(version_kind = 'resolved', version, '')");
    expect(db.last.sql).toContain('sum(repositories)');
  });

  it('breaks every tie in the second hop', async () => {
    const dataset = new ClickHouseDataset(
      spy([{ name: 'bytes', repositories: 3589 }], []),
    );
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.dependencyTree('body-parser');
    expect(db.last.sql.replace(/\s+/g, ' ')).toMatch(
      /ORDER BY repositories DESC, child, parent$/,
    );
  });
});
