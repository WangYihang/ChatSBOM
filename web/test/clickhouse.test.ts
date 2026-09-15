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
 */
import { describe, expect, it, vi } from 'vitest';

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
    await dataset.topPackages({ language: HOSTILE });

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

  it('reports the manifest count so the fact is not just hidden',
    async () => {
      const dataset = new ClickHouseDataset(spy([
        {
          owner: 'affaan-m', repo: 'everything-claude-code', stars: 258219,
          version: '', url: 'https://github.com/affaan-m/everything-claude-code',
          language: 'python', relationship: 'direct', type: 'pypi',
          observed_at: '2026-09-13', manifests: 4,
        },
      ]));
      const [row] = await dataset.dependentsOf({ name: 'requests' });
      expect(row!.manifests).toBe(4);
      expect(row!.ecosystem).toBe('pypi');
      expect(row!.language).toBe('python');
    });

  it('lowercases a language filter on both sides', async () => {
    // `repositories.language` is capitalised as GitHub spells it —
    // `PHP`, `JavaScript` — and the dashboard's filter sends lowercase.
    // A rollup keyed on the raw value matched nothing while this was
    // being built, which reads as a language with no packages.
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.dependentsOf({ name: 'mail', language: 'Ruby' });
    expect(db.last.sql).toContain("dictGet('dict_repositories', 'language'");
    expect(db.last.sql).toContain('lower(');
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

  it('merges the two spellings of one ecosystem', async () => {
    /**
     * Syft says `php-composer` and the dependency graph says
     * `composer`; they are one registry. `repositories` is a distinct
     * count per raw type, so the merge takes the larger rather than
     * the sum — adding them would overstate a repository scanned by
     * both collectors.
     */
    const dataset = new ClickHouseDataset(spy([
      { name: 'laravel/framework', type: 'composer', repository_count: 183, name_total: 198 },
      { name: 'laravel/framework', type: 'php-composer', repository_count: 97, name_total: 198 },
    ]));
    const matches = await dataset.searchPackages('laravel/framework');
    expect(matches).toHaveLength(1);
    expect(matches[0]!.ecosystem).toBe('composer');
    expect(matches[0]!.repositoryCount).toBe(183);
  });

  it('returns nothing for an empty term without asking', async () => {
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    expect(await dataset.searchPackages('')).toEqual([]);
    expect(db.calls).toHaveLength(0);
  });
});

describe('the overview reads rollups', () => {
  it('takes the totals from one stored row', async () => {
    const dataset = new ClickHouseDataset(
      spy([{ repositories: 24339, dependencies: 19361638, packages: 225400, classified: 19000000 }]),
    );
    const db = (dataset as unknown as { db: Spy }).db;
    const totals = await dataset.totals();
    expect(totals.repositories).toBe(24339);
    expect(db.last.sql).toContain('FROM mv_totals');
    // Not the fact table: that was 77.8 ms.
    expect(db.last.sql).not.toContain('artifacts');
  });

  it('takes the ranking from the stored ranks', async () => {
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.topPackages({ directOnly: true, language: 'PHP', limit: 30 });
    expect(db.last.sql).toContain('FROM mv_top_packages');
    expect(db.last.params).toMatchObject({
      language: 'php',
      direct: 1,
      limit: 30,
    });
  });

  it('asks for the whole corpus as the empty language', async () => {
    // The same convention the D1 aggregates use, so a reader comparing
    // the two stores is not also comparing two conventions.
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.topPackages({});
    expect(db.last.params['language']).toBe('');
  });

  it('counts languages from repositories, so the empty ones survive', async () => {
    /**
     * 3,736 repositories have no dependency row at all. They are the
     * finding this panel exists to show, and a rollup built from
     * `artifacts` cannot contain them — hence the LEFT JOIN rather
     * than reading the rollup alone.
     */
    const dataset = new ClickHouseDataset(
      spy([{ language: 'coffeescript', repositories: 1, with_sbom: 0 }]),
    );
    const db = (dataset as unknown as { db: Spy }).db;
    const rows = await dataset.languageCoverage();
    expect(db.last.sql).toContain('FROM mv_language_coverage');
    // The rollup behind it reads `repositories`, not `artifacts` — the
    // property that matters is that a language with zero dependency
    // rows still has a row here.
    expect(rows[0]).toEqual({
      language: 'coffeescript',
      repositories: 1,
      withSbom: 0,
    });
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
    expect(db.last.sql).toContain('FROM mv_package_type');
    expect(db.last.sql).toContain('WHERE name = {name:String}');
    expect(db.last.sql).not.toContain('uniqExact');
  });

  it('reads the version spread as a point lookup', async () => {
    const dataset = new ClickHouseDataset(spy([]));
    const db = (dataset as unknown as { db: Spy }).db;
    await dataset.versionSpread('laravel/framework');
    expect(db.last.sql).toContain('FROM mv_package_version');
    expect(db.last.sql).not.toContain('GROUP BY');
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
