/**
 * The Worker-side query layer, against the normalised D1 schema.
 *
 * Two things are being pinned here, and they are different in kind.
 *
 * The **SQL shape**: artifact rows carry integer references now, so a
 * package lookup joins through `packages` rather than comparing a string
 * on the fact table. Getting that wrong does not fail loudly — it
 * returns nothing, or everything.
 *
 * The **API boundary**: the browser names a method, never SQL. A page
 * that can send SQL is a page that can send any SQL, and this one is
 * public. The method registry is the allow-list.
 */
import { describe, expect, it } from 'vitest';

import { D1Dataset, type D1Queryable } from '../src/d1/queries';

/** Records the SQL and bindings it was asked to run. */
class SpyD1 implements D1Queryable {
  calls: { sql: string; params: unknown[] }[] = [];
  constructor(private rows: unknown[] = []) {}

  async all<T>(sql: string, params: unknown[] = []): Promise<T[]> {
    this.calls.push({ sql, params });
    return this.rows as T[];
  }

  get last() {
    return this.calls[this.calls.length - 1]!;
  }
}

const ROW = {
  owner: 'rails',
  repo: 'rails',
  stars: 58182,
  version: '2.8.1',
  url: 'https://github.com/rails/rails',
  relationship: 'transitive',
  observed_at: '2026-09-13',
};

describe('dependentsOf', () => {
  it('matches the package through the lookup table, not the fact table', async () => {
    const db = new SpyD1([ROW]);
    await new D1Dataset(db).dependentsOf({ name: 'mail' });
    // `artifacts` has no `name` column any more: it carries package_id.
    expect(db.last.sql).toContain('packages');
    expect(db.last.sql).toMatch(/p\.name\s*=\s*\?/);
    expect(db.last.sql).not.toMatch(/a\.name/);
  });

  it('binds the package name rather than interpolating it', async () => {
    const db = new SpyD1([]);
    await new D1Dataset(db).dependentsOf({ name: "mail'; DROP TABLE x;--" });
    expect(db.last.params[0]).toBe("mail'; DROP TABLE x;--");
    expect(db.last.sql).not.toContain('DROP TABLE');
  });

  it('resolves version and relationship through their lookups', async () => {
    const db = new SpyD1([ROW]);
    await new D1Dataset(db).dependentsOf({ name: 'mail' });
    expect(db.last.sql).toContain('versions');
    expect(db.last.sql).toContain('kinds');
  });

  it('filters by ecosystem on the kinds table', async () => {
    const db = new SpyD1([]);
    await new D1Dataset(db).dependentsOf({ name: 'mail', type: 'gem' });
    expect(db.last.sql).toMatch(/k\.type\s*=\s*\?/);
    expect(db.last.params).toContain('gem');
  });

  it('filters declared-only on the kinds table', async () => {
    const db = new SpyD1([]);
    await new D1Dataset(db).dependentsOf({ name: 'mail', directOnly: true });
    expect(db.last.sql).toMatch(/k\.relationship\s*=\s*\?/);
    expect(db.last.params).toContain('direct');
  });

  it('caps the row count, whatever the caller asks for', async () => {
    const db = new SpyD1([]);
    await new D1Dataset(db).dependentsOf({ name: 'mail', limit: 10_000 });
    expect(db.last.params.at(-1)).toBeLessThanOrEqual(500);
  });

  it('returns typed rows with the observation date', async () => {
    const db = new SpyD1([ROW]);
    const [dep] = await new D1Dataset(db).dependentsOf({ name: 'mail' });
    expect(dep).toEqual({
      owner: 'rails',
      repo: 'rails',
      stars: 58182,
      version: '2.8.1',
      url: 'https://github.com/rails/rails',
      relationship: 'transitive',
      observedAt: '2026-09-13',
    });
  });
});

describe('aggregates read the precomputed tables', () => {
  /**
   * The overview's panels cost 3,122 ms and 1,082 ms when aggregated
   * live, because they read every one of 6,062,896 artifact rows by
   * definition. They are precomputed at export time; a query that
   * aggregates them again would put the cost straight back.
   */
  it('relationshipSplit selects, never aggregates', async () => {
    const db = new SpyD1([{ relationship: 'direct', records: 463150 }]);
    await new D1Dataset(db).relationshipSplit();
    expect(db.last.sql).toContain('agg_relationship_split');
    expect(db.last.sql).not.toContain('artifacts');
    expect(db.last.sql.toUpperCase()).not.toContain('GROUP BY');
  });

  it('relationshipSplit asks for the whole-corpus row by default', async () => {
    const db = new SpyD1([]);
    await new D1Dataset(db).relationshipSplit();
    expect(db.last.params).toContain('');
  });

  it('relationshipSplit asks for one language when given one', async () => {
    const db = new SpyD1([]);
    await new D1Dataset(db).relationshipSplit('Ruby');
    expect(db.last.params).toContain('ruby');
  });

  it('topPackages selects a precomputed rank window', async () => {
    const db = new SpyD1([]);
    await new D1Dataset(db).topPackages({ directOnly: true, limit: 20 });
    expect(db.last.sql).toContain('agg_top_packages');
    expect(db.last.sql).toContain('rank');
    expect(db.last.sql.toUpperCase()).not.toContain('COUNT(');
  });

  it('topPackages passes the filter combination it was precomputed under', async () => {
    const db = new SpyD1([]);
    await new D1Dataset(db).topPackages({ directOnly: false, language: 'Go' });
    expect(db.last.params).toContain(0);
    expect(db.last.params).toContain('go');
  });

  it('totals reads the single precomputed row', async () => {
    const db = new SpyD1([
      { repositories: 28075, dependencies: 6062896, packages: 141938, classified: 6053469 },
    ]);
    const totals = await new D1Dataset(db).totals();
    expect(db.last.sql).toContain('agg_totals');
    expect(totals.repositories).toBe(28075);
  });

  it('languageCoverage and sourceComparison read their tables', async () => {
    const db = new SpyD1([]);
    await new D1Dataset(db).languageCoverage();
    expect(db.last.sql).toContain('agg_language_coverage');
    await new D1Dataset(db).sourceComparison();
    expect(db.last.sql).toContain('agg_source_comparison');
  });

  it('dependencyDistribution keeps the buckets in their declared order', async () => {
    const db = new SpyD1([]);
    await new D1Dataset(db).dependencyDistribution();
    // Labels are not ordinal, so the export stores a position.
    expect(db.last.sql).toContain('position');
  });
});

describe('countDependents', () => {
  it('counts distinct repositories through the lookup', async () => {
    const db = new SpyD1([{ total: 124 }]);
    const total = await new D1Dataset(db).countDependents({ name: 'mail' });
    expect(total).toBe(124);
    expect(db.last.sql).toContain('count(DISTINCT');
    expect(db.last.sql).toContain('packages');
  });

  it('applies the same predicates as the row query', async () => {
    const filters = { name: 'mail', type: 'gem', directOnly: true } as const;
    const rows = new SpyD1([]);
    const count = new SpyD1([{ total: 0 }]);
    await new D1Dataset(rows).dependentsOf(filters);
    await new D1Dataset(count).countDependents(filters);

    const where = (sql: string) =>
      sql.slice(sql.indexOf('WHERE')).replace(/\s+/g, ' ').split('ORDER')[0]!.trim();
    expect(where(count.last.sql)).toBe(where(rows.last.sql));
  });

  it('carries no LIMIT, which is the whole point', async () => {
    const db = new SpyD1([{ total: 1 }]);
    await new D1Dataset(db).countDependents({ name: 'mail' });
    expect(db.last.sql.toUpperCase()).not.toContain('LIMIT');
  });
});

describe('meta', () => {
  /**
   * The Parquet path answers "what am I looking at" with a manifest.
   * D1 has no files, so the checksum list has no analogue — but the
   * build, the contract version and the freshness span do, and those
   * are the ones that explain a surprising number.
   */
  it('reads the one provenance row', async () => {
    const db = new SpyD1([
      {
        generator: 'chatsbom/0.5.4',
        schema_version: '5',
        observed_from: '2026-02-11',
        observed_to: '2026-09-13',
      },
    ]);
    const meta = await new D1Dataset(db).meta();
    expect(db.last.sql).toContain('meta');
    expect(meta).toEqual({
      generator: 'chatsbom/0.5.4',
      schemaVersion: '5',
      observedFrom: '2026-02-11',
      observedTo: '2026-09-13',
    });
  });

  it('reports an unknown span rather than inventing one', async () => {
    const db = new SpyD1([]);
    const meta = await new D1Dataset(db).meta();
    expect(meta.observedFrom).toBe('');
    expect(meta.observedTo).toBe('');
    expect(meta.generator).toBe('');
  });
});

describe('licenseShares and adoptionOverTime', () => {
  it('licenceShares reads the precomputed licences table', async () => {
    const db = new SpyD1([
      { license: 'MIT', repository_count: 2714, package_count: 20137 },
    ]);
    const shares = await new D1Dataset(db).licenseShares(12);
    expect(db.last.sql).toContain('licenses');
    expect(shares[0]).toEqual({
      license: 'MIT',
      repositoryCount: 2714,
      packageCount: 20137,
    });
  });

  it('licenceShares keeps unknown rather than dropping it', async () => {
    // "We do not know" is a finding about SBOM quality; hiding it would
    // overstate coverage. 14,947 repositories are in that row.
    const db = new SpyD1([]);
    await new D1Dataset(db).licenseShares();
    expect(db.last.sql.toUpperCase()).not.toContain("!= ''");
    expect(db.last.sql.toUpperCase()).not.toContain('IS NOT NULL');
  });

  it('adoptionOverTime reads the monthly series for one package', async () => {
    const db = new SpyD1([
      { month: '2026-09', repository_count: 124, direct_count: 21 },
    ]);
    const points = await new D1Dataset(db).adoptionOverTime('mail');
    expect(db.last.sql).toContain('history');
    expect(db.last.params).toContain('mail');
    expect(points[0]).toEqual({
      month: '2026-09',
      repositoryCount: 124,
      directCount: 21,
    });
  });

  it('adoptionOverTime returns months in order', async () => {
    const db = new SpyD1([]);
    await new D1Dataset(db).adoptionOverTime('mail');
    expect(db.last.sql.toUpperCase()).toContain('ORDER BY');
    expect(db.last.sql).toContain('month');
  });

  it('versionSpread ranks versions for one package', async () => {
    const db = new SpyD1([{ version: '2.9.0', repository_count: 42 }]);
    const spread = await new D1Dataset(db).versionSpread('mail', 10);
    expect(db.last.sql).toContain('versions');
    expect(db.last.sql).toContain('packages');
    expect(spread[0]).toEqual({ version: '2.9.0', repositoryCount: 42 });
  });

  it('ecosystemsFor splits a name across ecosystems', async () => {
    const db = new SpyD1([
      { type: 'gem', repository_count: 118, direct_count: 17 },
      { type: 'java-archive', repository_count: 6, direct_count: 6 },
    ]);
    const found = await new D1Dataset(db).ecosystemsFor('mail');
    // `mail` is the case this exists for: a gem and a Maven artifactId.
    expect(found).toHaveLength(2);
    expect(db.last.sql).toContain('kinds');
  });
});

describe('searchPackages', () => {
  it('searches the lookup table, not the fact table', async () => {
    const db = new SpyD1([{ name: 'mail', repository_count: 124 }]);
    await new D1Dataset(db).searchPackages('mai');
    // 141,938 rows in `packages` against 6,062,896 in `artifacts`.
    expect(db.last.sql).toContain('packages');
    expect(db.last.sql).not.toContain('JOIN artifacts');
  });

  it('anchors the pattern at the start so the index is usable', async () => {
    const db = new SpyD1([]);
    await new D1Dataset(db).searchPackages('mai');
    // A leading wildcard cannot use an index: SQLite would scan all
    // 141,938 names. Prefix matching keeps it a range lookup.
    expect(db.last.params[0]).toBe('mai%');
  });

  it('escapes LIKE metacharacters in the term', async () => {
    const db = new SpyD1([]);
    await new D1Dataset(db).searchPackages('100%_real');
    // Otherwise '%' and '_' in a package name become wildcards and the
    // search silently matches far more than the reader typed.
    expect(String(db.last.params[0])).toContain('100\\%\\_real');
    expect(db.last.sql.toUpperCase()).toContain('ESCAPE');
  });

  it('caps the number of suggestions', async () => {
    const db = new SpyD1([]);
    await new D1Dataset(db).searchPackages('a', 10_000);
    expect(db.last.params.at(-1)).toBeLessThanOrEqual(500);
  });

  it('returns nothing for an empty term rather than everything', async () => {
    const db = new SpyD1([{ name: 'x', repository_count: 1 }]);
    expect(await new D1Dataset(db).searchPackages('')).toEqual([]);
    expect(db.calls).toHaveLength(0);
  });
});

describe('the edge table, in both directions', () => {
  /**
   * The two directions are not the same query with the operands
   * swapped, and confusing them is silent: `pulledInBy('ms')` written
   * against `parent_id` returns what `ms` pulls in, which is a short
   * plausible list rather than an error.
   */
  it('looks up forward edges by the parent', async () => {
    const db = new SpyD1([{ name: 'ms', repositories: 7999 }]);
    const edges = await new D1Dataset(db).dependenciesOf('debug');
    expect(db.last.sql).toMatch(/WHERE\s+p\.name\s*=\s*\?/);
    expect(db.last.sql).toMatch(/p\.id\s*=\s*e\.parent_id/);
    expect(db.last.params[0]).toBe('debug');
    expect(edges).toEqual([{ name: 'ms', repositories: 7999 }]);
  });

  it('looks up reverse edges by the child', async () => {
    const db = new SpyD1([{ name: 'debug', repositories: 7999 }]);
    const edges = await new D1Dataset(db).pulledInBy('ms');
    // The bound name must be compared against the *child* end, which is
    // the column `idx_agg_edges_child_id` covers.
    expect(db.last.sql).toMatch(/WHERE\s+c\.name\s*=\s*\?/);
    expect(db.last.sql).toMatch(/c\.id\s*=\s*e\.child_id/);
    expect(db.last.params[0]).toBe('ms');
    expect(edges).toEqual([{ name: 'debug', repositories: 7999 }]);
  });

  it('names the other end of the edge, not the end that was asked for', async () => {
    // Forward returns the child's name, reverse the parent's. A query
    // that selected the bound end would return the search term back,
    // once per edge.
    const forward = new SpyD1([]);
    await new D1Dataset(forward).dependenciesOf('debug');
    expect(forward.last.sql).toMatch(/c\.name\s+AS\s+name/);

    const reverse = new SpyD1([]);
    await new D1Dataset(reverse).pulledInBy('ms');
    expect(reverse.last.sql).toMatch(/p\.name\s+AS\s+name/);
  });

  it('orders by the repository count, widest first', async () => {
    const db = new SpyD1([]);
    await new D1Dataset(db).pulledInBy('ms');
    expect(db.last.sql).toMatch(/ORDER BY\s+e\.repositories DESC/);
  });

  it('binds the name in both directions rather than interpolating it', async () => {
    const hostile = "ms'; DROP TABLE agg_edges;--";
    for (const run of [
      (d: D1Dataset) => d.dependenciesOf(hostile),
      (d: D1Dataset) => d.pulledInBy(hostile),
    ]) {
      const db = new SpyD1([]);
      await run(new D1Dataset(db));
      expect(db.last.params[0]).toBe(hostile);
      expect(db.last.sql).not.toContain('DROP TABLE');
    }
  });

  it('caps the rows returned', async () => {
    const db = new SpyD1([]);
    await new D1Dataset(db).pulledInBy('ms', 10_000);
    expect(db.last.sql).toMatch(/LIMIT \?/);
    expect(db.last.params[1]).toBe(500);
  });
});

describe('dependencyTree', () => {
  const FIRST = [
    { name: 'debug', repositories: 3580 },
    { name: 'qs', repositories: 3574 },
  ];

  /** First call answers the forward edges, second the second hop. */
  class TwoStep implements D1Queryable {
    calls: { sql: string; params: unknown[] }[] = [];
    constructor(
      private readonly first: unknown[],
      private readonly second: unknown[],
    ) {}
    async all<T>(sql: string, params: unknown[] = []): Promise<T[]> {
      this.calls.push({ sql, params });
      return (this.calls.length === 1 ? this.first : this.second) as T[];
    }
  }

  it('asks the first hop before it can ask the second', async () => {
    const db = new TwoStep(FIRST, []);
    await new D1Dataset(db).dependencyTree('body-parser');
    expect(db.calls).toHaveLength(2);
    // The second statement is parameterised by the first hop's names,
    // so it cannot be issued speculatively.
    expect(db.calls[1]!.params.slice(0, 2)).toEqual(['debug', 'qs']);
  });

  it('asks nothing further when the package pulls in nothing', async () => {
    const db = new TwoStep([], []);
    const tree = await new D1Dataset(db).dependencyTree('left-pad');
    expect(db.calls).toHaveLength(1);
    expect(tree).toEqual({ root: 'left-pad', children: [], grandchildren: [] });
  });

  it('bounds the second hop per parent, not globally', async () => {
    /**
     * A global cap would be spent almost entirely on whichever child has
     * the widest edges — `debug → ms` is 7,999 — and every other parent
     * would draw as a leaf with no children, which the data does not
     * claim.
     */
    const db = new TwoStep(FIRST, []);
    await new D1Dataset(db).dependencyTree('body-parser', { branch: 3 });
    expect(db.calls[1]!.sql).toMatch(/PARTITION BY\s+e\.parent_id/);
    expect(db.calls[1]!.params.at(-1)).toBe(3);
  });

  it('filters the window function from outside its own SELECT', async () => {
    // SQLite will not accept ROW_NUMBER() in the WHERE clause of the
    // SELECT that computes it; the subquery is required, not stylistic.
    const db = new TwoStep(FIRST, []);
    await new D1Dataset(db).dependencyTree('body-parser');
    const sql = db.calls[1]!.sql;
    const window = sql.indexOf('ROW_NUMBER()');
    const filter = sql.indexOf('branch_rank <=');
    expect(window).toBeGreaterThan(-1);
    expect(filter).toBeGreaterThan(window);
  });

  it('emits one placeholder per first-hop package', async () => {
    const db = new TwoStep(FIRST, []);
    await new D1Dataset(db).dependencyTree('body-parser');
    const inList = /pp\.name IN \(([^)]*)\)/.exec(db.calls[1]!.sql);
    expect(inList?.[1]!.split(',')).toHaveLength(FIRST.length);
    // Names are bound, never spliced: a package may be called `o'reilly`.
    expect(db.calls[1]!.sql).not.toContain('debug');
  });

  it('clamps the shape a caller asks for', async () => {
    const db = new TwoStep(FIRST, []);
    await new D1Dataset(db).dependencyTree('body-parser', {
      children: 10_000,
      branch: 10_000,
    });
    // The first statement's LIMIT, and the second's per-parent rank.
    expect(db.calls[0]!.params[1]).toBe(30);
    expect(db.calls[1]!.params.at(-1)).toBe(12);
  });

  it('does not draw the root again as its own grandchild', async () => {
    /**
     * Found by querying the real table, not by reading the code. The
     * edges genuinely run both ways: `bytes -> body-parser` is recorded
     * in one repository as well as `body-parser -> bytes` in 3,589. So
     * the unfiltered second hop puts `body-parser` back in the third
     * column, where it reads as the path
     * `body-parser -> bytes -> body-parser` — a claim the data does not
     * make.
     */
    const db = new TwoStep(FIRST, []);
    await new D1Dataset(db).dependencyTree('body-parser');
    expect(db.calls[1]!.sql).toMatch(/cc\.name\s*<>\s*\?/);
    expect(db.calls[1]!.params).toContain('body-parser');
  });

  it('excludes the root before ranking, not after', async () => {
    // Filtered in the outer WHERE the row is dropped but its rank is
    // spent, so a parent whose widest edge points back at the root
    // would show two children where three were asked for.
    const db = new TwoStep(FIRST, []);
    await new D1Dataset(db).dependencyTree('body-parser', { branch: 3 });
    const sql = db.calls[1]!.sql;
    const exclusion = sql.indexOf('cc.name <> ?');
    const rankFilter = sql.indexOf('branch_rank <=');
    expect(exclusion).toBeGreaterThan(-1);
    expect(exclusion).toBeLessThan(rankFilter);
  });

  it('keeps a first-hop package that is also reached the long way', async () => {
    /**
     * `body-parser` pulls in `bytes` directly (3,589 repositories) and
     * again through `raw-body` (3,878). That is not a cycle and not a
     * duplicate — it is the finding — so only the root is excluded, not
     * every name already drawn.
     */
    const db = new TwoStep(
      [
        { name: 'bytes', repositories: 3589 },
        { name: 'raw-body', repositories: 3589 },
      ],
      [{ parent: 'raw-body', child: 'bytes', repositories: 3878 }],
    );
    const tree = await new D1Dataset(db).dependencyTree('body-parser');
    expect(tree.grandchildren).toEqual([
      { parent: 'raw-body', child: 'bytes', repositories: 3878 },
    ]);
  });

  it('keeps a second-hop package attached to the parent it came from', async () => {
    // `depd` is genuinely pulled in by both `http-errors` and
    // `body-parser`. A nested shape would have to duplicate it or pick
    // one parent; naming the parent on the row does neither.
    const db = new TwoStep(FIRST, [
      { parent: 'debug', child: 'ms', repositories: 7999 },
      { parent: 'qs', child: 'side-channel', repositories: 3370 },
    ]);
    const tree = await new D1Dataset(db).dependencyTree('body-parser');
    expect(tree.root).toBe('body-parser');
    expect(tree.children).toEqual(FIRST);
    expect(tree.grandchildren).toEqual([
      { parent: 'debug', child: 'ms', repositories: 7999 },
      { parent: 'qs', child: 'side-channel', repositories: 3370 },
    ]);
  });
});
