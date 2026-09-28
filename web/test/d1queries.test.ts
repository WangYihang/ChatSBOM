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
 *
 * What the answers are is not here. `contract.test.ts` asks this store
 * and ClickHouse the same questions about one exported corpus and
 * expects one answer; the tests that fed each store a row by hand and
 * checked what came back became that suite (#41).
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
  language: 'ruby',
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

  it('bounds the offset as the other store must', async () => {
    // One bound for both stores (#31): past it no package has rows, and
    // ClickHouse cannot bind a larger one at all.
    const db = new SpyD1([]);
    await new D1Dataset(db).dependentsOf({ name: 'mail', offset: 1e12 });
    expect(db.last.params.at(-1)).toBe(2 ** 32 - 1);
  });

  it('caps the row count, whatever the caller asks for', async () => {
    const db = new SpyD1([]);
    await new D1Dataset(db).dependentsOf({ name: 'mail', limit: 10_000 });
    expect(db.last.params.at(-2)).toBeLessThanOrEqual(500);
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
    await new D1Dataset(db).topPackages({ directOnly: false, ecosystem: 'Maven' });
    expect(db.last.sql).toContain('ecosystem = ?');
    expect(db.last.params).toContain(0);
    expect(db.last.params).toContain('maven');
  });

  it('filters dependants on the folded language, not the raw one', async () => {
    // The language filter offers the coverage panel's rows: the top
    // twelve, `other` and `none` (#55 D7).
    const db = new SpyD1([]);
    await new D1Dataset(db).dependentsOf({ name: 'mail', language: 'Other' });
    expect(db.last.sql).toContain('r.language_bucket = ?');
    expect(db.last.params).toContain('other');
  });

  it('reads the relationship split per ecosystem', async () => {
    const db = new SpyD1([
      { ecosystem: 'npm', relationship: 'direct', records: 1 },
      { ecosystem: 'npm', relationship: 'transitive', records: 3 },
      { ecosystem: 'maven', relationship: 'direct', records: 2 },
    ]);
    const rows = await new D1Dataset(db).relationshipByEcosystem();
    expect(db.last.sql).toContain("WHERE ecosystem <> ''");
    expect(rows).toEqual([
      { ecosystem: 'npm', direct: 1, transitive: 3, unknown: 0, records: 4 },
      { ecosystem: 'maven', direct: 2, transitive: 0, unknown: 0, records: 2 },
    ]);
  });

  it.each([
    ['totals', (d: D1Dataset) => d.totals(), 'agg_totals'],
    ['languageCoverage', (d: D1Dataset) => d.languageCoverage(), 'agg_language_coverage'],
    ['ecosystemCoverage', (d: D1Dataset) => d.ecosystemCoverage(), 'agg_ecosystem_coverage'],
    ['sourceComparison', (d: D1Dataset) => d.sourceComparison(), 'agg_source_comparison'],
    ['dependencyDistribution', (d: D1Dataset) => d.dependencyDistribution(), 'agg_dependency_buckets'],
    ['licenseShares', (d: D1Dataset) => d.licenseShares(), 'licenses'],
    ['adoptionOverTime', (d: D1Dataset) => d.adoptionOverTime('mail'), 'history'],
  ] as const)('%s reads its own stored table and aggregates nothing', async (_, ask, table) => {
    const db = new SpyD1([]);
    await ask(new D1Dataset(db));
    expect(db.last.sql).toMatch(new RegExp(`\\bFROM ${table}\\b`));
    expect(db.last.sql).not.toContain('artifacts');
    expect(db.last.sql.toUpperCase()).not.toMatch(/GROUP BY|COUNT\(|SUM\(/);
  });

  it('binds what a stored read is asked, in the order it asks', async () => {
    // The placeholders are written once for both stores as
    // `{name:Type}`; here each becomes a `?`, its value in order.
    const db = new SpyD1([]);
    await new D1Dataset(db).adoptionOverTime("mail'; DROP TABLE history;--");
    expect(db.last.sql).toContain('WHERE name = ?');
    expect(db.last.params).toEqual(["mail'; DROP TABLE history;--"]);
    await new D1Dataset(db).licenseShares(10_000);
    expect(db.last.sql).toMatch(/LIMIT \?$/);
    expect(db.last.params).toEqual([500]);
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

    // Stops at GROUP BY as well as ORDER: the row query collapses the
    // per-manifest duplicates the dependency graph reports, and that
    // clause is not a predicate.
    const where = (sql: string) =>
      sql
        .slice(sql.indexOf('WHERE'))
        .replace(/\s+/g, ' ')
        .split(/GROUP BY|ORDER/)[0]!
        .trim();
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
      // Named by the backend, not prefixed by the panel: the stored
      // value is a bare contract number and reads as nothing alone.
      schemaVersion: 'd1 v5',
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

describe('one package, through the joins', () => {
  it('licenceShares keeps unknown rather than dropping it', async () => {
    // "We do not know" is a finding about SBOM quality; hiding it would
    // overstate coverage. 14,947 repositories are in that row.
    const db = new SpyD1([]);
    await new D1Dataset(db).licenseShares();
    expect(db.last.sql.toUpperCase()).not.toContain("!= ''");
    expect(db.last.sql.toUpperCase()).not.toContain('IS NOT NULL');
  });

  it('versionSpread counts versions through the lookups', async () => {
    const db = new SpyD1([]);
    await new D1Dataset(db).versionSpread('mail', 10);
    expect(db.last.sql).toContain('versions');
    expect(db.last.sql).toContain('packages');
    expect(db.last.params).toEqual(['mail']);
  });

  it('versionSpread asks for every kind, then slices', async () => {
    // The top ten *resolved* versions are not the resolved rows among
    // the top ten of everything, so the LIMIT cannot be in the SQL.
    const db = new SpyD1([]);
    await new D1Dataset(db).versionSpread('mail', 10);
    expect(db.last.sql).toContain('k.version_kind');
    expect(db.last.sql).not.toContain('LIMIT');
  });

  it('ecosystemsFor splits a name by the kinds it was seen as', async () => {
    // `kinds.type` holds the name shown, so grouping by it counts a
    // repository once however its collectors spelled the ecosystem.
    const db = new SpyD1([]);
    await new D1Dataset(db).ecosystemsFor('mail');
    expect(db.last.sql).toContain('kinds');
    expect(db.last.sql).toMatch(/GROUP BY k\.type/);
    expect(db.last.sql).toContain('count(DISTINCT a.repository_id)');
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

  it('clamps a tree of no children, or fewer, to the smallest tree', async () => {
    /**
     * #31. `Math.min(children, 30)` let -1 through, and the row query
     * read -1 as "no limit given" and fetched its default of 50 — past
     * the 30 the diagram is bounded at.
     */
    for (const children of [0, -1, -10_000]) {
      const db = new TwoStep(FIRST, []);
      await new D1Dataset(db).dependencyTree('body-parser', { children });
      expect([children, db.calls[0]!.params[1]]).toEqual([children, 1]);
    }
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

  it('excludes the root alone, not every name already drawn', async () => {
    /**
     * `body-parser` pulls in `bytes` directly (3,589 repositories) and
     * again through `raw-body` (3,878). That is not a cycle and not a
     * duplicate — it is the finding — so only the root is excluded, not
     * every name already drawn. (The contract suite draws it.)
     */
    const db = new TwoStep(FIRST, []);
    await new D1Dataset(db).dependencyTree('body-parser');
    const bound = db.calls[1]!.params;
    // The first hop's names, then the root: nothing else is excluded.
    expect(bound.slice(0, -1)).toEqual(['debug', 'qs', 'body-parser']);
    expect(db.calls[1]!.sql.match(/<>/g)).toHaveLength(1);
  });
});

describe('what the contract needs of the statements (#41)', () => {
  /** A clause's comma-separated terms, each without its direction. */
  function terms(sql: string, clause: 'GROUP BY' | 'ORDER BY'): string[] {
    const flat = sql.replace(/\s+/g, ' ');
    const after = flat.slice(flat.lastIndexOf(clause) + clause.length);
    const body = after.split(/ GROUP BY | ORDER BY | LIMIT | HAVING |\)/)[0]!;
    return body.split(',').map((term) => term.replace(/ (ASC|DESC)$/i, '').trim());
  }

  it('orders the dependants by every key it groups them on', async () => {
    // An order that stops at the stars cannot say which of two tied
    // rows comes first, and each page is a separate statement: OFFSET
    // paging can then repeat a row, or skip one.
    const db = new SpyD1([]);
    await new D1Dataset(db).dependentsOf({ name: 'mail' });
    const ordered = terms(db.last.sql, 'ORDER BY');
    for (const key of terms(db.last.sql, 'GROUP BY')) {
      expect(ordered).toContain(key);
    }
  });

  it("dates each row by its own source's observation", async () => {
    // A repository seen by both collectors was shown at the newer date
    // on every row (#24): `repositories.observed_at` is one date.
    const db = new SpyD1([]);
    await new D1Dataset(db).dependentsOf({ name: 'mail' });
    expect(db.last.sql).toMatch(
      /JOIN observations AS o\s+ON o\.repository_id = a\.repository_id\s+AND o\.source = k\.source/,
    );
  });

  it('breaks every tie in the second hop', async () => {
    // `debug` at 3 under two parents came back in either order.
    const db = new SpyD1([{ name: 'debug', repositories: 3 }]);
    await new D1Dataset(db).dependencyTree('express');
    expect(db.last.sql.replace(/\s+/g, ' ')).toMatch(
      /ORDER BY repositories DESC, child, parent$/,
    );
  });

  it('reads a type under its canonical name, as the kinds table stores it', async () => {
    const db = new SpyD1([]);
    await new D1Dataset(db).dependentsOf({
      name: 'laravel/framework',
      type: 'php-composer',
    });
    expect(db.last.params).toContain('composer');
    expect(db.last.params).not.toContain('php-composer');
  });
});
