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
