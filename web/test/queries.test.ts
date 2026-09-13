import { describe, expect, it } from 'vitest';

import { Dataset, isRelationship, type Queryable } from '../src/queries';

/** Records the SQL and params it was asked to run. */
class SpyDb implements Queryable {
  calls: { sql: string; params: unknown[] }[] = [];
  constructor(private rows: unknown[] = []) {}

  async query<T>(sql: string, params: unknown[] = []): Promise<T[]> {
    this.calls.push({ sql, params });
    return this.rows as T[];
  }

  get last() {
    const call = this.calls.at(-1);
    if (!call) throw new Error('no query was run');
    return call;
  }
}

describe('relationship narrowing', () => {
  it('accepts the generated union members', () => {
    expect(isRelationship('direct')).toBe(true);
    expect(isRelationship('transitive')).toBe(true);
    expect(isRelationship('unknown')).toBe(true);
  });

  it('rejects anything else', () => {
    expect(isRelationship('DIRECT')).toBe(false);
    expect(isRelationship('dev')).toBe(false);
  });
});

describe('dependentsOf', () => {
  it('binds the package name rather than interpolating it', async () => {
    const db = new SpyDb();
    await new Dataset(db).dependentsOf({ name: "mail'; DROP TABLE x;--" });

    expect(db.last.sql).not.toContain('DROP TABLE');
    expect(db.last.params[0]).toBe("mail'; DROP TABLE x;--");
  });

  it('filters to direct dependants on request', async () => {
    const db = new SpyDb();
    await new Dataset(db).dependentsOf({ name: 'mail', directOnly: true });

    expect(db.last.sql).toContain('a.relationship = ?');
    expect(db.last.params).toContain('direct');
  });

  it('does not filter by relationship by default', async () => {
    const db = new SpyDb();
    await new Dataset(db).dependentsOf({ name: 'mail' });
    expect(db.last.params).not.toContain('direct');
  });

  it('lowercases the language filter', async () => {
    const db = new SpyDb();
    await new Dataset(db).dependentsOf({ name: 'mail', language: 'Ruby' });
    expect(db.last.params).toContain('ruby');
  });

  it('orders by stars descending', async () => {
    const db = new SpyDb();
    await new Dataset(db).dependentsOf({ name: 'mail' });
    expect(db.last.sql).toMatch(/ORDER BY r\.stars DESC/);
  });

  it('caps the limit so one query cannot pull the whole table', async () => {
    const db = new SpyDb();
    await new Dataset(db).dependentsOf({ name: 'mail', limit: 10_000 });
    expect(db.last.params.at(-1)).toBe(500);
  });

  it('falls back to the default limit for nonsense values', async () => {
    const db = new SpyDb();
    for (const limit of [0, -5, Number.NaN]) {
      await new Dataset(db).dependentsOf({ name: 'mail', limit });
      expect(db.last.params.at(-1)).toBe(50);
    }
  });

  it('coerces an unexpected relationship to unknown', async () => {
    const db = new SpyDb([
      {
        owner: 'a', repo: 'b', stars: 1, version: '1',
        url: '', relationship: 'weird',
      },
    ]);
    const [dep] = await new Dataset(db).dependentsOf({ name: 'mail' });
    expect(dep?.relationship).toBe('unknown');
  });

  it('reads the parquet files named by the generated schema', async () => {
    const db = new SpyDb();
    await new Dataset(db, 'https://cdn.example/data').dependentsOf({
      name: 'mail',
    });
    expect(db.last.sql).toContain(
      "read_parquet('https://cdn.example/data/artifacts.parquet')",
    );
    expect(db.last.sql).toContain(
      "read_parquet('https://cdn.example/data/repositories.parquet')",
    );
  });
});

describe('searchPackages', () => {
  it('wraps the fragment in wildcards as a bound parameter', async () => {
    const db = new SpyDb();
    await new Dataset(db).searchPackages('mai');
    expect(db.last.params[0]).toBe('%mai%');
  });

  it('reports direct and total counts separately', async () => {
    const db = new SpyDb([
      { name: 'mail', repository_count: 118, direct_count: 17 },
    ]);
    const [row] = await new Dataset(db).searchPackages('mail');
    expect(row).toEqual({
      name: 'mail',
      repositoryCount: 118,
      directCount: 17,
    });
  });
});

describe('topPackages', () => {
  it('omits the WHERE clause when unfiltered', async () => {
    const db = new SpyDb();
    await new Dataset(db).topPackages();
    expect(db.last.sql).not.toContain('WHERE');
  });

  it('can restrict to declared dependencies', async () => {
    const db = new SpyDb();
    await new Dataset(db).topPackages({ directOnly: true });
    expect(db.last.params).toContain('direct');
  });
});

describe('repositoryProfile', () => {
  it('returns null without querying dependencies for an unknown id', async () => {
    const db = new SpyDb([]);
    const profile = await new Dataset(db).repositoryProfile(42);
    expect(profile.repository).toBeNull();
    expect(profile.dependencies).toEqual([]);
    expect(db.calls).toHaveLength(1);
  });
});
