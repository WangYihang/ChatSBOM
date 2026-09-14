import { describe, expect, it } from 'vitest';

import {
  Dataset,
  absoluteBase,
  filesFromManifest,
  isRelationship,
  type Queryable,
} from '../src/queries';

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

const TEST_FILES = {
  repositories: 'repositories-aaaaaaaa.parquet',
  artifacts: 'artifacts-bbbbbbbb.parquet',
  licenses: 'licenses-cccccccc.parquet',
  history: 'history-dddddddd.parquet',
};

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
    await new Dataset(db, 'https://x.example/data', TEST_FILES).dependentsOf({ name: "mail'; DROP TABLE x;--" });

    expect(db.last.sql).not.toContain('DROP TABLE');
    expect(db.last.params[0]).toBe("mail'; DROP TABLE x;--");
  });

  it('filters to direct dependants on request', async () => {
    const db = new SpyDb();
    await new Dataset(db, 'https://x.example/data', TEST_FILES).dependentsOf({ name: 'mail', directOnly: true });

    expect(db.last.sql).toContain('a.relationship = ?');
    expect(db.last.params).toContain('direct');
  });

  it('does not filter by relationship by default', async () => {
    const db = new SpyDb();
    await new Dataset(db, 'https://x.example/data', TEST_FILES).dependentsOf({ name: 'mail' });
    expect(db.last.params).not.toContain('direct');
  });

  it('lowercases the language filter', async () => {
    const db = new SpyDb();
    await new Dataset(db, 'https://x.example/data', TEST_FILES).dependentsOf({ name: 'mail', language: 'Ruby' });
    expect(db.last.params).toContain('ruby');
  });

  it('orders by stars descending', async () => {
    const db = new SpyDb();
    await new Dataset(db, 'https://x.example/data', TEST_FILES).dependentsOf({ name: 'mail' });
    expect(db.last.sql).toMatch(/ORDER BY r\.stars DESC/);
  });

  it('caps the limit so one query cannot pull the whole table', async () => {
    const db = new SpyDb();
    await new Dataset(db, 'https://x.example/data', TEST_FILES).dependentsOf({ name: 'mail', limit: 10_000 });
    expect(db.last.params.at(-1)).toBe(500);
  });

  it('falls back to the default limit for nonsense values', async () => {
    const db = new SpyDb();
    for (const limit of [0, -5, Number.NaN]) {
      await new Dataset(db, 'https://x.example/data', TEST_FILES).dependentsOf({ name: 'mail', limit });
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
    const [dep] = await new Dataset(db, 'https://x.example/data', TEST_FILES).dependentsOf({ name: 'mail' });
    expect(dep?.relationship).toBe('unknown');
  });

  it('reads the parquet files the manifest named, at the given base', async () => {
    const db = new SpyDb();
    await new Dataset(db, 'https://cdn.example/data', TEST_FILES).dependentsOf({
      name: 'mail',
    });
    // The filenames come from the manifest, not from the generated
    // schema: they are content-addressed so that `immutable` is
    // truthful, and the schema only says which tables exist.
    expect(db.last.sql).toContain(
      "read_parquet('https://cdn.example/data/artifacts-bbbbbbbb.parquet')",
    );
    expect(db.last.sql).toContain(
      "read_parquet('https://cdn.example/data/repositories-aaaaaaaa.parquet')",
    );
  });

  it('refuses to guess when the manifest names no file for a table', async () => {
    const db = new SpyDb();
    await expect(
      new Dataset(db, 'https://cdn.example/data', {}).dependentsOf({
        name: 'mail',
      }),
    ).rejects.toThrow(/names no file/);
  });
});

describe('searchPackages', () => {
  it('wraps the fragment in wildcards as a bound parameter', async () => {
    const db = new SpyDb();
    await new Dataset(db, 'https://x.example/data', TEST_FILES).searchPackages('mai');
    expect(db.last.params[0]).toBe('%mai%');
  });

  it('reports direct and total counts separately', async () => {
    const db = new SpyDb([
      { name: 'mail', repository_count: 118, direct_count: 17 },
    ]);
    const [row] = await new Dataset(db, 'https://x.example/data', TEST_FILES).searchPackages('mail');
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
    await new Dataset(db, 'https://x.example/data', TEST_FILES).topPackages();
    expect(db.last.sql).not.toContain('WHERE');
  });

  it('can restrict to declared dependencies', async () => {
    const db = new SpyDb();
    await new Dataset(db, 'https://x.example/data', TEST_FILES).topPackages({ directOnly: true });
    expect(db.last.params).toContain('direct');
  });
});

describe('repositoryProfile', () => {
  it('returns null without querying dependencies for an unknown id', async () => {
    const db = new SpyDb([]);
    const profile = await new Dataset(db, 'https://x.example/data', TEST_FILES).repositoryProfile(42);
    expect(profile.repository).toBeNull();
    expect(profile.dependencies).toEqual([]);
    expect(db.calls).toHaveLength(1);
  });
});

describe('ecosystem disambiguation', () => {
  it('can scope a lookup to one ecosystem', async () => {
    // `mail` is a Ruby gem and also a Maven artifactId (javax.mail).
    // Counting them together reported 124 dependants where the gem has
    // 118 — a package name is not unique across ecosystems.
    const db = new SpyDb();
    await new Dataset(db, 'https://x.example/data', TEST_FILES).dependentsOf({ name: 'mail', type: 'gem' });

    expect(db.last.sql).toContain('a.type = ?');
    expect(db.last.params).toContain('gem');
  });

  it('does not filter by ecosystem unless asked', async () => {
    const db = new SpyDb();
    await new Dataset(db, 'https://x.example/data', TEST_FILES).dependentsOf({ name: 'mail' });
    expect(db.last.sql).not.toContain('a.type = ?');
  });

  it('lists the ecosystems a name appears in', async () => {
    const db = new SpyDb([
      { type: 'gem', repository_count: 118, direct_count: 17 },
      { type: 'maven', repository_count: 6, direct_count: 6 },
    ]);
    const rows = await new Dataset(db, 'https://x.example/data', TEST_FILES).ecosystemsFor('mail');

    expect(rows).toEqual([
      { type: 'gem', repositoryCount: 118, directCount: 17 },
      { type: 'maven', repositoryCount: 6, directCount: 6 },
    ]);
  });
});

describe('countDependents', () => {
  // The row query is capped, so its length is a display limit and not a
  // count. Reporting it as "N dependants" states a truncation as a
  // finding — the same mistake as a silently truncated export.
  it('counts distinct repositories, not artifact rows', async () => {
    const db = new SpyDb([{ total: 124 }]);
    await new Dataset(db, 'https://x.example/data', TEST_FILES).countDependents({ name: 'mail' });
    expect(db.calls[0]!.sql).toContain('count(DISTINCT');
  });

  it('applies exactly the filters the row query applies', async () => {
    const filtered = { name: 'mail', type: 'gem', language: 'Ruby' } as const;
    const rows = new SpyDb([]);
    const count = new SpyDb([{ total: 0 }]);
    await new Dataset(rows, 'https://x.example/data', TEST_FILES).dependentsOf(filtered);
    await new Dataset(count, 'https://x.example/data', TEST_FILES).countDependents(filtered);
    // Same predicates, same parameter order; only the projection and the
    // limit differ. A count that filters differently is worse than none.
    const where = (sql: string) =>
      sql.slice(sql.indexOf('WHERE')).replace(/\s+/g, ' ').split('ORDER')[0]!.trim();
    expect(where(count.calls[0]!.sql)).toBe(where(rows.calls[0]!.sql));
    expect(count.calls[0]!.params).toEqual(
      rows.calls[0]!.params.slice(0, count.calls[0]!.params.length),
    );
  });

  it('carries no LIMIT, which is the whole point', async () => {
    const db = new SpyDb([{ total: 7 }]);
    await new Dataset(db, 'https://x.example/data', TEST_FILES).countDependents({ name: 'mail' });
    expect(db.calls[0]!.sql).not.toContain('LIMIT');
  });

  it('returns the total as a number', async () => {
    const db = new SpyDb([{ total: 124 }]);
    expect(await new Dataset(db, 'https://x.example/data', TEST_FILES).countDependents({ name: 'mail' })).toBe(124);
  });

  it('reports zero when the package is absent', async () => {
    const db = new SpyDb([]);
    expect(await new Dataset(db, 'https://x.example/data', TEST_FILES).countDependents({ name: 'nope' })).toBe(0);
  });
});

describe('absoluteBase', () => {
  // DuckDB's HTTP filesystem reads a leading-slash base as a *local*
  // filesystem path, so `/data/repositories.parquet` fails with
  // `IO Error: No files found that match the pattern`. Every base handed
  // to read_parquet has to be absolute. Measured against the real
  // engine, not inferred.
  it('resolves a root-relative path against the page origin', () => {
    expect(absoluteBase('/data', 'https://sbom.example/query')).toBe(
      'https://sbom.example/data',
    );
  });

  it('leaves an absolute URL alone', () => {
    expect(absoluteBase('https://cdn.example/data', 'https://a.example')).toBe(
      'https://cdn.example/data',
    );
  });

  it('never emits a trailing slash, which would double up in the SQL', () => {
    expect(absoluteBase('/data/', 'https://sbom.example')).toBe(
      'https://sbom.example/data',
    );
    expect(absoluteBase('https://cdn.example/data/', 'https://a.example')).toBe(
      'https://cdn.example/data',
    );
  });

  it('resolves a relative path against the origin, not the page path', () => {
    expect(absoluteBase('data', 'https://sbom.example/deep/page')).toBe(
      'https://sbom.example/data',
    );
  });
});

describe('dataset file resolution', () => {
  // Filenames are content-addressed, so the names live in the manifest
  // and nowhere else. There is deliberately no default: a default would
  // be wrong in production the first time the data changed, which is
  // exactly the failure this replaced — a manifest advertising sha
  // 659592a2 while the browser queried a file it already held.
  it('queries the files the manifest names', async () => {
    const db = new SpyDb([]);
    await new Dataset(db, 'https://x.example/data', {
      repositories: 'repositories-aaaaaaaa.parquet',
      artifacts: 'artifacts-bbbbbbbb.parquet',
      licenses: 'licenses-cccccccc.parquet',
      history: 'history-dddddddd.parquet',
    }).dependentsOf({ name: 'mail' });

    const sql = db.calls[0]!.sql;
    expect(sql).toContain('artifacts-bbbbbbbb.parquet');
    expect(sql).toContain('repositories-aaaaaaaa.parquet');
    expect(sql).not.toContain("'artifacts.parquet'");
  });

  it('reads the file map out of a manifest', () => {
    expect(
      filesFromManifest({
        schemaVersion: '5',
        generator: 'x',
        rowCounts: {},
        files: [
          { name: 'artifacts-bbbbbbbb.parquet', bytes: 1, sha256: 'b' },
          { name: 'repositories-aaaaaaaa.parquet', bytes: 1, sha256: 'a' },
        ],
      }),
    ).toEqual({
      artifacts: 'artifacts-bbbbbbbb.parquet',
      repositories: 'repositories-aaaaaaaa.parquet',
    });
  });

  it('ignores a file whose name it cannot attribute to a table', () => {
    expect(
      filesFromManifest({
        schemaVersion: '5',
        generator: 'x',
        rowCounts: {},
        files: [{ name: 'stray.parquet', bytes: 1, sha256: 'x' }],
      }),
    ).toEqual({});
  });
});
