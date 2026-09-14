/**
 * The query endpoint.
 *
 * The browser names a method and passes parameters; it never sends SQL.
 * That is not defence in depth, it is the only defence: this page is
 * public, and a page that can send SQL can send any SQL. The method
 * registry is the allow-list, and anything not in it is a 400 rather
 * than an error from the database.
 */
import { describe, expect, it, vi } from 'vitest';

import { handleQuery, METHODS } from '../src/d1/api';

function env(rows: unknown[] = []) {
  const prepare = vi.fn((_sql: string) => ({
    bind: vi.fn(() => ({ all: () => Promise.resolve({ results: rows }) })),
    all: () => Promise.resolve({ results: rows }),
  }));
  return { DB: { prepare } as unknown as D1Database, prepare };
}

function post(body: unknown): Request {
  return new Request('https://x.example/api/q', {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(body),
  });
}

describe('method allow-list', () => {
  it('answers a known method', async () => {
    const { DB } = env([{ relationship: 'direct', records: 1 }]);
    const response = await handleQuery(post({ method: 'relationshipSplit' }), { DB });
    expect(response.status).toBe(200);
  });

  it('refuses an unknown method without touching the database', async () => {
    const { DB, prepare } = env();
    const response = await handleQuery(post({ method: 'dropEverything' }), { DB });
    expect(response.status).toBe(400);
    expect(prepare).not.toHaveBeenCalled();
  });

  it('refuses a method named as a prototype property', async () => {
    /** `constructor` and `toString` are on every object. */
    const { DB, prepare } = env();
    for (const method of ['constructor', 'toString', '__proto__']) {
      const response = await handleQuery(post({ method }), { DB });
      expect(response.status).toBe(400);
    }
    expect(prepare).not.toHaveBeenCalled();
  });

  it('never accepts SQL, under any parameter name', async () => {
    const { DB, prepare } = env();
    const response = await handleQuery(
      post({ method: 'relationshipSplit', params: { sql: 'DROP TABLE artifacts' } }),
      { DB },
    );
    // The parameter is ignored, not executed; the method decides its SQL.
    expect(response.status).toBe(200);
    const sql = prepare.mock.calls.map((c) => String(c[0])).join();
    expect(sql).not.toContain('DROP');
  });

  it('exposes exactly the methods the dashboard needs', () => {
    expect(Object.keys(METHODS).sort()).toEqual([
      'adoptionOverTime',
      'countDependents',
      'dependenciesOf',
      'dependencyDistribution',
      'dependencyTree',
      'dependentsOf',
      'ecosystemsFor',
      'edgeAmbiguity',
      'languageCoverage',
      'licenseShares',
      'meta',
      'pulledInBy',
      'relationshipSplit',
      'searchPackages',
      'sourceComparison',
      'topPackages',
      'totals',
      'versionSpread',
    ]);
  });
});

describe('request validation', () => {
  it('rejects a body that is not an object', async () => {
    const { DB } = env();
    const response = await handleQuery(post('nope'), { DB });
    expect(response.status).toBe(400);
  });

  it('rejects a body that is not JSON at all', async () => {
    const { DB } = env();
    const request = new Request('https://x.example/api/q', {
      method: 'POST',
      body: 'not json',
    });
    expect((await handleQuery(request, { DB })).status).toBe(400);
  });

  it('rejects a method that needs a package name without one', async () => {
    const { DB, prepare } = env();
    const response = await handleQuery(post({ method: 'dependentsOf' }), { DB });
    expect(response.status).toBe(400);
    expect(prepare).not.toHaveBeenCalled();
  });

  it('rejects a package name that is not a string', async () => {
    const { DB } = env();
    const response = await handleQuery(
      post({ method: 'dependentsOf', params: { name: { $ne: null } } }),
      { DB },
    );
    expect(response.status).toBe(400);
  });

  it('refuses anything but POST', async () => {
    const { DB } = env();
    const response = await handleQuery(
      new Request('https://x.example/api/q'),
      { DB },
    );
    expect(response.status).toBe(405);
    expect(response.headers.get('allow')).toBe('POST');
  });
});

describe('responses', () => {
  it('returns JSON that is not cached', async () => {
    const { DB } = env([{ repositories: 1, dependencies: 2, packages: 3, classified: 4 }]);
    const response = await handleQuery(post({ method: 'totals' }), { DB });
    expect(response.headers.get('content-type')).toContain('application/json');
    // Freshness is the point of having a database behind this.
    expect(response.headers.get('cache-control')).toContain('no-store');
  });

  it('returns the method result as the body', async () => {
    const { DB } = env([{ repositories: 28075, dependencies: 6062896, packages: 141938, classified: 6053469 }]);
    const response = await handleQuery(post({ method: 'totals' }), { DB });
    expect(await response.json()).toEqual({
      repositories: 28075,
      dependencies: 6062896,
      packages: 141938,
      classified: 6053469,
    });
  });

  it('never leaks database error text to the client', async () => {
    const DB = {
      prepare: () => {
        throw new Error('D1_ERROR: no such table: artifacts at line 3');
      },
    } as unknown as D1Database;
    const response = await handleQuery(post({ method: 'totals' }), { DB });
    expect(response.status).toBe(500);
    const body = await response.text();
    expect(body).not.toContain('no such table');
    expect(body).not.toContain('artifacts');
  });
});

describe('the edge methods, at the endpoint', () => {
  it('requires a package name', async () => {
    const { DB, prepare } = env();
    for (const method of ['pulledInBy', 'dependenciesOf', 'dependencyTree']) {
      const response = await handleQuery(post({ method }), { DB });
      expect(response.status).toBe(400);
    }
    expect(prepare).not.toHaveBeenCalled();
  });

  it('rejects a tree bound that is not a number', async () => {
    // Coerced at the edge rather than inside the query: a string here
    // would reach a LIMIT binding and SQLite would take it.
    const { DB } = env();
    const response = await handleQuery(
      post({ method: 'dependencyTree', params: { name: 'ms', branch: '99' } }),
      { DB },
    );
    expect(response.status).toBe(400);
  });

  it('answers the reverse lookup from the database', async () => {
    const { DB } = env([{ name: 'debug', repositories: 7999 }]);
    const response = await handleQuery(
      post({ method: 'pulledInBy', params: { name: 'ms' } }),
      { DB },
    );
    expect(response.status).toBe(200);
    expect(await response.json()).toEqual([
      { name: 'debug', repositories: 7999 },
    ]);
  });
});
