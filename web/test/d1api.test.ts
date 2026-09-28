/**
 * The query endpoint.
 *
 * The browser names a method and passes parameters; it never sends SQL.
 * That is not defence in depth, it is the only defence: this page is
 * public, and a page that can send SQL can send any SQL. The method
 * registry is the allow-list, and anything not in it is a 400 rather
 * than an error from the database.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';

import { handleQuery, METHODS } from '../src/d1/api';

afterEach(() => vi.unstubAllGlobals());

function env(rows: unknown[] = []) {
  const prepare = vi.fn((_sql: string) => ({
    bind: vi.fn((..._params: unknown[]) => ({
      all: () => Promise.resolve({ results: rows }),
    })),
    all: () => Promise.resolve({ results: rows }),
  }));
  return { DB: { prepare } as unknown as D1Database, prepare };
}

/** What each prepared statement was bound with, in the order prepared. */
function bindings(prepare: ReturnType<typeof env>['prepare']): unknown[][] {
  return prepare.mock.results.flatMap((result) => result.value.bind.mock.calls);
}

/**
 * A ClickHouse deployment whose server answers every statement with no
 * rows. `fetch` is the Worker's only way to it, so it is also the record
 * of whether the server was asked anything at all.
 */
function clickHouse() {
  const fetch = vi.fn(
    async (_url: string | URL | Request, _init?: RequestInit) =>
      new Response(JSON.stringify({ data: [] }), {
        headers: { 'content-type': 'application/json' },
      }),
  );
  vi.stubGlobal('fetch', fetch);
  return { env: { CLICKHOUSE_URL: 'http://clickhouse.test:8123' }, fetch };
}

function post(body: unknown, headers: Record<string, string> = {}): Request {
  return new Request('https://x.example/api/q', {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...headers },
    body: JSON.stringify(body),
  });
}

/** A rate limiter that answers `success` and keeps the keys it was asked about. */
function limiter(success: boolean) {
  const keys: string[] = [];
  const binding = {
    limit: vi.fn(async ({ key }: { key: string }) => {
      keys.push(key);
      return { success };
    }),
  } as unknown as RateLimit;
  return { binding, keys };
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
    // `versionKindShares` is not among them. Nothing called it, and on
    // D1 it counted every one of 6,062,896 artifact rows per request —
    // one call could spend a day of the free tier's reads (#31).
    expect(Object.keys(METHODS).sort()).toEqual([
      'adoptionOverTime',
      'countDependentRows',
      'countDependents',
      'dependenciesOf',
      'dependencyDistribution',
      'dependencyTree',
      'dependentsOf',
      'ecosystemCoverage',
      'ecosystemsFor',
      'edgeAmbiguity',
      'languageCoverage',
      'licenseShares',
      'meta',
      'pulledInBy',
      'relationshipByEcosystem',
      // Kept one release for pages loaded before #55 §4.13.
      'relationshipByLanguage',
      'relationshipSplit',
      'searchPackages',
      'sourceComparison',
      'topPackages',
      'totals',
      'versionSpread',
    ]);
  });

  it('refuses versionKindShares without asking the database', async () => {
    const { DB, prepare } = env([{ kind: 'resolved', records: 1 }]);
    const response = await handleQuery(post({ method: 'versionKindShares' }), { DB });
    expect(response.status).toBe(400);
    expect(prepare).not.toHaveBeenCalled();
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
    const { DB } = env([{ repositories: 28075, dependencies: 6062896, packages: 141938, classified: 6053469, tracked: 60017 }]);
    const response = await handleQuery(post({ method: 'totals' }), { DB });
    expect(await response.json()).toEqual({
      repositories: 28075,
      dependencies: 6062896,
      packages: 141938,
      classified: 6053469,
      tracked: 60017,
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

describe('each method against its own parameters', () => {
  /**
   * #31. A number was any finite number and a string any length, so a
   * limit of -1 or 2.5 reached a store that had to guess what it meant
   * — `children: -1` drew 50 children past a bound of 30 — and nothing
   * stopped a megabyte of package name reaching a statement. Each is a
   * 400 now, decided before the database is asked anything.
   */
  it.each([
    ['a negative limit', 'dependentsOf', { name: 'mail', limit: -1 }],
    ['a fractional limit', 'dependentsOf', { name: 'mail', limit: 2.5 }],
    ['a limit too large to be exact', 'dependentsOf', { name: 'mail', limit: 2 ** 60 }],
    ['a negative offset', 'dependentsOf', { name: 'mail', offset: -50 }],
    ['a fractional offset', 'countDependentRows', { name: 'mail', offset: 0.5 }],
    ['a limit of nothing', 'searchPackages', { term: 'ma', limit: 0 }],
    ['a negative version count', 'versionSpread', { name: 'mail', limit: -1 }],
    ['a negative licence count', 'licenseShares', { limit: -3 }],
    ['a fractional ranking depth', 'topPackages', { limit: 1.5 }],
    ['a negative edge count', 'pulledInBy', { name: 'ms', limit: -1 }],
    ['a fractional edge count', 'dependenciesOf', { name: 'ms', limit: 7.5 }],
    ['a tree of no children', 'dependencyTree', { name: 'ms', children: 0 }],
    ['a tree of negative children', 'dependencyTree', { name: 'ms', children: -1 }],
    ['a tree with a fractional branch', 'dependencyTree', { name: 'ms', branch: 1.5 }],
  ])('refuses %s', async (_, method, params) => {
    const { DB, prepare } = env();
    const response = await handleQuery(post({ method, params }), { DB });
    expect(response.status).toBe(400);
    expect(prepare).not.toHaveBeenCalled();
  });

  it('refuses a number JSON can only spell as infinity', async () => {
    const { DB, prepare } = env();
    const request = new Request('https://x.example/api/q', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: '{"method":"dependentsOf","params":{"name":"mail","limit":1e400}}',
    });
    expect((await handleQuery(request, { DB })).status).toBe(400);
    expect(prepare).not.toHaveBeenCalled();
  });

  it.each([
    ['package name', 'dependentsOf', { name: 'x'.repeat(257) }],
    ['search term', 'searchPackages', { term: 'x'.repeat(257) }],
    ['name for the tree', 'dependencyTree', { name: 'x'.repeat(257) }],
    ['repository language', 'dependentsOf', { name: 'mail', language: 'x'.repeat(65) }],
    ['legacy language', 'topPackages', { language: 'x'.repeat(65) }],
    ['ecosystem to rank', 'topPackages', { ecosystem: 'x'.repeat(65) }],
    ['ecosystem to split', 'relationshipSplit', { ecosystem: 'x'.repeat(65) }],
    ['type', 'countDependents', { name: 'mail', type: 'x'.repeat(65) }],
  ])('refuses an over-long %s', async (_, method, params) => {
    const { DB, prepare } = env();
    const response = await handleQuery(post({ method, params }), { DB });
    expect(response.status).toBe(400);
    expect(prepare).not.toHaveBeenCalled();
  });

  it('takes a string at its cap: 256 for a name, 64 for a language or an ecosystem', async () => {
    const { DB } = env();
    const response = await handleQuery(
      post({
        method: 'dependentsOf',
        params: { name: 'x'.repeat(256), type: 'y'.repeat(64), language: 'z'.repeat(64) },
      }),
      { DB },
    );
    expect(response.status).toBe(200);
  });

  it('refuses a flag that is not true or false', async () => {
    const { DB, prepare } = env();
    const response = await handleQuery(
      post({ method: 'topPackages', params: { directOnly: 'yes' } }),
      { DB },
    );
    expect(response.status).toBe(400);
    expect(prepare).not.toHaveBeenCalled();
  });

  it('still answers every call the page and the chat tools make, unchanged', async () => {
    const { DB, prepare } = env();
    const calls: [string, Record<string, unknown>][] = [
      ['dependentsOf', { name: 'mail', type: 'gem', language: 'ruby', directOnly: true, limit: 100, offset: 200 }],
      ['countDependents', { name: 'mail', directOnly: false }],
      ['countDependentRows', { name: 'mail', offset: 0 }],
      ['searchPackages', { term: 'mai', limit: 8 }],
      ['topPackages', { directOnly: true, ecosystem: 'npm', limit: 100 }],
      ['topPackages', { directOnly: true, language: 'ruby', limit: 100 }],
      ['licenseShares', { limit: 12 }],
      ['versionSpread', { name: 'mail', limit: 10 }],
      ['pulledInBy', { name: 'ms', limit: 15 }],
      ['dependenciesOf', { name: 'ms', limit: 20 }],
      ['dependencyTree', { name: 'body-parser', children: 12, branch: 3 }],
      ['relationshipSplit', { ecosystem: 'maven' }],
      ['relationshipSplit', { language: 'ruby' }],
      ['adoptionOverTime', { name: 'mail' }],
      ['ecosystemsFor', { name: 'mail' }],
      ['ecosystemCoverage', {}],
      ['relationshipByEcosystem', {}],
      ['relationshipByLanguage', {}],
    ];
    for (const [method, params] of calls) {
      const response = await handleQuery(post({ method, params }), { DB });
      expect([method, response.status]).toEqual([method, 200]);
    }
    // The values arrive as they were sent, not re-clamped on the way.
    expect(bindings(prepare)[0]).toEqual(['mail', 'gem', 'ruby', 'direct', 100, 200]);
  });
});

describe('an ecosystem named after a JavaScript built-in', () => {
  /**
   * `ecosystemMembers` looked a type up on a plain object, so `toString`
   * found a function and `__proto__` found Object.prototype, and the
   * ClickHouse backend failed on either with a TypeError — a 500. No
   * registry is called either, so it is a 400.
   */
  it.each(['__proto__', 'toString', 'constructor', 'hasOwnProperty'])(
    'is a 400 for %s, not a 500',
    async (type) => {
      const { env: live, fetch } = clickHouse();
      for (const method of ['dependentsOf', 'countDependents', 'countDependentRows']) {
        const response = await handleQuery(post({ method, params: { name: 'mail', type } }), live);
        expect([method, response.status]).toEqual([method, 400]);
      }
      expect(fetch).not.toHaveBeenCalled();
    },
  );

  it.each(['__proto__', 'toString', 'constructor'])(
    'is a 400 for %s as the ecosystem an aggregate is asked for',
    async (ecosystem) => {
      const { DB, prepare } = env();
      for (const method of ['topPackages', 'relationshipSplit']) {
        const response = await handleQuery(post({ method, params: { ecosystem } }), { DB });
        expect([method, response.status]).toEqual([method, 400]);
      }
      expect(prepare).not.toHaveBeenCalled();
    },
  );

  it.each(['__proto__', 'constructor'])(
    'reads %s as a legacy language with no ecosystem: the whole corpus, not a 500',
    async (language) => {
      // `LANGUAGE_ECOSYSTEM['constructor']` is Object, and the store
      // failed calling `toLowerCase` on it.
      const { DB, prepare } = env();
      for (const method of ['topPackages', 'relationshipSplit']) {
        const response = await handleQuery(post({ method, params: { language } }), { DB });
        expect([method, response.status]).toEqual([method, 200]);
      }
      expect(bindings(prepare).flat()).toContain('');
    },
  );

  it('still passes on an ecosystem the table does not map', async () => {
    // `github-action` is in the data and not in `ecosystems.ts`: the
    // page offers it as itself, so it must come back working.
    const { env: live, fetch } = clickHouse();
    const response = await handleQuery(
      post({ method: 'dependentsOf', params: { name: 'actions/checkout', type: 'github-action' } }),
      live,
    );
    expect(response.status).toBe(200);
    expect(String(fetch.mock.calls[0]![0])).toContain('param_type=github-action');
  });
});

describe('the body', () => {
  /**
   * The largest call the page makes is a few hundred bytes. The body was
   * read whole, whatever its size, before anything looked at it.
   */
  it('refuses one larger than any call needs, by its declared length', async () => {
    const { DB, prepare } = env();
    const response = await handleQuery(
      post({ method: 'totals' }, { 'content-length': String(64 * 1024) }),
      { DB },
    );
    expect(response.status).toBe(413);
    expect(prepare).not.toHaveBeenCalled();
  });

  it('refuses one that declares no length, and stops reading it', async () => {
    const { DB, prepare } = env();
    // A valid call padded with a megabyte of JSON whitespace, streamed:
    // no Content-Length, and nothing wrong with it but its size.
    const bytes = new TextEncoder().encode(
      JSON.stringify({ method: 'totals' }) + ' '.repeat(1024 * 1024),
    );
    const chunk = 16 * 1024;
    let offset = 0;
    let cancelled = false;
    const request = new Request('https://x.example/api/q', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: new ReadableStream<Uint8Array>({
        pull(controller) {
          if (offset >= bytes.length) {
            controller.close();
            return;
          }
          controller.enqueue(bytes.subarray(offset, offset + chunk));
          offset += chunk;
        },
        cancel() {
          cancelled = true;
        },
      }),
      duplex: 'half',
    } as RequestInit);
    expect(request.headers.get('content-length')).toBeNull();

    const response = await handleQuery(request, { DB });

    expect(response.status).toBe(413);
    expect(prepare).not.toHaveBeenCalled();
    expect(cancelled).toBe(true);
  });

  it('takes the largest body a real call sends', async () => {
    const { DB } = env();
    // Every string at its cap, in characters JSON has to escape.
    const response = await handleQuery(
      post({
        method: 'countDependentRows',
        params: {
          name: '\u0001'.repeat(256),
          type: '\u0001'.repeat(64),
          language: '\u0001'.repeat(64),
          directOnly: true,
          limit: 100,
          offset: 4_000_000_000,
        },
      }),
      { DB },
    );
    expect(response.status).toBe(200);
  });
});

describe('rate limiting', () => {
  /**
   * Every visitor shares the one ClickHouse account and its 16
   * concurrent queries, and on D1 every call is billed reads. Nothing
   * bounded how fast one client could spend either.
   */
  it('answers 429 past the budget, before the database is asked', async () => {
    const { DB, prepare } = env();
    const { binding } = limiter(false);
    const response = await handleQuery(post({ method: 'totals' }), {
      DB,
      QUERY_RATE_LIMITER: binding,
    });
    expect(response.status).toBe(429);
    expect(response.headers.get('cache-control')).toContain('no-store');
    expect(prepare).not.toHaveBeenCalled();
  });

  it('reads the body of a request it turns away', async () => {
    // Under `wrangler dev` a 429 sent with the body unread lost the
    // connection now and then, and the dev proxy answered 500 instead.
    const { DB } = env();
    const { binding } = limiter(false);
    const request = post({ method: 'totals' });
    const response = await handleQuery(request, { DB, QUERY_RATE_LIMITER: binding });
    expect(response.status).toBe(429);
    expect(request.bodyUsed).toBe(true);
  });

  it.each([
    ['a method other than POST', 'PUT', false],
    ['a deployment with no database bound', 'POST', true],
  ])('reads the body of %s before refusing it (#42)', async (_, method, unbound) => {
    // The 429 above read it; the 405 and the 503 did not, and under
    // `wrangler dev` about half of those came back as a 500 once the
    // body was large (#32).
    const request = new Request('https://x.example/api/q', {
      method,
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ method: 'totals', params: { pad: 'x'.repeat(3000) } }),
    });
    const response = await handleQuery(request, unbound ? {} : env());
    expect(response.status).toBe(unbound ? 503 : 405);
    expect(request.bodyUsed).toBe(true);
  });

  it('counts a request before reading its body', async () => {
    const { DB } = env();
    const { binding } = limiter(false);
    const response = await handleQuery(
      post({ method: 'totals' }, { 'content-length': String(64 * 1024) }),
      { DB, QUERY_RATE_LIMITER: binding },
    );
    expect(response.status).toBe(429);
  });

  it('answers within the budget', async () => {
    const { DB } = env([{ repositories: 1, dependencies: 2, packages: 3, classified: 4 }]);
    const { binding } = limiter(true);
    const response = await handleQuery(post({ method: 'totals' }), {
      DB,
      QUERY_RATE_LIMITER: binding,
    });
    expect(response.status).toBe(200);
  });

  it('keys on the visitor Cloudflare names', async () => {
    const { DB } = env();
    const { binding, keys } = limiter(true);
    await handleQuery(
      post({ method: 'totals' }, { 'cf-connecting-ip': '203.0.113.7' }),
      { DB, QUERY_RATE_LIMITER: binding },
    );
    expect(keys).toEqual(['203.0.113.7']);
  });

  it('keys every request the edge did not vouch for to one bucket', async () => {
    /**
     * A client that reaches wrangler's port directly sets
     * `CF-Connecting-IP` itself, and could claim a new address on every
     * request. With EDGE_SECRET set, only a request carrying it is
     * believed; the rest share one budget, whatever they claim.
     */
    const { DB } = env();
    const { binding, keys } = limiter(true);
    const edge = { DB, QUERY_RATE_LIMITER: binding, EDGE_SECRET: 'the-edge-secret' };
    for (const address of ['198.51.100.1', '198.51.100.2']) {
      await handleQuery(post({ method: 'totals' }, { 'cf-connecting-ip': address }), edge);
    }
    await handleQuery(
      post(
        { method: 'totals' },
        { 'cf-connecting-ip': '198.51.100.3', 'x-edge-secret': 'the-edge-secret' },
      ),
      edge,
    );
    expect(keys[0]).toBe(keys[1]);
    expect(keys[0]).not.toContain('198.51.100');
    expect(keys[2]).toBe('198.51.100.3');
  });
});

describe('the language parameter, kept one release (#55 §4.13)', () => {
  it('reads a legacy language as the ecosystem its list stood for', async () => {
    const { DB, prepare } = env([]);
    await handleQuery(
      post({ method: 'topPackages', params: { language: 'php', directOnly: true } }),
      { DB },
    );
    const bound = prepare.mock.results
      .map((r) => (r.value as { bind: { mock: { calls: unknown[][] } } }).bind)
      .flatMap((bind) => bind.mock.calls.flat());
    expect(bound).toContain('composer');
  });

  it('prefers an ecosystem named outright', async () => {
    const { DB, prepare } = env([]);
    await handleQuery(
      post({
        method: 'relationshipSplit',
        params: { ecosystem: 'maven', language: 'php' },
      }),
      { DB },
    );
    const bound = prepare.mock.results
      .map((r) => (r.value as { bind: { mock: { calls: unknown[][] } } }).bind)
      .flatMap((bind) => bind.mock.calls.flat());
    expect(bound).toContain('maven');
    expect(bound).not.toContain('composer');
  });

  it('reads a language with no ecosystem as the whole corpus', async () => {
    const { DB, prepare } = env([]);
    const response = await handleQuery(
      post({ method: 'topPackages', params: { language: 'c++' } }),
      { DB },
    );
    expect(response.status).toBe(200);
    const bound = prepare.mock.results
      .map((r) => (r.value as { bind: { mock: { calls: unknown[][] } } }).bind)
      .flatMap((bind) => bind.mock.calls.flat());
    expect(bound).toContain('');
  });
});
