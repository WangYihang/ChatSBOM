/**
 * The query endpoint's answers, kept in the Worker's cache (#42).
 *
 * Every visitor's page asks the overview's dozen questions, and each
 * was a query: every visitor shares one ClickHouse account and its 16
 * concurrent queries, and on D1 every call is billed reads. An answer
 * is now kept for its method's time under what it answers — which
 * dataset, which question, which arguments — so a repeated call is
 * answered without the store, and a new dataset is a miss rather than
 * a stale hit.
 *
 * `caches` is the Workers runtime's; Node has none, so each test gives
 * the Worker one that keeps what it is given.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';

import { handleQuery } from '../src/d1/api';
import { limiters } from './limiters';

afterEach(() => {
  vi.unstubAllGlobals();
  vi.restoreAllMocks();
});

/** The Worker's cache: what it was given, by the key it was given under. */
function workerCache() {
  const kept = new Map<string, Response>();
  const url = (key: RequestInfo | URL) =>
    key instanceof Request ? key.url : String(key);
  const cache = {
    match: vi.fn(async (key: RequestInfo | URL) => kept.get(url(key))?.clone()),
    put: vi.fn(async (key: RequestInfo | URL, response: Response) => {
      kept.set(url(key), response.clone());
    }),
  };
  vi.stubGlobal('caches', { default: cache });
  return { cache, kept };
}

const TOTALS = {
  repositories: 28075, dependencies: 6062896, packages: 141938,
  classified: 6053469, tracked: 60017,
};

/**
 * A D1 database: `meta` answers the dataset's provenance, anything else
 * the totals row. `asked` is every statement it ran.
 */
function store() {
  const meta = {
    generator: 'chatsbom/0.5.4',
    schema_version: '5',
    observed_from: '2026-02-11',
    observed_to: '2026-09-13',
  };
  const asked: string[] = [];
  let failing = false;
  const prepare = vi.fn((sql: string) => {
    const all = async () => {
      asked.push(sql);
      if (failing) throw new Error('D1_ERROR: no such table: agg_totals');
      return { results: /\bFROM meta\b/.test(sql) ? [{ ...meta }] : [TOTALS] };
    };
    return { all, bind: () => ({ all }) };
  });
  return {
    env: { DB: { prepare } as unknown as D1Database },
    meta,
    asked,
    /** Statements other than the provenance: the questions themselves. */
    questions: () => asked.filter((sql) => !/\bFROM meta\b/.test(sql)),
    fail: () => {
      failing = true;
    },
  };
}

function post(body: unknown, headers: Record<string, string> = {}): Request {
  return new Request('https://x.example/api/q', {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...headers },
    body: JSON.stringify(body),
  });
}

describe('the Worker cache', () => {
  it('answers a repeated call without asking the store', async () => {
    workerCache();
    const { env, asked } = store();

    const first = await handleQuery(post({ method: 'totals' }), env);
    expect(first.status).toBe(200);
    expect(first.headers.get('x-cache')).toBe('MISS');
    const statements = asked.length;
    expect(statements).toBeGreaterThan(0);

    const second = await handleQuery(post({ method: 'totals' }), env);
    expect(second.status).toBe(200);
    expect(second.headers.get('x-cache')).toBe('HIT');
    expect(await second.json()).toEqual(TOTALS);
    // Not the question, and not the provenance either: the version is
    // believed for a minute, so a hit costs the store nothing.
    expect(asked).toHaveLength(statements);
  });

  it('misses once the dataset is a new version', async () => {
    workerCache();
    const { env, meta, questions } = store();
    await handleQuery(post({ method: 'totals' }), env);

    // An import lands: the store's provenance moves on.
    meta.observed_to = '2026-09-20';
    // Within the minute the version is believed for, the old one holds.
    const now = Date.now();
    vi.spyOn(Date, 'now').mockReturnValue(now + 30_000);
    const within = await handleQuery(post({ method: 'totals' }), env);
    expect(within.headers.get('x-cache')).toBe('HIT');

    vi.spyOn(Date, 'now').mockReturnValue(now + 61_000);
    const after = await handleQuery(post({ method: 'totals' }), env);
    expect(after.headers.get('x-cache')).toBe('MISS');
    expect(questions()).toHaveLength(2);
  });

  it('keeps an answer for its method’s time, and says so on the copy it keeps', async () => {
    const { kept } = workerCache();
    const { env } = store();
    await handleQuery(post({ method: 'totals' }), env);

    const [entry] = [...kept.values()];
    // What the Cache API reads to decide how long to keep it: a no-store
    // copy would not be kept at all, and a private one not by a cache
    // every visitor shares.
    expect(entry?.headers.get('cache-control')).toMatch(/^public, max-age=[1-9]\d*$/);
  });

  it('keeps an answer under what was asked, not under what was sent', async () => {
    // A parameter no method reads is dropped before the key is made, so
    // it can neither split one answer across keys nor mint new ones.
    workerCache();
    const { env, questions } = store();
    await handleQuery(post({ method: 'totals', params: { junk: 'a' } }), env);
    const again = await handleQuery(post({ method: 'totals', params: { junk: 'b' } }), env);
    expect(again.headers.get('x-cache')).toBe('HIT');
    expect(questions()).toHaveLength(1);
  });

  it('never serves another question’s answer from a slot they share', async () => {
    // Keys are a fixed number of slots, so two questions can land in
    // one; the slot says whose answer it holds.
    const { kept } = workerCache();
    const { env } = store();
    await handleQuery(post({ method: 'totals' }), env);
    const [totalsKey] = [...kept.keys()];
    await handleQuery(post({ method: 'languageCoverage' }), env);
    const coverageKey = [...kept.keys()].find((key) => key !== totalsKey)!;

    kept.set(totalsKey!, kept.get(coverageKey)!.clone());
    const response = await handleQuery(post({ method: 'totals' }), env);
    expect(response.headers.get('x-cache')).toBe('MISS');
    expect(await response.json()).toEqual(TOTALS);
  });

  it('keys on no host a client chose', async () => {
    // Under `wrangler dev` the request's URL takes its host from the
    // Host header, which a client reaching the port sets as it likes.
    const { kept } = workerCache();
    const { env } = store();
    await handleQuery(
      new Request('https://anything.example/api/q', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ method: 'totals' }),
      }),
      env,
    );
    for (const key of kept.keys()) expect(key).not.toContain('anything.example');
  });

  it('never keeps the provenance, which is what a version is read from', async () => {
    const { cache } = workerCache();
    const { env, asked } = store();
    await handleQuery(post({ method: 'meta' }), env);
    await handleQuery(post({ method: 'meta' }), env);
    expect(asked.filter((sql) => /\bFROM meta\b/.test(sql)).length).toBeGreaterThanOrEqual(2);
    expect(cache.put).not.toHaveBeenCalled();
  });

  it('never keeps a failure', async () => {
    const { cache } = workerCache();
    const { env, fail } = store();
    vi.spyOn(console, 'error').mockImplementation(() => {});
    fail();
    expect((await handleQuery(post({ method: 'totals' }), env)).status).toBe(500);
    expect(cache.put).not.toHaveBeenCalled();
  });

  it('refuses a malformed call before looking in the cache', async () => {
    const { cache } = workerCache();
    const { env } = store();
    const response = await handleQuery(
      post({ method: 'dependentsOf', params: { name: 'mail', limit: -1 } }),
      env,
    );
    expect(response.status).toBe(400);
    expect(cache.match).not.toHaveBeenCalled();
  });

  it('counts a call against the rate limit before looking in the cache', async () => {
    // A hit is still a call: the limit is on what a client asks, and a
    // flood of hits is a flood all the same.
    const { cache } = workerCache();
    const { env } = store();
    await handleQuery(post({ method: 'totals' }), env);
    cache.match.mockClear();

    const limited = await handleQuery(post({ method: 'totals' }), {
      ...env,
      RATE_LIMITER: limiters(false).namespace,
      QUERY_RATE_LIMIT: { limit: 100, period: 10 },
    });
    expect(limited.status).toBe(429);
    expect(cache.match).not.toHaveBeenCalled();
  });

  it('answers as before where there is no cache', async () => {
    // Node, a preview: nothing is kept, nothing claims to have been.
    const { env } = store();
    const response = await handleQuery(post({ method: 'totals' }), env);
    expect(response.status).toBe(200);
    expect(response.headers.get('cache-control')).toContain('no-store');
    expect(response.headers.get('x-cache')).toBeNull();
  });
});
