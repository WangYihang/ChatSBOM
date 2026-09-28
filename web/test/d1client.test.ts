/**
 * The browser client.
 *
 * Its job is narrow — name a method, pass parameters, surface failures —
 * and the tests here are about exactly that narrowness. In particular
 * that it sends no SQL, because the whole point of the boundary is that
 * the page cannot express any.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';

import { DatasetClient, QueryError } from '../src/d1/client';

afterEach(() => vi.unstubAllGlobals());

function stubFetch(
  reply: unknown,
  init: { ok?: boolean; status?: number } = {},
) {
  const calls: { url: string; body: unknown }[] = [];
  vi.stubGlobal('fetch', (url: string, options: RequestInit) => {
    calls.push({ url, body: JSON.parse(String(options.body)) });
    return Promise.resolve({
      ok: init.ok ?? true,
      status: init.status ?? 200,
      json: () => Promise.resolve(reply),
    } as Response);
  });
  return calls;
}

describe('DatasetClient', () => {
  it('posts a method name and its parameters', async () => {
    const calls = stubFetch([]);
    await new DatasetClient().dependentsOf({ name: 'mail', directOnly: true });
    expect(calls[0]!.url).toBe('/api/q');
    expect(calls[0]!.body).toEqual({
      method: 'dependentsOf',
      params: { name: 'mail', directOnly: true },
    });
  });

  it('sends no SQL, for any method', async () => {
    const calls = stubFetch([]);
    const client = new DatasetClient();
    await client.dependentsOf({ name: 'mail' });
    await client.topPackages({ directOnly: true });
    await client.relationshipSplit('Ruby');
    await client.totals();
    await client.meta();

    const wire = JSON.stringify(calls);
    for (const word of ['SELECT', 'FROM', 'JOIN', 'WHERE', 'artifacts']) {
      expect(wire).not.toContain(word);
    }
  });

  it('omits an absent language rather than sending an empty one', async () => {
    const calls = stubFetch({});
    await new DatasetClient().relationshipSplit();
    expect(calls[0]!.body).toEqual({ method: 'relationshipSplit', params: {} });
  });

  it('returns the parsed result', async () => {
    stubFetch({ repositories: 28075, dependencies: 6062896, packages: 141938, classified: 6053469 });
    const totals = await new DatasetClient().totals();
    expect(totals.repositories).toBe(28075);
  });

  it('rejects with the message the Worker wrote', async () => {
    stubFetch({ error: 'Unknown method: nope' }, { ok: false, status: 400 });
    await expect(new DatasetClient().totals()).rejects.toThrow(
      'Unknown method: nope',
    );
  });

  it('rejects with a readable message when the body is not JSON', async () => {
    vi.stubGlobal('fetch', () =>
      Promise.resolve({
        ok: false,
        status: 502,
        json: () => Promise.reject(new Error('not json')),
      } as unknown as Response),
    );
    await expect(new DatasetClient().totals()).rejects.toThrow(/502/);
  });

  it('rejects rather than returning an error-shaped result', async () => {
    stubFetch({ error: 'nope' }, { ok: false, status: 500 });
    // A UI that styles failures differently needs them separable from
    // answers; returning `{error}` as a value makes every caller check.
    await expect(new DatasetClient().totals()).rejects.toBeInstanceOf(
      QueryError,
    );
  });
});

/**
 * The same question asked by two parts of the page at once (#42): the
 * root and the overview both asked for the ecosystems and the
 * languages, and the header and the metadata panel both for the totals,
 * on every visit.
 */
describe('DatasetClient, asked twice at once', () => {
  /** `/api/q` answering when told to, with each request's signal kept. */
  function held() {
    const requests: { body: unknown; signal: AbortSignal | undefined; answer(): void }[] = [];
    vi.stubGlobal('fetch', (_url: string, options: RequestInit) =>
      new Promise((resolve, reject) => {
        const signal = options.signal ?? undefined;
        signal?.addEventListener('abort', () =>
          reject(new DOMException('The operation was aborted.', 'AbortError')),
        );
        requests.push({
          body: JSON.parse(String(options.body)),
          signal,
          answer: () =>
            resolve({
              ok: true,
              status: 200,
              json: () => Promise.resolve({ repositories: 3 }),
            } as Response),
        });
      }),
    );
    return requests;
  }

  it('sends one request for the same question asked at once', async () => {
    const requests = held();
    const client = new DatasetClient();
    const first = client.totals();
    const second = client.totals();
    expect(requests).toHaveLength(1);
    requests[0]!.answer();
    expect(await first).toEqual({ repositories: 3 });
    expect(await second).toEqual({ repositories: 3 });
  });

  it('asks again once the first has been answered: it shares a request, it keeps no answers', async () => {
    const requests = held();
    const client = new DatasetClient();
    const first = client.totals();
    requests[0]!.answer();
    await first;
    const again = client.totals();
    expect(requests).toHaveLength(2);
    requests[1]!.answer();
    await again;
  });

  it('keeps different questions apart, parameters included', () => {
    const requests = held();
    const client = new DatasetClient();
    void client.totals();
    void client.languageCoverage();
    void client.topPackages({ directOnly: true, limit: 20 });
    void client.topPackages({ directOnly: false, limit: 20 });
    expect(requests.map(({ body }) => body)).toEqual([
      { method: 'totals' },
      { method: 'languageCoverage' },
      { method: 'topPackages', params: { directOnly: true, limit: 20 } },
      { method: 'topPackages', params: { directOnly: false, limit: 20 } },
    ]);
  });

  it('abandons the request when the one caller waiting on it gives up', async () => {
    const requests = held();
    const client = new DatasetClient();
    const giveUp = new AbortController();
    const asked = client.totals(giveUp.signal);
    giveUp.abort();
    await expect(asked).rejects.toMatchObject({ name: 'AbortError' });
    expect(requests[0]!.signal?.aborted).toBe(true);
  });

  it('keeps the request while another caller still waits on it', async () => {
    const requests = held();
    const client = new DatasetClient();
    const giveUp = new AbortController();
    const abandoned = client.totals(giveUp.signal);
    const kept = client.totals();
    giveUp.abort();
    await expect(abandoned).rejects.toMatchObject({ name: 'AbortError' });
    expect(requests[0]!.signal?.aborted).toBe(false);
    requests[0]!.answer();
    expect(await kept).toEqual({ repositories: 3 });
  });
});

describe('DatasetClient edge questions', () => {
  it('names the direction it is asking about', async () => {
    const calls = stubFetch([]);
    const client = new DatasetClient();
    await client.pulledInBy('ms', 15);
    await client.dependenciesOf('body-parser');
    expect(calls[0]!.body).toEqual({
      method: 'pulledInBy',
      params: { name: 'ms', limit: 15 },
    });
    // No limit given means the store's default, not a limit of
    // undefined serialised into the request.
    expect(calls[1]!.body).toEqual({
      method: 'dependenciesOf',
      params: { name: 'body-parser' },
    });
  });

  it('passes the tree bounds through, and omits the ones not set', async () => {
    const calls = stubFetch({ root: 'ms', children: [], grandchildren: [] });
    const client = new DatasetClient();
    await client.dependencyTree('ms', { children: 12, branch: 3 });
    await client.dependencyTree('ms');
    expect(calls[0]!.body).toEqual({
      method: 'dependencyTree',
      params: { name: 'ms', children: 12, branch: 3 },
    });
    expect(calls[1]!.body).toEqual({
      method: 'dependencyTree',
      params: { name: 'ms' },
    });
  });

  it('sends no SQL for the edge questions either', async () => {
    const calls = stubFetch([]);
    const client = new DatasetClient();
    await client.pulledInBy('ms');
    await client.dependencyTree('ms', { branch: 3 });
    const wire = JSON.stringify(calls);
    for (const word of ['SELECT', 'agg_edges', 'parent_id', 'ROW_NUMBER']) {
      expect(wire).not.toContain(word);
    }
  });
});
