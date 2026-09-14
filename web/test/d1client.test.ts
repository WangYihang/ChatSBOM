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
