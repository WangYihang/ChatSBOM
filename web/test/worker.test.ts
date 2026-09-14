/**
 * The Worker's routes.
 *
 * Two, plus a fallback. The tests that used to live here were about
 * ranged Parquet reads — `parseRange`, suffix ranges for a footer,
 * `Content-Length` on HEAD, content-addressed keys — and they went with
 * the route they described. Each existed for a real reason, recorded in
 * the file header, and none of those reasons survives a client that
 * holds no copy of the data.
 */
import { describe, expect, it, vi } from 'vitest';

import worker from '../src/worker';

function env(overrides: Record<string, unknown> = {}) {
  return {
    ASSETS: { fetch: vi.fn(() => new Response('the spa')) },
    ...overrides,
  } as unknown as Parameters<typeof worker.fetch>[1];
}

const ctx = {} as ExecutionContext;

describe('routing', () => {
  it('serves the SPA from static assets', async () => {
    const e = env();
    const response = await worker.fetch(
      new Request('https://x.example/'),
      e,
      ctx,
    );
    expect(await response.text()).toBe('the spa');
  });

  it('serves a deep link from static assets too', async () => {
    // The SPA owns its own routing; `#/query/mail` never reaches here,
    // but `/query/mail` as a path must not 404.
    const response = await worker.fetch(
      new Request('https://x.example/query/mail'),
      env(),
      ctx,
    );
    expect(await response.text()).toBe('the spa');
  });

  it('answers /api/q from the database', async () => {
    const prepare = vi.fn(() => ({
      all: () => Promise.resolve({ results: [{ repositories: 1, dependencies: 2, packages: 3, classified: 4 }] }),
    }));
    const response = await worker.fetch(
      new Request('https://x.example/api/q', {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ method: 'totals' }),
      }),
      env({ DB: { prepare } }),
      ctx,
    );
    expect(response.status).toBe(200);
    expect(prepare).toHaveBeenCalled();
  });

  it('says so when no database is bound, rather than failing obscurely', async () => {
    const response = await worker.fetch(
      new Request('https://x.example/api/q', { method: 'POST', body: '{}' }),
      env(),
      ctx,
    );
    expect(response.status).toBe(503);
    expect(await response.text()).toContain('no database bound');
  });

  it('says so when the chat is not configured', async () => {
    const response = await worker.fetch(
      new Request('https://x.example/api/chat', { method: 'POST', body: '{}' }),
      env(),
      ctx,
    );
    expect(response.status).toBe(503);
  });

  it('no longer serves /data — that route is gone, not broken', async () => {
    // Falls through to the SPA, which is the honest answer for a path
    // this deployment does not have.
    const response = await worker.fetch(
      new Request('https://x.example/data/artifacts.parquet'),
      env(),
      ctx,
    );
    expect(await response.text()).toBe('the spa');
  });
});
