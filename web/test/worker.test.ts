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
import { afterEach, describe, expect, it, vi } from 'vitest';

import worker, { RateLimiter, SpendCounter } from '../src/worker';
import { counters } from './counters';

function env(overrides: Record<string, unknown> = {}) {
  return {
    ASSETS: { fetch: vi.fn(() => new Response('the spa')) },
    ...overrides,
  } as unknown as Parameters<typeof worker.fetch>[1];
}

/** What the runtime passes third. Only the chat route uses it. */
function ctx() {
  return {
    waitUntil: vi.fn(),
    passThroughOnException: vi.fn(),
  } as unknown as Parameters<typeof worker.fetch>[2];
}

afterEach(() => vi.unstubAllGlobals());

describe('routing', () => {
  it('serves the SPA from static assets', async () => {
    const e = env();
    const response = await worker.fetch(
      new Request('https://x.example/'),
      e,
      ctx(),
    );
    expect(await response.text()).toBe('the spa');
  });

  it('serves a deep link from static assets too', async () => {
    // The SPA owns its own routing; `#/query/mail` never reaches here,
    // but `/query/mail` as a path must not 404.
    const response = await worker.fetch(
      new Request('https://x.example/query/mail'),
      env(),
      ctx(),
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
      ctx(),
    );
    expect(response.status).toBe(200);
    expect(prepare).toHaveBeenCalled();
  });

  it('says so when no database is bound, rather than failing obscurely', async () => {
    const response = await worker.fetch(
      new Request('https://x.example/api/q', { method: 'POST', body: '{}' }),
      env(),
      ctx(),
    );
    expect(response.status).toBe(503);
    expect(await response.text()).toMatch(/no database bound/i);
  });

  it('says so when the chat is not configured', async () => {
    const response = await worker.fetch(
      new Request('https://x.example/api/chat', { method: 'POST', body: '{}' }),
      env(),
      ctx(),
    );
    expect(response.status).toBe(503);
  });

  it('gives the chat its ExecutionContext, to settle spend after answering', async () => {
    // The Messages API, answering once. Nothing here leaves the process.
    vi.stubGlobal(
      'fetch',
      vi.fn(async () =>
        new Response(
          JSON.stringify({
            id: 'msg_1', type: 'message', role: 'assistant', model: 'claude-opus-5',
            content: [{ type: 'text', text: 'ok', citations: null }],
            stop_reason: 'end_turn', stop_sequence: null,
            usage: { input_tokens: 10, output_tokens: 5 },
          }),
          { headers: { 'content-type': 'application/json' } },
        ),
      ),
    );
    const context = ctx();
    const response = await worker.fetch(
      new Request('https://x.example/api/chat', {
        method: 'POST',
        headers: { 'content-type': 'application/json', origin: 'https://x.example' },
        body: JSON.stringify({ messages: [{ role: 'user', content: 'hi' }] }),
      }),
      env({
        ANTHROPIC_API_KEY: 'k',
        DAILY_SPEND_CAP_USD: '5',
        SPEND_COUNTER: counters().namespace,
      }),
      context,
    );
    expect(response.status).toBe(200);
    expect(context.waitUntil).toHaveBeenCalledTimes(1);
  });

  it('exports the spend counter from its entry, where the runtime looks for it', () => {
    // A Durable Object's class is found among the Worker's exports by
    // the name its binding gives (#33); spend.integration.test.ts runs
    // it under wrangler.jsonc's binding.
    expect(SpendCounter).toBeTypeOf('function');
    expect(SpendCounter.name).toBe('SpendCounter');
  });

  it('exports the rate limiter from its entry too (#115)', () => {
    // ratelimit.integration.test.ts runs it under wrangler.jsonc's binding.
    expect(RateLimiter).toBeTypeOf('function');
    expect(RateLimiter.name).toBe('RateLimiter');
  });

  it('no longer serves /data — that route is gone, not broken', async () => {
    // Falls through to the SPA, which is the honest answer for a path
    // this deployment does not have.
    const response = await worker.fetch(
      new Request('https://x.example/data/artifacts.parquet'),
      env(),
      ctx(),
    );
    expect(await response.text()).toBe('the spa');
  });
});
