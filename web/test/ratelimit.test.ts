/**
 * Who a request is from, as far as a rate limiter is concerned.
 *
 * `CF-Connecting-IP` names the visitor when Cloudflare's edge set it.
 * Under `wrangler dev` behind a tunnel, a client that reaches the port
 * directly sets it too, to anything, and a new address per request was
 * a new budget per request (#18, #31). The edge can vouch for a request
 * by adding a shared secret, and only then is the address believed.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { clientAddress, clientKey, RateLimiter } from '../src/ratelimit';

function request(headers: Record<string, string> = {}): Request {
  return new Request('https://x.example/api/q', { method: 'POST', headers });
}

const SECRET = 'a-secret-the-edge-adds';

describe('with no edge secret configured', () => {
  it('is the address Cloudflare names', () => {
    expect(clientKey(request({ 'cf-connecting-ip': '203.0.113.7' }), {})).toBe(
      '203.0.113.7',
    );
  });

  it('is one anonymous bucket when no address is named', () => {
    expect(clientKey(request(), {})).toBe(clientKey(request(), {}));
    expect(clientKey(request(), {})).not.toBe('');
  });

  it('treats an empty secret as none, as the entrypoint does', () => {
    expect(
      clientKey(request({ 'cf-connecting-ip': '203.0.113.7' }), { EDGE_SECRET: '' }),
    ).toBe('203.0.113.7');
  });
});

describe('with an edge secret configured', () => {
  const env = { EDGE_SECRET: SECRET };

  it('believes the address on a request carrying the secret', () => {
    const vouched = request({ 'cf-connecting-ip': '203.0.113.7', 'x-edge-secret': SECRET });
    expect(clientKey(vouched, env)).toBe('203.0.113.7');
  });

  it('puts every request without it in one bucket, whatever it claims', () => {
    const keys = ['198.51.100.1', '198.51.100.2', '203.0.113.7'].map((address) =>
      clientKey(request({ 'cf-connecting-ip': address }), env),
    );
    expect(new Set(keys).size).toBe(1);
    expect(keys[0]).not.toMatch(/\d+\.\d+\.\d+\.\d+/);
    // And not the anonymous bucket: a request the edge vouched for with
    // no address is still one the edge vouched for.
    expect(keys[0]).not.toBe(clientKey(request({ 'x-edge-secret': SECRET }), env));
  });

  it.each([
    ['a wrong secret', 'not-the-secret'],
    ['a prefix of the secret', SECRET.slice(0, -1)],
    ['the secret and more', `${SECRET}x`],
    ['an empty secret', ''],
  ])('does not believe %s', (_, given) => {
    const claimed = request({ 'cf-connecting-ip': '203.0.113.7', 'x-edge-secret': given });
    expect(clientKey(claimed, env)).toBe(
      clientKey(request({ 'cf-connecting-ip': '198.51.100.9' }), env),
    );
  });
});

describe('the address the edge vouched for (#115)', () => {
  /**
   * What else may be told the visitor's address — Turnstile's siteverify
   * — is told the one the limiters believe, or none: never one a client
   * chose for itself.
   */
  const env = { EDGE_SECRET: SECRET };

  it('is the address Cloudflare names, with no edge secret configured', () => {
    expect(clientAddress(request({ 'cf-connecting-ip': '203.0.113.7' }), {})).toBe(
      '203.0.113.7',
    );
  });

  it('is the address on a request carrying the secret', () => {
    const vouched = request({ 'cf-connecting-ip': '203.0.113.7', 'x-edge-secret': SECRET });
    expect(clientAddress(vouched, env)).toBe('203.0.113.7');
  });

  it.each([
    ['no secret', {}],
    ['a wrong secret', { 'x-edge-secret': 'not-the-secret' }],
  ])('is none on a request with %s, whatever it claims', (_, headers) => {
    const claimed = request({ 'cf-connecting-ip': '203.0.113.7', ...headers });
    expect(clientAddress(claimed, env)).toBeNull();
  });

  it('is none when no address is named', () => {
    expect(clientAddress(request(), {})).toBeNull();
    expect(clientAddress(request({ 'x-edge-secret': SECRET }), env)).toBeNull();
  });
});

describe('the limiter (#115)', () => {
  /**
   * The limiters counted in windows aligned to the wall clock, so a
   * client's budget came back whole at every multiple of the period: its
   * budget just before one and again just after, twice it in moments.
   * `RateLimiter` counts over a window that slides, with the same two
   * settings: at most `limit` requests from a client in `period` seconds.
   */
  const LIMIT = 20;
  const PERIOD = 60;
  /** A multiple of the period, where a fixed window would start. */
  const BOUNDARY = Date.parse('2026-09-14T10:01:00Z');
  const at = (seconds: number) => vi.setSystemTime(BOUNDARY + seconds * 1000);

  beforeEach(() => void vi.useFakeTimers({ toFake: ['Date'] }));
  afterEach(() => void vi.useRealTimers());

  /** A limiter over `kept`, as the runtime starts one, once it has loaded. */
  async function limiterOver(kept = new Map<string, unknown>()) {
    const loading: Promise<unknown>[] = [];
    const state = {
      storage: {
        list: async () =>
          new Map(
            [...kept]
              .sort(([a], [b]) => (a < b ? -1 : 1))
              .map(([key, value]) => [key, structuredClone(value)]),
          ),
        put: async (key: string, value: unknown) => void kept.set(key, structuredClone(value)),
        delete: async (keys: string[]) => keys.filter((key) => kept.delete(key)).length,
      },
      blockConcurrencyWhile: <T>(load: () => Promise<T>): Promise<T> => {
        const loaded = load();
        loading.push(loaded);
        return loaded;
      },
    };
    const limiter = new RateLimiter(state as unknown as DurableObjectState, {});
    await Promise.all(loading);
    return limiter;
  }

  /** How many of `count` requests from `client` are let through. */
  const burst = (limiter: RateLimiter, count: number, client = '203.0.113.7') =>
    Array.from({ length: count }, () => limiter.admit(client, LIMIT, PERIOD)).filter(Boolean)
      .length;

  it('lets a client’s budget through, and not one request more', async () => {
    const limiter = await limiterOver();
    at(10);
    expect(burst(limiter, LIMIT + 5)).toBe(LIMIT);
  });

  it('gives a burst across a window boundary one budget, not two', async () => {
    const limiter = await limiterOver();
    at(-0.001);
    expect(burst(limiter, LIMIT)).toBe(LIMIT);
    at(0.001);
    // A fixed window let all of these through: its count starts over
    // at the boundary, and the last burst was a moment ago.
    expect(burst(limiter, LIMIT)).toBe(0);
  });

  it('gives the budget back as the window slides past what it counted', async () => {
    const limiter = await limiterOver();
    at(-0.001);
    burst(limiter, LIMIT);
    // Half a period on, half of that burst is taken to have slid out.
    at(PERIOD / 2);
    expect(burst(limiter, LIMIT)).toBe(LIMIT / 2);
    // A whole period of quiet after it, all of it.
    at(2 * PERIOD + 1);
    expect(burst(limiter, LIMIT)).toBe(LIMIT);
  });

  it('counts nothing it refuses', async () => {
    // A client told to wait is not kept waiting longer for asking again.
    const limiter = await limiterOver();
    at(-0.001);
    burst(limiter, LIMIT + 50);
    at(PERIOD / 2);
    expect(burst(limiter, LIMIT)).toBe(LIMIT / 2);
  });

  it('keeps each client’s count apart', async () => {
    const limiter = await limiterOver();
    at(1);
    expect(burst(limiter, LIMIT, '203.0.113.7')).toBe(LIMIT);
    expect(burst(limiter, LIMIT, '198.51.100.9')).toBe(LIMIT);
    expect(burst(limiter, 1, '203.0.113.7')).toBe(0);
  });

  it('keeps its counts across a restart', async () => {
    // Under `wrangler dev` an object idle for ten seconds is evicted, and
    // one that kept its counts in memory alone forgot them every time:
    // a fresh budget for anyone who paused for ten seconds.
    const kept = new Map<string, unknown>();
    at(-0.001);
    burst(await limiterOver(kept), LIMIT);
    at(0.001);
    expect(burst(await limiterOver(kept), LIMIT)).toBe(0);
  });

  it('forgets a window once nothing it counted weighs any more', async () => {
    const kept = new Map<string, unknown>();
    const limiter = await limiterOver(kept);
    at(1);
    burst(limiter, LIMIT, '203.0.113.7');
    at(PERIOD + 1);
    burst(limiter, 1, '198.51.100.9');
    expect(kept.size).toBe(2);
    // Two periods on, the first window is two behind, and gone.
    at(2 * PERIOD + 1);
    expect(burst(limiter, 1, '192.0.2.1')).toBe(1);
    expect(kept.size).toBe(2);
    expect([...kept.keys()].some((key) => key.endsWith('203.0.113.7'))).toBe(false);
  });

  it('refuses everything under a setting that is not a number', async () => {
    // The Worker checks the settings; this does not take one on trust.
    const limiter = await limiterOver();
    at(1);
    expect(limiter.admit('203.0.113.7', Number.NaN, PERIOD)).toBe(false);
    expect(limiter.admit('203.0.113.7', LIMIT, Number.NaN)).toBe(false);
  });
});
