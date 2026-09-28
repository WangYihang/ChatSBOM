/**
 * Who a request is from, as far as a rate limiter is concerned.
 *
 * `CF-Connecting-IP` names the visitor when Cloudflare's edge set it.
 * Under `wrangler dev` behind a tunnel, a client that reaches the port
 * directly sets it too, to anything, and a new address per request was
 * a new budget per request (#18, #31). The edge can vouch for a request
 * by adding a shared secret, and only then is the address believed.
 */
import { describe, expect, it } from 'vitest';

import { clientKey } from '../src/ratelimit';

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
