import { beforeEach, describe, expect, it, vi } from 'vitest';

import {
  ChatError,
  checkSpendCap,
  estimateCostUsd,
  handleChat,
  parseChatRequest,
  recordSpend,
  spendKey,
  type ChatEnv,
} from '../src/chat';

const NOW = new Date('2026-09-14T10:00:00Z');

function post(body: unknown, headers: Record<string, string> = {}): Request {
  return new Request('https://example.com/api/chat', {
    method: 'POST',
    headers: { 'content-type': 'application/json', ...headers },
    body: JSON.stringify(body),
  });
}

function memoryKv(initial: Record<string, string> = {}) {
  const store = new Map(Object.entries(initial));
  return {
    store,
    get: async (k: string) => store.get(k) ?? null,
    put: async (k: string, v: string) => void store.set(k, v),
  } as unknown as KVNamespace;
}

describe('request validation', () => {
  it('rejects a non-object body', () => {
    expect(() => parseChatRequest('nope')).toThrow(ChatError);
  });

  it('rejects an empty conversation', () => {
    expect(() => parseChatRequest({ messages: [] })).toThrow(/non-empty/);
  });

  it('rejects an over-long conversation rather than paying for it', () => {
    const messages = Array.from({ length: 41 }, () => ({
      role: 'user', content: 'hi',
    }));
    expect(() => parseChatRequest({ messages })).toThrow(/too long/);
  });

  it('rejects an unexpected role', () => {
    expect(() =>
      parseChatRequest({ messages: [{ role: 'system', content: 'x' }] }),
    ).toThrow(/role/);
  });

  it('accepts a well-formed conversation', () => {
    const parsed = parseChatRequest({
      messages: [{ role: 'user', content: 'who uses mail' }],
      turnstileToken: 'tok',
    });
    expect(parsed.messages).toHaveLength(1);
    expect(parsed.turnstileToken).toBe('tok');
  });
});

describe('cost estimation', () => {
  it('prices input and output separately', () => {
    const usd = estimateCostUsd({
      input_tokens: 1_000_000,
      output_tokens: 1_000_000,
    } as never);
    expect(usd).toBeCloseTo(30, 5);
  });

  it('counts cached input tokens too', () => {
    const usd = estimateCostUsd({
      input_tokens: 0,
      output_tokens: 0,
      cache_read_input_tokens: 1_000_000,
    } as never);
    expect(usd).toBeCloseTo(5, 5);
  });
});

describe('daily spend cap', () => {
  it('is a no-op when unconfigured', async () => {
    await expect(checkSpendCap({} as ChatEnv, NOW)).resolves.toBeUndefined();
  });

  it('allows requests below the cap', async () => {
    const env = {
      SPEND: memoryKv({ [spendKey(NOW)]: '1.0' }),
      DAILY_SPEND_CAP_USD: '5',
    } as ChatEnv;
    await expect(checkSpendCap(env, NOW)).resolves.toBeUndefined();
  });

  it('refuses once the cap is reached', async () => {
    const env = {
      SPEND: memoryKv({ [spendKey(NOW)]: '5.0' }),
      DAILY_SPEND_CAP_USD: '5',
    } as ChatEnv;
    await expect(checkSpendCap(env, NOW)).rejects.toThrow(/budget/);
  });

  it('accumulates spend under a per-day key', async () => {
    const kv = memoryKv();
    const env = { SPEND: kv, DAILY_SPEND_CAP_USD: '5' } as ChatEnv;
    await recordSpend(env, NOW, 0.25);
    await recordSpend(env, NOW, 0.25);
    expect(await kv.get(spendKey(NOW))).toBe('0.5');
  });

  it('keys by UTC day so the reset boundary is unambiguous', () => {
    expect(spendKey(new Date('2026-09-14T23:59:59Z'))).toBe('spend:2026-09-14');
    expect(spendKey(new Date('2026-09-15T00:00:01Z'))).toBe('spend:2026-09-15');
  });
});

describe('handleChat', () => {
  beforeEach(() => vi.restoreAllMocks());

  it('rejects non-POST', async () => {
    const response = await handleChat(
      new Request('https://example.com/api/chat'),
      { ANTHROPIC_API_KEY: 'k' } as ChatEnv,
    );
    expect(response.status).toBe(405);
    expect(response.headers.get('Allow')).toBe('POST');
  });

  it('reports plainly when AI answers are not configured', async () => {
    const response = await handleChat(post({ messages: [] }), {} as ChatEnv);
    expect(response.status).toBe(503);
    await expect(response.json()).resolves.toMatchObject({
      error: expect.stringContaining('not configured'),
    });
  });

  it('refuses oversized requests before reading them', async () => {
    const response = await handleChat(
      post({ messages: [] }, { 'content-length': String(2 * 1024 * 1024) }),
      { ANTHROPIC_API_KEY: 'k' } as ChatEnv,
    );
    expect(response.status).toBe(413);
  });

  it('throttles when the rate limiter says no', async () => {
    const env = {
      ANTHROPIC_API_KEY: 'k',
      CHAT_RATE_LIMITER: { limit: async () => ({ success: false }) },
    } as unknown as ChatEnv;
    const response = await handleChat(
      post({ messages: [{ role: 'user', content: 'hi' }] }),
      env,
    );
    expect(response.status).toBe(429);
  });

  it('requires a Turnstile token when a secret is configured', async () => {
    const env = {
      ANTHROPIC_API_KEY: 'k',
      TURNSTILE_SECRET: 's',
    } as ChatEnv;
    const response = await handleChat(
      post({ messages: [{ role: 'user', content: 'hi' }] }),
      env,
    );
    expect(response.status).toBe(400);
  });

  it('rejects a failing Turnstile token', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => new Response(JSON.stringify({ success: false }))),
    );
    const env = {
      ANTHROPIC_API_KEY: 'k',
      TURNSTILE_SECRET: 's',
    } as ChatEnv;
    const response = await handleChat(
      post({
        messages: [{ role: 'user', content: 'hi' }],
        turnstileToken: 'bad',
      }),
      env,
    );
    expect(response.status).toBe(403);
  });

  it('rejects malformed JSON', async () => {
    const request = new Request('https://example.com/api/chat', {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: '{oops',
    });
    const response = await handleChat(request, {
      ANTHROPIC_API_KEY: 'k',
    } as ChatEnv);
    expect(response.status).toBe(400);
  });

  it('never caches a chat response', async () => {
    const response = await handleChat(post({ messages: [] }), {} as ChatEnv);
    expect(response.headers.get('cache-control')).toBe('no-store');
  });
});
