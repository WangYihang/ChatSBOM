import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { Agent } from '../src/agent';
import {
  ChatError,
  estimateCostUsd,
  handleChat,
  parseChatRequest,
  spendDay,
  worstCaseUsd,
  type ChatEnv,
} from '../src/chat';
import type { DatasetClient } from '../src/d1/client';
import type { SpendCounter } from '../src/spend';
import { SYSTEM_PROMPT, TOOL_DEFINITIONS } from '../src/tools';
import { counters } from './counters';

const NOW = new Date('2026-09-14T10:00:00Z');
const ORIGIN = 'https://example.com';
const CONFIGURED = { ANTHROPIC_API_KEY: 'k' } as ChatEnv;
/** Turnstile's public half: what the page renders the widget with. */
const SITE_KEY = '0x4AAAAAAA-the-site-key';

/**
 * A request as the page sends it: JSON, from the page's own origin.
 *
 * The page never sets `origin` — it cannot, the header is the browser's
 * — but every browser adds it to a POST, so a request without it is not
 * one the page made.
 */
function post(body: unknown, headers: Record<string, string> = {}): Request {
  return new Request(`${ORIGIN}/api/chat`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', origin: ORIGIN, ...headers },
    body: JSON.stringify(body),
  });
}

/** An ExecutionContext that keeps what it is handed, so a test can await it. */
function executionContext() {
  const pending: Promise<unknown>[] = [];
  return {
    pending,
    waitUntil: (promise: Promise<unknown>) => void pending.push(promise),
  };
}

/* ---- conversations, shaped the way the page's own loop shapes them ---- */

const QUESTION = { role: 'user', content: 'who declares mail?' };

/**
 * An assistant turn that calls one tool, as the page posts it back: the
 * Worker's `content` verbatim, response-only fields (`citations`,
 * `caller`) included.
 */
function toolTurn(id = 'toolu_01', name = 'ecosystems_for') {
  return {
    role: 'assistant',
    content: [
      { type: 'thinking', thinking: 'Check the ecosystems first.', signature: 'sig' },
      { type: 'text', text: 'Looking it up.', citations: null },
      { type: 'tool_use', id, name, input: { name: 'mail' }, caller: { type: 'direct' } },
    ],
  };
}

function result(id: string, content = '{"rows":[]}') {
  return { type: 'tool_result', tool_use_id: id, content };
}

/** The user turn that answers tool calls, as the page builds it. */
function answering(...results: unknown[]) {
  return { role: 'user', content: results };
}

function conversation(...turns: unknown[]) {
  return { messages: [QUESTION, ...turns] };
}

const IMAGE = {
  type: 'image',
  source: { type: 'url', url: 'https://example.org/cat.png' },
};

/** What `parseChatRequest` refused with; a failure if it accepted. */
function refusal(body: unknown): ChatError {
  try {
    parseChatRequest(body);
  } catch (error) {
    if (error instanceof ChatError) return error;
    throw error;
  }
  throw new Error('parseChatRequest accepted it');
}

/* ---- the network, replaced ---- */

/** A reply from the Messages API, shaped as it arrives. */
function reply(id: string, stopReason: string, content: unknown[]) {
  return {
    id,
    type: 'message',
    role: 'assistant',
    model: 'claude-opus-5',
    content,
    stop_reason: stopReason,
    stop_sequence: null,
    usage: {
      input_tokens: 1_000,
      output_tokens: 100,
      cache_creation_input_tokens: 0,
      cache_read_input_tokens: 0,
    },
  };
}

const REPLY = reply('msg_1', 'end_turn', [
  { type: 'text', text: '17 projects declare it.', citations: null },
]);

function asJson(payload: unknown): Response {
  return new Response(JSON.stringify(payload), {
    headers: { 'content-type': 'application/json' },
  });
}

/**
 * The Messages API, answering `REPLY` and recording each request body.
 *
 * Matched on the path, not the host: the SDK honours ANTHROPIC_BASE_URL,
 * so where it sends depends on the environment the tests run in.
 */
function stubUpstream(): Record<string, unknown>[] {
  const sent: Record<string, unknown>[] = [];
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: string, init?: RequestInit) => {
      if (!String(input).endsWith('/v1/messages')) {
        throw new Error(`unexpected fetch in a test: ${String(input)}`);
      }
      sent.push(JSON.parse(String(init?.body)) as Record<string, unknown>);
      return asJson(REPLY);
    }),
  );
  return sent;
}

// Nothing here may reach the network. A test that forgets to stub the
// Messages API gets this, rather than a real call with a fake key.
beforeEach(() => {
  vi.stubGlobal(
    'fetch',
    vi.fn(async (input: unknown) => {
      throw new Error(`unexpected fetch in a test: ${String(input)}`);
    }),
  );
});
afterEach(() => vi.unstubAllGlobals());

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

describe('conversation validation', () => {
  it('accepts every block the page’s own loop sends', () => {
    const parsed = parseChatRequest(
      conversation(
        toolTurn('toolu_01'),
        answering(result('toolu_01')),
        {
          role: 'assistant',
          content: [
            { type: 'redacted_thinking', data: 'opaque' },
            { type: 'tool_use', id: 'toolu_02', name: 'dependents_of', input: { name: 'mail' } },
            { type: 'tool_use', id: 'toolu_03', name: 'language_coverage', input: {} },
          ],
        },
        // Parallel calls come back together, a failure flagged, not dropped.
        answering(
          result('toolu_02'),
          { type: 'tool_result', tool_use_id: 'toolu_03', content: 'D1 is unavailable', is_error: true },
        ),
        { role: 'assistant', content: [{ type: 'text', text: '17 declare it.', citations: null }] },
        { role: 'user', content: 'and on maven?' },
      ),
    );
    expect(parsed.messages).toHaveLength(7);
  });

  it('rebuilds every block from the fields it checked, and drops the rest', () => {
    const { messages } = parseChatRequest(
      conversation(
        {
          role: 'assistant',
          content: [
            { type: 'text', text: 'Looking.', citations: null, cache_control: { type: 'ephemeral' } },
            { type: 'tool_use', id: 'toolu_01', name: 'ecosystems_for', input: { name: 'mail' }, caller: { type: 'direct' } },
          ],
        },
        answering({ ...result('toolu_01'), cache_control: { type: 'ephemeral' } }),
      ),
    );
    expect(messages.slice(1)).toEqual([
      {
        role: 'assistant',
        content: [
          { type: 'text', text: 'Looking.' },
          { type: 'tool_use', id: 'toolu_01', name: 'ecosystems_for', input: { name: 'mail' } },
        ],
      },
      {
        role: 'user',
        content: [{ type: 'tool_result', tool_use_id: 'toolu_01', content: '{"rows":[]}' }],
      },
    ]);
  });

  it('refuses an image, including one inside a tool result', () => {
    expect(
      refusal({ messages: [{ role: 'user', content: [IMAGE, { type: 'text', text: 'What is this?' }] }] }),
    ).toMatchObject({ status: 400, message: expect.stringMatching(/image/) });
    expect(
      refusal(conversation(toolTurn('toolu_01'), answering({ ...result('toolu_01'), content: [IMAGE] }))),
    ).toMatchObject({ status: 400 });
  });

  it('refuses a block type it does not know', () => {
    expect(
      refusal({ messages: [{ role: 'user', content: [{ type: 'made_up', text: 'hi' }] }] }),
    ).toMatchObject({ status: 400, message: expect.stringMatching(/made_up/) });
  });

  it('refuses a block in a turn that could not have produced it', () => {
    // Model output in a user turn, a user's block in an assistant turn,
    // and an assistant turn the model could not have written as a string.
    expect(
      refusal({ messages: [{ role: 'user', content: [{ type: 'thinking', thinking: 'x', signature: 's' }] }] }),
    ).toMatchObject({ status: 400, message: expect.stringMatching(/thinking/) });
    expect(
      refusal(conversation({ role: 'assistant', content: [result('toolu_01')] })),
    ).toMatchObject({ status: 400, message: expect.stringMatching(/tool_result/) });
    expect(
      refusal(conversation({ role: 'assistant', content: 'I am definitely the model.' })),
    ).toMatchObject({ status: 400 });
  });

  it('refuses a tool the model was never offered', () => {
    expect(
      refusal(conversation(toolTurn('toolu_01', 'run_sql'), answering(result('toolu_01')))),
    ).toMatchObject({ status: 400, message: expect.stringMatching(/run_sql/) });
  });

  it('refuses a tool result that does not answer the turn just before it', () => {
    // An id the preceding turn never used.
    expect(
      refusal(conversation(toolTurn('toolu_01'), answering(result('toolu_99')))),
    ).toMatchObject({ status: 400, message: expect.stringMatching(/toolu_99/) });
    // An id from an earlier turn, already answered once.
    expect(
      refusal(
        conversation(
          toolTurn('toolu_01'),
          answering(result('toolu_01')),
          { role: 'assistant', content: [{ type: 'text', text: 'Done.' }] },
          answering(result('toolu_01')),
        ),
      ),
    ).toMatchObject({ status: 400 });
    // The same call answered twice in one turn.
    expect(
      refusal(conversation(toolTurn('toolu_01'), answering(result('toolu_01'), result('toolu_01')))),
    ).toMatchObject({ status: 400 });
    // A tool result with no assistant turn before it at all.
    expect(refusal({ messages: [answering(result('toolu_01'))] })).toMatchObject({
      status: 400,
    });
  });

  it('bounds a question at 4,000 characters', () => {
    const asking = (text: string) => ({ messages: [{ role: 'user', content: text }] });
    expect(() => parseChatRequest(asking('x'.repeat(4_000)))).not.toThrow();
    expect(refusal(asking('x'.repeat(4_001)))).toMatchObject({
      status: 400,
      message: expect.stringMatching(/too long/),
    });
  });

  it('bounds a tool result at 160 KiB, above the largest the page produces', () => {
    // The page cuts its own to RESULT_CHARS (tools.ts); before it did,
    // the largest was dependents_of at 500 rows: ~120 kB.
    const answered = (size: number) =>
      conversation(toolTurn('toolu_01'), answering(result('toolu_01', 'x'.repeat(size))));
    expect(() => parseChatRequest(answered(160 * 1024))).not.toThrow();
    expect(refusal(answered(160 * 1024 + 1))).toMatchObject({ status: 400 });
  });

  it('bounds model output at what one turn of MAX_TOKENS can hold', () => {
    const thought = (size: number) =>
      conversation({
        role: 'assistant',
        content: [{ type: 'thinking', thinking: 'x'.repeat(size), signature: 's' }],
      });
    expect(() => parseChatRequest(thought(8 * 8192))).not.toThrow();
    expect(refusal(thought(8 * 8192 + 1))).toMatchObject({ status: 400 });
  });

  it('bounds the conversation as a whole, not only block by block', () => {
    // Two tool results, each far inside its own cap, together over the total.
    const twoLookups = (size: number) =>
      conversation(
        toolTurn('toolu_01'),
        answering(result('toolu_01', 'x'.repeat(size))),
        toolTurn('toolu_02'),
        answering(result('toolu_02', 'x'.repeat(size))),
      );
    expect(() => parseChatRequest(twoLookups(99_000))).not.toThrow();
    expect(refusal(twoLookups(100_000))).toMatchObject({
      status: 400,
      message: expect.stringMatching(/Start a new one/),
    });
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

  it('prices cache writes at 1.25× the input rate', () => {
    // The five-minute TTL, which is the one the request asks for.
    const usd = estimateCostUsd({
      input_tokens: 0,
      output_tokens: 0,
      cache_creation_input_tokens: 1_000_000,
    } as never);
    expect(usd).toBeCloseTo(6.25, 5);
  });

  it('prices cache reads at a tenth of the input rate', () => {
    const usd = estimateCostUsd({
      input_tokens: 0,
      output_tokens: 0,
      cache_read_input_tokens: 1_000_000,
    } as never);
    expect(usd).toBeCloseTo(0.5, 5);
  });

  it('adds every kind of input to the output', () => {
    const usd = estimateCostUsd({
      input_tokens: 1_000_000,
      cache_creation_input_tokens: 1_000_000,
      cache_read_input_tokens: 1_000_000,
      output_tokens: 1_000_000,
    } as never);
    expect(usd).toBeCloseTo(5 + 6.25 + 0.5 + 25, 5);
  });
});

describe('what a turn is reserved at (#33)', () => {
  const asked = (...turns: unknown[]) => parseChatRequest(conversation(...turns)).messages;

  it('is at least the most the turn can cost', () => {
    // Every token is at least a byte of what is sent, the dearest input
    // is a cache write, and output stops at MAX_TOKENS.
    const messages = asked();
    const bytes = new TextEncoder().encode(
      SYSTEM_PROMPT + JSON.stringify(TOOL_DEFINITIONS) + JSON.stringify(messages),
    ).length;
    const dearest = estimateCostUsd({
      input_tokens: 0,
      cache_creation_input_tokens: bytes,
      cache_read_input_tokens: 0,
      output_tokens: 8192,
    } as never);
    expect(worstCaseUsd(messages)).toBeGreaterThanOrEqual(dearest);
  });

  it('grows with the conversation it is sent', () => {
    const short = worstCaseUsd(asked());
    const long = worstCaseUsd(
      asked(toolTurn('toolu_01'), answering(result('toolu_01', 'x'.repeat(20_000)))),
    );
    // 20,000 more characters: at least that many more tokens' worth.
    expect(long - short).toBeGreaterThanOrEqual(estimateCostUsd({
      input_tokens: 20_000, output_tokens: 0,
    } as never));
  });

  it('counts against the UTC day, which names its counter', () => {
    expect(spendDay(new Date('2026-09-14T23:59:59Z'))).toBe('2026-09-14');
    expect(spendDay(new Date('2026-09-15T00:00:01Z'))).toBe('2026-09-15');
  });
});

describe('handleChat', () => {
  beforeEach(() => vi.restoreAllMocks());

  it('rejects what is neither a question nor a request for the settings', async () => {
    // GET answers what the page must do before asking (#32); nothing
    // else but a POST is anything.
    const response = await handleChat(
      new Request('https://example.com/api/chat', { method: 'PUT', body: '{}' }),
      { ANTHROPIC_API_KEY: 'k' } as ChatEnv,
      executionContext(),
    );
    expect(response.status).toBe(405);
    expect(response.headers.get('Allow')).toBe('GET, POST');
  });

  it('reports plainly when AI answers are not configured', async () => {
    const response = await handleChat(
      post({ messages: [] }),
      {} as ChatEnv,
      executionContext(),
    );
    expect(response.status).toBe(503);
    await expect(response.json()).resolves.toMatchObject({
      error: expect.stringContaining('not configured'),
    });
  });

  it('refuses oversized requests before reading them', async () => {
    const response = await handleChat(
      post({ messages: [] }, { 'content-length': String(2 * 1024 * 1024) }),
      { ANTHROPIC_API_KEY: 'k' } as ChatEnv,
      executionContext(),
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
      executionContext(),
    );
    expect(response.status).toBe(429);
  });

  it('keys the limiter on an address only the edge vouched for', async () => {
    /**
     * #31, carried over from #18. A client that reaches wrangler's port
     * directly, rather than through the tunnel, sets `CF-Connecting-IP`
     * itself, and a new address on every question was a new budget on
     * every question. With EDGE_SECRET set, a request without it is one
     * of a single bucket, whatever address it claims.
     */
    const keys: string[] = [];
    const env = {
      ANTHROPIC_API_KEY: 'k',
      EDGE_SECRET: 'the-edge-secret',
      CHAT_RATE_LIMITER: {
        limit: async ({ key }: { key: string }) => {
          keys.push(key);
          return { success: false };
        },
      },
    } as unknown as ChatEnv;
    const question = { messages: [{ role: 'user', content: 'hi' }] };
    for (const address of ['198.51.100.1', '198.51.100.2']) {
      await handleChat(
        post(question, { 'cf-connecting-ip': address }),
        env,
        executionContext(),
      );
    }
    await handleChat(
      post(question, {
        'cf-connecting-ip': '198.51.100.3',
        'x-edge-secret': 'the-edge-secret',
      }),
      env,
      executionContext(),
    );
    expect(keys[0]).toBe(keys[1]);
    expect(keys[0]).not.toContain('198.51.100');
    expect(keys[2]).toBe('198.51.100.3');
  });

  it('requires a Turnstile token when a secret is configured', async () => {
    const env = {
      ANTHROPIC_API_KEY: 'k',
      TURNSTILE_SECRET: 's',
      TURNSTILE_SITE_KEY: SITE_KEY,
    } as ChatEnv;
    const response = await handleChat(
      post({ messages: [{ role: 'user', content: 'hi' }] }),
      env,
      executionContext(),
    );
    expect(response.status).toBe(400);
    // With what the page needs to pass: the widget's site key.
    await expect(response.json()).resolves.toMatchObject({
      turnstile: { siteKey: SITE_KEY },
    });
  });

  it('rejects a failing Turnstile token', async () => {
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => new Response(JSON.stringify({ success: false }))),
    );
    const env = {
      ANTHROPIC_API_KEY: 'k',
      TURNSTILE_SECRET: 's',
      TURNSTILE_SITE_KEY: SITE_KEY,
    } as ChatEnv;
    const response = await handleChat(
      post({
        messages: [{ role: 'user', content: 'hi' }],
        turnstileToken: 'bad',
      }),
      env,
      executionContext(),
    );
    expect(response.status).toBe(403);
  });

  it('rejects malformed JSON', async () => {
    const request = new Request(`${ORIGIN}/api/chat`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', origin: ORIGIN },
      body: '{oops',
    });
    const response = await handleChat(
      request,
      { ANTHROPIC_API_KEY: 'k' } as ChatEnv,
      executionContext(),
    );
    expect(response.status).toBe(400);
  });

  it('never caches a chat response', async () => {
    const response = await handleChat(
      post({ messages: [] }),
      {} as ChatEnv,
      executionContext(),
    );
    expect(response.headers.get('cache-control')).toBe('no-store');
  });
});

describe('handleChat: a request turned away is read first', () => {
  /**
   * Carried over from #31. Under `wrangler dev`, which serves this under
   * compose, a response sent with the request body unread lost the
   * connection now and then, and the dev proxy answered 500 in its
   * place: about one 429 in five on /api/q. The chat's early refusals
   * answered unread too.
   */
  const question = { messages: [QUESTION] };
  const refusals: Array<[string, () => Request, ChatEnv, number]> = [
    [
      'a method it does not take',
      () =>
        new Request(`${ORIGIN}/api/chat`, {
          method: 'PUT',
          headers: { 'content-type': 'application/json', origin: ORIGIN },
          body: JSON.stringify(question),
        }),
      CONFIGURED,
      405,
    ],
    ['a deployment with no key', () => post(question), {} as ChatEnv, 503],
    [
      'a request from another site',
      () =>
        post(question, {
          origin: 'https://elsewhere.example',
          'sec-fetch-site': 'cross-site',
        }),
      CONFIGURED,
      403,
    ],
    [
      'a body not declared as JSON',
      () => post(question, { 'content-type': 'text/plain;charset=UTF-8' }),
      CONFIGURED,
      415,
    ],
    [
      'a client over its budget',
      () => post(question),
      {
        ...CONFIGURED,
        CHAT_RATE_LIMITER: { limit: async () => ({ success: false }) },
      } as unknown as ChatEnv,
      429,
    ],
  ];

  it.each(refusals)('reads the body of %s before answering', async (_, make, env, status) => {
    const request = make();
    const response = await handleChat(request, env, executionContext());
    expect(response.status).toBe(status);
    expect(request.bodyUsed).toBe(true);
  });

  it('still leaves unread a body declared over the cap', async () => {
    // Reading it is what the cap is there to prevent.
    const request = post(question, { 'content-length': String(2 * 1024 * 1024) });
    const response = await handleChat(request, CONFIGURED, executionContext());
    expect(response.status).toBe(413);
    expect(request.bodyUsed).toBe(false);
  });
});

describe('handleChat: human verification (#32)', () => {
  /**
   * A Turnstile token is good for one siteverify, and a question is
   * several turns: the page resent one token every turn, so a second
   * turn could never pass. Now the first turn of a question carries a
   * token, which Cloudflare checks; its answer carries a session, which
   * the question's later turns present instead.
   */
  const SECRET = 'the-turnstile-secret';
  const TOKEN = 'a-token-cloudflare-issued';
  const SITEVERIFY = 'https://challenges.cloudflare.com/turnstile/v0/siteverify';
  const VERIFIED = {
    ...CONFIGURED,
    TURNSTILE_SECRET: SECRET,
    TURNSTILE_SITE_KEY: SITE_KEY,
  } as ChatEnv;

  /** The first turn of a question, and the turn after its tool call. */
  const FIRST = { messages: [QUESTION] };
  const SECOND = conversation(toolTurn('toolu_01'), answering(result('toolu_01')));

  /** `minutes` after NOW. */
  const after = (minutes: number) => new Date(NOW.getTime() + minutes * 60_000);

  /** Cloudflare's siteverify, passing TOKEN alone, and the Messages API. */
  function stubServices() {
    const verified: Record<string, unknown>[] = [];
    const sent: Record<string, unknown>[] = [];
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: string, init?: RequestInit) => {
        const body = JSON.parse(String(init?.body)) as Record<string, unknown>;
        if (String(input) === SITEVERIFY) {
          verified.push(body);
          return asJson({ success: body['response'] === TOKEN });
        }
        if (String(input).endsWith('/v1/messages')) {
          sent.push(body);
          return asJson(REPLY);
        }
        throw new Error(`unexpected fetch in a test: ${String(input)}`);
      }),
    );
    return { verified, sent };
  }

  async function ask(
    body: unknown,
    { env = VERIFIED, at = NOW, headers = {} }: {
      env?: ChatEnv;
      at?: Date;
      headers?: Record<string, string>;
    } = {},
  ): Promise<{ status: number; payload: Record<string, unknown> }> {
    const response = await handleChat(post(body, headers), env, executionContext(), at);
    return {
      status: response.status,
      payload: (await response.json()) as Record<string, unknown>,
    };
  }

  /** A session, as the answer to a verified first turn carries it. */
  async function session(
    options: { env?: ChatEnv; at?: Date; headers?: Record<string, string> } = {},
  ): Promise<string> {
    const { status, payload } = await ask({ ...FIRST, turnstileToken: TOKEN }, options);
    expect(status).toBe(200);
    expect(payload['session']).toEqual(expect.any(String));
    return payload['session'] as string;
  }

  describe('what the page is told before it asks', () => {
    const settings = (env: ChatEnv) =>
      handleChat(new Request(`${ORIGIN}/api/chat`), env, executionContext());

    it('is that nothing is needed, with Turnstile off', async () => {
      const response = await settings(CONFIGURED);
      expect(response.status).toBe(200);
      expect(response.headers.get('cache-control')).toBe('no-store');
      await expect(response.json()).resolves.toEqual({ turnstile: null });
    });

    it('is the site key to render the widget with, with Turnstile on', async () => {
      const response = await settings(VERIFIED);
      expect(response.status).toBe(200);
      const payload = await response.json();
      expect(payload).toEqual({ turnstile: { siteKey: SITE_KEY } });
      // The site key is public; the secret is not, and is not in it.
      expect(JSON.stringify(payload)).not.toContain(SECRET);
    });

    it('is that there is nothing to ask, without an API key', async () => {
      const response = await settings({} as ChatEnv);
      expect(response.status).toBe(503);
      await expect(response.json()).resolves.toMatchObject({
        error: expect.stringContaining('not configured'),
      });
    });

    it('is a refusal, with a secret and no site key to pass it with', async () => {
      // Every question would be refused for want of a token the page
      // could never get. Said once, here, rather than as a failed turn.
      const logged = vi.spyOn(console, 'error').mockImplementation(() => {});
      const env = { ...CONFIGURED, TURNSTILE_SECRET: SECRET } as ChatEnv;
      expect((await settings(env)).status).toBe(503);
      expect((await ask({ ...FIRST, turnstileToken: TOKEN }, { env })).status).toBe(503);
      expect(logged).toHaveBeenCalledWith(expect.stringContaining('TURNSTILE_SITE_KEY'));
    });
  });

  it('checks the first turn with Cloudflare, and answers it with a session', async () => {
    const { verified, sent } = stubServices();
    const { status, payload } = await ask(
      { ...FIRST, turnstileToken: TOKEN },
      { headers: { 'cf-connecting-ip': '203.0.113.7' } },
    );
    expect(status).toBe(200);
    expect(verified).toEqual([
      { secret: SECRET, response: TOKEN, remoteip: '203.0.113.7' },
    ]);
    expect(sent).toHaveLength(1);
    expect(payload).toMatchObject({ content: REPLY.content, session: expect.any(String) });
  });

  it('refuses a first turn whose token Cloudflare turns down, before the model is asked', async () => {
    const { sent } = stubServices();
    const { status, payload } = await ask({ ...FIRST, turnstileToken: 'not-a-token' });
    expect(status).toBe(403);
    expect(payload['session']).toBeUndefined();
    expect(sent).toHaveLength(0);
  });

  it('accepts the session for the later turns of that question, without Cloudflare', async () => {
    const { verified, sent } = stubServices();
    const token = await session();

    const { status, payload } = await ask({ ...SECOND, session: token }, { at: after(1) });

    expect(status).toBe(200);
    expect(payload['content']).toEqual(REPLY.content);
    expect(verified).toHaveLength(1);
    expect(sent).toHaveLength(2);
  });

  it('refuses a session once it has expired, and says how to pass again', async () => {
    stubServices();
    const token = await session();

    // Ten minutes: see SESSION_SECONDS in src/session.ts for why.
    expect((await ask({ ...SECOND, session: token }, { at: after(9) })).status).toBe(200);
    const expired = await ask({ ...SECOND, session: token }, { at: after(11) });

    expect(expired.status).toBe(403);
    expect(expired.payload).toMatchObject({ turnstile: { siteKey: SITE_KEY } });
  });

  it.each([
    ['nonsense', () => 'not-a-session'],
    ['a signature nobody made', (real: string) => `${real.split('.')[0]}.${'0'.repeat(64)}`],
    [
      'a real one with its expiry moved',
      (real: string) => {
        const [expiry, signature] = real.split('.');
        return `${Number(expiry) + 3600}.${signature}`;
      },
    ],
    [
      'a real one with its signature altered',
      (real: string) => real.slice(0, -1) + (real.endsWith('0') ? '1' : '0'),
    ],
  ])('refuses a forged session: %s', async (_, forge) => {
    const { sent } = stubServices();
    const real = await session();

    const { status, payload } = await ask({ ...SECOND, session: forge(real) });

    expect(status).toBe(403);
    expect(payload).toMatchObject({ turnstile: { siteKey: SITE_KEY } });
    expect(sent).toHaveLength(1);
  });

  it('refuses a session minted with another secret', async () => {
    stubServices();
    const theirs = await session({ env: { ...VERIFIED, TURNSTILE_SECRET: 'another-secret' } });
    expect((await ask({ ...SECOND, session: theirs })).status).toBe(403);
  });

  it('refuses a session presented for the next question in the same conversation', async () => {
    const { sent } = stubServices();
    const token = await session();
    const answered = { role: 'assistant', content: [{ type: 'text', text: '17 projects declare it.' }] };
    const next = { role: 'user', content: 'and on maven?' };

    // The next question's first turn, and a later turn of it.
    const first = await ask({ messages: [QUESTION, answered, next], session: token });
    const later = await ask({
      messages: [QUESTION, answered, next, toolTurn('toolu_09'), answering(result('toolu_09'))],
      session: token,
    });

    expect([first.status, later.status]).toEqual([403, 403]);
    expect(sent).toHaveLength(1);
  });

  it('refuses a session presented for the same question in another conversation', async () => {
    stubServices();
    const token = await session();
    const elsewhere = [
      { role: 'user', content: 'hello' },
      { role: 'assistant', content: [{ type: 'text', text: 'Hello.' }] },
      QUESTION,
    ];
    expect((await ask({ messages: elsewhere, session: token })).status).toBe(403);
  });

  it('refuses a session presented by another client', async () => {
    stubServices();
    const token = await session({ headers: { 'cf-connecting-ip': '203.0.113.7' } });

    const same = await ask({ ...SECOND, session: token }, { headers: { 'cf-connecting-ip': '203.0.113.7' } });
    const other = await ask({ ...SECOND, session: token }, { headers: { 'cf-connecting-ip': '198.51.100.9' } });

    expect([same.status, other.status]).toEqual([200, 403]);
  });

  it('binds a session to the client as the rate limiter sees it', async () => {
    // With EDGE_SECRET set, an address is believed only on a request
    // the edge vouched for (ratelimit.ts). Claiming the address without
    // the edge's word does not borrow its session.
    stubServices();
    const env = { ...VERIFIED, EDGE_SECRET: 'the-edge-secret' } as ChatEnv;
    const vouched = { 'cf-connecting-ip': '203.0.113.7', 'x-edge-secret': 'the-edge-secret' };
    const token = await session({ env, headers: vouched });

    const again = await ask({ ...SECOND, session: token }, { env, headers: vouched });
    const claimed = await ask(
      { ...SECOND, session: token },
      { env, headers: { 'cf-connecting-ip': '203.0.113.7' } },
    );

    expect([again.status, claimed.status]).toEqual([200, 403]);
  });

  it('takes a fresh token in place of a session it refuses', async () => {
    // How the page recovers from a session that lapsed mid-question.
    const { verified } = stubServices();
    const stale = await session();

    const { status, payload } = await ask(
      { ...SECOND, session: stale, turnstileToken: TOKEN },
      { at: after(11) },
    );

    expect(status).toBe(200);
    expect(verified).toHaveLength(2);
    expect(payload['session']).toEqual(expect.any(String));
    expect(payload['session']).not.toBe(stale);
  });

  it('refuses a token longer than any Cloudflare issues, without asking it', async () => {
    const { verified } = stubServices();
    const { status } = await ask({ ...FIRST, turnstileToken: 'x'.repeat(2049) });
    expect(status).toBe(403);
    expect(verified).toHaveLength(0);
  });

  it('with Turnstile off, needs neither and answers with no session', async () => {
    const { verified, sent } = stubServices();
    const first = await ask(FIRST, { env: CONFIGURED });
    const second = await ask({ ...SECOND, session: 'whatever' }, { env: CONFIGURED });
    expect([first.status, second.status]).toEqual([200, 200]);
    expect(first.payload['session']).toBeUndefined();
    expect(verified).toHaveLength(0);
    expect(sent).toHaveLength(2);
  });
});

describe('handleChat: what may reach the model', () => {
  beforeEach(() => vi.restoreAllMocks());

  it.each([
    [
      'an image',
      { messages: [{ role: 'user', content: [IMAGE, { type: 'text', text: 'What is this?' }] }] },
      /image/,
    ],
    [
      'a block type it does not know',
      { messages: [{ role: 'user', content: [{ type: 'made_up', text: 'hi' }] }] },
      /made_up/,
    ],
    [
      'a tool the page does not have',
      conversation(toolTurn('toolu_01', 'run_sql'), answering(result('toolu_01'))),
      /run_sql/,
    ],
  ])('refuses %s with 400, and the model never sees it', async (_, body, reason) => {
    const sent = stubUpstream();
    const response = await handleChat(post(body), CONFIGURED, executionContext());
    expect(response.status).toBe(400);
    await expect(response.json()).resolves.toMatchObject({
      error: expect.stringMatching(reason),
    });
    expect(sent).toHaveLength(0);
  });

  it('refuses an oversized body that declares no length, and stops reading it', async () => {
    const sent = stubUpstream();
    // A well-formed question padded with 2 MiB of JSON whitespace, sent as
    // a stream: no Content-Length, and nothing wrong with it but its size.
    const bytes = new TextEncoder().encode(
      JSON.stringify({ messages: [QUESTION] }) + ' '.repeat(2 * 1024 * 1024),
    );
    const chunk = 64 * 1024;
    let offset = 0;
    let cancelled = false;
    const body = new ReadableStream<Uint8Array>({
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
    });
    const request = new Request(`${ORIGIN}/api/chat`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', origin: ORIGIN },
      body,
      duplex: 'half',
    } as RequestInit);
    expect(request.headers.get('content-length')).toBeNull();

    const response = await handleChat(request, CONFIGURED, executionContext());

    expect(response.status).toBe(413);
    expect(sent).toHaveLength(0);
    // Abandoned at the chunk that crossed the cap, not buffered whole.
    expect(cancelled).toBe(true);
  });

  it('answers 400, not 500, when the client hangs up mid-body', async () => {
    const sent = stubUpstream();
    const request = new Request(`${ORIGIN}/api/chat`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', origin: ORIGIN },
      body: new ReadableStream<Uint8Array>({
        pull(controller) {
          controller.error(new Error('connection reset'));
        },
      }),
      duplex: 'half',
    } as RequestInit);
    const response = await handleChat(request, CONFIGURED, executionContext());
    expect(response.status).toBe(400);
    await expect(response.json()).resolves.toMatchObject({
      error: expect.stringMatching(/could not be read/),
    });
    expect(sent).toHaveLength(0);
  });

  it('refuses a request from another site', async () => {
    const sent = stubUpstream();
    const response = await handleChat(
      post(
        { messages: [QUESTION] },
        { origin: 'https://elsewhere.example', 'sec-fetch-site': 'cross-site' },
      ),
      CONFIGURED,
      executionContext(),
    );
    expect(response.status).toBe(403);
    expect(sent).toHaveLength(0);
  });

  it('refuses a request that says nothing of where it came from', async () => {
    // Every browser names the origin of a POST; a caller that names none
    // is not our page. See `isSameOrigin` for why that is refused.
    const sent = stubUpstream();
    const request = new Request(`${ORIGIN}/api/chat`, {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({ messages: [QUESTION] }),
    });
    const response = await handleChat(request, CONFIGURED, executionContext());
    expect(response.status).toBe(403);
    expect(sent).toHaveLength(0);
  });

  it('accepts Sec-Fetch-Site: same-origin without an Origin header', async () => {
    stubUpstream();
    const request = new Request(`${ORIGIN}/api/chat`, {
      method: 'POST',
      headers: { 'content-type': 'application/json', 'sec-fetch-site': 'same-origin' },
      body: JSON.stringify({ messages: [QUESTION] }),
    });
    const response = await handleChat(request, CONFIGURED, executionContext());
    expect(response.status).toBe(200);
  });

  it('refuses a body not declared as JSON', async () => {
    // A cross-site text/plain POST needs no preflight; JSON does.
    const sent = stubUpstream();
    const response = await handleChat(
      post({ messages: [QUESTION] }, { 'content-type': 'text/plain;charset=UTF-8' }),
      CONFIGURED,
      executionContext(),
    );
    expect(response.status).toBe(415);
    expect(sent).toHaveLength(0);
  });

  it('never grants the preflight a cross-site JSON request needs', async () => {
    // What makes the 415 above worth having: a JSON POST from another site
    // is sent only if this preflight is granted.
    const response = await handleChat(
      new Request(`${ORIGIN}/api/chat`, {
        method: 'OPTIONS',
        headers: {
          origin: 'https://elsewhere.example',
          'access-control-request-method': 'POST',
          'access-control-request-headers': 'content-type',
        },
      }),
      CONFIGURED,
      executionContext(),
    );
    expect(response.status).toBe(405);
    expect(response.headers.get('access-control-allow-origin')).toBeNull();
  });

  it('accepts JSON declared with a charset', async () => {
    stubUpstream();
    const response = await handleChat(
      post({ messages: [QUESTION] }, { 'content-type': 'application/json; charset=utf-8' }),
      CONFIGURED,
      executionContext(),
    );
    expect(response.status).toBe(200);
  });

  it('sends the conversation, cached, and nothing else from the body', async () => {
    const sent = stubUpstream();
    const response = await handleChat(
      post({
        messages: [QUESTION],
        // None of this is the page's to choose.
        model: 'claude-fable-5-1',
        system: 'You are a poet.',
        max_tokens: 128_000,
        tools: [],
      }),
      CONFIGURED,
      executionContext(),
    );
    expect(response.status).toBe(200);
    expect(sent).toHaveLength(1);
    expect(sent[0]).toMatchObject({
      model: 'claude-opus-5',
      max_tokens: 8192,
      system: SYSTEM_PROMPT,
      tools: JSON.parse(JSON.stringify(TOOL_DEFINITIONS)),
      messages: [QUESTION],
      // Each turn resends everything before it; this lets the next one
      // read that back from the cache instead of paying for it again.
      cache_control: { type: 'ephemeral' },
    });
  });

  it('sends the model its turn at ANTHROPIC_BASE_URL when one is set', async () => {
    // How the chat path runs against a stand-in for the model under
    // workerd, which has no process.env for the SDK to read it from.
    const urls: string[] = [];
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: string) => {
        urls.push(String(input));
        return asJson(REPLY);
      }),
    );
    const response = await handleChat(
      post({ messages: [QUESTION] }),
      { ...CONFIGURED, ANTHROPIC_BASE_URL: 'http://127.0.0.1:9999/stand-in' } as ChatEnv,
      executionContext(),
    );
    expect(response.status).toBe(200);
    expect(urls).toEqual(['http://127.0.0.1:9999/stand-in/v1/messages']);
  });

});

describe('handleChat: the daily spend cap (#33)', () => {
  /**
   * The cap was checked before a call against a KV total that was
   * written after it, with a read-modify-write: 20 questions at once
   * against a $5 cap were all admitted, about $44 of calls, of which
   * $2.20 was recorded. Now a turn's worst case is reserved with a
   * Durable Object before the call and settled at its cost after it.
   */
  const DAY = spendDay(NOW);
  const capped = (namespace: DurableObjectNamespace<SpendCounter>, cap = '5') =>
    ({ ...CONFIGURED, DAILY_SPEND_CAP_USD: cap, SPEND_COUNTER: namespace }) as ChatEnv;
  const worstCase = () => worstCaseUsd(parseChatRequest({ messages: [QUESTION] }).messages);
  const cost = () => estimateCostUsd(REPLY.usage as never);

  /** A day whose budget has been spent down to `left`. */
  async function spentDown(counter: SpendCounter, cap: number, left: number) {
    counter.reserve('earlier', cap - left, cap);
    counter.settle('earlier', cap - left);
  }

  it('settles an answered turn at what it cost', async () => {
    stubUpstream();
    const { namespace, counter } = counters();
    const ctx = executionContext();

    const response = await handleChat(post({ messages: [QUESTION] }), capped(namespace), ctx, NOW);

    expect(response.status).toBe(200);
    // Settled after the answer, never instead of it.
    expect(ctx.pending).toHaveLength(1);
    await Promise.all(ctx.pending);
    const usage = (await counter(DAY)).usage();
    expect(usage.held).toBe(0);
    expect(usage.spent).toBeCloseTo(cost(), 10);
  });

  it('answers 429 once what is left cannot cover a turn, without asking the model', async () => {
    const sent = stubUpstream();
    const { namespace, counter } = counters();
    await spentDown(await counter(DAY), 5, worstCase() / 2);
    const request = post({ messages: [QUESTION] });

    const response = await handleChat(request, capped(namespace), executionContext(), NOW);

    expect(response.status).toBe(429);
    await expect(response.json()).resolves.toMatchObject({
      error: expect.stringMatching(/daily budget/),
    });
    expect(sent).toHaveLength(0);
    // Read before it is refused, as every refusal here is (#31).
    expect(request.bodyUsed).toBe(true);
  });

  it('admits no more questions at once than the cap can pay for', async () => {
    const { namespace, counter } = counters();
    const sent: string[] = [];
    let release = () => {};
    const answered = new Promise<void>((resolve) => (release = resolve));
    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: string) => {
        if (!String(input).endsWith('/v1/messages')) {
          throw new Error(`unexpected fetch in a test: ${String(input)}`);
        }
        sent.push(String(input));
        await answered;
        return asJson(REPLY);
      }),
    );
    const cap = 1;
    const fits = Math.floor(cap / worstCase());
    expect(fits).toBeGreaterThan(0);
    const ctx = executionContext();
    const statuses: number[] = [];

    try {
      const questions = Array.from({ length: 20 }, () =>
        handleChat(post({ messages: [QUESTION] }), capped(namespace, String(cap)), ctx, NOW).then(
          (response) => void statuses.push(response.status),
        ),
      );
      // A refusal waits on nothing; an admitted question waits on the model.
      await vi.waitFor(() => expect(statuses.length + sent.length).toBe(20));
      expect(sent).toHaveLength(fits);
      release();
      await Promise.all(questions);
    } finally {
      release();
    }

    expect(statuses.filter((status) => status === 200)).toHaveLength(fits);
    expect(statuses.filter((status) => status === 429)).toHaveLength(20 - fits);
    await Promise.all(ctx.pending);
    const usage = (await counter(DAY)).usage();
    expect(usage.held).toBe(0);
    expect(usage.spent).toBeCloseTo(fits * cost(), 10);
    expect(usage.spent).toBeLessThanOrEqual(cap);
  });

  it('refunds a turn the model refused', async () => {
    // An error the API answered with: nothing was generated, so nothing
    // was billed. A 400, which the SDK does not retry.
    vi.stubGlobal(
      'fetch',
      vi.fn(async () =>
        new Response(
          JSON.stringify({ type: 'error', error: { type: 'invalid_request_error', message: 'no' } }),
          { status: 400, headers: { 'content-type': 'application/json' } },
        ),
      ),
    );
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const { namespace, counter } = counters();
    const ctx = executionContext();

    const response = await handleChat(post({ messages: [QUESTION] }), capped(namespace), ctx, NOW);

    expect(response.status).toBe(502);
    await Promise.all(ctx.pending);
    expect((await counter(DAY)).usage()).toEqual({ spent: 0, held: 0 });
  });

  it('keeps holding the worst case of a call lost on the way', async () => {
    // A call that never came back may have been answered, and billed:
    // refunding it would be the one way past the cap.
    vi.stubGlobal(
      'fetch',
      vi.fn(async () => {
        throw new TypeError('fetch failed');
      }),
    );
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const { namespace, counter } = counters();
    const ctx = executionContext();

    const response = await handleChat(post({ messages: [QUESTION] }), capped(namespace), ctx, NOW);

    expect(response.status).toBe(502);
    await Promise.all(ctx.pending);
    const usage = (await counter(DAY)).usage();
    expect(usage.spent).toBe(0);
    expect(usage.held).toBeCloseTo(worstCase(), 10);
  }, 15_000);

  it('starts each UTC day at nothing', async () => {
    stubUpstream();
    const { namespace, counter } = counters();
    await spentDown(await counter('2026-09-14'), 5, worstCase() / 2);
    const ctx = executionContext();

    const lastSecond = await handleChat(
      post({ messages: [QUESTION] }),
      capped(namespace),
      executionContext(),
      new Date('2026-09-14T23:59:59Z'),
    );
    const nextDay = await handleChat(
      post({ messages: [QUESTION] }),
      capped(namespace),
      ctx,
      new Date('2026-09-15T00:00:01Z'),
    );
    await Promise.all(ctx.pending);

    expect([lastSecond.status, nextDay.status]).toEqual([429, 200]);
    expect((await counter('2026-09-15')).usage().spent).toBeCloseTo(cost(), 10);
  });

  it('settles a turn against the day that took it, whatever the day is by then', async () => {
    // The reservation was made against that day's cap, so the call's
    // cost belongs to it; the next day starts clean.
    stubUpstream();
    const { namespace, counter, days } = counters();
    const ctx = executionContext();

    await handleChat(
      post({ messages: [QUESTION] }),
      capped(namespace),
      ctx,
      new Date('2026-09-14T23:59:59Z'),
    );
    await Promise.all(ctx.pending);

    expect((await counter('2026-09-14')).usage()).toEqual({ spent: cost(), held: 0 });
    expect([...days.keys()]).toEqual(['2026-09-14']);
  });

  it('still answers when the turn cannot be settled', async () => {
    // The model is paid for either way; a counter that fails to hear so
    // must not turn the answer into a 500. Its worst case stays held.
    stubUpstream();
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {});
    const failing = {
      getByName: () => ({
        reserve: async () => true,
        settle: async () => {
          throw new Error('Durable Object reset because its code was updated.');
        },
        refund: async () => {},
      }),
    } as unknown as DurableObjectNamespace<SpendCounter>;
    const ctx = executionContext();

    const response = await handleChat(post({ messages: [QUESTION] }), capped(failing), ctx, NOW);

    expect(response.status).toBe(200);
    await expect(response.json()).resolves.toMatchObject({ content: REPLY.content });
    await expect(Promise.all(ctx.pending)).resolves.toBeDefined();
    expect(logged).toHaveBeenCalledWith('spend not settled', expect.any(Error));
  });

  it('refuses when the counter cannot be reached, rather than go uncounted', async () => {
    const sent = stubUpstream();
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const unreachable = {
      getByName: () => ({
        reserve: async () => {
          throw new Error('Network connection lost.');
        },
        refund: async () => {},
      }),
    } as unknown as DurableObjectNamespace<SpendCounter>;

    const response = await handleChat(post({ messages: [QUESTION] }), capped(unreachable), executionContext(), NOW);

    expect(response.status).toBe(503);
    expect(sent).toHaveLength(0);
  });

  it('refuses every question when a cap is set and nothing counts against it', async () => {
    const sent = stubUpstream();
    const logged = vi.spyOn(console, 'error').mockImplementation(() => {});
    const response = await handleChat(
      post({ messages: [QUESTION] }),
      { ...CONFIGURED, DAILY_SPEND_CAP_USD: '5' } as ChatEnv,
      executionContext(),
      NOW,
    );
    expect(response.status).toBe(503);
    expect(sent).toHaveLength(0);
    expect(logged).toHaveBeenCalledWith(expect.stringContaining('SPEND_COUNTER'));
  });

  it('says so when the page asks what a question needs, before it solves a challenge for one', async () => {
    vi.spyOn(console, 'error').mockImplementation(() => {});
    const response = await handleChat(
      new Request(`${ORIGIN}/api/chat`),
      { ...CONFIGURED, DAILY_SPEND_CAP_USD: '5' } as ChatEnv,
      executionContext(),
    );
    expect(response.status).toBe(503);
  });

  it.each(['five', '-1', 'Infinity'])(
    'refuses every question under a cap of %j, rather than lifting it',
    async (cap) => {
      const sent = stubUpstream();
      vi.spyOn(console, 'error').mockImplementation(() => {});
      const { namespace } = counters();
      const response = await handleChat(
        post({ messages: [QUESTION] }),
        capped(namespace, cap),
        executionContext(),
        NOW,
      );
      expect(response.status).toBe(503);
      expect(sent).toHaveLength(0);
    },
  );

  it.each([undefined, '', '0'])('has no cap, and needs no counter, at %j', async (cap) => {
    stubUpstream();
    const env = { ...CONFIGURED, ...(cap === undefined ? {} : { DAILY_SPEND_CAP_USD: cap }) } as ChatEnv;
    const ctx = executionContext();
    const response = await handleChat(post({ messages: [QUESTION] }), env, ctx, NOW);
    expect(response.status).toBe(200);
    expect(ctx.pending).toHaveLength(0);
  });
});

describe('the page’s own conversations', () => {
  /** Tools that answer the way the dataset does, one of them failing. */
  function dataset(): DatasetClient {
    return {
      ecosystemsFor: async () => [{ ecosystem: 'gem', repositories: 118 }],
      searchPackages: async () => [
        { name: 'mail', repositoryCount: 118, directCount: 17 },
      ],
      // dependents_of counts as well as lists, and both go to D1.
      dependentsOf: async () => {
        throw new Error('D1 is unavailable');
      },
      countDependents: async () => {
        throw new Error('D1 is unavailable');
      },
    } as unknown as DatasetClient;
  }

  it('pass validation turn after turn, and reach the model intact', async () => {
    // Four model turns across two questions: reasoning, prose, parallel
    // calls, redacted reasoning, a tool that fails, and a follow-up.
    const replies = [
      reply('msg_1', 'tool_use', [
        { type: 'thinking', thinking: 'A name can span ecosystems.', signature: 'sig-1' },
        { type: 'text', text: 'Checking the ecosystems first.', citations: null },
        { type: 'tool_use', id: 'toolu_01', name: 'ecosystems_for', input: { name: 'mail' }, caller: { type: 'direct' } },
        { type: 'tool_use', id: 'toolu_02', name: 'search_packages', input: { fragment: 'mail' }, caller: { type: 'direct' } },
      ]),
      reply('msg_2', 'tool_use', [
        { type: 'redacted_thinking', data: 'opaque-2' },
        { type: 'tool_use', id: 'toolu_03', name: 'dependents_of', input: { name: 'mail', type: 'gem', direct_only: true }, caller: { type: 'direct' } },
      ]),
      reply('msg_3', 'end_turn', [
        { type: 'thinking', thinking: 'The lookup failed; say so.', signature: 'sig-3' },
        { type: 'text', text: 'The lookup failed, so I cannot give a count.', citations: null },
      ]),
      reply('msg_4', 'end_turn', [
        { type: 'text', text: 'Six Maven projects do.', citations: null },
      ]),
    ];
    const posted: Array<{ messages: unknown[] }> = [];
    const upstream: Array<{ messages: unknown[] }> = [];

    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: string, init: RequestInit) => {
        if (input === '/api/chat') {
          // The browser's part: resolve against the page, name the origin.
          posted.push(JSON.parse(String(init.body)) as { messages: unknown[] });
          return handleChat(
            new Request(`${ORIGIN}${input}`, {
              ...init,
              headers: { ...(init.headers as Record<string, string>), origin: ORIGIN },
            }),
            CONFIGURED,
            executionContext(),
          );
        }
        if (input.endsWith('/v1/messages')) {
          upstream.push(JSON.parse(String(init.body)) as { messages: unknown[] });
          return asJson(replies.shift());
        }
        throw new Error(`unexpected fetch in a test: ${input}`);
      }),
    );

    const agent = new Agent(dataset());
    await expect(agent.ask('Who declares mail?')).resolves.toBe(
      'The lookup failed, so I cannot give a count.',
    );
    await expect(agent.ask('And on Maven?')).resolves.toBe('Six Maven projects do.');

    // Every turn the page posted was accepted and passed on whole...
    expect(posted.map((body) => body.messages.length)).toEqual([1, 3, 5, 7]);
    expect(upstream.map((body) => body.messages.length)).toEqual([1, 3, 5, 7]);
    // ...with what the API checks unchanged: signatures, tool ids, errors.
    expect(upstream.at(-1)!.messages).toMatchObject([
      { role: 'user', content: 'Who declares mail?' },
      {
        role: 'assistant',
        content: [
          { type: 'thinking', thinking: 'A name can span ecosystems.', signature: 'sig-1' },
          { type: 'text', text: 'Checking the ecosystems first.' },
          { type: 'tool_use', id: 'toolu_01', name: 'ecosystems_for', input: { name: 'mail' } },
          { type: 'tool_use', id: 'toolu_02', name: 'search_packages', input: { fragment: 'mail' } },
        ],
      },
      {
        role: 'user',
        content: [
          { type: 'tool_result', tool_use_id: 'toolu_01' },
          { type: 'tool_result', tool_use_id: 'toolu_02' },
        ],
      },
      {
        role: 'assistant',
        content: [
          { type: 'redacted_thinking', data: 'opaque-2' },
          { type: 'tool_use', id: 'toolu_03', name: 'dependents_of' },
        ],
      },
      {
        role: 'user',
        content: [
          { type: 'tool_result', tool_use_id: 'toolu_03', content: 'D1 is unavailable', is_error: true },
        ],
      },
      {
        role: 'assistant',
        content: [
          { type: 'thinking', signature: 'sig-3' },
          { type: 'text', text: 'The lookup failed, so I cannot give a count.' },
        ],
      },
      { role: 'user', content: 'And on Maven?' },
    ]);
  });

  it('keep large lookups inside what the Worker accepts', async () => {
    // Two dependents_of calls at limit 500, as a model asked who uses
    // `react` makes them. Each came back as 500 rows, over 100,000
    // characters, and the turn carrying the second was refused.
    const rows = Array.from({ length: 500 }, (_, index) => ({
      owner: `owner-${index}`,
      repo: `project-${index}`,
      stars: 250_000 - index,
      version: '18.3.1',
      url: `https://github.com/owner-${index}/project-${index}`,
      relationship: 'transitive',
      observedAt: '2026-09-13',
      ecosystem: 'npm',
      language: 'typescript',
      manifests: 1,
    }));
    const lookups = {
      dependentsOf: async (query: { limit?: number }) =>
        rows.slice(0, query.limit ?? 50),
      countDependents: async (query: { directOnly?: boolean }) =>
        query.directOnly ? 1_207 : 5_095,
    } as unknown as DatasetClient;
    const replies = [
      reply('msg_1', 'tool_use', [
        { type: 'tool_use', id: 'toolu_01', name: 'dependents_of', input: { name: 'react', limit: 500 } },
      ]),
      reply('msg_2', 'tool_use', [
        { type: 'tool_use', id: 'toolu_02', name: 'dependents_of', input: { name: 'react', type: 'npm', limit: 500 } },
      ]),
      reply('msg_3', 'end_turn', [
        { type: 'text', text: '5,095 repositories depend on react.', citations: null },
      ]),
    ];
    const upstream: Array<{ messages: Array<{ content: unknown }> }> = [];

    vi.stubGlobal(
      'fetch',
      vi.fn(async (input: string, init: RequestInit) => {
        if (input === '/api/chat') {
          return handleChat(
            new Request(`${ORIGIN}${input}`, {
              ...init,
              headers: { ...(init.headers as Record<string, string>), origin: ORIGIN },
            }),
            CONFIGURED,
            executionContext(),
          );
        }
        if (input.endsWith('/v1/messages')) {
          upstream.push(JSON.parse(String(init.body)) as (typeof upstream)[number]);
          return asJson(replies.shift());
        }
        throw new Error(`unexpected fetch in a test: ${input}`);
      }),
    );

    await expect(new Agent(lookups).ask('Who uses react?')).resolves.toBe(
      '5,095 repositories depend on react.',
    );

    // Both results reached the model, and each says what the count is.
    const [, , first, , second] = upstream.at(-1)!.messages;
    const resultOf = (message: { content: unknown } | undefined) =>
      JSON.parse((message!.content as Array<{ content: string }>)[0]!.content) as unknown;
    expect([resultOf(first), resultOf(second)]).toMatchObject([
      { total: 5_095, direct_total: 1_207 },
      { total: 5_095, direct_total: 1_207 },
    ]);
  });
});
