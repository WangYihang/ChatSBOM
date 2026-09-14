/**
 * The chat endpoint: an authenticated relay to the Messages API.
 *
 * The dataset is in the browser, so the Worker cannot run the tools. It
 * performs exactly one model turn per request and hands the response back;
 * the page executes any `tool_use` blocks against DuckDB and posts the
 * results for the next turn. The loop lives on the client, which means the
 * Worker never holds the API key *and* the data at the same time — it
 * never sees query results at all.
 *
 * What the Worker is responsible for is everything the client cannot be
 * trusted with: the API key, verifying the visitor, bounding the request,
 * and capping spend.
 */
import Anthropic from '@anthropic-ai/sdk';

import { SYSTEM_PROMPT, TOOL_DEFINITIONS } from './tools';

export interface ChatEnv {
  ANTHROPIC_API_KEY: string;
  TURNSTILE_SECRET?: string;
  CHAT_RATE_LIMITER?: RateLimit;
  DAILY_SPEND_CAP_USD?: string;
  SPEND?: KVNamespace;
}

/** A model turn's worth of conversation, as posted by the page. */
export interface ChatRequest {
  messages: Anthropic.MessageParam[];
  /** Turnstile token, required when TURNSTILE_SECRET is configured. */
  turnstileToken?: string;
}

const MODEL = 'claude-opus-5';
const MAX_TOKENS = 8192;

/** Bounds on what a client may submit, so one request cannot be huge. */
const MAX_MESSAGES = 40;
const MAX_REQUEST_BYTES = 256 * 1024;

/** Published per-MTok rates for the model above, for the spend cap. */
const INPUT_USD_PER_MTOK = 5;
const OUTPUT_USD_PER_MTOK = 25;

export function estimateCostUsd(usage: Anthropic.Usage): number {
  const input =
    usage.input_tokens +
    (usage.cache_creation_input_tokens ?? 0) +
    (usage.cache_read_input_tokens ?? 0);
  return (
    (input / 1_000_000) * INPUT_USD_PER_MTOK +
    (usage.output_tokens / 1_000_000) * OUTPUT_USD_PER_MTOK
  );
}

/** UTC day key, so the cap resets on a boundary both sides agree on. */
export function spendKey(now: Date): string {
  return `spend:${now.toISOString().slice(0, 10)}`;
}

export class ChatError extends Error {
  constructor(
    readonly status: number,
    message: string,
  ) {
    super(message);
  }
}

export function parseChatRequest(body: unknown): ChatRequest {
  if (typeof body !== 'object' || body === null) {
    throw new ChatError(400, 'Expected a JSON object.');
  }
  const { messages, turnstileToken } = body as Record<string, unknown>;

  if (!Array.isArray(messages) || messages.length === 0) {
    throw new ChatError(400, 'Expected a non-empty messages array.');
  }
  if (messages.length > MAX_MESSAGES) {
    throw new ChatError(
      400,
      `Conversation too long: ${messages.length} messages, limit ${MAX_MESSAGES}. Start a new one.`,
    );
  }
  for (const message of messages) {
    const role = (message as Record<string, unknown>)['role'];
    if (role !== 'user' && role !== 'assistant') {
      throw new ChatError(400, `Unexpected message role: ${String(role)}`);
    }
  }

  return {
    messages: messages as Anthropic.MessageParam[],
    ...(typeof turnstileToken === 'string' ? { turnstileToken } : {}),
  };
}

export async function verifyTurnstile(
  secret: string,
  token: string | undefined,
  remoteIp: string | null,
): Promise<void> {
  if (!token) {
    throw new ChatError(400, 'Human verification is required.');
  }

  const response = await fetch(
    'https://challenges.cloudflare.com/turnstile/v0/siteverify',
    {
      method: 'POST',
      headers: { 'content-type': 'application/json' },
      body: JSON.stringify({
        secret,
        response: token,
        ...(remoteIp ? { remoteip: remoteIp } : {}),
      }),
    },
  );

  const result = (await response.json()) as { success?: boolean };
  if (!result.success) {
    throw new ChatError(403, 'Human verification failed. Reload and retry.');
  }
}

/**
 * Refuse once the day's spend cap is reached.
 *
 * Read-modify-write on KV is not atomic, so concurrent requests can
 * overshoot slightly. That is acceptable for a backstop whose job is to
 * stop a runaway from becoming a large bill, and the alternative — a
 * Durable Object per day — is more machinery than the guarantee is worth.
 */
export async function checkSpendCap(env: ChatEnv, now: Date): Promise<void> {
  const cap = Number(env.DAILY_SPEND_CAP_USD ?? '0');
  if (!env.SPEND || !Number.isFinite(cap) || cap <= 0) return;

  const spent = Number((await env.SPEND.get(spendKey(now))) ?? '0');
  if (spent >= cap) {
    throw new ChatError(
      429,
      'The daily budget for AI answers is used up. The dashboard itself still works.',
    );
  }
}

export async function recordSpend(
  env: ChatEnv,
  now: Date,
  usd: number,
): Promise<void> {
  if (!env.SPEND || usd <= 0) return;
  const key = spendKey(now);
  const spent = Number((await env.SPEND.get(key)) ?? '0');
  await env.SPEND.put(key, String(spent + usd), {
    expirationTtl: 60 * 60 * 48,
  });
}

export async function handleChat(
  request: Request,
  env: ChatEnv,
  now: Date = new Date(),
): Promise<Response> {
  if (request.method !== 'POST') {
    return json({ error: 'Method not allowed' }, 405, {
      Allow: 'POST',
    });
  }
  if (!env.ANTHROPIC_API_KEY) {
    return json(
      { error: 'AI answers are not configured on this deployment.' },
      503,
    );
  }

  const length = Number(request.headers.get('content-length') ?? '0');
  if (length > MAX_REQUEST_BYTES) {
    return json({ error: 'Request too large.' }, 413);
  }

  const clientIp = request.headers.get('cf-connecting-ip');

  try {
    if (env.CHAT_RATE_LIMITER) {
      const { success } = await env.CHAT_RATE_LIMITER.limit({
        key: clientIp ?? 'anonymous',
      });
      if (!success) {
        throw new ChatError(429, 'Too many questions. Wait a moment.');
      }
    }

    await checkSpendCap(env, now);

    const body = await request.json().catch(() => {
      throw new ChatError(400, 'Body is not valid JSON.');
    });
    const chat = parseChatRequest(body);

    if (env.TURNSTILE_SECRET) {
      await verifyTurnstile(env.TURNSTILE_SECRET, chat.turnstileToken, clientIp);
    }

    const client = new Anthropic({ apiKey: env.ANTHROPIC_API_KEY });
    const message = await client.messages.create({
      model: MODEL,
      max_tokens: MAX_TOKENS,
      system: SYSTEM_PROMPT,
      // Thinking is on by default for this model; a summary is worth the
      // tokens here because the reasoning explains which tool was chosen.
      thinking: { type: 'adaptive', display: 'summarized' },
      tools: TOOL_DEFINITIONS as unknown as Anthropic.Tool[],
      messages: chat.messages,
    });

    await recordSpend(env, now, estimateCostUsd(message.usage));

    // One turn only. The page executes any tool_use blocks and posts back.
    return json({
      id: message.id,
      stop_reason: message.stop_reason,
      content: message.content,
      usage: message.usage,
    });
  } catch (error) {
    if (error instanceof ChatError) {
      return json({ error: error.message }, error.status);
    }
    if (error instanceof Anthropic.APIError) {
      // Never surface upstream error text: it can echo request content.
      console.error('anthropic error', error.status, error.message);
      return json(
        { error: 'The model could not be reached. Try again shortly.' },
        502,
      );
    }
    console.error('chat failure', error);
    return json({ error: 'Unexpected failure.' }, 500);
  }
}

function json(
  payload: unknown,
  status = 200,
  headers: Record<string, string> = {},
): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      'content-type': 'application/json; charset=utf-8',
      'cache-control': 'no-store',
      ...headers,
    },
  });
}
