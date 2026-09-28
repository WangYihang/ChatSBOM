/**
 * The chat endpoint: an authenticated relay to the Messages API.
 *
 * One model turn per request. The response goes back to the page, which
 * executes any `tool_use` blocks and posts the results for the next
 * turn, so the agent loop lives on the client even though the data no
 * longer does.
 *
 * A note on what changed, because this file used to claim otherwise.
 * When the dataset was Parquet in the browser, the Worker held the API
 * key and never saw a query result, while the page held the data and
 * never saw the key — neither side had both. Queries run against D1 now,
 * so results pass through the Worker and it has both.
 *
 * The key's exposure is unchanged: it has always been a Worker secret
 * and has never been in the browser. What is lost is the property that
 * a compromised or mis-logged Worker could leak *what was asked* but
 * not *what the data says*. For this corpus the practical risk is
 * slight — it is public repository metadata, served from R2 to anyone
 * who asks — but the property is gone, and pretending otherwise in a
 * comment is worse than losing it.
 *
 * What the Worker is responsible for is everything the client cannot be
 * trusted with: the API key, verifying the visitor, bounding the
 * request — its size, and what it may ask the model to read — and
 * capping spend. A relay that forwarded whatever it was posted would be
 * a free model, paid for by this deployment, for anyone who found it.
 */
import Anthropic from '@anthropic-ai/sdk';

import { BodyError, readBody } from './body';
import { clientKey, type EdgeEnv } from './ratelimit';
import {
  isToolName,
  MAX_CONVERSATION_CHARS,
  MAX_TOOL_RESULT_CHARS,
  SYSTEM_PROMPT,
  TOOL_DEFINITIONS,
} from './tools';

export interface ChatEnv extends EdgeEnv {
  ANTHROPIC_API_KEY: string;
  TURNSTILE_SECRET?: string;
  CHAT_RATE_LIMITER?: RateLimit;
  DAILY_SPEND_CAP_USD?: string;
  SPEND?: KVNamespace;
}

/** A model turn's worth of conversation: what the page posted, checked. */
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

/**
 * Bounds on the text in a conversation, by who is meant to have written it.
 *
 * None of it can be checked for authorship — the loop runs in the page,
 * so every block, the model's included, arrives from the client — but
 * each kind has a size the page's own loop never exceeds:
 *
 *   - a question is typed into a one-line box;
 *   - model output (prose, reasoning, a tool call's input) is at most
 *     MAX_TOKENS tokens a turn, and eight characters a token is generous;
 *   - a tool result is cut by the page to RESULT_CHARS, a tenth of the
 *     conversation. MAX_TOOL_RESULT_CHARS stays well above that: a tab
 *     still running the page from before the cut sends results of up to
 *     about 120 kB, and one of those alone should not end its
 *     conversation.
 *
 * MAX_TOOL_RESULT_CHARS and MAX_CONVERSATION_CHARS are declared in
 * tools.ts, which the page shares, so the page's cap is set against these
 * numbers rather than a copy of them.
 *
 * The total sits below MAX_REQUEST_BYTES on purpose. The byte cap bounds
 * the wire, where JSON escaping inflates text; this one bounds what the
 * model is handed, however the body was encoded: about 50,000 tokens, or
 * $0.25 of input at the rate below.
 */
const MAX_QUESTION_CHARS = 4_000;
const MAX_MODEL_OUTPUT_CHARS = 8 * MAX_TOKENS;

/** The shape of the ids the API gives tool calls, with room to spare. */
const TOOL_USE_ID = /^[\w-]{1,128}$/;

/**
 * Published per-MTok rates for the model above, for the spend cap.
 *
 * Cached input is billed as a multiple of the input rate: a write at
 * 1.25× — for the default five-minute TTL, the one requested below; a
 * one-hour write would be 2× — and a read at 0.1×.
 */
const INPUT_USD_PER_MTOK = 5;
const OUTPUT_USD_PER_MTOK = 25;
const CACHE_WRITE_MULTIPLIER = 1.25;
const CACHE_READ_MULTIPLIER = 0.1;

export function estimateCostUsd(usage: Anthropic.Usage): number {
  const input =
    usage.input_tokens +
    (usage.cache_creation_input_tokens ?? 0) * CACHE_WRITE_MULTIPLIER +
    (usage.cache_read_input_tokens ?? 0) * CACHE_READ_MULTIPLIER;
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

/**
 * Read a posted body into the request we are willing to pay for.
 *
 * The conversation is checked against what the page's own loop
 * (agent.ts) produces, because that page is this endpoint's only
 * client: questions, the model's turns echoed back, and results for the
 * tools in tools.ts. Anything else — an image, a document, a tool we
 * never offered, a result for a call nobody made — is someone using the
 * key for something other than this dashboard, and is refused with a
 * 400 before anything is sent.
 */
export function parseChatRequest(body: unknown): ChatRequest {
  if (!isRecord(body)) {
    throw new ChatError(400, 'Expected a JSON object.');
  }
  const { messages, turnstileToken } = body;

  if (!Array.isArray(messages) || messages.length === 0) {
    throw new ChatError(400, 'Expected a non-empty messages array.');
  }
  if (messages.length > MAX_MESSAGES) {
    throw new ChatError(
      400,
      `Conversation too long: ${messages.length} messages, limit ${MAX_MESSAGES}. Start a new one.`,
    );
  }

  return {
    messages: new ConversationReader().read(messages),
    ...(typeof turnstileToken === 'string' ? { turnstileToken } : {}),
  };
}

/**
 * Rebuilds a posted conversation block by block, checking as it goes.
 *
 * Rebuilt rather than passed through: each block sent upstream is made
 * here from fields that were checked, so a key this file does not know —
 * `cache_control`, `citations`, whatever the API adds next — never
 * reaches the model. The page posts the model's turns back verbatim,
 * response-only fields (`citations: null`, `caller`) and all; those are
 * dropped rather than refused, so a field the API starts returning does
 * not break every conversation after it.
 */
class ConversationReader {
  /** Characters of text read so far, against MAX_CONVERSATION_CHARS. */
  private chars = 0;

  read(messages: unknown[]): Anthropic.MessageParam[] {
    // A tool result may answer only a call made by the message just
    // before it, and only once — which is what the page's loop sends.
    let calls = new Set<string>();
    return messages.map((raw, index) => {
      const message = this.message(raw, `messages[${index}]`, calls);
      calls = callsIn(message);
      return message;
    });
  }

  private message(
    raw: unknown,
    at: string,
    calls: Set<string>,
  ): Anthropic.MessageParam {
    if (!isRecord(raw)) refuse(at, 'expected a message object.');
    const { role, content } = raw;
    if (role !== 'user' && role !== 'assistant') {
      refuse(at, `unexpected message role ${quote(role)}.`);
    }

    if (typeof content === 'string') {
      // How the page sends a question, and only a question: the model's
      // turns go back as the blocks it returned.
      if (role !== 'user') {
        refuse(at, 'an assistant turn is the blocks the model returned.');
      }
      // The one bound a person can reach by typing, so it says so plainly.
      if (content.length > MAX_QUESTION_CHARS) {
        throw new ChatError(
          400,
          `Question too long: ${content.length} characters, limit ${MAX_QUESTION_CHARS}.`,
        );
      }
      return { role, content: this.text(content, MAX_QUESTION_CHARS, at) };
    }
    if (!Array.isArray(content)) {
      refuse(at, 'content must be a string or an array of blocks.');
    }
    return {
      role,
      content: content.map((block, index) =>
        role === 'user'
          ? this.userBlock(block, `${at}.content[${index}]`, calls)
          : this.assistantBlock(block, `${at}.content[${index}]`),
      ),
    };
  }

  /** A block of a user turn: a question's text, or a tool's result. */
  private userBlock(
    block: unknown,
    at: string,
    calls: Set<string>,
  ): Anthropic.ContentBlockParam {
    if (!isRecord(block)) refuse(at, 'expected a content block.');
    switch (block['type']) {
      case 'text':
        return {
          type: 'text',
          text: this.text(block['text'], MAX_QUESTION_CHARS, at),
        };

      case 'tool_result': {
        const id = block['tool_use_id'];
        // Deleted as it is answered, so no call is answered twice.
        if (typeof id !== 'string' || !calls.delete(id)) {
          refuse(
            at,
            `tool_result answers ${quote(id)}, which the preceding assistant turn did not call.`,
          );
        }
        const isError = block['is_error'];
        if (isError !== undefined && typeof isError !== 'boolean') {
          refuse(at, 'is_error must be a boolean.');
        }
        return {
          type: 'tool_result',
          tool_use_id: id,
          // A string, as the page sends it. The API would take blocks
          // here too — images and documents among them.
          content: this.text(block['content'], MAX_TOOL_RESULT_CHARS, at),
          ...(isError === undefined ? {} : { is_error: isError }),
        };
      }

      default:
        return refuse(at, `a user turn cannot carry ${quote(block['type'])} blocks.`);
    }
  }

  /** A block of an assistant turn: what the model returned, echoed back. */
  private assistantBlock(block: unknown, at: string): Anthropic.ContentBlockParam {
    if (!isRecord(block)) refuse(at, 'expected a content block.');
    switch (block['type']) {
      case 'text':
        return {
          type: 'text',
          text: this.text(block['text'], MAX_MODEL_OUTPUT_CHARS, at),
        };

      // Reasoning must go back exactly as it came: the API verifies the
      // signature, and the opaque fields are its to check, not ours.
      case 'thinking':
        return {
          type: 'thinking',
          thinking: this.text(block['thinking'], MAX_MODEL_OUTPUT_CHARS, at),
          signature: opaque(block['signature'], at),
        };
      case 'redacted_thinking':
        return { type: 'redacted_thinking', data: opaque(block['data'], at) };

      case 'tool_use': {
        const { id, name, input } = block;
        if (typeof id !== 'string' || !TOOL_USE_ID.test(id)) {
          refuse(at, 'a tool_use id is 1–128 letters, digits, _ or -.');
        }
        if (typeof name !== 'string' || !isToolName(name)) {
          refuse(at, `unknown tool ${quote(name)}.`);
        }
        if (!isRecord(input)) refuse(at, 'tool_use input must be an object.');
        this.count(JSON.stringify(input).length, MAX_MODEL_OUTPUT_CHARS, at);
        return { type: 'tool_use', id, name, input };
      }

      default:
        return refuse(
          at,
          `an assistant turn cannot carry ${quote(block['type'])} blocks.`,
        );
    }
  }

  /** A string of text, within its own limit and the conversation's. */
  private text(value: unknown, limit: number, at: string): string {
    if (typeof value !== 'string') refuse(at, 'expected a string.');
    this.count(value.length, limit, at);
    return value;
  }

  private count(chars: number, limit: number, at: string): void {
    if (chars > limit) {
      refuse(at, `too long: ${chars} characters, limit ${limit}.`);
    }
    this.chars += chars;
    if (this.chars > MAX_CONVERSATION_CHARS) {
      throw new ChatError(
        400,
        `Conversation too long: over ${MAX_CONVERSATION_CHARS} characters. Start a new one.`,
      );
    }
  }
}

/** The tool calls an assistant turn made, for the turn after to answer. */
function callsIn(message: Anthropic.MessageParam): Set<string> {
  const ids = new Set<string>();
  if (message.role === 'assistant' && Array.isArray(message.content)) {
    for (const block of message.content) {
      if (block.type === 'tool_use') ids.add(block.id);
    }
  }
  return ids;
}

/** A signature or redacted reasoning: passed on as it came, for the API to verify. */
function opaque(value: unknown, at: string): string {
  if (typeof value !== 'string') refuse(at, 'expected a string.');
  return value;
}

function refuse(at: string, why: string): never {
  throw new ChatError(400, `${at}: ${why}`);
}

/** A client-supplied value, quoted for an error message and kept short. */
function quote(value: unknown): string {
  return JSON.stringify(String(value).slice(0, 64));
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

/**
 * Whether the request came from this deployment's own page.
 *
 * Every call here is paid for, so no other site may make one. Without
 * this, any page could have its visitors' browsers post questions — each
 * from a different address, so the per-IP limiter never sees a pattern.
 *
 * Browsers say where a request came from in two headers a page cannot
 * set: `Sec-Fetch-Site`, which every current browser sends, and `Origin`,
 * which they have put on every POST for longer. Either one naming this
 * origin is enough.
 *
 * A request with neither is refused. It did not come from a browser —
 * curl, a script — and nothing but the page is meant to call this. A
 * client that is not a browser could set both headers to anything, so
 * admitting headerless requests would protect no one while letting in
 * exactly the callers the other checks are for. Nor is this
 * authentication: it stops a browser being turned against us by another
 * site, and a determined script is bounded by the rate limiter,
 * Turnstile and the spend cap.
 *
 * Behind `wrangler dev` and a tunnel the comparison still holds: the dev
 * proxy rewrites `Origin` along with the URL it hands the Worker, and
 * `Sec-Fetch-Site` passes through untouched.
 */
function isSameOrigin(request: Request): boolean {
  if (request.headers.get('sec-fetch-site') === 'same-origin') return true;
  const origin = request.headers.get('origin');
  return origin !== null && origin === new URL(request.url).origin;
}

/**
 * Whether the body is declared as JSON.
 *
 * Not pedantry. A cross-site POST of `text/plain` is a "simple" request:
 * the browser sends it without asking first, and this endpoint used to
 * parse the body whatever its type. A JSON one must be preflighted, and
 * the preflight is never granted — so the browser never sends it.
 */
function isJson(request: Request): boolean {
  const type = request.headers.get('content-type') ?? '';
  return type.split(';')[0]!.trim().toLowerCase() === 'application/json';
}

function parseJson(text: string): unknown {
  try {
    return JSON.parse(text);
  } catch {
    throw new ChatError(400, 'Body is not valid JSON.');
  }
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
  ctx: Pick<ExecutionContext, 'waitUntil'>,
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
  if (!isSameOrigin(request)) {
    return json({ error: 'Requests must come from this site\'s own page.' }, 403);
  }
  if (!isJson(request)) {
    return json({ error: 'Expected content-type: application/json.' }, 415);
  }

  // Free to check, so checked before anything else is spent on the
  // request; but only a claim, which `readBody` does not take on trust.
  const length = Number(request.headers.get('content-length') ?? '0');
  if (length > MAX_REQUEST_BYTES) {
    return json({ error: 'Request too large.' }, 413);
  }

  const clientIp = request.headers.get('cf-connecting-ip');

  try {
    if (env.CHAT_RATE_LIMITER) {
      // Keyed as the query endpoint is: on the address only when the
      // edge vouched for it (`ratelimit.ts`).
      const { success } = await env.CHAT_RATE_LIMITER.limit({
        key: clientKey(request, env),
      });
      if (!success) {
        throw new ChatError(429, 'Too many questions. Wait a moment.');
      }
    }

    await checkSpendCap(env, now);

    const chat = parseChatRequest(
      parseJson(await readBody(request, MAX_REQUEST_BYTES)),
    );

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
      // Each turn resends the whole conversation before it. Caching up to
      // the last block lets the next turn — that prefix plus a little —
      // read it back at a tenth of the input rate instead of paying for
      // it again. The default five-minute TTL: a loop's turns are seconds
      // apart, and a one-hour entry costs twice as much to write.
      cache_control: { type: 'ephemeral' },
    });

    // After the answer, never instead of it. The model is paid for
    // either way, so a KV write that fails — KV takes one write a second
    // per key — must not turn the answer into a 500 that records nothing.
    ctx.waitUntil(
      recordSpend(env, now, estimateCostUsd(message.usage)).catch(
        (error: unknown) => console.error('spend not recorded', error),
      ),
    );

    // One turn only. The page executes any tool_use blocks and posts back.
    return json({
      id: message.id,
      stop_reason: message.stop_reason,
      content: message.content,
      usage: message.usage,
    });
  } catch (error) {
    if (error instanceof ChatError || error instanceof BodyError) {
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
