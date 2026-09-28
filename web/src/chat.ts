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
 * never saw the key — neither side had both. Queries run in the Worker
 * now, against ClickHouse or D1, so results pass through it and it has
 * both.
 *
 * The key's exposure is unchanged: it has always been a Worker secret
 * and has never been in the browser. What is lost is the property that
 * a compromised or mis-logged Worker could leak *what was asked* but
 * not *what the data says*. For this corpus the practical risk is
 * slight — it is public repository metadata, which `/api/q` serves to
 * anyone who asks — but the property is gone, and pretending otherwise
 * in a comment is worse than losing it.
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
import { checkSession, issueSession, sessionScope } from './session';
import type { SpendCounter } from './spend';
import {
  isToolName,
  MAX_CONVERSATION_CHARS,
  MAX_TOOL_RESULT_CHARS,
  SYSTEM_PROMPT,
  TOOL_DEFINITIONS,
} from './tools';

export interface ChatEnv extends EdgeEnv {
  ANTHROPIC_API_KEY: string;
  /**
   * Turnstile's secret key. Set, every question must first pass a
   * Turnstile challenge (#32); it signs the sessions that carry that
   * pass to the question's later turns, too (`session.ts`).
   */
  TURNSTILE_SECRET?: string;
  /**
   * The same widget's site key: the public half, which the page renders
   * the widget with. The secret without it refuses every question.
   */
  TURNSTILE_SITE_KEY?: string;
  CHAT_RATE_LIMITER?: RateLimit;
  /** The most the AI answers may spend in a UTC day, in dollars; unset or 0, no cap. */
  DAILY_SPEND_CAP_USD?: string;
  /** The cap's counters, one Durable Object per day (`spend.ts`, #33). */
  SPEND_COUNTER?: DurableObjectNamespace<SpendCounter>;
  /**
   * Where model calls go. Unset, Anthropic's API. Set for a gateway of
   * your own, or for a stand-in for the model when the Worker runs
   * under workerd, which has no process.env for the SDK to read it from.
   */
  ANTHROPIC_BASE_URL?: string;
}

/** A model turn's worth of conversation: what the page posted, checked. */
export interface ChatRequest {
  messages: Anthropic.MessageParam[];
  /**
   * A Turnstile token, when TURNSTILE_SECRET is configured: on the first
   * turn of a question, and on a turn whose session was refused.
   */
  turnstileToken?: string;
  /** What the answer to a verified turn carried, on the question's later turns. */
  session?: string;
}

/** What the page must pass before it asks: a Turnstile challenge. */
export interface Challenge {
  /** The widget's site key, which is public. */
  siteKey: string;
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

const encoder = new TextEncoder();

/** What every turn sends before its conversation: its instructions and tools. */
const FIXED_INPUT_BYTES = encoder.encode(
  SYSTEM_PROMPT + JSON.stringify(TOOL_DEFINITIONS),
).length;

/**
 * Tokens the API adds of its own around what is sent: the instructions
 * that introduce tools, a few hundred tokens by its own account, and the
 * markers between turns.
 */
const FRAMING_TOKENS = 2_048;

/**
 * The most a turn with `messages` can cost: what is reserved against
 * the day's cap before it is made (#33).
 *
 * A bound rather than a guess, because a reservation that could be
 * exceeded would make the cap one too. Input is counted a token for
 * every byte sent — a token is never less than a byte, however text is
 * split — and priced as a cache write, the dearest input there is;
 * output is MAX_TOKENS at the output rate, since thinking counts
 * against the same limit. A question's first turn is reserved at about
 * 26 cents and costs a few; a turn carrying the largest conversation
 * the Worker takes, at about $1.90. What it actually cost replaces it
 * the moment the answer says, so the gap only ever holds budget back
 * for as long as a call is in flight.
 */
export function worstCaseUsd(messages: Anthropic.MessageParam[]): number {
  const tokens =
    FIXED_INPUT_BYTES +
    encoder.encode(JSON.stringify(messages)).length +
    FRAMING_TOKENS;
  return (
    (tokens / 1_000_000) * INPUT_USD_PER_MTOK * CACHE_WRITE_MULTIPLIER +
    (MAX_TOKENS / 1_000_000) * OUTPUT_USD_PER_MTOK
  );
}

/** The UTC day a turn counts against, which names that day's counter. */
export function spendDay(now: Date): string {
  return now.toISOString().slice(0, 10);
}

export class ChatError extends Error {
  constructor(
    readonly status: number,
    message: string,
    /** Sent beside the message: for a refused verification, how to pass. */
    readonly detail: Record<string, unknown> = {},
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
  const { messages, turnstileToken, session } = body;

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
    ...(typeof session === 'string' ? { session } : {}),
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

/** The longest token Cloudflare issues. */
const MAX_TURNSTILE_TOKEN = 2048;

export async function verifyTurnstile(
  secret: string,
  token: string | undefined,
  remoteIp: string | null,
  challenge?: Challenge,
): Promise<void> {
  // Every refusal says how to pass, so the page can try once more.
  const detail = challenge ? { turnstile: challenge } : {};
  if (!token) {
    throw new ChatError(400, 'Human verification is required.', detail);
  }
  // Not one of Cloudflare's, so not worth asking them about.
  if (token.length > MAX_TURNSTILE_TOKEN) {
    throw new ChatError(403, 'Human verification failed. Reload and retry.', detail);
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
    throw new ChatError(403, 'Human verification failed. Reload and retry.', detail);
  }
}

/**
 * What the page must pass before it asks: a Turnstile challenge, or
 * nothing when TURNSTILE_SECRET is unset.
 *
 * The secret alone is refused rather than enforced. The page renders
 * the widget with the site key, so without one it could never obtain a
 * token, and every question would fail as a refused turn; this says so
 * once, as a setting that is missing.
 */
export function turnstileChallenge(env: ChatEnv): Challenge | null {
  if (!env.TURNSTILE_SECRET) return null;
  if (!env.TURNSTILE_SITE_KEY) {
    console.error(
      'TURNSTILE_SECRET is set without TURNSTILE_SITE_KEY: the page cannot ' +
        'show the widget, so no question could pass. Set both, or neither.',
    );
    throw new ChatError(503, 'AI answers are not set up correctly on this deployment.');
  }
  return { siteKey: env.TURNSTILE_SITE_KEY };
}

/**
 * Check that a person is asking: with Cloudflare, once a question (#32).
 *
 * A turn that presents a current session for its question, from its
 * client, passes as it is. Any other turn needs a Turnstile token,
 * which Cloudflare checks; its answer then carries a session for the
 * question's later turns, returned here. A turn whose session is
 * refused may carry a fresh token in its place, which is how the page
 * recovers from one that lapsed mid-question.
 */
async function verifyVisitor(
  request: Request,
  env: ChatEnv,
  secret: string,
  challenge: Challenge,
  chat: ChatRequest,
  now: Date,
): Promise<string | undefined> {
  const scope = await sessionScope(chat.messages, clientKey(request, env));
  if (chat.session && scope && (await checkSession(secret, chat.session, scope, now))) {
    return undefined;
  }
  if (chat.session && !chat.turnstileToken) {
    throw new ChatError(
      403,
      'Human verification has expired, or was for another question. Ask again.',
      { turnstile: challenge },
    );
  }
  await verifyTurnstile(
    secret,
    chat.turnstileToken,
    request.headers.get('cf-connecting-ip'),
    challenge,
  );
  return scope === null ? undefined : issueSession(secret, scope, now);
}

/**
 * What the page must do before it asks: GET /api/chat (#32).
 *
 * Asked before each question rather than learned from a refused turn,
 * so the first turn can carry its token. The site key is public — every
 * page that shows the widget carries it — and the secret stays here.
 */
function settings(env: ChatEnv): Response {
  if (!env.ANTHROPIC_API_KEY) {
    return json({ error: NOT_CONFIGURED }, 503);
  }
  try {
    // A spend cap that could not be kept refuses every question (#33):
    // said here too, before the page solves a challenge for one.
    spendBudget(env);
    return json({ turnstile: turnstileChallenge(env) });
  } catch (error) {
    if (error instanceof ChatError) {
      return json({ error: error.message }, error.status);
    }
    throw error;
  }
}

const NOT_CONFIGURED = 'AI answers are not configured on this deployment.';

/**
 * Read what is left of a request's body, and discard it.
 *
 * Every refusal does this first. Under `wrangler dev`, which serves
 * this under compose, a response sent with the request body unread lost
 * the connection now and then, and the dev proxy answered 500 in its
 * place: about one 429 in five on /api/q (#31). Read against the cap,
 * as every body here is — so a body declared larger than the cap is
 * still refused unread, since reading it is what the cap prevents.
 */
async function drain(request: Request): Promise<void> {
  if (request.bodyUsed) return;
  await readBody(request, MAX_REQUEST_BYTES).catch(() => undefined);
}

/** A refusal made before the body was read: read it, then answer. */
async function turnAway(
  request: Request,
  status: number,
  error: string,
  headers: Record<string, string> = {},
): Promise<Response> {
  await drain(request);
  return json({ error }, status, headers);
}

/** The day's cap, and the counters that keep it. */
interface Budget {
  cap: number;
  counters: DurableObjectNamespace<SpendCounter>;
}

/**
 * The day's cap, or null for none: DAILY_SPEND_CAP_USD unset, empty
 * or 0.
 *
 * Anything else that is not a number of dollars, or a cap with no
 * counter bound to keep it, refuses every question rather than lifting
 * the cap: a typo in a bound is no reason for it to stop being one.
 */
function spendBudget(env: ChatEnv): Budget | null {
  const setting = (env.DAILY_SPEND_CAP_USD ?? '').trim();
  const cap = Number(setting);
  if (setting === '' || cap === 0) return null;
  if (!Number.isFinite(cap) || cap < 0) {
    console.error(`DAILY_SPEND_CAP_USD is not a number of dollars: ${JSON.stringify(setting)}.`);
    throw new ChatError(503, 'AI answers are not set up correctly on this deployment.');
  }
  if (!env.SPEND_COUNTER) {
    console.error('DAILY_SPEND_CAP_USD is set, but no SPEND_COUNTER is bound to keep it.');
    throw new ChatError(503, 'AI answers are not set up correctly on this deployment.');
  }
  return { cap, counters: env.SPEND_COUNTER };
}

/** A turn's worst case, held against its day's cap until it is settled. */
interface Reservation {
  /** Replace the worst case with what the turn cost. */
  settle(usd: number): Promise<void>;
  /** Release it: the API refused the turn, and did not bill it. */
  refund(): Promise<void>;
}

/**
 * Hold a turn's worst case against the day's cap before it is made, or
 * refuse it with a 429 (#33).
 *
 * The counter is the day's own Durable Object, so turns arriving
 * together are held one after another, and one that would take the day
 * past its cap is refused whatever else is in flight. A counter that
 * cannot be reached refuses the turn too: an uncounted turn is the
 * failure the cap is there to prevent.
 */
async function reserve(
  { cap, counters }: Budget,
  messages: Anthropic.MessageParam[],
  now: Date,
  ctx: Pick<ExecutionContext, 'waitUntil'>,
): Promise<Reservation> {
  // Named for the day that admits the turn, so it settles there too.
  const counter = counters.getByName(spendDay(now));
  const id = crypto.randomUUID();

  let held: boolean;
  try {
    held = await counter.reserve(id, worstCaseUsd(messages), cap);
  } catch (error) {
    console.error('spend counter unreachable', error);
    // It may have held the turn and failed only to say so.
    ctx.waitUntil(counter.refund(id).catch(() => undefined));
    throw new ChatError(503, 'AI answers are unavailable for a moment. Try again shortly.');
  }
  if (!held) {
    throw new ChatError(
      429,
      'The daily budget for AI answers is used up. The dashboard itself still works.',
    );
  }

  return {
    settle: (usd) =>
      counter.settle(id, usd).catch((error: unknown) => {
        console.error('spend not settled', error);
      }),
    refund: () =>
      counter.refund(id).catch((error: unknown) => {
        console.error('reservation not refunded', error);
      }),
  };
}

export async function handleChat(
  request: Request,
  env: ChatEnv,
  ctx: Pick<ExecutionContext, 'waitUntil'>,
  now: Date = new Date(),
): Promise<Response> {
  // What a question needs, asked before it is posted (#32).
  if (request.method === 'GET') {
    return settings(env);
  }
  if (request.method !== 'POST') {
    return turnAway(request, 405, 'Method not allowed', { Allow: 'GET, POST' });
  }
  if (!env.ANTHROPIC_API_KEY) {
    return turnAway(request, 503, NOT_CONFIGURED);
  }
  if (!isSameOrigin(request)) {
    return turnAway(request, 403, 'Requests must come from this site\'s own page.');
  }
  if (!isJson(request)) {
    return turnAway(request, 415, 'Expected content-type: application/json.');
  }

  // Free to check, so checked before anything else is spent on the
  // request; but only a claim, which `readBody` does not take on trust.
  // Refused unread, unlike the refusals above: not reading it is the point.
  const length = Number(request.headers.get('content-length') ?? '0');
  if (length > MAX_REQUEST_BYTES) {
    return json({ error: 'Request too large.' }, 413);
  }

  try {
    // Before the body is read, as the missing key above is: a deployment
    // that could not answer anyone says so before anything else.
    const challenge = turnstileChallenge(env);
    const budget = spendBudget(env);

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

    const chat = parseChatRequest(
      parseJson(await readBody(request, MAX_REQUEST_BYTES)),
    );

    // Once a question rather than once a turn: the first turn's token
    // buys a session, which the question's later turns present.
    const session =
      challenge && env.TURNSTILE_SECRET
        ? await verifyVisitor(request, env, env.TURNSTILE_SECRET, challenge, chat, now)
        : undefined;

    // The turn's worst case, held before it is made; last of the checks,
    // so that nothing refused for another reason holds any of the day.
    const reservation = budget ? await reserve(budget, chat.messages, now, ctx) : null;

    const client = new Anthropic({
      apiKey: env.ANTHROPIC_API_KEY,
      ...(env.ANTHROPIC_BASE_URL ? { baseURL: env.ANTHROPIC_BASE_URL } : {}),
    });
    let message: Anthropic.Message;
    try {
      message = await client.messages.create({
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
    } catch (error) {
      // Refunded only when the API answered with an error, which it does
      // not bill. A call lost on the way — a timeout, a dropped
      // connection — may have been answered and billed all the same, so
      // its worst case stays held for the rest of the day. (The SDK sends
      // a call again when its connection drops, so one reservation can
      // cover two attempts; were the first billed, only one would be
      // counted. Rare, and nothing a visitor can bring about.)
      if (reservation && error instanceof Anthropic.APIError && error.status !== undefined) {
        ctx.waitUntil(reservation.refund());
      }
      throw error;
    }

    // After the answer, never instead of it. The model is paid for either
    // way, so a counter that fails to hear of it must not turn the answer
    // into a 500; the turn's worst case then stays held in its place.
    if (reservation) {
      ctx.waitUntil(reservation.settle(estimateCostUsd(message.usage)));
    }

    // One turn only. The page executes any tool_use blocks and posts back.
    return json({
      id: message.id,
      stop_reason: message.stop_reason,
      content: message.content,
      usage: message.usage,
      ...(session === undefined ? {} : { session }),
    });
  } catch (error) {
    // Anything refused before the body was read reads it now.
    await drain(request);
    if (error instanceof ChatError) {
      return json({ error: error.message, ...error.detail }, error.status);
    }
    if (error instanceof BodyError) {
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
