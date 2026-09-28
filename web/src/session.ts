/**
 * The session a verified question's later turns present (#32).
 *
 * Cloudflare accepts a Turnstile token once, and a question is several
 * turns: the page's loop posts one per round of tool calls, eight at
 * most. The page resent its one token every turn, so a question that
 * called a tool could never pass its second. Now the Worker checks the
 * token on the question's first turn and answers with a session, which
 * the question's later turns present instead.
 *
 * A session is an HMAC, not a record: the Worker keeps nothing, and a
 * session is good wherever the secret is. So it is worth exactly what
 * it is bound to and how long it lasts, and both are narrow:
 *
 *   - **The question.** A turn belongs to the question asked last in
 *     its conversation, and the session signs a digest of the
 *     conversation up to and including that question. Every later turn
 *     of the question begins with it, since the loop only appends, and
 *     nothing else does: not the next question of the same conversation,
 *     nor the same words asked after a different conversation. A
 *     conversation id would bind less — one id can carry any question.
 *   - **The client**, as the rate limiters know it (`clientKey`): the
 *     address the edge vouched for, or the one bucket for everything
 *     else. A session handed to another client is no use to it, so one
 *     solved challenge cannot be spread across addresses to multiply
 *     the per-client limit.
 *   - **Ten minutes** (SESSION_SECONDS, below).
 *
 * The key is TURNSTILE_SECRET itself, so there is no second secret to
 * set, and the signed text begins with a label of its own, so nothing
 * else signed with it could pass for a session. That makes the secret
 * worth more than it was: whoever holds it can now mint sessions, where
 * before it only let them ask Cloudflare about tokens.
 */
import type Anthropic from '@anthropic-ai/sdk';

/**
 * How long a session lasts.
 *
 * The page gives up on a question after eight turns, and ten minutes
 * holds eight turns of reasoning and a query each for all but the
 * slowest questions; one that outlives its session is refused on its
 * next turn with what it needs to pass again, and the page solves a
 * fresh challenge and carries on (agent.ts).
 *
 * What the length bounds is replay. The turns after a question are
 * whatever the client posts, so within its ten minutes a session admits
 * as many as the chat's rate limiter lets one client make — 200 at 20 a
 * minute — for that one question, and the spend cap bounds what they
 * cost. Shorter would bound that more tightly and send more real
 * questions back to the widget halfway through.
 */
export const SESSION_SECONDS = 10 * 60;

/** Begins everything signed as a session, and nothing else. */
const LABEL = 'chatsbom chat session v1';

const encoder = new TextEncoder();

/**
 * `text` as UTF-8, in a buffer of its own: the DOM's types, which the
 * tests compile against, take no view that could be a shared buffer.
 */
function utf8(text: string): Uint8Array<ArrayBuffer> {
  return new Uint8Array(encoder.encode(text));
}

/**
 * What a session for this turn is bound to: the question it belongs to,
 * and who is asking. Null for a conversation with no question in it,
 * which no session covers.
 */
export async function sessionScope(
  messages: readonly Anthropic.MessageParam[],
  client: string,
): Promise<string | null> {
  let asked = messages.length - 1;
  while (asked >= 0 && !isQuestion(messages[asked]!)) asked -= 1;
  if (asked < 0) return null;
  // The conversation as `parseChatRequest` rebuilt it, so a field the
  // page echoes back and the Worker drops cannot change the digest.
  const digest = await crypto.subtle.digest(
    'SHA-256',
    utf8(JSON.stringify(messages.slice(0, asked + 1))),
  );
  return JSON.stringify([client, hex(new Uint8Array(digest))]);
}

/**
 * A user turn that asks something, rather than answering the model's
 * tool calls: the page sends a question as a string, and tool results
 * as blocks.
 */
function isQuestion(message: Anthropic.MessageParam): boolean {
  if (message.role !== 'user') return false;
  if (typeof message.content === 'string') return true;
  return !message.content.some((block) => block.type === 'tool_result');
}

/** A session for `scope`, good for SESSION_SECONDS from `now`. */
export async function issueSession(
  secret: string,
  scope: string,
  now: Date,
): Promise<string> {
  const expires = seconds(now) + SESSION_SECONDS;
  const signature = await crypto.subtle.sign(
    'HMAC',
    await key(secret),
    signed(expires, scope),
  );
  return `${expires}.${hex(new Uint8Array(signature))}`;
}

/** Whether `token` is a session for `scope` that has not yet expired. */
export async function checkSession(
  secret: string,
  token: string,
  scope: string,
  now: Date,
): Promise<boolean> {
  const match = /^(\d{1,15})\.([0-9a-f]{64})$/.exec(token);
  if (!match) return false;
  const expires = Number(match[1]);
  // Expiry is checked before the signature, but it cannot be forged
  // past it: the expiry is part of what is signed.
  if (expires <= seconds(now) || expires > seconds(now) + SESSION_SECONDS) {
    return false;
  }
  // `verify`, not a comparison of two signatures: it takes the same
  // time however much of a forgery is right.
  return crypto.subtle.verify(
    'HMAC',
    await key(secret),
    bytes(match[2]!),
    signed(expires, scope),
  );
}

function signed(expires: number, scope: string): Uint8Array<ArrayBuffer> {
  return utf8(JSON.stringify([LABEL, expires, scope]));
}

function key(secret: string): Promise<CryptoKey> {
  return crypto.subtle.importKey(
    'raw',
    utf8(secret),
    { name: 'HMAC', hash: 'SHA-256' },
    false,
    ['sign', 'verify'],
  );
}

function seconds(now: Date): number {
  return Math.floor(now.getTime() / 1000);
}

function hex(data: Uint8Array): string {
  return Array.from(data, (byte) => byte.toString(16).padStart(2, '0')).join('');
}

function bytes(text: string): Uint8Array<ArrayBuffer> {
  const data = new Uint8Array(text.length / 2);
  for (let i = 0; i < data.length; i += 1) {
    data[i] = parseInt(text.slice(2 * i, 2 * i + 2), 16);
  }
  return data;
}
