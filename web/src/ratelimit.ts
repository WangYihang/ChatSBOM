/**
 * The per-client rate limits: who a request is from, and how many it
 * may make.
 *
 * Who it is from. Both limiters — the chat's and the query endpoint's —
 * key on `clientKey`, so the two cannot disagree about who a visitor is.
 *
 * `CF-Connecting-IP` names the visitor when Cloudflare's edge set it:
 * the edge overwrites whatever a client sent. A deployed Worker only
 * ever sees requests that came that way. `wrangler dev` behind a tunnel
 * does not: a client that reaches its port directly — from the LAN, or
 * the docker bridge — sets the header itself, and could claim a new
 * address, and so a new budget, on every request (#18, #31). Nothing in
 * such a request tells it apart from one the tunnel delivered; every
 * header the tunnel adds, a client can add too.
 *
 * So the edge vouches for a request with something a client cannot
 * know: `EDGE_SECRET`, a secret this Worker holds, which a Cloudflare
 * Transform Rule adds to every request for the site as `X-Edge-Secret`.
 * With it set:
 *
 *   - a request carrying the secret came through Cloudflare, and is
 *     keyed on the address Cloudflare named;
 *   - any other request did not, and all of them share one bucket,
 *     whatever address each claims — so claiming a new one buys nothing.
 *
 * Without it the Worker cannot tell the two apart, and keys on the
 * header as it always did. That is right for a deployed Worker, and for
 * a tunnel whose origin port nothing but the tunnel can reach
 * (`WEB_BIND`). Anywhere else, set the secret: DEPLOY.md says how.
 *
 * The address itself goes one place besides: Turnstile's siteverify,
 * which can check a token against the address that solved it. It is
 * told the address the limiters believe, and on a request the edge did
 * not vouch for, none (`clientAddress`, #115).
 *
 * How many. At most `limit` requests from a client in `period` seconds,
 * counted by `RateLimiter` below over a window that slides (#115).
 */
import { DurableObject } from 'cloudflare:workers';

export interface EdgeEnv {
  /**
   * A secret Cloudflare's edge adds to each request as `X-Edge-Secret`.
   * Empty or unset: the Worker takes `CF-Connecting-IP` on trust.
   */
  EDGE_SECRET?: string;
}

/** The header the edge's Transform Rule sets. */
export const EDGE_SECRET_HEADER = 'x-edge-secret';

/** Every request the edge did not vouch for, whatever address it claims. */
const UNVERIFIED = 'unverified';

/** A vouched-for request that names no address: one bucket, as before. */
const ANONYMOUS = 'anonymous';

/** The rate limiter's key for `request`. */
export function clientKey(request: Request, env: EdgeEnv): string {
  if (!vouched(request, env)) return UNVERIFIED;
  return request.headers.get('cf-connecting-ip') ?? ANONYMOUS;
}

/**
 * The visitor's address as the edge named it, or null: none named, or
 * named on a request the edge did not vouch for, where the client chose
 * it.
 */
export function clientAddress(request: Request, env: EdgeEnv): string | null {
  return vouched(request, env) ? request.headers.get('cf-connecting-ip') : null;
}

/** Whether the edge vouched for `request`: any request, with no secret set. */
function vouched(request: Request, env: EdgeEnv): boolean {
  if (!env.EDGE_SECRET) return true;
  const given = request.headers.get(EDGE_SECRET_HEADER);
  return given !== null && sameSecret(given, env.EDGE_SECRET);
}

/**
 * Whether two secrets are equal, in time that does not depend on where
 * they first differ.
 *
 * A plain `===` returns at the first differing character, which is in
 * principle a way to learn the secret a character at a time. The length
 * is not hidden, and needs no hiding: it is not the secret.
 */
function sameSecret(given: string, expected: string): boolean {
  const a = new TextEncoder().encode(given);
  const b = new TextEncoder().encode(expected);
  if (a.length !== b.length) return false;
  let difference = 0;
  for (let i = 0; i < a.length; i++) difference |= a[i]! ^ b[i]!;
  return difference === 0;
}

/* ---------------- how many (#115) ---------------- */

/**
 * A limit, as wrangler.jsonc sets one under `vars`: at most `limit`
 * requests from a client in `period` seconds. The two numbers the
 * `ratelimits` bindings took, under `simple`, before this.
 */
export interface RateLimitSetting {
  limit: number;
  period: number;
}

export interface RateLimitEnv extends EdgeEnv {
  /** Where the limits are counted: `RateLimiter`, one object a limiter and location. */
  RATE_LIMITER?: DurableObjectNamespace<RateLimiter>;
}

/** What counting a request came to. */
export type Admission =
  /** Within the limit, or no limit set: go on. */
  | 'admitted'
  /** Past it: 429. */
  | 'limited'
  /** A setting that is not one, or nothing bound to count: 503 until it is fixed. */
  | 'misconfigured'
  /** The counter could not be reached: 503, for a moment. */
  | 'unreachable';

/**
 * Count `request` against the limit `name` sets, `setting`: what
 * `<NAME>_RATE_LIMIT` holds, and unset for none.
 *
 * A limit that cannot be kept — a setting that is not one, no counter
 * bound, a counter that cannot be reached — refuses the request rather
 * than let it through uncounted. The limits keep a flood off the one
 * ClickHouse account every visitor shares, and off the paid model; one
 * that gave way when its counter did is one a flood could open. A typo
 * in a limit is no reason for it to stop being one.
 */
export async function rateLimit(
  request: Request,
  env: RateLimitEnv,
  name: string,
  setting: unknown,
): Promise<Admission> {
  if (setting === undefined) return 'admitted';
  const variable = `${name.toUpperCase()}_RATE_LIMIT`;
  if (!isSetting(setting)) {
    console.error(
      `${variable} is not {"limit": whole requests, "period": seconds}: ${JSON.stringify(setting)}.`,
    );
    return 'misconfigured';
  }
  if (!env.RATE_LIMITER) {
    console.error(`${variable} is set, but no RATE_LIMITER is bound to count it.`);
    return 'misconfigured';
  }

  // One object a limiter and Cloudflare location, as the binding before
  // it counted: deployed, near the requests it counts, rather than one
  // object somewhere that every visitor's every query goes to first.
  // `wrangler dev` names one location, so compose has one per limiter.
  const colo = (request as { cf?: { colo?: unknown } }).cf?.colo;
  const counter = env.RATE_LIMITER.getByName(typeof colo === 'string' ? `${name}@${colo}` : name);
  try {
    const admitted = await counter.admit(clientKey(request, env), setting.limit, setting.period);
    return admitted ? 'admitted' : 'limited';
  } catch (error) {
    console.error('rate limiter unreachable', error);
    return 'unreachable';
  }
}

/** Whether `value` is a limit: a whole number of requests, in a positive number of seconds. */
function isSetting(value: unknown): value is RateLimitSetting {
  if (typeof value !== 'object' || value === null) return false;
  const { limit, period } = value as Record<string, unknown>;
  return (
    typeof limit === 'number' &&
    Number.isInteger(limit) &&
    limit >= 1 &&
    typeof period === 'number' &&
    Number.isFinite(period) &&
    period > 0
  );
}

/**
 * One limiter's counts, where it is counted: each client's count in the
 * current window of the period, and in the one before it.
 *
 * It replaces Cloudflare's `ratelimits` bindings. `wrangler dev`, which
 * serves the dashboard under compose, simulates those with a count per
 * window aligned to the wall clock, starting over at every multiple of
 * the period: a client's budget just before one and its budget again
 * just after got through, twice the limit in moments (#31, measured in
 * ratelimit.integration.test.ts). Deployed, Cloudflare's own limiter is
 * "permissive, eventually consistent", in its words, and says nothing of
 * its windows. And the binding can only be asked yes or no: it cannot
 * say how many a window has counted, which a window that slides needs.
 *
 * So the window slides. A request is let through if the client's
 * requests in the last `period` seconds, one more with it, are within
 * the limit, where the last `period` seconds are this window so far and
 * as much of the one before as they still cover: the previous window's
 * count, weighted by that share, since how those requests fell within
 * it is not kept. Right after a boundary, then, the previous window
 * counts almost whole, and a burst on either side of it gets one budget
 * between them; the budget comes back as the window slides past it. A
 * token bucket would do as well; this keeps the settings' meaning, a
 * number of requests in a period, exactly as it was.
 *
 * A refused request is not counted: a client told to wait is not kept
 * waiting longer for having asked again.
 *
 * The counts are stored as well as held. Under `wrangler dev` an object
 * idle for ten seconds is evicted (measured), and one whose counts lived
 * in memory alone would forget them each time: a fresh budget for any
 * client that paused. The object takes one call at a time, and nothing
 * is awaited between the count and the decision, so calls arriving
 * together are counted one after another. Windows that no longer weigh
 * on any count are deleted as the object moves past them.
 */
export class RateLimiter extends DurableObject<unknown> {
  /** Requests let through, by the start of the window they fell in, then by client. */
  private windows = new Map<number, Map<string, number>>();

  constructor(ctx: DurableObjectState, env: unknown) {
    super(ctx, env);
    // Nothing is counted until what was counted before is loaded.
    void ctx.blockConcurrencyWhile(async () => {
      for (const [key, count] of await ctx.storage.list<number>()) {
        const slot = parseSlot(key);
        if (slot) this.window(slot.start).set(slot.client, count);
      }
    });
  }

  /**
   * Count a request from `client` against at most `limit` in `period`
   * seconds, and say whether it may go on. A setting that is not a
   * number refuses, and touches nothing: the Worker checks them, and
   * this takes none on trust.
   */
  admit(client: string, limit: number, period: number): boolean {
    const length = period * 1000;
    if (!(length > 0 && length < Infinity)) return false;
    const now = Date.now();
    const start = now - (now % length);
    this.forgetBefore(start - length);

    const key = client.slice(0, MAX_CLIENT);
    const previous = this.windows.get(start - length)?.get(key) ?? 0;
    const current = this.windows.get(start)?.get(key) ?? 0;
    // The last `period` seconds: this window so far, and the share of the
    // one before that they still cover.
    const recent = previous * (1 - (now - start) / length) + current;
    if (!(recent + 1 <= limit)) return false;

    this.window(start).set(key, current + 1);
    // Not awaited: the runtime holds the answer until the write is
    // durable, and nothing reads the stored copy but the next start.
    void this.ctx.storage.put(slotKey(start, key), current + 1);
    return true;
  }

  private window(start: number): Map<string, number> {
    let clients = this.windows.get(start);
    if (!clients) this.windows.set(start, (clients = new Map()));
    return clients;
  }

  /** Forget every window that started before `oldest`: none weighs on a count. */
  private forgetBefore(oldest: number): void {
    for (const [start, clients] of this.windows) {
      if (start >= oldest) continue;
      this.windows.delete(start);
      const keys = [...clients.keys()].map((client) => slotKey(start, client));
      for (let at = 0; at < keys.length; at += MAX_DELETE) {
        void this.ctx.storage.delete(keys.slice(at, at + MAX_DELETE));
      }
    }
  }
}

/** The most keys one `delete` takes. */
const MAX_DELETE = 128;

/**
 * The longest client key counted as it is. An address is at most 45
 * characters; one a client claims can be anything (#31), and storage
 * keys are bounded.
 */
const MAX_CLIENT = 128;

/** Where `client`'s count in the window that started at `start` is stored. */
function slotKey(start: number, client: string): string {
  return `${start}/${client}`;
}

function parseSlot(key: string): { start: number; client: string } | null {
  const slash = key.indexOf('/');
  const start = Number(key.slice(0, slash));
  return slash > 0 && Number.isFinite(start) ? { start, client: key.slice(slash + 1) } : null;
}
