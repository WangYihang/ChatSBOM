/**
 * Who a request is from, as far as a rate limiter is concerned.
 *
 * Both limiters — the chat's and the query endpoint's — key on this, so
 * the two cannot disagree about who a visitor is.
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
 */

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
  if (env.EDGE_SECRET) {
    const given = request.headers.get(EDGE_SECRET_HEADER);
    if (given === null || !sameSecret(given, env.EDGE_SECRET)) {
      return UNVERIFIED;
    }
  }
  return request.headers.get('cf-connecting-ip') ?? ANONYMOUS;
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
