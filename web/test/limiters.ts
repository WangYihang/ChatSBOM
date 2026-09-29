/**
 * The rate limiters' counters (#115), as the endpoints' tests stand
 * them in: a `RATE_LIMITER` namespace whose every object answers as the
 * test says, and keeps what it was asked. `ratelimit.test.ts` runs the
 * real `RateLimiter`; `ratelimit.integration.test.ts` runs it in workerd.
 */
import type { RateLimiter } from '../src/ratelimit';

/** One request counted against a limiter. */
export interface Asked {
  /** The object asked: the limiter's name, and where the request came in. */
  object: string;
  client: string;
  limit: number;
  period: number;
}

/**
 * A namespace of counters that let a request through when `admit` says
 * so, or fail as `admit` does.
 */
export function limiters(admit: boolean | (() => Promise<boolean>)) {
  const asked: Asked[] = [];
  const namespace = {
    getByName: (object: string) => ({
      admit: async (client: string, limit: number, period: number) => {
        asked.push({ object, client, limit, period });
        return typeof admit === 'function' ? admit() : admit;
      },
    }),
  } as unknown as DurableObjectNamespace<RateLimiter>;
  return { namespace, asked, clients: () => asked.map(({ client }) => client) };
}
