/**
 * The query endpoint's rate limit, run in workerd (#115).
 *
 * The runtime `wrangler dev` serves the dashboard with under compose,
 * started here by wrangler's own test harness, with the Worker's
 * limiters exactly as wrangler.jsonc configures them. Nothing leaves
 * the machine: the one store bound is never asked, since the method
 * called is not one.
 *
 * The limiters counted in windows aligned to the wall clock, so a
 * client's budget came back whole at every multiple of the period, and
 * a burst just before one and another just after got twice the budget,
 * moments apart. Here one client sends its budget's worth just before
 * such a boundary, and again just after.
 */
import { afterAll, beforeAll, expect, it } from 'vitest';
import { createTestHarness, unstable_readConfig } from 'wrangler';

/** The web project: where wrangler.jsonc and the Worker's sources are. */
const WEB = decodeURIComponent(new URL('..', import.meta.url).pathname);

/** The query endpoint's budget, as wrangler.jsonc sets it: 100 calls in 10 seconds. */
const LIMIT = 100;
const PERIOD_MS = 10_000;

let harness: ReturnType<typeof createTestHarness> | undefined;

beforeAll(async () => {
  const config = unstable_readConfig({ config: `${WEB}/wrangler.jsonc` }, { hideWarnings: true });
  harness = createTestHarness({
    root: WEB,
    workers: [
      {
        // The Worker as deployed, less the assets, which are a build
        // output; its limiters as configured, whatever keeps them.
        config: {
          name: 'chatsbom-rate-limit-test',
          main: 'src/worker.ts',
          compatibility_date: config.compatibility_date,
          ratelimits: config.ratelimits,
          durable_objects: config.durable_objects,
          migrations: config.migrations,
          vars: {
            ...config.vars,
            // A store, so that /api/q gets as far as its limiter.
            CLICKHOUSE_URL: 'http://127.0.0.1:9',
          },
        },
      },
    ],
  });
  await harness.listen();
}, 120_000);

afterAll(async () => {
  await harness?.close();
});

/** How many of `count` calls sent at once from `client` the Worker let through. */
async function burst(client: string, count: number): Promise<number> {
  const statuses = await Promise.all(
    Array.from({ length: count }, async () => {
      const response = await harness!.fetch('/api/q', {
        method: 'POST',
        headers: { 'content-type': 'application/json', 'cf-connecting-ip': client },
        body: JSON.stringify({ method: 'notAMethod' }),
      });
      await response.text();
      return response.status;
    }),
  );
  // 400: let through, and refused as a method that does not exist.
  // 429: over the limit. Anything else is not this test's answer.
  expect(statuses.filter((status) => status !== 400 && status !== 429)).toEqual([]);
  return statuses.filter((status) => status === 400).length;
}

const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, Math.max(0, ms)));

it('gives a burst across a window boundary one budget, not two', async () => {
  const client = '198.51.100.7';
  // The first multiple of the period at least 2.5 s away: the first
  // burst starts 2.5 s before it, and ends well before it.
  const boundary = Math.ceil((Date.now() + 2_500) / PERIOD_MS) * PERIOD_MS;
  await sleep(boundary - 2_500 - Date.now());
  const before = await burst(client, LIMIT);
  expect(Date.now(), 'the first burst took until the boundary').toBeLessThan(boundary);

  await sleep(boundary + 50 - Date.now());
  const after = await burst(client, LIMIT);
  const elapsed = Date.now() - boundary;

  expect(before).toBe(LIMIT);
  // The first burst still counts after the boundary, for less the
  // further the window has slid past it: when the second ended, it had
  // slid `elapsed` of the period's way, and let through as much of the
  // budget. One more for the clock's granularity.
  expect(after).toBeLessThanOrEqual(Math.floor((LIMIT * elapsed) / PERIOD_MS) + 1);
}, 60_000);
