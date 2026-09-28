/**
 * The daily spend cap, run in workerd (#33).
 *
 * The runtime `wrangler dev` serves the dashboard with under compose,
 * started here by wrangler's own test harness, with the Durable Object
 * binding and migration from wrangler.jsonc and a stand-in for the
 * Messages API on this machine. Nothing leaves it: the Worker is told
 * to send its model calls to the stand-in, and wrangler not to fetch
 * the request metadata it would otherwise ask Cloudflare for
 * (vitest.config.ts).
 *
 * What the unit tests cannot show is that the binding, the migration
 * and the class exported from the Worker's entry make one object that
 * many requests reach at once, and that it holds the cap there.
 */
import { afterAll, beforeAll, expect, it, vi } from 'vitest';
import { createTestHarness, unstable_readConfig } from 'wrangler';

import { standIn } from './standin.mjs';

/** The web project: where wrangler.jsonc and the Worker's sources are. */
const WEB = decodeURIComponent(new URL('..', import.meta.url).pathname);

/** What the Worker is told a day may spend. */
const CAP = 1;

/**
 * The least any turn can be reserved at: its output alone, 8,192
 * tokens at $25 a million. So no more turns than this can be in flight
 * against the cap at once, whatever their input.
 */
const MOST_AT_ONCE = Math.floor(CAP / ((8192 * 25) / 1_000_000));

const model = standIn();
let harness: ReturnType<typeof createTestHarness> | undefined;

beforeAll(async () => {
  const config = unstable_readConfig({ config: `${WEB}/wrangler.jsonc` }, { hideWarnings: true });
  harness = createTestHarness({
    root: WEB,
    workers: [
      {
        // The Worker as deployed, less the assets, which are a build
        // output, and the limiters, which would refuse a burst for
        // reasons of their own.
        config: {
          name: 'chatsbom-spend-cap-test',
          main: 'src/worker.ts',
          compatibility_date: config.compatibility_date,
          durable_objects: config.durable_objects,
          migrations: config.migrations,
          vars: {
            ANTHROPIC_API_KEY: 'not-a-key',
            ANTHROPIC_BASE_URL: await model.listen(),
            DAILY_SPEND_CAP_USD: String(CAP),
          },
        },
      },
    ],
  });
  await harness.listen();
}, 120_000);

afterAll(async () => {
  model.release();
  await harness?.close();
  await model.close();
});

it('admits no more questions at once than the cap can pay for, and settles them', async () => {
  const { url } = await harness!.listen();
  const day = new Date().toISOString().slice(0, 10);
  const statuses: number[] = [];

  const questions = Array.from({ length: 12 }, () =>
    harness!
      .fetch('/api/chat', {
        method: 'POST',
        headers: { 'content-type': 'application/json', origin: url.origin },
        body: JSON.stringify({ messages: [{ role: 'user', content: 'who declares mail?' }] }),
      })
      .then(async (response) => {
        await response.text();
        statuses.push(response.status);
      }),
  );

  try {
    // A refusal waits on nothing; an admitted question waits on the model.
    await vi.waitFor(() => expect(statuses.length + model.calls()).toBe(12), {
      timeout: 30_000,
      interval: 50,
    });
    const admitted = model.calls();
    expect(admitted).toBeGreaterThan(0);
    expect(admitted).toBeLessThanOrEqual(MOST_AT_ONCE);
    expect(statuses).toEqual(Array(12 - admitted).fill(429));
  } finally {
    model.release();
  }
  await Promise.all(questions);

  // Each settled at what it cost, once answered.
  const env = (await harness!.getWorker().getEnv()) as {
    SPEND_COUNTER: DurableObjectNamespace;
  };
  const usage = () =>
    (
      env.SPEND_COUNTER.getByName(day) as unknown as {
        usage(): Promise<{ spent: number; held: number }>;
      }
    ).usage();
  const cost = (1_000 * 5 + 100 * 25) / 1_000_000;
  await vi.waitFor(async () => expect((await usage()).held).toBe(0), {
    timeout: 30_000,
    interval: 50,
  });
  expect((await usage()).spent).toBeCloseTo(model.calls() * cost, 10);
}, 120_000);
