/**
 * ALTCHA's widget, in the tests (#144): the page's own, `altcha/external`
 * as `src/ask/altcha.ts` sets it up, with only its workers stood in for.
 *
 * jsdom runs no Web Worker, and does not say it is a secure context,
 * which the widget refuses to solve without. So a test that asks a
 * question says it is one, and gives the widget workers that answer any
 * challenge with SOLUTION, as a worker that had found it would, or fail
 * as one that could not start. What the widget does with the challenge
 * it fetched and the solution it was given is its own: the payload that
 * goes with the question is the widget's writing, not the test's.
 */
import { vi } from 'vitest';

/** The algorithm the service's challenges use (`chatsbom/server/challenge.py`). */
const ALGORITHM = 'PBKDF2/SHA-256';

/**
 * A challenge as the service issues one: its shape, with made-up hex,
 * expiring in ten minutes. The widget checks its shape, not its
 * signature: that is the service's to check.
 */
export function challenge(): { parameters: Record<string, unknown>; signature: string } {
  return {
    parameters: {
      algorithm: ALGORITHM,
      nonce: '00112233445566778899aabbccddeeff',
      salt: 'ffeeddccbbaa99887766554433221100',
      cost: 5000,
      keyLength: 32,
      keyPrefix: 'abcdef0123456789abcdef0123456789',
      keySignature: '1f'.repeat(32),
      expiresAt: Math.floor(Date.now() / 1000) + 600,
      data: { client: '203.0.113.7' },
    },
    signature: '0f'.repeat(32),
  };
}

/** What the stand-in workers find, for any challenge. */
export const SOLUTION = {
  counter: 742,
  derivedKey: `abcdef0123456789abcdef0123456789${'00'.repeat(16)}`,
  time: 12.5,
};

/**
 * What the widget sends with the question for `issued`, solved as
 * SOLUTION: the challenge and what solves it, base64 of their JSON, as
 * `chatsbom/server/challenge.py` reads a payload.
 */
export function payload(issued: ReturnType<typeof challenge>): string {
  return btoa(
    JSON.stringify({
      challenge: { parameters: issued.parameters, signature: issued.signature },
      solution: SOLUTION,
    }),
  );
}

/** A Web Worker, stood in for: it answers the widget's `work` at once. */
export class StandInWorker extends EventTarget {
  terminated = false;
  readonly posted: unknown[] = [];

  constructor(private readonly outcome: 'solve' | 'fail' | 'hold') {
    super();
  }

  postMessage(message: { type?: string }): void {
    this.posted.push(message);
    if (message.type !== 'work' || this.outcome === 'hold') return;
    setTimeout(() => {
      if (this.outcome === 'fail') {
        this.dispatchEvent(new Event('error'));
      } else {
        this.dispatchEvent(new MessageEvent('message', { data: SOLUTION }));
      }
    }, 0);
  }

  terminate(): void {
    this.terminated = true;
  }
}

/**
 * The widget, able to solve in the tests: a secure context, and workers
 * that do as `outcome` says. Resolves with the workers it starts, as it
 * starts them.
 *
 * The page's widget module is loaded first: it sets the widget up, and
 * gives it the page's own worker for the algorithm, which this replaces.
 */
export async function solving(
  outcome: 'solve' | 'fail' | 'hold' = 'solve',
): Promise<StandInWorker[]> {
  vi.stubGlobal('isSecureContext', true);
  await import('../src/ask/altcha');
  const started: StandInWorker[] = [];
  globalThis.$altcha.algorithms.set(ALGORITHM, () => {
    const worker = new StandInWorker(outcome);
    started.push(worker);
    return worker as unknown as Worker;
  });
  return started;
}
