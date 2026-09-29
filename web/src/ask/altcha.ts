/**
 * ALTCHA's widget: the proof of work each question carries (#144).
 *
 * Turnstile goes, since Cloudflare says it is not supported in mainland
 * China (#128, section 2.8, and the owner's decision on Q6). In its
 * place the service issues an ALTCHA challenge, `GET
 * /api/ask/challenge`, signed for the client that asked, and the widget
 * solves it in Web Workers, and its solution goes with the question,
 * which the service verifies once (`chatsbom/server/challenge.py`).
 *
 * The page's policy is this origin's alone, with nothing inline
 * (`chatsbom/server/app.py`). The widget's default entry writes its
 * styles into a <style> it makes, and starts its workers from blob:
 * URLs, and the policy refuses both. So the page takes its `external`
 * entry, its stylesheet as a file of the build, and the one worker the
 * service's challenges need, PBKDF2 over SHA-256, with `?worker`, which
 * the build makes a file of its own too.
 *
 * Out of sight, and started by the page: the reader asks, and the
 * challenge is solved before the question goes, as Turnstile's
 * `interaction-only` widget was. Loaded with the Ask panel, and never
 * with the page (`test/split.test.ts`).
 */
import 'altcha/external';
import 'altcha/altcha.css';
import Pbkdf2Worker from 'altcha/workers/pbkdf2?worker';

import { AskError, refusal } from './stream';

/** The algorithm the service's challenges use (`chatsbom/server/challenge.py`). */
const ALGORITHM = 'PBKDF2/SHA-256';

/** Where the service issues a challenge. */
const CHALLENGE = '/api/ask/challenge';

globalThis.$altcha.algorithms.set(ALGORITHM, () => new Pbkdf2Worker());
globalThis.$altcha.defaults.set({
  // Solved when the page says, and nothing drawn: no box to tick, no
  // logo, no footer.
  auto: 'off',
  display: 'invisible',
  hideFooter: true,
  hideLogo: true,
  // No pointer or keys watched for a signature the service never asks
  // for, and no pause added to a solve that was quick.
  humanInteractionSignature: false,
  minDuration: 0,
});

/** What the page says when the widget could not solve, and no refusal said why. */
const UNVERIFIED = 'The human verification check could not be completed. Reload and retry.';

/**
 * Solves a challenge each time it is called, in a widget drawn in the
 * element `host` returns, and resolves with what goes with the
 * question. The host is asked each time: it can be redrawn.
 *
 * A challenge the service refused, one of too many questions or with
 * the chat off, fails with the service's own code, as the question
 * would have; anything else with `unverified`.
 */
export function altchaSolver(
  host: () => HTMLElement | null,
  challenge = CHALLENGE,
): () => Promise<string> {
  return async () => {
    const place = host();
    if (!place) {
      throw new AskError({ code: 'unverified', said: UNVERIFIED, status: null });
    }
    let refused: AskError | null = null;
    const widget = document.createElement('altcha-widget');
    const loaded = new Promise<void>((resolve) => {
      widget.addEventListener('load', () => resolve(), { once: true });
    });
    place.appendChild(widget);
    try {
      await loaded;
      await widget.configure({
        challenge,
        // The widget's own fetch, which keeps what a refusal said: the
        // widget says only that it failed.
        fetch: async (input, init) => {
          const response = await fetch(input, init);
          if (!response.ok) refused = await refusal(response.clone());
          return response;
        },
      });
      const solved = await widget.verify();
      if (solved?.payload) return solved.payload;
      throw refused ?? new AskError({ code: 'unverified', said: UNVERIFIED, status: null });
    } finally {
      widget.remove();
    }
  };
}
