/**
 * The page's side of the natural-language seam.
 *
 * Adapts the service's chat (`stream.ts`, #144) to the `AskFn` in
 * ./contract.ts, so whatever UI sits in the slot needs to know only
 * "question in, prose out, progress on the way" — not that the model's
 * loop runs in the service, nor that each question carries a proof of
 * work its widget solves first (`altcha.ts`).
 *
 * The widget is drawn in `challengeHost`, an element the page owns
 * beside the slot, so the slot still needs no DOM host of its own. It is
 * loaded when the first question is asked, not with the panel: it is
 * most of what a question needs, and a reader who asks nothing is not
 * sent it, as Turnstile's script was not (`test/split.test.ts`).
 *
 * A conversation is the questions asked since the last `reset`, and the
 * answers they were given: each question brings the last of them to the
 * service as text (`earlier`), which is all the service takes of what
 * came before.
 */
import { useCallback, useMemo, useRef, type RefObject } from 'react';

import type { AskFn, AskProgress } from './contract';
import { ask as askService, AskError, earlier, type Exchange } from './stream';

export interface Asking {
  ask: AskFn;
  /** Start a new conversation (#42). */
  reset: () => void;
}

/** A solver of challenges, as `altcha.ts` makes one. */
type Solve = () => Promise<string>;

export function useAsk(challengeHost: RefObject<HTMLElement | null>): Asking {
  // What each question brings: the conversation so far, a list of its
  // own after each reset, so a question still on its way when the
  // reader starts over adds nothing to the new one.
  const conversation = useRef<Exchange[]>([]);
  const solve = useMemo(() => {
    let loading: Promise<Solve> | null = null;
    return async () => {
      loading ??= import('./altcha').then(({ altchaSolver }) =>
        altchaSolver(() => challengeHost.current),
      );
      let solver: Solve;
      try {
        solver = await loading;
      } catch (error) {
        // Asked for again by the next question, rather than failing it
        // too: a load that failed once may not again.
        loading = null;
        throw new AskError({
          code: 'unverified',
          said: `The human verification check could not be loaded (${String(error)}).`,
          status: null,
        });
      }
      return solver();
    };
  }, [challengeHost]);

  const ask = useCallback(
    async (question: string, progress: AskProgress = {}) => {
      const asked = conversation.current;
      // Before the question goes: it carries what solves the challenge.
      const altcha = await solve();
      const answer = await askService({ question, prior: earlier(asked), altcha }, progress);
      // Only an answer joins the conversation: a question that failed
      // leaves it as it was (#42).
      asked.push({ q: question, a: answer });
      return answer;
    },
    [solve],
  );
  const reset = useCallback(() => {
    conversation.current = [];
  }, []);

  return { ask, reset };
}
