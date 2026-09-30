/**
 * The Ask panel's body as this page fills it: the question, the element
 * ALTCHA's widget is drawn in, and the UI in the slot.
 *
 * The page's side of the seam (`useAsk`), drawn. The query view loads
 * it when it first draws the panel, not with the page (#44), and draws
 * the panel once the view is first shown (#123): the chat's client and
 * the proof of work's widget are what a question needs, and nothing
 * else on the page does. So only `QueryView` imports this, with
 * `import()`, and nothing the page loads first may import it, or what
 * it alone imports (`test/split.test.ts`, `test/askchunk.test.tsx`).
 */
import { useRef } from 'react';

import type { Dictionary } from '../i18n/strings';
import { AskPlaceholder } from './Placeholder';
import { useAsk } from './useAsk';

export function AskSlot({
  words,
  onPackage,
  suggestions,
}: {
  words: Dictionary;
  /** Send the reader to a package's view. */
  onPackage: (name: string) => void;
  /** Questions to offer while the box is empty. */
  suggestions: readonly string[];
}) {
  // The slot's only dependency on the page — and the element ALTCHA's
  // widget is drawn in while a question's challenge is solved (#144).
  const challengeHost = useRef<HTMLDivElement>(null);
  const { ask, reset } = useAsk(challengeHost);

  return (
    <>
      <AskPlaceholder
        ask={ask}
        reset={reset}
        words={words}
        onPackage={onPackage}
        suggestions={suggestions}
      />
      {/* ALTCHA's widget, drawn while a question's challenge is solved,
          and never seen: the page solves it for the reader. */}
      <div ref={challengeHost} className="challenge" />
    </>
  );
}
