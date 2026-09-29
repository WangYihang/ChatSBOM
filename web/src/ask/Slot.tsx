/**
 * The Ask panel's body as this page fills it: the agent, the element
 * Turnstile draws in, and the UI in the slot.
 *
 * The page's side of the seam (`useAsk`), drawn. The query view loads
 * it when it first draws the panel, not with the page (#44), and draws
 * the panel once the view is first shown (#123): the agent loop, the
 * tools it runs and the challenge are what a question needs, and
 * nothing else on the page does. So only `QueryView` imports this, with
 * `import()`, and nothing the page loads first may import it, or what
 * it alone imports (`test/split.test.ts`, `test/askchunk.test.tsx`).
 */
import { useRef } from 'react';

import type { DatasetClient } from '../d1/client';
import type { Locale } from '../i18n/locale';
import type { Dictionary } from '../i18n/strings';
import { AskPlaceholder } from './Placeholder';
import { useAsk } from './useAsk';

export function AskSlot({
  dataset,
  locale,
  words,
  onPackage,
  suggestions,
}: {
  dataset: DatasetClient;
  locale: Locale;
  words: Dictionary;
  /** Send the reader to a package's view. */
  onPackage: (name: string) => void;
  /** Questions to offer while the box is empty. */
  suggestions: readonly string[];
}) {
  // The slot's only dependency on the page — and the element Turnstile
  // draws in, for a deployment that requires it (#32).
  const challengeHost = useRef<HTMLDivElement>(null);
  const { ask, reset } = useAsk(dataset, challengeHost, locale);

  return (
    <>
      <AskPlaceholder
        ask={ask}
        reset={reset}
        words={words}
        onPackage={onPackage}
        suggestions={suggestions}
      />
      {/* Turnstile's widget, drawn while a question is being
          verified and seen only if Cloudflare wants a click. */}
      <div ref={challengeHost} className="challenge" />
    </>
  );
}
