/**
 * Which theme the page renders in, and the reader's say in it.
 *
 * The stylesheet already handles three states — `:root` carries light,
 * `@media (prefers-color-scheme: dark)` is guarded so an explicit light
 * choice still wins, and `[data-theme='dark']` lets a toggle win in
 * both directions. So this only has to set the attribute and remember
 * the choice.
 *
 * `system` removes the attribute rather than writing the resolved
 * value. Writing it would freeze the page at whatever the OS said when
 * the tab opened: a reader whose machine switches at sunset would keep
 * the daytime theme until they reloaded.
 *
 * The media-query subscription is load-bearing for a second reason.
 * `chartTheme()` reads the palette at draw time and nothing listened,
 * so an OS switch left every chart in the previous theme's colours
 * until some unrelated state change forced a redraw. Holding this at
 * the app root means a change re-renders the tree that draws them.
 *
 * The attribute is written where the choice is made, never in an
 * effect (#42). An effect runs after the render it follows, and the
 * charts read the attribute during that render: a click on Dark redrew
 * every chart in the light palette, and each click after drew the one
 * before it.
 *
 * What remains is the first paint, which happens before the bundle
 * runs: a stored choice that differs from the system's shows the
 * system's background until then. It stays. The page's policy refuses
 * an inline script (#31), and a script file of its own would be fetched
 * before every first paint, for every visitor — from `public/`, where
 * nothing is content-hashed, so revalidated each time — to spare that
 * flash to the few whose choice differs from their system's. And the
 * page is empty until the bundle has run; what flashes is its
 * background.
 */
import { useCallback, useEffect, useState } from 'react';

export type ThemeChoice = 'light' | 'dark' | 'system';
export type ResolvedTheme = 'light' | 'dark';

const STORAGE_KEY = 'chatsbom:theme';
const QUERY = '(prefers-color-scheme: dark)';

/** The three choices, in the order the control offers them. */
export const THEME_CHOICES: readonly ThemeChoice[] = [
  'light',
  'dark',
  'system',
];

function isChoice(value: unknown): value is ThemeChoice {
  return value === 'light' || value === 'dark' || value === 'system';
}

/**
 * The stored choice, or `system`.
 *
 * Wrapped because `localStorage` throws rather than returning null in
 * a few real configurations — Safari's private mode, a blocked
 * third-party context — and a theme preference is not worth failing a
 * page load over.
 */
function storedChoice(): ThemeChoice {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    return isChoice(raw) ? raw : 'system';
  } catch {
    return 'system';
  }
}

function systemPrefers(): ResolvedTheme {
  if (typeof window === 'undefined' || !window.matchMedia) return 'light';
  return window.matchMedia(QUERY).matches ? 'dark' : 'light';
}

/** Put a choice on the document, where the stylesheet and the charts read it. */
function apply(choice: ThemeChoice): void {
  const root = document.documentElement;
  if (choice === 'system') {
    root.removeAttribute('data-theme');
  } else {
    root.dataset['theme'] = choice;
  }
}

export function useTheme(): {
  choice: ThemeChoice;
  resolved: ResolvedTheme;
  setChoice: (next: ThemeChoice) => void;
} {
  // Applied as it is read, before anything is drawn: a stored choice
  // left to an effect drew the first charts in the system's palette.
  const [choice, setStored] = useState<ThemeChoice>(() => {
    const stored = storedChoice();
    apply(stored);
    return stored;
  });
  const [system, setSystem] = useState<ResolvedTheme>(systemPrefers);

  // Tracked even while an explicit choice is in force, so switching
  // back to `system` lands on the current preference rather than the
  // one that applied when the tab opened.
  useEffect(() => {
    if (typeof window === 'undefined' || !window.matchMedia) return;
    const media = window.matchMedia(QUERY);
    const update = () => setSystem(media.matches ? 'dark' : 'light');
    media.addEventListener('change', update);
    return () => media.removeEventListener('change', update);
  }, []);

  const resolved: ResolvedTheme = choice === 'system' ? system : choice;

  const setChoice = useCallback((next: ThemeChoice) => {
    // Before the state that re-renders the charts, not after it.
    apply(next);
    setStored(next);
    try {
      localStorage.setItem(STORAGE_KEY, next);
    } catch {
      // The choice still applies to this tab; only persistence is lost.
    }
  }, []);

  return { choice, resolved, setChoice };
}
