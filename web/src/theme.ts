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

export function useTheme(): {
  choice: ThemeChoice;
  resolved: ResolvedTheme;
  setChoice: (next: ThemeChoice) => void;
} {
  const [choice, setStored] = useState<ThemeChoice>(storedChoice);
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

  useEffect(() => {
    const root = document.documentElement;
    if (choice === 'system') {
      root.removeAttribute('data-theme');
    } else {
      root.dataset['theme'] = choice;
    }
  }, [choice]);

  const setChoice = useCallback((next: ThemeChoice) => {
    setStored(next);
    try {
      localStorage.setItem(STORAGE_KEY, next);
    } catch {
      // The choice still applies to this tab; only persistence is lost.
    }
  }, []);

  return { choice, resolved, setChoice };
}
