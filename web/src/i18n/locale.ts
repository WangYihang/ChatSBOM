/**
 * Which language the page speaks, and the reader's say in it.
 *
 * Kept apart from the strings themselves so a component can take the
 * locale without pulling in both dictionaries, and so the choice can be
 * read once at the root rather than negotiated per panel.
 *
 * The default comes from the browser rather than from a fixed `en`: a
 * reader whose machine is set to Chinese should not have to find a
 * control to be understood. `navigator.languages` is consulted in
 * order, so `['en-GB', 'zh-CN']` means English — the first preference
 * wins rather than the first one this page happens to support.
 */
import { useCallback, useEffect, useState } from 'react';

export type Locale = 'en' | 'zh';

const STORAGE_KEY = 'chatsbom:locale';

/** The choices, in the order the control offers them. */
export const LOCALES: readonly Locale[] = ['en', 'zh'];

/** What each is called, in itself — never translated. */
export const LOCALE_NAMES: Readonly<Record<Locale, string>> = {
  en: 'English',
  zh: '中文',
};

function isLocale(value: unknown): value is Locale {
  return value === 'en' || value === 'zh';
}

/**
 * The reader's preference, as far as it can be known.
 *
 * `localStorage` is wrapped because it throws rather than returning
 * null in a few real configurations — Safari's private mode, a blocked
 * third-party context — and a language preference is not worth failing
 * a page load over.
 */
function initialLocale(): Locale {
  try {
    const stored = localStorage.getItem(STORAGE_KEY);
    if (isLocale(stored)) return stored;
  } catch {
    // Fall through to the browser's preference.
  }
  const preferred =
    typeof navigator === 'undefined'
      ? []
      : (navigator.languages ?? [navigator.language]).filter(Boolean);
  for (const tag of preferred) {
    // `zh`, `zh-CN`, `zh-Hant` all mean Chinese here; there is one
    // Chinese dictionary and matching the subtag would reject most of
    // the tags a browser actually sends.
    const base = String(tag).toLowerCase().split('-')[0];
    if (base === 'zh') return 'zh';
    if (base === 'en') return 'en';
  }
  return 'en';
}

export function useLocale(): {
  locale: Locale;
  setLocale: (next: Locale) => void;
} {
  const [locale, setStored] = useState<Locale>(initialLocale);

  // `lang` on the root element, because it is not decoration: it
  // selects the font stack for CJK, tells a screen reader which voice
  // to use, and decides how `text-wrap` breaks a line.
  useEffect(() => {
    document.documentElement.lang = locale === 'zh' ? 'zh-CN' : 'en';
  }, [locale]);

  const setLocale = useCallback((next: Locale) => {
    setStored(next);
    try {
      localStorage.setItem(STORAGE_KEY, next);
    } catch {
      // The choice still applies to this tab; only persistence is lost.
    }
  }, []);

  return { locale, setLocale };
}
