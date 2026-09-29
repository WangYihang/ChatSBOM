/**
 * The document's own words, in the page's language (#123).
 *
 * `index.html` gives the page its title, which a tab and a bookmark
 * show, and the description a search result shows, both in English,
 * and nothing changed them: the Chinese page said everything in Chinese
 * but those. They are said where the page says which language it is in,
 * the root's `lang`, so the language switch changes the three together.
 */
// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import html from '../index.html?raw';
import { App } from '../src/app';
import { DICTIONARIES } from '../src/i18n/strings';

const EN = DICTIONARIES.en;
const ZH = DICTIONARIES.zh;

/** A Chinese character. */
const CJK = /[一-鿿]/;

/** The page as it is served, before the script has run. */
const served = new DOMParser().parseFromString(html, 'text/html');

const description = (from: Document = document) =>
  from.querySelector('meta[name="description"]')?.getAttribute('content') ?? null;

/** What the page's questions answer: little, but in the shapes they answer in. */
const ANSWERS: Record<string, unknown> = {
  meta: {
    generator: 'chatsbom/test',
    schemaVersion: 'd1 v5',
    observedFrom: '2026-02-11',
    observedTo: '2026-09-13',
  },
  totals: { repositories: 3, dependencies: 10, packages: 4, classified: 9, tracked: 5 },
  relationshipSplit: { direct: 1, transitive: 4, unknown: 0 },
  edgeAmbiguity: null,
};

beforeEach(() => {
  cleanup();
  localStorage.clear();
  // jsdom starts with an empty head: the one the page is served with.
  document.head.innerHTML = served.head.innerHTML;
  document.documentElement.lang = served.documentElement.lang;
  window.history.replaceState(null, '', '#/overview');
  vi.stubGlobal('matchMedia', (query: string) => ({
    matches: false,
    media: query,
    addEventListener: () => {},
    removeEventListener: () => {},
  }));
  vi.stubGlobal(
    'fetch',
    vi.fn(async (_url: string, init?: RequestInit) => {
      const { method } = JSON.parse(String(init?.body)) as { method: string };
      return new Response(JSON.stringify(method in ANSWERS ? ANSWERS[method] : []), {
        headers: { 'content-type': 'application/json' },
      });
    }),
  );
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe('the title and the description', () => {
  it('are served in English, as the dictionary says them', () => {
    // index.html's copy is what a reader sees before the script has run,
    // and all a crawler that runs none ever reads, so it is kept to the
    // English page's word for word.
    expect(served.title).toBe(EN.documentTitle);
    expect(description(served)).toBe(EN.documentDescription);
    expect(served.documentElement.lang).toBe('en');
  });

  it('are in Chinese on the Chinese page', async () => {
    localStorage.setItem('chatsbom:locale', 'zh');
    render(<App />);
    await waitFor(() => expect(document.title).toMatch(CJK));
    expect(document.title).toBe(ZH.documentTitle);
    expect(description()).toMatch(CJK);
    expect(description()).toBe(ZH.documentDescription);
    expect(document.documentElement.lang).toBe('zh-CN');
  });

  it('follow the language switch, with the page’s language', async () => {
    render(<App />);
    const chinese = await screen.findByRole('button', { name: '中文' });
    expect([document.documentElement.lang, document.title, description()]).toEqual([
      'en',
      EN.documentTitle,
      EN.documentDescription,
    ]);

    fireEvent.click(chinese);
    expect([document.documentElement.lang, document.title, description()]).toEqual([
      'zh-CN',
      ZH.documentTitle,
      ZH.documentDescription,
    ]);

    fireEvent.click(screen.getByRole('button', { name: 'English' }));
    expect([document.documentElement.lang, document.title, description()]).toEqual([
      'en',
      served.title,
      description(served),
    ]);
  });
});
