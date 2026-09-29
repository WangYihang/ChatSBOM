/**
 * The page in Chinese says nothing in English (#43).
 *
 * The dictionary had both languages for things the page never asked it
 * for — the loading note, the tree's bound, the empty panels — while the
 * markup beside them said the same in English, and the charts wrote
 * their tooltips, legends and accessible names in English whichever
 * language was chosen. `literals.test.ts` reads the markup for words;
 * this renders the page in Chinese, as a reader gets it, and reads what
 * it says: every text node, every name and label a screen reader is
 * given, and the tooltip of every mark.
 *
 * Latin words are allowed where the Chinese copy keeps them on purpose,
 * each with its reason below, and where they are the data's own: a
 * package, an ecosystem, a licence is named as the data names it.
 */
// @vitest-environment jsdom
import { act, cleanup, fireEvent, render, waitFor } from '@testing-library/react';
import { afterEach, beforeAll, beforeEach, describe, expect, it, vi } from 'vitest';

import { App } from '../src/app';
import { DICTIONARIES } from '../src/i18n/strings';
import { WHOLE_PAGE } from './answers';

/**
 * Latin words the Chinese page is right to show, and why.
 *
 * The Chinese copy keeps a product's name, and the technical terms
 * Chinese technical writing leaves in English, as they are.
 */
const KEPT: Record<string, string> = {
  Chat: 'the product name, ChatSBOM, is drawn as Chat and SBOM',
  SBOM: 'the product name, and the term the Chinese copy uses',
  English: 'the language switch names each language in its own',
  GitHub: 'a product',
  Syft: 'a tool',
  Gradle: 'a build tool',
  Anthropic: 'a company',
  SQL: 'a language the copy says the model cannot write',
  npm: 'a registry the copy names',
  Maven: 'a registry the copy names',
  TypeScript: 'a language the copy names',
  Go: 'a language the copy names',
  lockfile: 'kept in English by the Chinese copy, a term of the trade',
  manifest: 'kept in English by the Chinese copy, a term of the trade',
  other: 'a folded language, spelled as the data spells it',
  none: 'the folded language of a repository GitHub names none, likewise',
  semver: 'a package the ranking note quotes',
  debug: 'a package the ranking note quotes',
  ms: 'a package the ranking note quotes',
  laravel: 'a package the search box offers as an example',
  express: 'a package the search box offers as an example',
  'spring-boot-starter-web': 'a package the search box offers as an example',
};

/** What each method answers: every panel has something to draw. */
const FULL: Record<string, unknown> = {
  meta: {
    generator: 'chatsbom/test',
    schemaVersion: 'd1 v5',
    observedFrom: '2026-02-11',
    observedTo: '2026-09-13',
  },
  totals: {
    repositories: 24_339, dependencies: 6_062_896, packages: 141_938,
    classified: 6_053_469, tracked: 60_017,
  },
  relationshipSplit: { direct: 463_150, transitive: 5_590_319, unknown: 9_427 },
  relationshipByEcosystem: [
    { ecosystem: 'npm', direct: 3_000, transitive: 9_000, unknown: 10, records: 12_010 },
    { ecosystem: 'cargo', direct: 400, transitive: 410, unknown: 0, records: 810 },
  ],
  languageCoverage: [
    { language: 'rust', repositories: 800, withSbom: 700, withSyft: 600, withDepgraph: 500, withManifest: 0 },
    { language: '', repositories: 90, withSbom: 20, withSyft: 20, withDepgraph: 0, withManifest: 0 },
  ],
  ecosystemCoverage: [
    { ecosystem: 'npm', repositories: 5_000, withAny: 4_000, withSyft: 3_000, withDepgraph: 2_000, withManifest: 0 },
  ],
  dependencyDistribution: [
    { label: '1-9', repositories: 4_228 },
    { label: '10-24', repositories: 1_589 },
  ],
  sourceComparison: [{ ecosystem: 'maven', syft: 9_648, depgraph: 47_329, manifest: 1_200 }],
  licenseShares: [
    { license: 'MIT', repositoryCount: 1_200, packageCount: 3_400 },
    { license: '', repositoryCount: 300, packageCount: 900 },
  ],
  topPackages: [{ name: 'serde', repositoryCount: 6_863, directCount: 6_820 }],
  ecosystemsFor: [
    { type: 'gem', repositoryCount: 1_167, directCount: 30 },
    { type: 'pypi', repositoryCount: 6, directCount: 2 },
  ],
  countDependents: 1_234,
  countDependentRows: 1_500,
  dependentsOf: [
    {
      owner: 'rails', repo: 'rails', stars: 58_182, version: '2.8.1',
      url: 'https://github.com/rails/rails', relationship: 'transitive',
      observedAt: '2026-09-13', ecosystem: 'gem', language: 'ruby', manifests: 3,
    },
  ],
  versionSpread: {
    versions: [{ version: '2.8.1', repositoryCount: 1_100, kind: 'resolved' }],
    constrained: 12,
    unversioned: 3,
  },
  adoptionOverTime: [
    { source: 'syft', month: '2026-02', repositoryCount: 1_124, directCount: 30 },
  ],
  edgeAmbiguity: {
    names: 225_582, ambiguousNames: 2_730, edges: 614_221,
    ambiguousEdges: 63_384, largestRepository: 5_388,
  },
  pulledInBy: [{ name: 'actionmailer', repositories: 7_999 }],
  dependencyTree: {
    root: 'mail',
    children: [{ name: 'mini_mime', repositories: 3_580 }],
    grandchildren: [{ parent: 'mini_mime', child: 'net-imap', repositories: 1_200 }],
  },
  searchPackages: [{ name: 'mail', ecosystem: 'gem', repositoryCount: 1_167, nameTotal: 1_173 }],
};

/** The same questions, answered with nothing: every panel's empty state. */
const EMPTY: Record<string, unknown> = {
  ...FULL,
  relationshipSplit: { direct: 0, transitive: 0, unknown: 0 },
  relationshipByEcosystem: [],
  languageCoverage: [],
  ecosystemCoverage: [],
  dependencyDistribution: [],
  sourceComparison: [],
  licenseShares: [],
  topPackages: [],
  ecosystemsFor: [],
  // One row, so the versions and adoption panels are drawn, and empty.
  countDependents: 1,
  countDependentRows: 1,
  versionSpread: { versions: [], constrained: 0, unversioned: 0 },
  adoptionOverTime: [],
  edgeAmbiguity: null,
  pulledInBy: [],
  dependencyTree: { root: 'mail', children: [], grandchildren: [] },
  searchPackages: [],
};

/** Keys whose values the page never shows as text. */
const UNSHOWN = new Set(['url', 'relationship', 'kind']);

/**
 * A word: a Latin letter, and what an identifier carries after it —
 * `mini_mime`, `net-imap`, `chatsbom/test` — up to a letter or digit,
 * so a full stop after one is not part of it.
 */
const WORD = /[A-Za-z](?:[\w@./-]*\w)?/g;

/** Every word in the answers' values: the data's own names. */
function dataWords(value: unknown, into = new Set<string>()): Set<string> {
  if (typeof value === 'string') {
    for (const word of value.match(WORD) ?? []) into.add(word);
  } else if (Array.isArray(value)) {
    for (const item of value) dataWords(item, into);
  } else if (value && typeof value === 'object') {
    for (const [key, item] of Object.entries(value)) {
      if (!UNSHOWN.has(key)) dataWords(item, into);
    }
  }
  return into;
}

/** `/api/q`, answering from `answers`; the methods in `hold` wait for `release`. */
function stubQueries(answers: Record<string, unknown>, hold: readonly string[] = []) {
  let release = () => {};
  const held = new Promise<void>((resolve) => (release = resolve));
  vi.stubGlobal(
    'fetch',
    vi.fn(async (_url: string, init?: RequestInit) => {
      const { method } = JSON.parse(String(init?.body)) as { method: string };
      if (hold.includes(method)) await held;
      return new Response(JSON.stringify(method in answers ? answers[method] : []), {
        headers: { 'content-type': 'application/json' },
      });
    }),
  );
  return { release };
}

/** The attributes whose value is read out or shown. */
const SPOKEN = ['aria-label', 'aria-description', 'title', 'placeholder', 'alt'];

/** Every piece of text the page says, where it says it. */
function said(): { where: string; text: string }[] {
  const found: { where: string; text: string }[] = [];
  const walker = document.createTreeWalker(document.body, NodeFilter.SHOW_TEXT);
  for (let node = walker.nextNode(); node; node = walker.nextNode()) {
    const parent = node.parentElement;
    if (parent?.closest('script, style')) continue;
    found.push({ where: `<${parent?.tagName.toLowerCase()}>`, text: node.textContent ?? '' });
  }
  for (const element of document.body.querySelectorAll('*')) {
    for (const name of SPOKEN) {
      const value = element.getAttribute(name);
      if (value) found.push({ where: `${element.tagName.toLowerCase()}[${name}]`, text: value });
    }
  }
  // What each mark says under the pointer, a line at a time.
  for (const mark of document.querySelectorAll('svg path, svg circle, svg text')) {
    fireEvent.mouseEnter(mark);
    for (const line of document.querySelectorAll('.chart-tooltip > *')) {
      found.push({ where: 'tooltip', text: line.textContent ?? '' });
    }
    fireEvent.mouseLeave(mark);
  }
  return found;
}

/** The words the page says that are neither kept on purpose nor data. */
function english(answers: Record<string, unknown>): string[] {
  const data = dataWords(answers);
  return said().flatMap(({ where, text }) =>
    (text.match(WORD) ?? [])
      .filter((word) => !(word in KEPT) && !data.has(word))
      .map((word) => `${word} (${where}: ${JSON.stringify(text.trim().slice(0, 80))})`),
  );
}

/** Words found, once each, so a failure reads as a list. */
const unique = (words: string[]) => [...new Set(words)];

// The parts of the page it loads when it draws them, loaded before the
// tests that wait for them: a module's first import is compiled, and on
// a busy machine that alone outlasted a second. The first test below
// failed about one full run in three on `main`, at a load average of 15
// on four cores, waiting for the tree (`WHOLE_PAGE`).
beforeAll(() =>
  Promise.all([
    import('../src/charts/DependencyTree'),
    import('../src/charts/TimeSeries'),
    import('../src/ask/Slot'),
  ]),
);

beforeEach(() => {
  cleanup();
  localStorage.clear();
  localStorage.setItem('chatsbom:locale', 'zh');
  document.documentElement.removeAttribute('data-theme');
  vi.stubGlobal('matchMedia', (query: string) => ({
    matches: false,
    media: query,
    addEventListener: () => {},
    removeEventListener: () => {},
  }));
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

describe('the page in Chinese', () => {
  it('says nothing in English on either view, with every panel drawn', async () => {
    stubQueries(FULL);
    window.history.replaceState(null, '', '#/query/mail');
    render(<App />);
    // Both views are drawn — the overview kept, hidden — once the last
    // of their answers is in.
    await waitFor(() => {
      expect(document.querySelector('g[data-row="serde"]')).not.toBeNull();
      expect(document.querySelector('g[data-node="net-imap"]')).not.toBeNull();
      expect(document.querySelector('g[data-row="actionmailer"]')).not.toBeNull();
      expect(document.querySelector('.pager')).not.toBeNull();
      expect(document.querySelector('footer')).not.toBeNull();
    }, WHOLE_PAGE);
    expect(unique(english(FULL))).toEqual([]);
  }, WHOLE_PAGE.timeout * 3);

  it('says nothing in English where the panels have nothing to draw', async () => {
    stubQueries(EMPTY);
    window.history.replaceState(null, '', '#/query/mail');
    render(<App />);
    // A panel still loading says so in the same place as an empty one
    // (`Answered`, #123), so the wait is for the empty ones alone.
    await waitFor(() => {
      const notes = [...document.querySelectorAll('.chart-empty')];
      expect(notes.length).toBeGreaterThan(5);
      expect(notes.map((note) => note.textContent)).not.toContain(
        DICTIONARIES.zh.loadingPart,
      );
      expect(document.querySelector('footer')).not.toBeNull();
    }, WHOLE_PAGE);
    expect(unique(english(EMPTY))).toEqual([]);
  }, WHOLE_PAGE.timeout * 3);

  it('says nothing in English while it is still loading', async () => {
    // The provenance and the tree held back: the page's loading note,
    // and the tree panel's.
    const { release } = stubQueries(FULL, ['meta', 'dependencyTree']);
    window.history.replaceState(null, '', '#/query/mail');
    render(<App />);
    try {
      await waitFor(
        () => expect(document.querySelector('g[data-row="actionmailer"]')).not.toBeNull(),
        WHOLE_PAGE,
      );
      expect(document.querySelector('footer')).toBeNull();
      expect(unique(english(FULL))).toEqual([]);
    } finally {
      await act(async () => release());
    }
  }, WHOLE_PAGE.timeout * 3);

  it('says nothing in English when its questions fail', async () => {
    // The Worker refuses every question but the provenance, and the
    // model cannot be reached: the failures a reader actually meets.
    vi.stubGlobal(
      'fetch',
      vi.fn(async (url: string, init?: RequestInit) => {
        const reply = (payload: unknown, status = 200) =>
          new Response(JSON.stringify(payload), {
            status,
            headers: { 'content-type': 'application/json' },
          });
        if (url === '/api/chat') {
          return (init?.method ?? 'GET') === 'GET'
            ? reply({ turnstile: null })
            : reply({ error: 'The model could not be reached. Try again shortly.' }, 502);
        }
        const { method } = JSON.parse(String(init?.body)) as { method: string };
        return method === 'meta'
          ? reply(FULL['meta'])
          : reply({ error: 'Too many queries. Wait a moment.' }, 429);
      }),
    );
    window.history.replaceState(null, '', '#/query/mail');
    render(<App />);
    await waitFor(
      () => expect(document.getElementById('status')!.textContent).not.toMatch(/…$/),
      WHOLE_PAGE,
    );

    // The Ask panel is drawn once its code has loaded, after the view (#44).
    const question = await waitFor(() => {
      const found = document.getElementById('question');
      expect(found).not.toBeNull();
      return found!;
    }, WHOLE_PAGE);
    fireEvent.change(question, { target: { value: '谁主动声明了 mail？' } });
    fireEvent.submit(question.closest('form')!);
    await waitFor(
      () => expect(document.querySelector('.answer.error')).not.toBeNull(),
      WHOLE_PAGE,
    );

    expect(unique(english(FULL))).toEqual([]);
  }, WHOLE_PAGE.timeout * 3);
});
