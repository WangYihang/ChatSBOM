/**
 * The page as `main.tsx` mounts it, against a stand-in for the service.
 *
 * Each of these was found by using the page rather than by reading it
 * (#42): a theme toggle the charts followed one click late, a link that
 * blanked the page for good, a search box that wrote a history entry
 * per word, and an overview that asked nothing until its provenance had
 * answered — and asked two of its questions twice.
 */
// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { App } from '../src/app';
import { DatasetClient } from '../src/d1/client';
import { SEQUENTIAL_DARK, SEQUENTIAL_LIGHT } from '../src/palette';
import { answering, asked as question } from './answers';

/** What each method answers, in the shape it really answers in. */
const ANSWERS: Record<string, unknown> = {
  meta: {
    generator: 'chatsbom/test',
    schemaVersion: 'd1 v5',
    observedFrom: '2026-02-11',
    observedTo: '2026-09-13',
  },
  totals: {
    repositories: 3, dependencies: 10, packages: 4, classified: 9, tracked: 5,
  },
  relationshipSplit: { direct: 1, transitive: 4, unknown: 0 },
  topPackages: [
    { name: 'typescript', repositoryCount: 6863, directCount: 6820 },
    { name: 'eslint', repositoryCount: 6440, directCount: 6401 },
  ],
  edgeAmbiguity: null,
  countDependents: 0,
  countDependentRows: 0,
  versionSpread: { versions: [], constrained: 0, unversioned: 0 },
  dependencyTree: { root: 'x', children: [], grandchildren: [] },
};

/**
 * The service, answering from `ANSWERS` and recording which method each
 * request named. `meta` answers when `release` is called, or at once.
 */
function stubQueries({ holdMeta = false } = {}) {
  const asked: string[] = [];
  let release = () => {};
  const held = new Promise<void>((resolve) => (release = resolve));
  vi.stubGlobal(
    'fetch',
    vi.fn(async (url: string) => {
      const { method } = question(url);
      asked.push(method);
      if (method === 'meta' && holdMeta) await held;
      return answering(url, method in ANSWERS ? ANSWERS[method] : []);
    }),
  );
  return { asked, release };
}

beforeEach(() => {
  cleanup();
  localStorage.clear();
  document.documentElement.removeAttribute('data-theme');
  window.history.replaceState(null, '', '#/overview');
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
  vi.restoreAllMocks();
});

/** The fill of the ranking's bar for `name`. */
const bar = (name: string) =>
  document.querySelector(`g[data-row="${name}"] path`)?.getAttribute('fill');

describe('the theme', () => {
  it('reaches the charts on the first toggle (#42)', async () => {
    stubQueries();
    render(<App />);
    await waitFor(() => expect(bar('typescript')).toBeTruthy());
    expect(SEQUENTIAL_LIGHT).toContain(bar('typescript'));

    fireEvent.click(screen.getByRole('button', { name: 'Dark' }));
    expect(SEQUENTIAL_DARK).toContain(bar('typescript'));

    fireEvent.click(screen.getByRole('button', { name: 'Light' }));
    expect(SEQUENTIAL_LIGHT).toContain(bar('typescript'));
  });
});

describe('the address', () => {
  it('draws the overview for a link it cannot read, not a blank page (#42)', async () => {
    stubQueries();
    window.history.replaceState(null, '', '#/query/%');
    render(<App />);
    expect(
      screen.getByRole('button', { name: 'Overview' }).getAttribute('aria-pressed'),
    ).toBe('true');
    await waitFor(() => expect(screen.getByText('typescript')).toBeTruthy());
  });

  it('keeps one history entry for a name being typed, not one per pause (#42)', async () => {
    /**
     * Every settled term was pushed, so a reader who typed a name in
     * three bursts needed three Backs to leave the view — and each
     * stopped on a half-typed name.
     */
    stubQueries();
    window.history.replaceState(null, '', '#/query');
    render(<App />);
    const input = await screen.findByLabelText('Package name');
    const entries = window.history.length;

    for (const typed of ['ma', 'mai', 'mail']) {
      fireEvent.change(input, { target: { value: typed } });
      await waitFor(() => expect(window.location.hash).toBe(`#/query/${typed}`));
    }
    expect(window.history.length).toBe(entries);
  });
});

describe('the overview', () => {
  it('asks its questions while the provenance is still on its way (#42)', async () => {
    // `meta` is one round trip, and every panel used to wait for it
    // before asking anything of its own. Each question is asked under
    // the snapshot `/api/meta` names (#144), so none can leave before
    // it answers: the panels have asked, and their questions go the
    // moment it does, with no round trip of their own before them.
    const splits = vi.spyOn(DatasetClient.prototype, 'relationshipSplit');
    const rankings = vi.spyOn(DatasetClient.prototype, 'topPackages');
    const { asked, release } = stubQueries({ holdMeta: true });
    render(<App />);
    try {
      await waitFor(() => {
        expect(splits).toHaveBeenCalled();
        expect(rankings).toHaveBeenCalled();
      });
      expect(asked).toEqual(['meta']);
    } finally {
      release();
    }
    await waitFor(() =>
      expect(asked).toEqual(expect.arrayContaining(['relationshipSplit', 'topPackages'])),
    );
  });

  it('asks each of its questions once (#42)', async () => {
    // The ecosystem and language lists were asked by the root and by
    // the overview, and the totals by the header and by the metadata
    // panel: the same answer, fetched twice, every visit.
    const { asked } = stubQueries();
    render(<App />);
    await waitFor(() => expect(screen.getByText('typescript')).toBeTruthy());
    await waitFor(() => expect(asked).toContain('totals'));
    const counted = (method: string) => asked.filter((m) => m === method).length;
    for (const method of ['languageCoverage', 'ecosystemCoverage', 'totals', 'meta']) {
      expect([method, counted(method)]).toEqual([method, 1]);
    }
  });

  it('says it is loading the dataset, not an engine (#42)', async () => {
    // The page once fetched a query engine of its own, about 33 MB, and
    // warned a first visit about it while it did. It fetches none now:
    // what it waits for is the dataset's provenance, and the note said
    // otherwise to every visitor.
    const { release } = stubQueries({ holdMeta: true });
    render(<App />);
    try {
      const note = await screen.findByText(/Loading the dataset/);
      expect(note.textContent).not.toMatch(/engine|MB/);
    } finally {
      release();
    }
  });
});
