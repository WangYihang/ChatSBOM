/**
 * Regression tests for the two defects the React migration exists to
 * remove. Both were found by rendering the page, not by reading it, and
 * both are classes rather than slips:
 *
 *   1. a status string written imperatively at boot and never revisited;
 *   2. the route and the search field kept as two sources of truth,
 *      synced by hand under a condition that could not hold.
 *
 * The assertions here are about derivation, so they fail if anyone
 * reintroduces an imperative write.
 */
// @vitest-environment jsdom
import { cleanup, render, screen, waitFor } from '@testing-library/react';
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { QueryView } from '../src/components/QueryView';
import { Dataset, type Queryable } from '../src/queries';

/** A Queryable that answers from canned rows, keyed by SQL shape. */
class FakeDb implements Queryable {
  calls: string[] = [];
  constructor(
    private readonly answers: {
      rows?: unknown[];
      total?: number;
      ecosystems?: unknown[];
    } = {},
  ) {}

  async query<T>(sql: string): Promise<T[]> {
    this.calls.push(sql);
    if (sql.includes('count(DISTINCT a.repository_id) AS total')) {
      return [{ total: this.answers.total ?? 0 }] as T[];
    }
    if (sql.includes('GROUP BY type')) {
      return (this.answers.ecosystems ?? []) as T[];
    }
    if (sql.includes('r.owner, r.repo')) {
      return (this.answers.rows ?? []) as T[];
    }
    return [] as T[];
  }
}

const ROW = {
  owner: 'rails',
  repo: 'rails',
  stars: 58182,
  version: '2.8.1',
  url: 'https://github.com/rails/rails',
  relationship: 'transitive',
};

beforeEach(() => {
  // Testing Library's auto-cleanup only runs with vitest globals, which
  // this project does not enable; without it every render accumulates
  // and queries find several matches.
  cleanup();
  vi.useRealTimers();
});

function mount(
  db: Queryable,
  route: { view: 'overview' | 'query'; package?: string },
  go = vi.fn(),
) {
  render(
    <QueryView
      dataset={new Dataset(db, 'https://x.example/data')}
      languages={['ruby']}
      route={route}
      go={go}
    />,
  );
  return go;
}

describe('QueryView status line', () => {
  it('invites a search when no package is named', () => {
    mount(new FakeDb(), { view: 'query' });
    expect(screen.getByText(/Type a package name/)).toBeTruthy();
  });

  // The original bug: the markup shipped "Loading dataset…" and nothing
  // ever replaced it, so the query view's only status was permanently a
  // lie. Here the text is a function of state, so there is no string
  // that can outlive the condition it described.
  it('never shows a boot message once it is rendering a query', async () => {
    mount(new FakeDb({ rows: [ROW], total: 1 }), {
      view: 'query',
      package: 'mail',
    });
    await waitFor(() =>
      expect(screen.getByText(/1 dependants on mail/)).toBeTruthy(),
    );
    expect(screen.queryByText(/Loading dataset/)).toBeNull();
  });

  it('reports the real total and scopes the split to the rows shown', async () => {
    mount(new FakeDb({ rows: [ROW], total: 124 }), {
      view: 'query',
      package: 'mail',
    });
    await waitFor(() =>
      expect(
        screen.getByText(
          /124 dependants on mail — 0 declare it, 1 inherit it among the 1 shown\./,
        ),
      ).toBeTruthy(),
    );
  });

  it('says so when nothing depends on the package', async () => {
    mount(new FakeDb({ rows: [], total: 0 }), {
      view: 'query',
      package: 'nope',
    });
    await waitFor(() =>
      expect(
        screen.getByText(/No repository in the dataset depends on nope\./),
      ).toBeTruthy(),
    );
  });

  it('surfaces a query failure instead of a stale count', async () => {
    const db: Queryable = {
      query: () => Promise.reject(new Error('IO Error: no files found')),
    };
    mount(db, { view: 'query', package: 'mail' });
    await waitFor(() =>
      expect(screen.getByText(/IO Error: no files found/)).toBeTruthy(),
    );
  });
});

describe('QueryView route coupling', () => {
  // The original bug: onRoute ran the query only when the route differed
  // from the input, but typing set the input first, so the two always
  // matched by the time the route changed and nothing was ever queried.
  it('queries for the package named by the route', async () => {
    const db = new FakeDb({ rows: [ROW], total: 1 });
    mount(db, { view: 'query', package: 'mail' });
    await waitFor(() =>
      expect(db.calls.some((sql) => sql.includes('r.owner, r.repo'))).toBe(
        true,
      ),
    );
  });

  it('fills the search field from the route, so a link is shareable', () => {
    mount(new FakeDb({ rows: [ROW], total: 1 }), {
      view: 'query',
      package: 'mail',
    });
    const input = screen.getByLabelText('Package name') as HTMLInputElement;
    expect(input.value).toBe('mail');
  });

  it('offers no ecosystem filter for an unambiguous name', async () => {
    mount(new FakeDb({ rows: [ROW], total: 1, ecosystems: [{ type: 'gem', repository_count: 118, direct_count: 17 }] }), {
      view: 'query',
      package: 'mail',
    });
    await waitFor(() =>
      expect(screen.getByText(/1 dependants on mail/)).toBeTruthy(),
    );
    expect(screen.queryByText(/ecosystems/)).toBeNull();
  });

  it('offers the filter once a name spans ecosystems', async () => {
    mount(
      new FakeDb({
        rows: [ROW],
        total: 124,
        ecosystems: [
          { type: 'gem', repository_count: 118, direct_count: 17 },
          { type: 'java-archive', repository_count: 6, direct_count: 6 },
        ],
      }),
      { view: 'query', package: 'mail' },
    );
    await waitFor(() =>
      expect(screen.getByText(/all 2 ecosystems/)).toBeTruthy(),
    );
  });
});
