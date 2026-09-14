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
import type { DatasetClient } from '../src/d1/client';

/**
 * A stand-in for the query client.
 *
 * The component's boundary is the client, not SQL — it never sees a
 * statement — so that is what a test should replace. Unlisted methods
 * throw rather than return empty: a view quietly rendering nothing
 * because a method was missing is the failure this catches.
 */
function fakeClient(
  answers: Partial<Record<string, unknown>> = {},
): DatasetClient {
  const handler: ProxyHandler<object> = {
    get(_target, key: string) {
      return (...args: unknown[]) => {
        if (key in answers) {
          const value = answers[key];
          return Promise.resolve(
            typeof value === 'function' ? value(...args) : value,
          );
        }
        return Promise.reject(new Error(`fakeClient: no answer for ${key}`));
      };
    },
  };
  return new Proxy({}, handler) as DatasetClient;
}

const ROW = {
  owner: 'rails',
  repo: 'rails',
  stars: 58182,
  version: '2.8.1',
  url: 'https://github.com/rails/rails',
  relationship: 'transitive' as const,
  observedAt: '2026-09-13',
};

beforeEach(() => {
  cleanup();
  vi.useRealTimers();
});

function mount(
  answers: Partial<Record<string, unknown>>,
  route: { view: 'overview' | 'query'; package?: string },
  go = vi.fn(),
) {
  render(
    <QueryView
      dataset={fakeClient(answers)}
      languages={['ruby']}
      route={route}
      go={go}
    />,
  );
  return go;
}

describe('QueryView status line', () => {
  it('invites a search when no package is named', () => {
    mount({}, { view: 'query' });
    expect(screen.getByText(/Type a package name/)).toBeTruthy();
  });

  // The original bug: the markup shipped "Loading dataset…" and nothing
  // ever replaced it, so the query view's only status was permanently a
  // lie. Here the text is a function of state, so there is no string
  // that can outlive the condition it described.
  it('never shows a boot message once it is rendering a query', async () => {
    mount({ dependentsOf: [ROW], countDependents: 1, ecosystemsFor: [] }, {
      view: 'query',
      package: 'mail',
    });
    await waitFor(() =>
      expect(screen.getByText(/1 dependants on mail/)).toBeTruthy(),
    );
    expect(screen.queryByText(/Loading dataset/)).toBeNull();
  });

  it('reports the real total and scopes the split to the rows shown', async () => {
    mount({ dependentsOf: [ROW], countDependents: 124, ecosystemsFor: [] }, {
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
    mount({ dependentsOf: [], countDependents: 0, ecosystemsFor: [] }, {
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
    // The message is written by the Worker now, for a reader — D1's own
    // error text carries table and column names and is never returned.
    render(
      <QueryView
        dataset={fakeClient({
          ecosystemsFor: [],
          dependentsOf: () => Promise.reject(new Error('Too many questions.')),
          countDependents: () => Promise.reject(new Error('Too many questions.')),
        })}
        languages={[]}
        route={{ view: 'query', package: 'mail' }}
        go={vi.fn()}
      />,
    );
    await waitFor(() =>
      expect(screen.getByText(/Too many questions\./)).toBeTruthy(),
    );
  });
});

describe('QueryView route coupling', () => {
  // The original bug: onRoute ran the query only when the route differed
  // from the input, but typing set the input first, so the two always
  // matched by the time the route changed and nothing was ever queried.
  it('queries for the package named by the route', async () => {
    const asked: string[] = [];
    render(
      <QueryView
        dataset={fakeClient({
          ecosystemsFor: [],
          countDependents: 1,
          dependentsOf: (query: { name: string }) => {
            asked.push(query.name);
            return [ROW];
          },
        })}
        languages={[]}
        route={{ view: 'query', package: 'mail' }}
        go={vi.fn()}
      />,
    );
    await waitFor(() => expect(asked).toContain('mail'));
  });

  it('fills the search field from the route, so a link is shareable', () => {
    mount({ dependentsOf: [ROW], countDependents: 1, ecosystemsFor: [] }, {
      view: 'query',
      package: 'mail',
    });
    const input = screen.getByLabelText('Package name') as HTMLInputElement;
    expect(input.value).toBe('mail');
  });

  it('offers no ecosystem filter for an unambiguous name', async () => {
    // One ecosystem is not ambiguous, so no control appears.
    mount(
      {
        dependentsOf: [ROW],
        countDependents: 1,
        ecosystemsFor: [{ type: 'gem', repositoryCount: 118, directCount: 17 }],
      },
      { view: 'query', package: 'mail' },
    );
    await waitFor(() =>
      expect(screen.getByText(/1 dependants on mail/)).toBeTruthy(),
    );
    expect(screen.queryByText(/ecosystems/)).toBeNull();
  });

  it('offers the filter once a name spans ecosystems', async () => {
    // `mail` is the case this exists for: a Ruby gem with 118
    // dependants and a Maven artifactId with 6. One count of 124 would
    // describe something that does not exist.
    mount(
      {
        dependentsOf: [ROW],
        countDependents: 124,
        ecosystemsFor: [
          { type: 'gem', repositoryCount: 118, directCount: 17 },
          { type: 'java-archive', repositoryCount: 6, directCount: 6 },
        ],
      },
      { view: 'query', package: 'mail' },
    );
    await waitFor(() =>
      expect(screen.getByText(/all 2 ecosystems/)).toBeTruthy(),
    );
  });
});

describe('QueryView row freshness', () => {
  // Debugging a suspicious row starts with "when did we last look at
  // this?". `pushed_at` answers a different question — upstream's last
  // push as of whenever the metadata was collected — and conflating the
  // two is what makes a stale row look like an inactive project.
  it('shows when each repository was last scanned', async () => {
    const db = ({
      dependentsOf: [{ ...ROW, observedAt: '2026-09-13' }],
      countDependents: 1,
      ecosystemsFor: [],
    });
    mount(db, { view: 'query', package: 'mail' });
    await waitFor(() =>
      expect(screen.getByText('2026-09-13')).toBeTruthy(),
    );
  });

  it('labels the column as an observation, not an update', async () => {
    const db = ({
      dependentsOf: [{ ...ROW, observedAt: '2026-09-13' }],
      countDependents: 1,
      ecosystemsFor: [],
    });
    mount(db, { view: 'query', package: 'mail' });
    await waitFor(() => expect(screen.getByText(/Scanned/i)).toBeTruthy());
  });

  it('shows a dash rather than a fabricated date when unknown', async () => {
    const { container } = render(
      <QueryView
        dataset={fakeClient({
          dependentsOf: [{ ...ROW, observedAt: '' }],
          countDependents: 1,
          ecosystemsFor: [],
        })}
        languages={[]}
        route={{ view: 'query', package: 'mail' }}
        go={vi.fn()}
      />,
    );
    await waitFor(() =>
      expect(container.querySelector('tbody tr')).not.toBeNull(),
    );
    const cells = [...container.querySelectorAll('tbody td')].map(
      (c) => c.textContent,
    );
    expect(cells).toContain('\u2014');
    expect(cells.join()).not.toContain('1970');
  });
});
