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
import {
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';
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
    // The control, not the word: a chart caveat elsewhere on the page
    // legitimately mentions ecosystems, and matching loose text made
    // this assertion depend on wording it does not care about.
    expect(screen.queryByLabelText(/Ecosystem/)).toBeNull();
    expect(
      screen.queryByRole('option', { name: /all \d+ ecosystems/ }),
    ).toBeNull();
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

/**
 * The edge panels.
 *
 * The one that matters is the last: these two panels read `agg_edges`,
 * which is aggregated by package name across the whole dataset and has
 * no language, ecosystem or relationship column. So the controls above
 * them cannot apply — and a panel that silently ignores a filter the
 * reader set is worse than one that refuses it, because the numbers
 * look filtered. Hence both the independence and the printed caveat.
 */
describe('QueryView edge panels', () => {
  const EDGES = {
    pulledInBy: [
      { name: 'debug', repositories: 7999 },
      { name: 'send', repositories: 3853 },
    ],
    dependencyTree: {
      root: 'ms',
      children: [{ name: 'nothing', repositories: 1 }],
      grandchildren: [],
    },
  };

  it('asks both directions for the named package', async () => {
    const asked: { method: string; name: string }[] = [];
    render(
      <QueryView
        dataset={fakeClient({
          ecosystemsFor: [],
          dependentsOf: [ROW],
          countDependents: 1,
          versionSpread: [],
          adoptionOverTime: [],
          pulledInBy: (name: string) => {
            asked.push({ method: 'pulledInBy', name });
            return EDGES.pulledInBy;
          },
          dependencyTree: (name: string) => {
            asked.push({ method: 'dependencyTree', name });
            return EDGES.dependencyTree;
          },
        })}
        languages={[]}
        route={{ view: 'query', package: 'ms' }}
        go={vi.fn()}
      />,
    );
    await waitFor(() => expect(asked).toHaveLength(2));
    expect(asked.map((call) => call.method).sort()).toEqual([
      'dependencyTree',
      'pulledInBy',
    ]);
    expect(new Set(asked.map((call) => call.name))).toEqual(new Set(['ms']));
  });

  it('shows what pulls the package in, ranked', async () => {
    mount(
      {
        ecosystemsFor: [],
        dependentsOf: [ROW],
        countDependents: 1,
        versionSpread: [],
        adoptionOverTime: [],
        ...EDGES,
      },
      { view: 'query', package: 'ms' },
    );
    await waitFor(() => expect(screen.getByText('debug')).toBeTruthy());
    expect(screen.getByText('7,999')).toBeTruthy();
    expect(screen.getByText(/What pulls it in/)).toBeTruthy();
  });

  it('opens a package named in the ranking', async () => {
    const go = mount(
      {
        ecosystemsFor: [],
        dependentsOf: [ROW],
        countDependents: 1,
        versionSpread: [],
        adoptionOverTime: [],
        ...EDGES,
      },
      { view: 'query', package: 'ms' },
    );
    await waitFor(() => expect(screen.getByText('debug')).toBeTruthy());
    fireEvent.click(
      document.querySelector('g[data-row="debug"] path')!,
    );
    expect(go).toHaveBeenCalledWith({ view: 'query', package: 'debug' });
  });

  it('draws the edge panels even when the filters empty the table', async () => {
    /**
     * The independence that matters. A language filter with no hits
     * leaves `dependentsOf` empty, which hides the table and the
     * version panels — but `agg_edges` is not filtered by language, so
     * "why is this package in my lockfile" is still answerable and must
     * still be answered.
     */
    mount(
      {
        ecosystemsFor: [],
        dependentsOf: [],
        countDependents: 0,
        ...EDGES,
      },
      { view: 'query', package: 'ms' },
    );
    await waitFor(() => expect(screen.getByText('debug')).toBeTruthy());
    expect(screen.getByText(/No repository in the dataset depends on ms\./))
      .toBeTruthy();
  });

  it('says that a package name is not unique across ecosystems', async () => {
    /**
     * The limitation is in the table, not the panel: `agg_edges` is
     * keyed on name and has no ecosystem column, so `bytes` is the npm
     * package and the Rust crate merged — which is why `serde` appears
     * under it in a drawn tree. 2,508 of 141,938 names are ambiguous
     * and they carry 107,974 of 455,281 edges, so this is not a corner
     * case to leave unsaid.
     */
    mount(
      {
        ecosystemsFor: [],
        dependentsOf: [ROW],
        countDependents: 1,
        versionSpread: [],
        adoptionOverTime: [],
        ...EDGES,
      },
      { view: 'query', package: 'ms' },
    );
    await waitFor(() =>
      expect(
        screen.getAllByText(/not unique across ecosystems/).length,
      ).toBeGreaterThanOrEqual(2),
    );
  });

  it('says that the filters above do not reach these panels', async () => {
    mount(
      {
        ecosystemsFor: [],
        dependentsOf: [ROW],
        countDependents: 1,
        versionSpread: [],
        adoptionOverTime: [],
        ...EDGES,
      },
      { view: 'query', package: 'ms' },
    );
    await waitFor(() =>
      expect(
        screen.getAllByText(/filters above do not reach this panel/)[0],
      ).toBeTruthy(),
    );
  });

  it('states that the drawn tree is bounded', async () => {
    // Two hops of a dozen packages is a diagram; the unbounded graph is
    // not a larger version of it. Saying so is part of the chart.
    mount(
      {
        ecosystemsFor: [],
        dependentsOf: [ROW],
        countDependents: 1,
        versionSpread: [],
        adoptionOverTime: [],
        ...EDGES,
      },
      { view: 'query', package: 'ms' },
    );
    await waitFor(() =>
      expect(screen.getByText(/Bounded to 12 packages and 3 per package/))
        .toBeTruthy(),
    );
  });

  it('asks nothing about edges until a package is named', () => {
    // `fakeClient` rejects any method it has no answer for, so an
    // ungated query would surface as a failure rather than as silence.
    mount({}, { view: 'query' });
    expect(screen.getByText(/Type a package name/)).toBeTruthy();
    expect(screen.queryByText(/What pulls it in/)).toBeNull();
  });
});
