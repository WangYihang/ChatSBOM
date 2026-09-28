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
  act,
  cleanup,
  fireEvent,
  render,
  screen,
  waitFor,
} from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { QueryView } from '../src/components/QueryView';
import { DatasetClient } from '../src/d1/client';
import { DICTIONARIES } from '../src/i18n/strings';

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

const EN = DICTIONARIES.en;

beforeEach(() => {
  cleanup();
  vi.useRealTimers();
});

// Unmount after each test as well, not only before the next one. The last
// test's QueryView otherwise stays mounted, and its debounce timer fires
// after jsdom is torn down: every test passes, and vitest still exits 1
// on "window is not defined".
afterEach(() => {
  cleanup();
});

function mount(
  answers: Partial<Record<string, unknown>>,
  route: { view: 'overview' | 'query'; package?: string },
  go = vi.fn(),
) {
  render(
    <QueryView words={EN} locale="en"
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
    mount({ dependentsOf: [ROW], countDependents: 1, countDependentRows: 1, ecosystemsFor: [] }, {
      view: 'query',
      package: 'mail',
    });
    await waitFor(() =>
      expect(screen.getByText(/1 dependant on mail/)).toBeTruthy(),
    );
    expect(screen.queryByText(/Loading dataset/)).toBeNull();
  });

  it('reports the real total and scopes the split to the rows shown', async () => {
    mount({ dependentsOf: [ROW], countDependents: 124, countDependentRows: 124, ecosystemsFor: [] }, {
      view: 'query',
      package: 'mail',
    });
    await waitFor(() =>
      expect(
        screen.getByText(
          /124 dependants on mail — 0 declare it, 1 inherits it among the 1 shown\./,
        ),
      ).toBeTruthy(),
    );
  });

  it('says so when nothing depends on the package', async () => {
    mount({ dependentsOf: [], countDependents: 0, countDependentRows: 0, ecosystemsFor: [] }, {
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
      <QueryView words={EN} locale="en"
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
      <QueryView words={EN} locale="en"
        dataset={fakeClient({
          ecosystemsFor: [],
          countDependents: 1, countDependentRows: 1,
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
    mount({ dependentsOf: [ROW], countDependents: 1, countDependentRows: 1, ecosystemsFor: [] }, {
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
        countDependents: 1, countDependentRows: 1,
        ecosystemsFor: [{ type: 'gem', repositoryCount: 118, directCount: 17 }],
      },
      { view: 'query', package: 'mail' },
    );
    await waitFor(() =>
      expect(screen.getByText(/1 dependant on mail/)).toBeTruthy(),
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
        countDependents: 124, countDependentRows: 124,
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

/**
 * The field, the route and the ecosystem filter are kept in step by
 * effects that each read one value they must not react to: the field
 * while it is typed in, the route while a typed name settles, the
 * filter while a name's ecosystems load. Each of these fails if that
 * value is made a dependency, as the rules of hooks ask of an effect
 * by default (#44).
 */
describe('QueryView keeps the field, the route and the filter in step', () => {
  /** `mail` and `net` are in two ecosystems each; `rails` and `express` in one. */
  const ECOSYSTEMS: Record<string, unknown[]> = {
    mail: [
      { type: 'gem', repositoryCount: 118, directCount: 17 },
      { type: 'java-archive', repositoryCount: 6, directCount: 6 },
    ],
    net: [
      { type: 'gem', repositoryCount: 30, directCount: 3 },
      { type: 'npm', repositoryCount: 9, directCount: 9 },
    ],
    rails: [{ type: 'gem', repositoryCount: 900, directCount: 850 }],
    express: [{ type: 'npm', repositoryCount: 5000, directCount: 4000 }],
  };

  /** The view for a name, over one client, and every dependants query it made. */
  function page() {
    const go = vi.fn();
    const asked: { name: string; type?: string }[] = [];
    const dataset = fakeClient({
      ecosystemsFor: (name: string) => ECOSYSTEMS[name] ?? [],
      dependentsOf: (query: { name: string; type?: string }) => {
        asked.push(query);
        return [ROW];
      },
      countDependents: 1,
      countDependentRows: 1,
      searchPackages: [
        { name: 'mail', ecosystem: 'gem', repositoryCount: 118, nameTotal: 124 },
      ],
    });
    const view = (name: string) => (
      <QueryView words={EN} locale="en"
        dataset={dataset}
        languages={[]}
        route={{ view: 'query', package: name }}
        go={go}
      />
    );
    return { go, asked, view };
  }

  const field = () => screen.getByLabelText(EN.searchAriaLabel) as HTMLInputElement;

  it('takes the name of a route that changes, and leaves one being typed alone', () => {
    const { view } = page();
    const { rerender } = render(view('mail'));
    fireEvent.change(field(), { target: { value: 'mai' } });
    // Drawn again under the same route: the half-typed name stays.
    rerender(view('mail'));
    expect(field().value).toBe('mai');
    // A route that moves on, by Back or a bar elsewhere, brings its name.
    rerender(view('express'));
    expect(field().value).toBe('express');
  });

  it('does not send an arrival from elsewhere back to the name still settling', async () => {
    const { go, asked, view } = page();
    const { rerender } = render(view('mail'));
    rerender(view('express'));
    // Asked for once the field has settled on it, and a turn back to
    // `mail` would have been taken before then.
    await waitFor(() => expect(asked.map((query) => query.name)).toContain('express'));
    expect(go).not.toHaveBeenCalled();
  });

  it("keeps an ecosystem picked in the search box while that name's ecosystems load", async () => {
    const { go, asked, view } = page();
    const { rerender } = render(view('rails'));
    await waitFor(() => expect(asked.map((query) => query.name)).toContain('rails'));
    // `mail · gem` from the list, while `rails`, in one ecosystem, is
    // what the filter was last judged against.
    fireEvent.focus(field());
    fireEvent.mouseDown(await screen.findByRole('option', { name: /mail/ }));
    expect(go).toHaveBeenCalledWith({ view: 'query', package: 'mail' });
    rerender(view('mail'));
    await waitFor(() =>
      expect(asked).toContainEqual(expect.objectContaining({ name: 'mail', type: 'gem' })),
    );
  });

  it('drops a filter the next name is not in, once its ecosystems are in', async () => {
    const { asked, view } = page();
    const { rerender } = render(view('mail'));
    fireEvent.change(await screen.findByLabelText(EN.ecosystemFilter), {
      target: { value: 'java-archive' },
    });
    await waitFor(() =>
      expect(asked).toContainEqual(
        expect.objectContaining({ name: 'mail', type: 'java-archive' }),
      ),
    );
    rerender(view('net'));
    await waitFor(() =>
      expect(asked.some((query) => query.name === 'net' && query.type === undefined)).toBe(
        true,
      ),
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
      countDependents: 1, countDependentRows: 1,
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
      countDependents: 1, countDependentRows: 1,
      ecosystemsFor: [],
    });
    mount(db, { view: 'query', package: 'mail' });
    await waitFor(() => expect(screen.getByText(/Scanned/i)).toBeTruthy());
  });

  it('shows a dash rather than a fabricated date when unknown', async () => {
    const { container } = render(
      <QueryView words={EN} locale="en"
        dataset={fakeClient({
          dependentsOf: [{ ...ROW, observedAt: '' }],
          countDependents: 1, countDependentRows: 1,
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
      <QueryView words={EN} locale="en"
        dataset={fakeClient({
          ecosystemsFor: [],
          dependentsOf: [ROW],
          countDependents: 1, countDependentRows: 1,
          versionSpread: { versions: [], constrained: 0, unversioned: 0 },
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
        countDependents: 1, countDependentRows: 1,
        versionSpread: { versions: [], constrained: 0, unversioned: 0 },
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
        countDependents: 1, countDependentRows: 1,
        versionSpread: { versions: [], constrained: 0, unversioned: 0 },
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
        countDependents: 0, countDependentRows: 0,
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
        countDependents: 1, countDependentRows: 1,
        versionSpread: { versions: [], constrained: 0, unversioned: 0 },
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
        countDependents: 1, countDependentRows: 1,
        versionSpread: { versions: [], constrained: 0, unversioned: 0 },
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
        countDependents: 1, countDependentRows: 1,
        versionSpread: { versions: [], constrained: 0, unversioned: 0 },
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

/**
 * The dead end, end to end.
 *
 * `laravel` is the case this was found on: 98 repositories depend on
 * `laravel/framework` and the page said nothing depends on `laravel`.
 */
describe('QueryView package search', () => {
  const LARAVEL = [
    { name: 'laravel/framework', repositoryCount: 98 },
    { name: 'laravel/tinker', repositoryCount: 70 },
  ];

  it('offers the real package when the typed name matches nothing', async () => {
    const go = mount(
      {
        ecosystemsFor: [],
        dependentsOf: [],
        countDependents: 0, countDependentRows: 0,
        searchPackages: LARAVEL,
        pulledInBy: [],
        dependencyTree: { root: 'laravel', children: [], grandchildren: [] },
      },
      { view: 'query', package: 'laravel' },
    );
    await waitFor(() =>
      expect(
        screen.getByText(/No repository in the dataset depends on laravel\./),
      ).toBeTruthy(),
    );
    // The sentence is still true, and no longer the end of the road.
    const option = await waitFor(() =>
      screen.getByRole('option', { name: /laravel\/framework/ }),
    );
    fireEvent.mouseDown(option);
    expect(go).toHaveBeenCalledWith({
      view: 'query',
      package: 'laravel/framework',
    });
  });

  it('asks for candidates with the term, not the whole alphabet', async () => {
    const asked: [string, number | undefined][] = [];
    render(
      <QueryView words={EN} locale="en"
        dataset={fakeClient({
          ecosystemsFor: [],
          dependentsOf: [],
          countDependents: 0, countDependentRows: 0,
          pulledInBy: [],
          dependencyTree: { root: 'la', children: [], grandchildren: [] },
          searchPackages: (term: string, limit?: number) => {
            asked.push([term, limit]);
            return LARAVEL;
          },
        })}
        languages={[]}
        route={{ view: 'query', package: 'laravel' }}
        go={vi.fn()}
      />,
    );
    await waitFor(() => expect(asked.length).toBeGreaterThan(0));
    expect(asked[0]![0]).toBe('laravel');
    // Bounded: the list is eight rows, so the query must not fetch the
    // default fifty and throw most away.
    expect(asked[0]![1]).toBe(8);
  });

  it('does not search on a single character', async () => {
    /**
     * `a` matches roughly ten thousand of 141,938 names, and ranking by
     * popularity reads every match before the limit applies. On D1 that
     * is billed rows for a keystroke.
     */
    const asked: string[] = [];
    render(
      <QueryView words={EN} locale="en"
        dataset={fakeClient({
          ecosystemsFor: [],
          dependentsOf: [],
          countDependents: 0, countDependentRows: 0,
          pulledInBy: [],
          dependencyTree: { root: 'a', children: [], grandchildren: [] },
          searchPackages: (term: string) => {
            asked.push(term);
            return [];
          },
        })}
        languages={[]}
        route={{ view: 'query', package: 'a' }}
        go={vi.fn()}
      />,
    );
    await waitFor(() =>
      expect(
        screen.getByText(/No repository in the dataset depends on a\./),
      ).toBeTruthy(),
    );
    expect(asked).toEqual([]);
  });

  it('offers no list when the exact name did find something', async () => {
    // A correct query must not sprout a dropdown over its own results.
    mount(
      {
        ecosystemsFor: [],
        dependentsOf: [ROW],
        countDependents: 98, countDependentRows: 98,
        versionSpread: { versions: [], constrained: 0, unversioned: 0 },
        adoptionOverTime: [],
        searchPackages: LARAVEL,
        pulledInBy: [],
        dependencyTree: {
          root: 'laravel/framework',
          children: [],
          grandchildren: [],
        },
      },
      { view: 'query', package: 'laravel/framework' },
    );
    await waitFor(() =>
      expect(screen.getByText(/98 dependants/)).toBeTruthy(),
    );
    // Scoped to the candidate list: a native <select> option carries
    // role="option" too, so an unscoped query counts the language
    // filter's own entries and can never be zero.
    expect(document.querySelectorAll('.suggestions [role="option"]'))
      .toHaveLength(0);
  });
});

/**
 * What the versions panel leaves out.
 *
 * It is headed "repositories on each resolved version" and was counting
 * manifest constraints alongside resolutions: for `laravel/framework`
 * the constraint `>= 13.0,< 14.0` was the top row with 11 repositories,
 * above the real leading version `v12.49.0` with 7.
 */
describe('QueryView versions panel', () => {
  const BASE = {
    ecosystemsFor: [],
    dependentsOf: [ROW],
    countDependents: 1, countDependentRows: 1,
    adoptionOverTime: [],
    pulledInBy: [],
    dependencyTree: { root: 'x', children: [], grandchildren: [] },
  };

  it('lists resolved versions only', async () => {
    mount(
      {
        ...BASE,
        versionSpread: {
          versions: [
            { kind: 'resolved', version: 'v12.49.0', repositoryCount: 7 },
          ],
          constrained: 11,
          unversioned: 3,
        },
      },
      { view: 'query', package: 'laravel/framework' },
    );
    await waitFor(() => expect(screen.getByText('v12.49.0')).toBeTruthy());
    expect(screen.queryByText(/13\.0,< 14\.0/)).toBeNull();
  });

  it('says how much it did not count', async () => {
    // Excluding them silently would trade one wrong answer for an
    // unexplained one.
    mount(
      {
        ...BASE,
        versionSpread: {
          versions: [
            { kind: 'resolved', version: 'v12.49.0', repositoryCount: 7 },
          ],
          constrained: 11,
          unversioned: 3,
        },
      },
      { view: 'query', package: 'laravel/framework' },
    );
    await waitFor(() =>
      expect(screen.getByText(/11 rows give a range/)).toBeTruthy(),
    );
    expect(screen.getByText(/3 give none at all/)).toBeTruthy();
  });

  it('adds no caveat when every row resolved', async () => {
    mount(
      {
        ...BASE,
        versionSpread: {
          versions: [{ kind: 'resolved', version: '2.9.0', repositoryCount: 4 }],
          constrained: 0,
          unversioned: 0,
        },
      },
      { view: 'query', package: 'mail' },
    );
    await waitFor(() => expect(screen.getByText('2.9.0')).toBeTruthy());
    expect(screen.queryByText(/Not counted above/)).toBeNull();
  });

});

describe('paging the dependants table', () => {
  const page = (n: number) =>
    Array.from({ length: n }, (_, i) => ({ ...ROW, repo: `r${i}` }));

  it('pages on the row count, not the dependant count', async () => {
    /**
     * `countDependents` counts repositories and the table shows one
     * row per distinct (repository, version, relationship, ecosystem,
     * date): 326 against 492 for `laravel/framework`, 5,095 against
     * 11,436 for `react`. Paging on the dependant count would run off
     * the end of one package and stop halfway through another.
     */
    mount(
      {
        dependentsOf: page(100),
        countDependents: 326,
        countDependentRows: 492,
        ecosystemsFor: [],
      },
      { view: 'query', package: 'laravel/framework' },
    );
    await waitFor(() =>
      expect(document.querySelector('.pager')).toBeTruthy());
    const pager = document.querySelector('.pager')!;
    expect(pager.textContent).toContain('492');
    expect(pager.textContent).not.toContain('326');
  });

  it('offers no pager when one page holds everything', async () => {
    mount(
      {
        dependentsOf: page(12),
        countDependents: 12,
        countDependentRows: 12,
        ecosystemsFor: [],
      },
      { view: 'query', package: 'mail' },
    );
    // The cell renders `owner/repo` as one link, not the repo alone.
    await waitFor(() =>
      expect(screen.getByText('rails/r0')).toBeTruthy());
    expect(document.querySelector('.pager')).toBeNull();
  });

  it('disables rather than hides the control at each end', async () => {
    // A control that vanishes makes the row jump, and its absence
    // reads as a bug rather than as "there is no previous page".
    mount(
      {
        dependentsOf: page(100),
        countDependents: 326,
        countDependentRows: 492,
        ecosystemsFor: [],
      },
      { view: 'query', package: 'laravel/framework' },
    );
    await waitFor(() =>
      expect(document.querySelector('.pager')).toBeTruthy());
    const buttons = [...document.querySelectorAll('.pager button')];
    expect(buttons).toHaveLength(2);
    expect((buttons[0] as HTMLButtonElement).disabled).toBe(true);
    expect((buttons[1] as HTMLButtonElement).disabled).toBe(false);
  });

  it('asks for the next page by offset', async () => {
    const seen: unknown[] = [];
    mount(
      {
        dependentsOf: (query: unknown) => {
          seen.push(query);
          return page(100);
        },
        countDependents: 326,
        countDependentRows: 492,
        ecosystemsFor: [],
      },
      { view: 'query', package: 'laravel/framework' },
    );
    await waitFor(() =>
      expect(document.querySelector('.pager')).toBeTruthy());
    fireEvent.click(document.querySelectorAll('.pager button')[1]!);
    await waitFor(() => expect(seen.length).toBeGreaterThan(1));
    expect((seen.at(-1) as { offset?: number }).offset).toBe(100);
  });
});

/**
 * What one reader's action costs in requests (#42).
 *
 * Counted at `fetch`, through the real client, because that is what the
 * Worker and the store see. A page flip used to send five — the page,
 * both counts, and the version and adoption panels, which were keyed on
 * whether the table had rows and so reloaded every time it emptied
 * while the next page loaded — and a filter change on page two sent
 * eight, the old page's offset asked once more before the reset.
 */
describe('what the table asks for', () => {
  interface Asked {
    method: string;
    params: Record<string, unknown>;
    signal: AbortSignal | undefined;
  }

  /** A page of dependants at `offset`, named so each page is told apart. */
  const rowsAt = (offset: number, n = 100) =>
    Array.from({ length: n }, (_, i) => ({ ...ROW, repo: `p${offset}-r${i}` }));

  /**
   * `/api/q`, answering each method from `answers` and recording what
   * each request asked. `hold` keeps a request unanswered until the
   * promise it returns settles.
   */
  function stubApi(
    answers: Record<string, (params: Record<string, unknown>) => unknown>,
    hold?: (asked: Asked) => Promise<void> | undefined,
  ) {
    const asked: Asked[] = [];
    vi.stubGlobal(
      'fetch',
      vi.fn(async (_url: string, init?: RequestInit) => {
        const { method, params = {} } = JSON.parse(String(init?.body)) as {
          method: string;
          params?: Record<string, unknown>;
        };
        const request = { method, params, signal: init?.signal ?? undefined };
        asked.push(request);
        await hold?.(request);
        const answer = answers[method];
        return new Response(JSON.stringify(answer ? answer(params) : []), {
          headers: { 'content-type': 'application/json' },
        });
      }),
    );
    return asked;
  }

  const LARAVEL: Record<string, (params: Record<string, unknown>) => unknown> = {
    dependentsOf: (params) => rowsAt(Number(params['offset'] ?? 0)),
    countDependents: () => 326,
    countDependentRows: () => 492,
    ecosystemsFor: () => [],
    versionSpread: () => ({
      versions: [{ kind: 'resolved', version: 'v12.49.0', repositoryCount: 7 }],
      constrained: 0,
      unversioned: 0,
    }),
    adoptionOverTime: () => [],
    pulledInBy: () => [],
    dependencyTree: () => ({ root: 'laravel/framework', children: [], grandchildren: [] }),
    searchPackages: () => [],
    edgeAmbiguity: () => null,
  };

  const show = (route: { view: 'query'; package: string }) =>
    render(
      <QueryView words={EN} locale="en"
        dataset={new DatasetClient()}
        languages={['ruby']}
        route={route}
        go={vi.fn()}
      />,
    );

  /** Let whatever the last answer set off be asked, before counting. */
  const settle = () => act(() => new Promise((resolve) => setTimeout(resolve, 50)));

  afterEach(() => vi.unstubAllGlobals());

  it('asks for one page when the page is turned, and nothing else', async () => {
    const asked = stubApi(LARAVEL);
    show({ view: 'query', package: 'laravel/framework' });
    await waitFor(() => expect(screen.getByText('rails/p0-r0')).toBeTruthy());
    await waitFor(() => expect(screen.getByText('v12.49.0')).toBeTruthy());
    await settle();
    const before = asked.length;

    fireEvent.click(screen.getByRole('button', { name: EN.pageNext }));
    await waitFor(() => expect(screen.getByText('rails/p100-r0')).toBeTruthy());
    await settle();

    expect(asked.slice(before).map(({ method, params }) => [method, params['offset']]))
      .toEqual([['dependentsOf', 100]]);
  });

  it('asks for the first page and its counts once when a filter changes on page two', async () => {
    const asked = stubApi(LARAVEL);
    show({ view: 'query', package: 'laravel/framework' });
    await waitFor(() => expect(screen.getByText('rails/p0-r0')).toBeTruthy());
    fireEvent.click(screen.getByRole('button', { name: EN.pageNext }));
    await waitFor(() => expect(screen.getByText('rails/p100-r0')).toBeTruthy());
    await settle();
    const before = asked.length;

    fireEvent.click(screen.getByLabelText(EN.declaredOnly));
    await waitFor(() =>
      expect(asked.slice(before).map(({ method }) => method)).toContain('dependentsOf'),
    );
    await waitFor(() => expect(screen.getByText('rails/p0-r0')).toBeTruthy());
    await settle();

    const since = asked.slice(before);
    expect(since.map(({ method }) => method).sort()).toEqual([
      'countDependentRows',
      'countDependents',
      'dependentsOf',
    ]);
    expect(since.every(({ params }) => params['directOnly'] === true)).toBe(true);
    expect(since.find(({ method }) => method === 'dependentsOf')!.params['offset']).toBe(0);
  });

  it('keeps the table while the next page loads, marked as busy', async () => {
    // It unmounted: `rows` was empty while loading, so the table and the
    // rails under it vanished and came back, and the page jumped.
    let release = () => {};
    const asked = stubApi(LARAVEL, ({ method, params }) =>
      method === 'dependentsOf' && params['offset'] === 100
        ? new Promise<void>((resolve) => (release = resolve))
        : undefined,
    );
    show({ view: 'query', package: 'laravel/framework' });
    await waitFor(() => expect(screen.getByText('rails/p0-r0')).toBeTruthy());

    fireEvent.click(screen.getByRole('button', { name: EN.pageNext }));
    await waitFor(() =>
      expect(asked.some(({ params }) => params['offset'] === 100)).toBe(true),
    );
    expect(screen.getByText('rails/p0-r0')).toBeTruthy();
    expect(document.querySelector('.tablewrap')!.getAttribute('aria-busy')).toBe('true');

    release();
    await waitFor(() => expect(screen.getByText('rails/p100-r0')).toBeTruthy());
    expect(document.querySelector('.tablewrap')!.getAttribute('aria-busy')).toBe('false');
  });

  it('abandons the requests of a package no longer being looked at', async () => {
    // `useAsync` discarded a stale answer but let its request run on,
    // so each abandoned package still cost the store its queries.
    const asked = stubApi(LARAVEL, ({ params }) =>
      params['name'] === 'mail' ? new Promise<void>(() => {}) : undefined,
    );
    const client = new DatasetClient();
    const view = (name: string) => (
      <QueryView words={EN} locale="en"
        dataset={client}
        languages={[]}
        route={{ view: 'query', package: name }}
        go={vi.fn()}
      />
    );
    const { rerender } = render(view('mail'));
    await waitFor(() =>
      expect(asked.some(({ method, params }) =>
        method === 'dependentsOf' && params['name'] === 'mail')).toBe(true),
    );

    rerender(view('laravel/framework'));
    await waitFor(() => expect(screen.getByText('rails/p0-r0')).toBeTruthy());

    const abandoned = asked.filter(({ params }) => params['name'] === 'mail');
    expect(abandoned.length).toBeGreaterThan(0);
    for (const request of abandoned) {
      expect([request.method, request.signal?.aborted]).toEqual([request.method, true]);
    }
  });
});
