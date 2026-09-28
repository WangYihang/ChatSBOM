/**
 * The charts' way in, from the keyboard (#43).
 *
 * A bar in the ranking and a package in the tree are how a reader gets
 * from the overview to a package, and both were `onClick` on a `<path>`
 * or a `<circle>`: no tab stop, no key, inside an SVG whose `img` role
 * told a screen reader it was a picture — so its children were not
 * there to reach. Their tooltips opened for a mouse only.
 */
// @vitest-environment jsdom
import { cleanup, fireEvent, render, screen, waitFor, within } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';

import { App } from '../src/app';
import { DependencyTree } from '../src/charts/DependencyTree';
import { RankedBars } from '../src/charts/RankedBars';
import { Overview } from '../src/components/Overview';
import { QueryView } from '../src/components/QueryView';
import type { DatasetClient } from '../src/d1/client';
import { DICTIONARIES } from '../src/i18n/strings';
import { focus, tab, tabOrder } from './keyboard';

const EN = DICTIONARIES.en;

beforeEach(() => {
  cleanup();
  localStorage.clear();
  window.history.replaceState(null, '', '#/overview');
});

afterEach(() => {
  cleanup();
  vi.unstubAllGlobals();
});

const tooltip = () => document.querySelector('.chart-tooltip');

/** A ranking whose rows open packages, after a control to start from. */
function ranking(onSelect = vi.fn()) {
  render(
    <>
      <button type="button">before</button>
      <RankedBars
        words={EN}
        locale="en"
        label="repositories"
        bars={[
          {
            label: 'serde',
            value: 6_863,
            href: '#/query/serde',
            onSelect: () => onSelect('serde'),
            detail: { title: 'serde', lines: ['6,863 dependants', '6,820 declared it'] },
          },
          {
            label: 'tokio',
            value: 5_120,
            href: '#/query/tokio',
            onSelect: () => onSelect('tokio'),
          },
        ]}
      />
    </>,
  );
  return onSelect;
}

describe('a ranking, from the keyboard', () => {
  it('reaches each row that opens a package with Tab, as a link to it', () => {
    ranking();
    focus(screen.getByRole('button', { name: 'before' }));
    expect(tab()).toBe(screen.getByRole('link', { name: 'serde' }));
    expect(tab()).toBe(screen.getByRole('link', { name: 'tokio' }));
    // A real address, so it can be opened in a tab of its own.
    const serde = screen.getByRole('link', { name: 'serde' });
    expect(serde.getAttribute('href')).toBe('#/query/serde');
    // And an SVG link, around the marks: an HTML one inside an SVG is
    // not drawn at all.
    expect(serde.namespaceURI).toBe('http://www.w3.org/2000/svg');
    expect(serde.querySelector('path')).not.toBeNull();
  });

  it('opens the row with Enter', () => {
    const onSelect = ranking();
    focus(screen.getByRole('link', { name: 'tokio' }));
    fireEvent.keyDown(document.activeElement!, { key: 'Enter' });
    expect(onSelect).toHaveBeenCalledWith('tokio');
  });

  it('opens it once for a click, not once per mark it is drawn with', () => {
    const onSelect = ranking();
    fireEvent.click(document.querySelector('g[data-row="serde"] path')!);
    expect(onSelect).toHaveBeenCalledTimes(1);
  });

  it('shows the row’s tooltip while it has focus, and not after', () => {
    ranking();
    focus(screen.getByRole('link', { name: 'serde' }));
    expect(tooltip()?.textContent).toContain('6,820 declared it');
    focus(screen.getByRole('button', { name: 'before' }));
    expect(tooltip()).toBeNull();
  });

  it('lets Escape put the tooltip away without moving focus', () => {
    ranking();
    const serde = screen.getByRole('link', { name: 'serde' });
    focus(serde);
    fireEvent.keyDown(serde, { key: 'Escape' });
    expect(tooltip()).toBeNull();
    expect(document.activeElement).toBe(serde);
  });

  it('chooses a row that changes the panel, a button, with Enter or Space', () => {
    // Coverage by ecosystem: choosing one filters the ranking, and goes
    // nowhere, so it is a button rather than a link.
    const onSelect = vi.fn();
    render(
      <RankedBars
        words={EN}
        locale="en"
        label="repositories"
        bars={[{ label: 'npm', value: 5_000, part: 3_000, onSelect }]}
        partLabel="resolved by Syft"
      />,
    );
    const npm = screen.getByRole('button', { name: 'npm' });
    expect(tabOrder()).toContain(npm);
    fireEvent.keyDown(npm, { key: 'Enter' });
    fireEvent.keyDown(npm, { key: ' ' });
    expect(onSelect).toHaveBeenCalledTimes(2);
  });

  it('leaves rows that open nothing out of the tab order', () => {
    render(
      <RankedBars words={EN} locale="en" label="repositories" bars={[{ label: 'MIT', value: 1_200 }]} />,
    );
    expect(tabOrder()).toEqual([]);
  });

  it('is a group of links, not a picture, when its rows open something', () => {
    // `img` makes an SVG's children presentational: the links in it
    // would not be there for a screen reader to reach.
    ranking();
    const chart = screen.getByRole('group', { name: 'repositories' });
    expect(chart.tagName.toLowerCase()).toBe('svg');
    expect(within(chart).getAllByRole('link')).toHaveLength(2);
  });
});

describe('the tree, from the keyboard', () => {
  const TREE = {
    root: 'body-parser',
    children: [
      { name: 'debug', repositories: 3_580 },
      { name: 'http-errors', repositories: 3_582 },
    ],
    grandchildren: [
      { parent: 'debug', child: 'ms', repositories: 7_999 },
      { parent: 'http-errors', child: 'statuses', repositories: 4_239 },
    ],
  };

  function tree(onSelect = vi.fn()) {
    render(
      <DependencyTree
        words={EN}
        locale="en"
        width={720}
        tree={TREE}
        onSelect={onSelect}
        href={(name) => `#/query/${name}`}
      />,
    );
    return onSelect;
  }

  it('reaches every package below the root with Tab, in the order drawn', () => {
    tree();
    expect(tabOrder().map((stop) => stop.getAttribute('aria-label'))).toEqual([
      'debug', 'ms', 'http-errors', 'statuses',
    ]);
    // The root is the package already open: nothing to go to.
    expect(screen.queryByRole('link', { name: 'body-parser' })).toBeNull();
  });

  it('opens one with Enter', () => {
    const onSelect = tree();
    focus(screen.getByRole('link', { name: 'ms' }));
    fireEvent.keyDown(document.activeElement!, { key: 'Enter' });
    expect(onSelect).toHaveBeenCalledWith('ms');
  });

  it('shows a package’s tooltip while it has focus', () => {
    tree();
    focus(screen.getByRole('link', { name: 'http-errors' }));
    expect(tooltip()?.textContent).toContain('Pulled in by body-parser in 3,582 repositories');
  });
});

describe('the views, from the keyboard', () => {
  /** The overview's questions, the ranking's answered. */
  const overviewClient = () =>
    new Proxy({}, {
      get(_target, key: string) {
        return () =>
          Promise.resolve(
            key === 'topPackages'
              ? [
                  { name: 'typescript', repositoryCount: 6_863, directCount: 6_820 },
                  { name: 'eslint', repositoryCount: 6_440, directCount: 6_401 },
                ]
              : key === 'relationshipSplit'
                ? { direct: 1, transitive: 4, unknown: 0 }
                : [],
          );
      },
    }) as DatasetClient;

  it('goes from the ranking’s filter to its first bar with Tab, and Enter opens the package', async () => {
    const go = vi.fn();
    render(
      <Overview words={EN} locale="en" dataset={overviewClient()} ecosystems={['npm']} go={go} />,
    );
    await waitFor(() => expect(screen.getByRole('link', { name: 'typescript' })).toBeTruthy());

    focus(screen.getByLabelText(EN.ecosystemFilter));
    expect(tab()).toBe(screen.getByRole('link', { name: 'typescript' }));
    expect(tab()).toBe(screen.getByRole('link', { name: 'eslint' }));
    fireEvent.keyDown(document.activeElement!, { key: 'Enter' });
    expect(go).toHaveBeenCalledWith({ view: 'query', package: 'eslint' });
  });

  it('opens a package that pulls this one in with Enter', async () => {
    const go = vi.fn();
    const dataset = new Proxy({}, {
      get(_target, key: string) {
        const answers: Record<string, unknown> = {
          pulledInBy: [{ name: 'debug', repositories: 7_999 }],
          dependencyTree: { root: 'ms', children: [], grandchildren: [] },
          countDependents: 0,
          countDependentRows: 0,
          edgeAmbiguity: null,
        };
        return () => Promise.resolve(key in answers ? answers[key] : []);
      },
    }) as DatasetClient;
    render(
      <QueryView
        words={EN}
        locale="en"
        dataset={dataset}
        languages={[]}
        route={{ view: 'query', package: 'ms' }}
        go={go}
      />,
    );
    const debug = await screen.findByRole('link', { name: 'debug' });
    expect(tabOrder()).toContain(debug);
    focus(debug);
    fireEvent.keyDown(debug, { key: 'Enter' });
    expect(go).toHaveBeenCalledWith({ view: 'query', package: 'debug' });
  });

  it('takes the page to the query view for a bar chosen with Enter', async () => {
    // End to end, as `main.tsx` mounts it: the address, and the view.
    vi.stubGlobal('matchMedia', () => ({
      matches: false,
      addEventListener: () => {},
      removeEventListener: () => {},
    }));
    vi.stubGlobal(
      'fetch',
      vi.fn(async (_url: string, init?: RequestInit) => {
        const { method } = JSON.parse(String(init?.body)) as { method: string };
        const answers: Record<string, unknown> = {
          meta: { generator: 'g', schemaVersion: 's', observedFrom: '', observedTo: '' },
          topPackages: [{ name: 'typescript', repositoryCount: 6_863, directCount: 6_820 }],
          relationshipSplit: { direct: 1, transitive: 4, unknown: 0 },
          countDependents: 0,
          countDependentRows: 0,
          edgeAmbiguity: null,
          versionSpread: { versions: [], constrained: 0, unversioned: 0 },
          dependencyTree: { root: 'typescript', children: [], grandchildren: [] },
        };
        return new Response(JSON.stringify(method in answers ? answers[method] : []));
      }),
    );
    render(<App />);
    const bar = await screen.findByRole('link', { name: 'typescript' });
    focus(bar);
    fireEvent.keyDown(bar, { key: 'Enter' });

    expect(window.location.hash).toBe('#/query/typescript');
    expect(
      screen.getByRole('button', { name: EN.viewQuery }).getAttribute('aria-pressed'),
    ).toBe('true');
    expect((screen.getByLabelText(EN.searchAriaLabel) as HTMLInputElement).value).toBe(
      'typescript',
    );
  });
});
