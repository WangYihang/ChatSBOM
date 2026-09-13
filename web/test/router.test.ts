// @vitest-environment jsdom
import { beforeEach, describe, expect, it, vi } from 'vitest';

import { formatRoute, isViewName, parseRoute, Router } from '../src/router';

describe('parseRoute', () => {
  it('defaults to the overview', () => {
    expect(parseRoute('')).toEqual({ view: 'overview' });
    expect(parseRoute('#')).toEqual({ view: 'overview' });
    expect(parseRoute('#/')).toEqual({ view: 'overview' });
  });

  it('reads a bare view', () => {
    expect(parseRoute('#/query')).toEqual({ view: 'query' });
  });

  it('reads a package handed over from a chart', () => {
    expect(parseRoute('#/query/mail')).toEqual({
      view: 'query', package: 'mail',
    });
  });

  it('decodes package names that need escaping', () => {
    expect(parseRoute('#/query/%40scope%2Fpkg')).toEqual({
      view: 'query', package: '@scope/pkg',
    });
  });

  it('keeps slashes inside a package name', () => {
    // Maven coordinates and scoped npm names both contain them.
    expect(parseRoute('#/query/laravel/framework').package)
      .toBe('laravel/framework');
  });

  it('falls back to the overview for an unknown view', () => {
    // A stale or hand-edited link should land somewhere useful.
    expect(parseRoute('#/nope/mail')).toEqual({ view: 'overview' });
  });
});

describe('formatRoute', () => {
  it('round-trips a package', () => {
    const route = { view: 'query' as const, package: '@scope/pkg' };
    expect(parseRoute(formatRoute(route))).toEqual(route);
  });

  it('omits an empty package', () => {
    expect(formatRoute({ view: 'query' })).toBe('#/query');
  });
});

describe('isViewName', () => {
  it('accepts the two views and nothing else', () => {
    expect(isViewName('overview')).toBe(true);
    expect(isViewName('query')).toBe(true);
    expect(isViewName('settings')).toBe(false);
  });
});

describe('Router', () => {
  beforeEach(() => {
    document.body.innerHTML = `
      <button id="to-overview" aria-pressed="false"></button>
      <button id="to-query" aria-pressed="false"></button>
      <section id="view-overview"></section>
      <section id="view-query" hidden></section>`;
    window.location.hash = '';
  });

  const sections = () => ({
    overview: document.getElementById('view-overview') as HTMLElement,
    query: document.getElementById('view-query') as HTMLElement,
  });

  it('shows the overview on start', () => {
    new Router(() => {}).start();
    expect(sections().overview.hidden).toBe(false);
    expect(sections().query.hidden).toBe(true);
  });

  it('marks the active view on the segmented control', () => {
    new Router(() => {}).start();
    expect(
      document.getElementById('to-overview')!.getAttribute('aria-pressed'),
    ).toBe('true');
  });

  it('swaps sections when navigating', () => {
    const router = new Router(() => {});
    router.start();
    router.go({ view: 'query', package: 'mail' });

    expect(sections().query.hidden).toBe(false);
    expect(sections().overview.hidden).toBe(true);
    expect(
      document.getElementById('to-query')!.getAttribute('aria-pressed'),
    ).toBe('true');
  });

  it('reports the route and the one it replaced', () => {
    const seen: unknown[] = [];
    const router = new Router((route, previous) => seen.push([route, previous]));
    router.start();
    router.go({ view: 'query', package: 'mail' });

    expect(seen.at(-1)).toEqual([
      { view: 'query', package: 'mail' },
      { view: 'overview' },
    ]);
  });

  it('re-applies when navigating to the hash already set', () => {
    // Clicking the same chart twice must still hand over the package.
    const onChange = vi.fn();
    const router = new Router(onChange);
    router.start();
    router.go({ view: 'query', package: 'mail' });
    const calls = onChange.mock.calls.length;
    router.go({ view: 'query', package: 'mail' });
    expect(onChange.mock.calls.length).toBe(calls + 1);
  });

  it('writes a linkable hash', () => {
    const router = new Router(() => {});
    router.start();
    router.go({ view: 'query', package: '@scope/pkg' });
    expect(window.location.hash).toBe('#/query/%40scope%2Fpkg');
  });

  it('exposes the current route', () => {
    const router = new Router(() => {});
    router.start();
    router.go({ view: 'query' });
    expect(router.route.view).toBe('query');
  });
});
