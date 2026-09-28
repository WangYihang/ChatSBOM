import { describe, expect, it } from 'vitest';

import { formatRoute, isViewName, parseRoute } from '../src/router';

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

  it('falls back to the overview for a name that cannot be decoded (#42)', () => {
    // `%` alone is not an escape. `decodeURIComponent` threw on it
    // while the page worked out its first route, nothing caught it,
    // and the page rendered blank — on every reload too, since the
    // hash is kept.
    expect(parseRoute('#/query/%')).toEqual({ view: 'overview' });
    expect(parseRoute('#/query/%E0%A4%A')).toEqual({ view: 'overview' });
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
