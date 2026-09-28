/**
 * Two peer views, navigable from each other and from a URL.
 *
 * The overview answers standing questions; the query view answers one
 * about a package. They are peers rather than a page and a sub-page, so
 * either can be linked to and either can hand off to the other — a bar in
 * a chart leads to the query view with that package already filled in,
 * which is the path someone actually takes.
 *
 * State lives in the hash so a view is shareable and the back button
 * works, without a router library or a server that knows about routes.
 */
export type ViewName = 'overview' | 'query';

export interface Route {
  view: ViewName;
  /** Package to inspect, when arriving at the query view from a chart. */
  package?: string;
}

const VIEWS: readonly ViewName[] = ['overview', 'query'];

export function isViewName(value: string): value is ViewName {
  return (VIEWS as readonly string[]).includes(value);
}

/**
 * Parse `#/query/mail` or `#/overview`.
 *
 * Anything unrecognised resolves to the overview rather than erroring: a
 * stale or hand-edited link should land somewhere useful.
 *
 * That includes a name that is not a valid escape. `#/query/%` made
 * `decodeURIComponent` throw while the page worked out its first route,
 * nothing caught it, and the page rendered blank — on every reload too,
 * since the hash is kept (#42).
 */
export function parseRoute(hash: string): Route {
  const parts = hash.replace(/^#\/?/, '').split('/').filter(Boolean);
  const [view, ...rest] = parts;

  if (!view || !isViewName(view)) return { view: 'overview' };

  let name: string;
  try {
    name = rest.length ? decodeURIComponent(rest.join('/')) : '';
  } catch {
    return { view: 'overview' };
  }
  return name ? { view, package: name } : { view };
}

/**
 * How to navigate: `replace` for a change that refines where the reader
 * already is — a name being typed — rather than going somewhere new.
 */
export interface Navigation {
  replace?: boolean;
}

/** Navigate to `route`, adding a history entry unless told to replace one. */
export type Go = (route: Route, how?: Navigation) => void;

export function formatRoute(route: Route): string {
  if (route.view === 'query' && route.package) {
    return `#/query/${encodeURIComponent(route.package)}`;
  }
  return `#/${route.view}`;
}
