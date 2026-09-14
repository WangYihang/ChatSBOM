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
 */
export function parseRoute(hash: string): Route {
  const parts = hash.replace(/^#\/?/, '').split('/').filter(Boolean);
  const [view, ...rest] = parts;

  if (!view || !isViewName(view)) return { view: 'overview' };

  const name = rest.length ? decodeURIComponent(rest.join('/')) : '';
  return name ? { view, package: name } : { view };
}

export function formatRoute(route: Route): string {
  if (route.view === 'query' && route.package) {
    return `#/query/${encodeURIComponent(route.package)}`;
  }
  return `#/${route.view}`;
}

/** Swap the visible section and keep the segmented control in step. */
export class Router {
  private current: Route = { view: 'overview' };

  constructor(
    private readonly onChange: (route: Route, previous: Route) => void,
  ) {}

  start(): void {
    // popstate covers back/forward over our own pushState entries;
    // hashchange covers someone editing the URL by hand.
    window.addEventListener('popstate', () => this.apply(this.read()));
    window.addEventListener('hashchange', () => this.apply(this.read()));
    this.apply(this.read());
  }

  /**
   * Navigate, and have the DOM reflect it before returning.
   *
   * Assigning `location.hash` fires `hashchange` asynchronously, so a
   * caller that navigated and then touched the new view would be talking
   * to a still-hidden section — focusing an input that is not yet
   * visible, for instance. pushState plus an immediate apply keeps `go`
   * synchronous, and still leaves a history entry for the back button.
   */
  go(route: Route): void {
    const next = formatRoute(route);
    if (window.location.hash !== next) {
      window.history.pushState(null, '', next);
    }
    this.apply(route);
  }

  get route(): Route {
    return this.current;
  }

  private read(): Route {
    return parseRoute(window.location.hash);
  }

  private apply(route: Route): void {
    const previous = this.current;
    this.current = route;

    for (const view of VIEWS) {
      const section = document.getElementById(`view-${view}`);
      if (section) section.hidden = view !== route.view;
      const button = document.getElementById(`to-${view}`);
      if (button) {
        button.setAttribute('aria-pressed', String(view === route.view));
      }
    }

    this.onChange(route, previous);
  }
}
