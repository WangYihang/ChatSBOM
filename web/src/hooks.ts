/**
 * The state this app actually has, as hooks.
 *
 * Written after two defects that were both symptoms of imperative DOM
 * work rather than one-off slips:
 *
 *   - the query view's status line stayed on "Loading dataset…" forever,
 *     because a string written into the DOM at boot has no relationship
 *     to the state that later makes it false;
 *   - typing a package name updated the URL and searched for nothing,
 *     because the route and the input were two sources of truth being
 *     synced by hand and the sync had a condition that could never hold.
 *
 * Both are impossible to express here: the route is the single source of
 * truth, and every displayed string is derived from state on each render.
 */
import { useCallback, useEffect, useRef, useState } from 'react';

import { DatasetClient } from './d1/client';
import type { DatasetMeta } from './d1/queries';
import { formatRoute, parseRoute, type Route } from './router';

/** The hash route, and the only way to change it. */
export function useRoute(): [Route, (next: Route) => void] {
  const [route, setRoute] = useState<Route>(() =>
    parseRoute(window.location.hash),
  );

  useEffect(() => {
    const onChange = () => setRoute(parseRoute(window.location.hash));
    window.addEventListener('hashchange', onChange);
    window.addEventListener('popstate', onChange);
    return () => {
      window.removeEventListener('hashchange', onChange);
      window.removeEventListener('popstate', onChange);
    };
  }, []);

  const go = useCallback((next: Route) => {
    const hash = formatRoute(next);
    if (hash === window.location.hash) return;
    // pushState so Back returns to the previous view, and an explicit
    // state update because pushState fires no event of its own.
    window.history.pushState(null, '', hash);
    setRoute(next);
  }, []);

  return [route, go];
}

export type Boot =
  | { status: 'loading' }
  | { status: 'ready'; dataset: DatasetClient; meta: DatasetMeta }
  | { status: 'failed'; message: string };

/**
 * Fetch the dataset's provenance, which doubles as a readiness check.
 *
 * There is no engine to boot any more: queries run in the Worker
 * against D1, so the page starts by asking who made the data rather
 * than by downloading 7.7 MB of WebAssembly and 20.6 MB of Parquet.
 * One round trip, a few hundred bytes.
 */
export function useBoot(): Boot {
  const [boot, setBoot] = useState<Boot>({ status: 'loading' });

  useEffect(() => {
    let live = true;
    const dataset = new DatasetClient();
    dataset
      .meta()
      .then((meta) => {
        if (live) setBoot({ status: 'ready', dataset, meta });
      })
      .catch((error: unknown) => {
        if (!live) return;
        setBoot({
          status: 'failed',
          message:
            error instanceof Error
              ? error.message
              : 'Could not reach the dataset.',
        });
      });
    return () => {
      live = false;
    };
  }, []);

  return boot;
}

export type Async<T> =
  | { status: 'idle' }
  | { status: 'loading' }
  | { status: 'ready'; value: T }
  | { status: 'failed'; message: string };

/**
 * Run a query and track its outcome, discarding stale answers.
 *
 * The discarding is the point. The old imperative version fired a
 * debounced query per keystroke and wrote whatever came back into the
 * table, so a slow query for `ma` could land after a fast one for `mail`
 * and leave the page showing results that matched neither the input nor
 * the URL. Each run takes a token here and only the newest token is
 * allowed to publish.
 */
export function useAsync<T>(
  run: (() => Promise<T>) | null,
  deps: readonly unknown[],
): Async<T> {
  const [state, setState] = useState<Async<T>>(
    run ? { status: 'loading' } : { status: 'idle' },
  );
  const token = useRef(0);

  useEffect(() => {
    if (!run) {
      token.current += 1;
      setState({ status: 'idle' });
      return;
    }
    const mine = (token.current += 1);
    setState({ status: 'loading' });
    run()
      .then((value) => {
        if (token.current === mine) setState({ status: 'ready', value });
      })
      .catch((error: unknown) => {
        if (token.current !== mine) return;
        setState({
          status: 'failed',
          message: error instanceof Error ? error.message : 'Query failed.',
        });
      });
    // The caller states the dependencies, because only it knows which
    // captured values the closure actually reads.
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, deps);

  return state;
}

/** Debounce a value, for a filter that runs a query on every keystroke. */
export function useDebounced<T>(value: T, ms: number): T {
  const [settled, setSettled] = useState(value);
  useEffect(() => {
    const timer = window.setTimeout(() => setSettled(value), ms);
    return () => window.clearTimeout(timer);
  }, [value, ms]);
  return settled;
}

/**
 * A counter that increments whenever the OS colour scheme changes.
 *
 * Charts read their palette at draw time rather than caching it, so a
 * theme switch has to re-run the draw; depending on this value is what
 * makes that happen. The listener is guarded because `matchMedia` is
 * absent in some test environments, and a chart that throws while
 * drawing leaves a blank panel.
 */
export function useThemeEpoch(): number {
  const [epoch, setEpoch] = useState(0);

  useEffect(() => {
    if (typeof window.matchMedia !== 'function') return;
    const media = window.matchMedia('(prefers-color-scheme: dark)');
    const bump = () => setEpoch((n) => n + 1);
    media.addEventListener('change', bump);
    return () => media.removeEventListener('change', bump);
  }, []);

  return epoch;
}
