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

import type { DatasetClient } from './d1/client';
import type { DatasetMeta } from './dataset/types';
import { formatRoute, type Go, parseRoute, type Route } from './router';

/** The hash route, and the only way to change it. */
export function useRoute(): [Route, Go] {
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

  const go = useCallback<Go>((next, how = {}) => {
    const hash = formatRoute(next);
    if (hash === window.location.hash) return;
    // pushState so Back returns to the previous view, and an explicit
    // state update because pushState fires no event of its own.
    //
    // A name being typed replaces the entry instead. Each pause in the
    // typing was pushed, so Back stepped through every half-typed name
    // before it left the view (#42).
    if (how.replace) {
      window.history.replaceState(null, '', hash);
    } else {
      window.history.pushState(null, '', hash);
    }
    setRoute(next);
  }, []);

  return [route, go];
}

export type Boot =
  | { status: 'loading' }
  | { status: 'ready'; meta: DatasetMeta }
  | { status: 'failed'; error: unknown };

/**
 * Fetch the dataset's provenance, which doubles as a readiness check.
 *
 * There is no engine to boot any more: queries run in the Worker
 * against D1, so the page starts by asking who made the data rather
 * than by downloading 7.7 MB of WebAssembly and 20.6 MB of Parquet.
 * One round trip, a few hundred bytes.
 *
 * Asked beside the page's own questions, not before them (#42). The
 * overview's dozen waited that round trip for an answer only the footer
 * and the metadata panel read.
 */
export function useBoot(dataset: DatasetClient): Boot {
  const meta = useAsync(
    useCallback((signal: AbortSignal) => dataset.meta(signal), [dataset]),
    [dataset],
  );
  if (meta.status === 'ready') return { status: 'ready', meta: meta.value };
  if (meta.status === 'failed') return meta;
  return { status: 'loading' };
}

export type Async<T> =
  | { status: 'idle' }
  /** `previous` is the last answer, while the next is on its way. */
  | { status: 'loading'; previous?: T }
  | { status: 'ready'; value: T }
  /**
   * What it failed with, kept rather than its message: the message is
   * English, and what a failure says is decided where it is shown, in
   * the page's language (`i18n/failure.ts`).
   */
  | { status: 'failed'; error: unknown };

/**
 * Run a query and track its outcome, discarding stale answers.
 *
 * The discarding is the point. The old imperative version fired a
 * debounced query per keystroke and wrote whatever came back into the
 * table, so a slow query for `ma` could land after a fast one for `mail`
 * and leave the page showing results that matched neither the input nor
 * the URL. Only the newest run is allowed to publish.
 *
 * A superseded run is abandoned, too, not only ignored (#42). Its
 * answer was discarded, but its request ran to the end, so each
 * keystroke still cost the store a query. Each run is handed a signal,
 * aborted when the question changes or the view goes, to pass on to
 * `fetch`; the same signal is what tells a late answer it is stale.
 *
 * While the next answer loads, the last one is kept as `previous`, for
 * a view that would rather go on showing it than empty. The table did
 * empty on every page turn, and took the panels under it along.
 */
export function useAsync<T>(
  run: ((signal: AbortSignal) => Promise<T>) | null,
  deps: readonly unknown[],
): Async<T> {
  const [state, setState] = useState<Async<T>>(
    run ? { status: 'loading' } : { status: 'idle' },
  );
  // Read when a run starts, never drawn, so a ref rather than state.
  const last = useRef<{ value: T } | null>(null);

  useEffect(() => {
    if (!run) {
      setState({ status: 'idle' });
      return;
    }
    const abandon = new AbortController();
    const kept = last.current;
    setState(
      kept ? { status: 'loading', previous: kept.value } : { status: 'loading' },
    );
    run(abandon.signal).then(
      (value) => {
        if (abandon.signal.aborted) return;
        last.current = { value };
        setState({ status: 'ready', value });
      },
      (error: unknown) => {
        if (abandon.signal.aborted) return;
        setState({ status: 'failed', error });
      },
    );
    return () => abandon.abort();
    // The caller states the dependencies, because only it knows which
    // captured values the closure actually reads, so the rule cannot
    // check this list: its callers' `useCallback` lists are where that
    // is checked. Nor is `run` one of them. A caller may make it anew on
    // each render, and as a dependency it would start a run on each.
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
