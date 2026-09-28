/**
 * The query endpoint's answers, kept in the Worker's cache (#42).
 *
 * Every visitor's page asks the overview's dozen questions, and each
 * was a query: every visitor shares one ClickHouse account and its 16
 * concurrent queries, and on D1 every call is billed reads. An answer
 * is kept for as long as its method allows, under what it answers —
 * the dataset's version, the question, and its arguments — so a
 * repeated call is answered without the store, and a new dataset is a
 * miss rather than a stale hit.
 *
 * Wrapped round the store rather than the endpoint, so that what an
 * answer is kept under is what the store was asked: the arguments each
 * method read from the request, after the endpoint checked them. The
 * parameters as sent could carry anything, and every variation of
 * nothing would be a key of its own. It also puts the cache where it
 * belongs, after the rate limiter and the checks: a hit is still a
 * call, and a malformed one is refused before anything is looked up.
 *
 * Kept in a fixed number of slots, not under a key per question.
 * `wrangler dev`, which serves this under compose, keeps its cache in
 * `.wrangler/state` — the `web-state` volume — and deletes an expired
 * entry only when its own key is read again. A key per question would
 * leave a file behind for every package name anyone ever typed, times
 * every version of the dataset, and a client could type random names at
 * the rate limiter's pace. In slots, what is kept is bounded whatever
 * is asked. A slot says whose answer it holds, so two questions that
 * land in one cost each other a miss, never a wrong answer.
 */
import type { DatasetQueries } from '../backend';

/**
 * How long each question's answer is kept, in seconds; 0 is never.
 *
 * One figure for every answer the page puts side by side. They change
 * together, when `db index` runs once a day or a D1 import lands; kept
 * for different times, a reader could see totals from before an index
 * pass beside a package's counts from after it. Five minutes is how
 * long ClickHouse's observation span is kept (`SPAN_KEPT_MS`), so no
 * answer is older than the provenance the page states beside it. A
 * change the version misses — an index pass that dated nothing newer —
 * is served within that.
 *
 * `meta` is never kept: it is what an entry's version is read from,
 * and the healthcheck and the watchdog that ask for it must reach the
 * store.
 *
 * Every method is named, so one added to the store has to say.
 */
export const KEPT_SECONDS = {
  meta: 0,
  totals: 300,
  languageCoverage: 300,
  ecosystemCoverage: 300,
  relationshipSplit: 300,
  relationshipByEcosystem: 300,
  topPackages: 300,
  dependencyDistribution: 300,
  sourceComparison: 300,
  licenseShares: 300,
  edgeAmbiguity: 300,
  searchPackages: 300,
  ecosystemsFor: 300,
  dependentsOf: 300,
  countDependents: 300,
  countDependentRows: 300,
  versionSpread: 300,
  adoptionOverTime: 300,
  dependenciesOf: 300,
  pulledInBy: 300,
  dependencyTree: 300,
} as const satisfies Record<keyof DatasetQueries, number>;

/**
 * How many slots: 16³, one per three hex digits of a key's digest.
 *
 * Room for many times the questions five minutes of this page asks,
 * with the overview's dozen unlikely to share a slot.
 */
const SLOT_DIGITS = 3;

/**
 * The largest answer kept, in characters of JSON.
 *
 * A page of the table, the largest thing the page asks for, is about a
 * fifth of this; what is larger is a tool asking for hundreds of rows,
 * and is answered without being kept. With the slots, it bounds what
 * the cache can hold at 128 MiB, whatever is asked.
 */
const KEPT_CHARS = 32 * 1024;

/**
 * Where the slots are. A host no request names: under `wrangler dev`
 * a request's URL takes its host from the Host header, which a client
 * reaching the port sets as it likes, and each host would be another
 * set of slots.
 */
const SLOTS = 'https://answers.query.invalid';

/** On a kept answer: the digest of what it answers. */
const ANSWERS = 'x-answers';

/**
 * How long a dataset's version is believed before the store is asked
 * again: a D1 import is noticed within the minute, and `meta` is read
 * at most once a minute per isolate rather than before every call. On
 * ClickHouse, whose span is kept five minutes, it costs nothing either
 * way.
 */
const VERSION_KEPT_MS = 60_000;

/**
 * Versions, by the deployment's bindings. The runtime hands one
 * isolate's requests the same `env`, so this is one entry per isolate.
 */
const VERSIONS = new WeakMap<object, { until: number; version: Promise<string> }>();

/** The dataset's version: its provenance, which moves with each import or index pass. */
function versionOf(dataset: DatasetQueries, bindings: object): Promise<string> {
  const now = Date.now();
  const known = VERSIONS.get(bindings);
  if (known && known.until > now) return known.version;
  const version = dataset.meta().then((meta) =>
    JSON.stringify([
      meta.generator,
      meta.schemaVersion,
      meta.observedFrom,
      meta.observedTo,
    ]),
  );
  VERSIONS.set(bindings, { until: now + VERSION_KEPT_MS, version });
  // A failure is not a version: the next call asks again.
  version.catch(() => {
    if (VERSIONS.get(bindings)?.version === version) VERSIONS.delete(bindings);
  });
  return version;
}

async function sha256(text: string): Promise<string> {
  const digest = await crypto.subtle.digest('SHA-256', new TextEncoder().encode(text));
  return Array.from(new Uint8Array(digest), (byte) =>
    byte.toString(16).padStart(2, '0'),
  ).join('');
}

/** Whether the store's answer came from the cache, for the response to say. */
export interface Kept {
  status?: 'HIT' | 'MISS';
}

type Ask = (...args: unknown[]) => Promise<unknown>;

/**
 * `dataset`, answered from `cache` where a method allows it.
 *
 * `bindings` is what the dataset's version is remembered by: the
 * Worker's `env`. `kept` is told whether a call was answered from the
 * cache. `waitUntil` takes the work of keeping an answer, which the
 * response need not wait for.
 */
export function cachedDataset(
  dataset: DatasetQueries,
  cache: Cache,
  bindings: object,
  kept: Kept,
  waitUntil: (work: Promise<unknown>) => void,
): DatasetQueries {
  const answer = async (
    method: string,
    args: unknown[],
    seconds: number,
    ask: Ask,
  ): Promise<unknown> => {
    let version: string;
    try {
      version = await versionOf(dataset, bindings);
    } catch {
      // Without a version there is no key: answered as if uncached.
      return ask(...args);
    }
    const digest = await sha256(JSON.stringify([version, method, args]));
    const slot = new Request(`${SLOTS}/${digest.slice(0, SLOT_DIGITS)}`);

    const hit = await cache.match(slot).catch(() => undefined);
    if (hit?.headers.get(ANSWERS) === digest) {
      const value = await hit.json().catch(() => undefined);
      if (value !== undefined) {
        kept.status = 'HIT';
        return value;
      }
    }

    kept.status = 'MISS';
    const value = await ask(...args);
    const body = JSON.stringify(value);
    if (body.length <= KEPT_CHARS) {
      waitUntil(
        cache
          .put(
            slot,
            new Response(body, {
              headers: {
                'content-type': 'application/json; charset=utf-8',
                // What the Cache API reads to decide how long to keep
                // it: `no-store` would not be kept at all, and `private`
                // not by a cache every visitor shares.
                'cache-control': `public, max-age=${seconds}`,
                [ANSWERS]: digest,
              },
            }),
          )
          .catch(() => undefined),
      );
    }
    return value;
  };

  const wrapped: Record<string, Ask> = {};
  for (const method of Object.keys(KEPT_SECONDS) as (keyof DatasetQueries)[]) {
    const ask: Ask = (...args) =>
      (dataset[method] as unknown as Ask).apply(dataset, args);
    const seconds: number = KEPT_SECONDS[method];
    wrapped[method] = seconds > 0 ? (...args) => answer(method, args, seconds, ask) : ask;
  }
  return wrapped as unknown as DatasetQueries;
}
