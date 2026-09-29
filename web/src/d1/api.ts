/**
 * The query endpoint: `POST /api/q` with `{ method, params }`.
 *
 * The browser names a method. It never sends SQL, and there is no
 * parameter through which it could: each method owns its statement and
 * takes only the values it declares. That is not defence in depth, it
 * is the only defence — this page is public, and a page that can send
 * SQL can send any SQL.
 *
 * The registry below is the allow-list. A name that is not a key is a
 * 400 before the database is touched, including names that exist on
 * every JavaScript object: `constructor`, `toString`, `__proto__`. A
 * plain lookup on an object literal would accept those.
 *
 * Each method's parameters are checked the same way, against what that
 * method takes: whole numbers where it counts, strings no longer than
 * what they name, and a body no larger than a call needs (#31).
 */
import type { DatasetQueries } from '../backend';
import { BodyError, readBody } from '../body';
import { ClickHouse } from '../clickhouse/client';
import { ecosystemForLanguage } from '../ecosystems';
import { ClickHouseDataset } from '../clickhouse/queries';
import { type Admission, rateLimit, type RateLimitEnv, type RateLimitSetting } from '../ratelimit';
import { D1Binding } from './binding';
import { cachedDataset, type Kept } from './cache';
import { D1Dataset } from './queries';

/**
 * What the endpoint needs to reach a store.
 *
 * Both are optional and exactly one is expected to be present. The
 * choice is made by which is configured rather than by a mode flag: a
 * flag can disagree with the bindings, and the failure then reads as
 * "the database is empty" rather than "you configured the other one".
 */
export interface QueryEnv extends RateLimitEnv {
  /** Cloudflare D1, for a deployment that ships a snapshot. */
  DB?: D1Database;
  /** ClickHouse over HTTP, for a deployment that reads the live data. */
  CLICKHOUSE_URL?: string;
  CLICKHOUSE_DB?: string;
  CLICKHOUSE_USER?: string;
  CLICKHOUSE_PASSWORD?: string;
  /** Reported by `meta()`, since the database cannot know it. */
  GENERATOR?: string;
  /**
   * A per-client budget, counted as the chat's is (`ratelimit.ts`). Every
   * visitor shares one ClickHouse account and its 16 concurrent queries,
   * and on D1 every call is billed reads; nothing bounded how fast one
   * client could spend either. Unset, no limit.
   */
  QUERY_RATE_LIMIT?: RateLimitSetting;
}

/** What a query refused by its limit is answered with. */
const OVER_LIMIT: Record<Exclude<Admission, 'admitted'>, [number, string]> = {
  limited: [429, 'Too many queries. Wait a moment.'],
  misconfigured: [503, 'The query endpoint is not set up correctly on this deployment.'],
  unreachable: [503, 'Queries cannot be counted for a moment. Try again shortly.'],
};

/**
 * Pick the store from what is configured.
 *
 * ClickHouse first when both are present: it holds the live data, and a
 * deployment with both bound is one mid-migration, where the newer
 * answer is the right one.
 */
export function selectDataset(env: QueryEnv): DatasetQueries | null {
  if (env.CLICKHOUSE_URL) {
    const dataset = new ClickHouseDataset(
      new ClickHouse({
        url: env.CLICKHOUSE_URL,
        database: env.CLICKHOUSE_DB ?? 'chatsbom',
        // Defaults match the development server's read-only account.
        // A deployment reachable from outside localhost supplies its
        // own; see `database/config/users.d/guest.xml`.
        user: env.CLICKHOUSE_USER ?? 'guest',
        password: env.CLICKHOUSE_PASSWORD ?? 'guest',
      }),
    );
    if (env.GENERATOR) dataset.generator = env.GENERATOR;
    return dataset;
  }
  if (env.DB) {
    return new D1Dataset(new D1Binding(env.DB));
  }
  return null;
}

/** Coerces one untrusted parameter bag into a method's arguments. */
type Reader<T> = (params: Record<string, unknown>) => T;

class BadRequest extends Error {}

/**
 * How long a string may be, by what it names.
 *
 * A package name, or the start of one: npm's own limit is 214
 * characters, the longest rule of any registry here, and 256 leaves room
 * above it while still bounding what reaches a statement. A language or
 * an ecosystem is a word.
 */
const NAME_CHARS = 256;
const WORD_CHARS = 64;

/**
 * The largest body a call needs, with room to spare.
 *
 * The longest call is a dependant query with every string at its cap,
 * each character one JSON has to escape: about 2.5 KiB.
 */
const MAX_BODY_BYTES = 4 * 1024;

function capped(key: string, value: string, max: number): string {
  if (value.length > max) {
    throw new BadRequest(`"${key}" must be at most ${max} characters`);
  }
  return value;
}

function str(
  params: Record<string, unknown>,
  key: string,
  max = NAME_CHARS,
): string {
  const value = params[key];
  if (typeof value !== 'string' || value === '') {
    throw new BadRequest(`"${key}" must be a non-empty string`);
  }
  return capped(key, value, max);
}

function optionalStr(
  params: Record<string, unknown>,
  key: string,
  max = WORD_CHARS,
): string | undefined {
  const value = params[key];
  if (value === undefined || value === null || value === '') return undefined;
  if (typeof value !== 'string') {
    throw new BadRequest(`"${key}" must be a string`);
  }
  return capped(key, value, max);
}

/**
 * An ecosystem, as `ecosystemsFor` spelled it.
 *
 * Not held to the table in `ecosystems.ts`: a type the table does not
 * map reads as itself, so the page offers — and sends back — types it
 * has never heard of. What no registry is called is a name every
 * JavaScript object already has; `toString` found a function in that
 * table, and the request failed as a 500.
 */
function optionalType(
  params: Record<string, unknown>,
  key: string,
): string | undefined {
  const value = optionalStr(params, key);
  if (value !== undefined && value in Object.prototype) {
    throw new BadRequest(`"${key}" is not an ecosystem`);
  }
  return value;
}

function optionalBool(params: Record<string, unknown>, key: string): boolean {
  const value = params[key];
  if (value === undefined || value === null) return false;
  if (typeof value !== 'boolean') {
    throw new BadRequest(`"${key}" must be true or false`);
  }
  return value;
}

/**
 * A count, or a position: a whole number no smaller than `min`.
 *
 * Refused rather than coerced. -1 and 2.5 mean nothing as a limit, and a
 * store left to guess read -1 as "no limit given". How *large* is the
 * stores' to bound (`dataset/shape.ts`): past a ceiling a value is not
 * wrong, only more than anyone gets.
 */
function optionalInt(
  params: Record<string, unknown>,
  key: string,
  min = 1,
): number | undefined {
  const value = params[key];
  if (value === undefined || value === null) return undefined;
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < min) {
    throw new BadRequest(`"${key}" must be a whole number, at least ${min}`);
  }
  return value;
}

/** A dependant query's filters, read from untrusted parameters. */
const dependentQuery: Reader<Parameters<DatasetQueries['dependentsOf']>[0]> = (
  params,
) => {
  const name = str(params, 'name');
  const type = optionalType(params, 'type');
  const language = optionalStr(params, 'language');
  const limit = optionalInt(params, 'limit');
  const offset = optionalInt(params, 'offset', 0);
  return {
    name,
    ...(type ? { type } : {}),
    ...(language ? { language } : {}),
    directOnly: optionalBool(params, 'directOnly'),
    ...(limit === undefined ? {} : { limit }),
    ...(offset === undefined ? {} : { offset }),
  };
};

/**
 * Names the registry answers for one release only, for pages loaded
 * before #55 §4.13, each by way of a method the interface declares.
 * They are not part of `DatasetQueries`, and are dropped with the
 * `language` parameter.
 */
export const LEGACY_METHODS: ReadonlySet<string> = new Set([
  'relationshipByLanguage',
]);

/**
 * The ecosystem an aggregate is asked for.
 *
 * `ecosystem` is the parameter. For one release, a request that names
 * only `language` — a page loaded before aggregates were re-keyed by
 * ecosystem (#55 §4.13) — is read as the ecosystem that language's list
 * used to stand for, `php` as Composer, and one with no such ecosystem
 * as the whole corpus rather than as an error.
 *
 * An ecosystem is read as `type` is, since it names the same thing.
 */
function aggregateEcosystem(
  params: Record<string, unknown>,
): string | undefined {
  const ecosystem = optionalType(params, 'ecosystem');
  if (ecosystem) return ecosystem;
  const language = optionalStr(params, 'language');
  return language ? ecosystemForLanguage(language) : undefined;
}

/**
 * Every method the dashboard may call, and how to read its arguments.
 *
 * Written against `DatasetQueries`, not against D1: swapping the store
 * is the one line below where the instance is built.
 *
 * `Object.create(null)` so the registry has no prototype: a lookup of
 * `constructor` returns undefined rather than a function.
 */
export const METHODS: Record<
  string,
  (dataset: DatasetQueries, params: Record<string, unknown>) => Promise<unknown>
> = Object.assign(Object.create(null) as Record<string, never>, {
  dependentsOf: (d: DatasetQueries, p: Record<string, unknown>) =>
    d.dependentsOf(dependentQuery(p)),
  countDependents: (d: DatasetQueries, p: Record<string, unknown>) =>
    d.countDependents(dependentQuery(p)),
  countDependentRows: (d: DatasetQueries, p: Record<string, unknown>) =>
    d.countDependentRows(dependentQuery(p)),
  relationshipSplit: (d: DatasetQueries, p: Record<string, unknown>) =>
    d.relationshipSplit(aggregateEcosystem(p)),
  totals: (d: DatasetQueries) => d.totals(),
  languageCoverage: (d: DatasetQueries) => d.languageCoverage(),
  ecosystemCoverage: (d: DatasetQueries) => d.ecosystemCoverage(),
  topPackages: (d: DatasetQueries, p: Record<string, unknown>) => {
    const ecosystem = aggregateEcosystem(p);
    const limit = optionalInt(p, 'limit');
    return d.topPackages({
      directOnly: optionalBool(p, 'directOnly'),
      ...(ecosystem ? { ecosystem } : {}),
      ...(limit === undefined ? {} : { limit }),
    });
  },
  dependencyDistribution: (d: DatasetQueries) => d.dependencyDistribution(),
  sourceComparison: (d: DatasetQueries) => d.sourceComparison(),
  searchPackages: (d: DatasetQueries, p: Record<string, unknown>) =>
    d.searchPackages(str(p, 'term'), optionalInt(p, 'limit')),
  edgeAmbiguity: (d: DatasetQueries) => d.edgeAmbiguity(),
  relationshipByEcosystem: (d: DatasetQueries) => d.relationshipByEcosystem(),
  // The name a page loaded before #55 §4.13 asks for, kept one release.
  // Its rows are per ecosystem now; `language` carries the ecosystem
  // so that page still draws its bars.
  relationshipByLanguage: async (d: DatasetQueries) =>
    (await d.relationshipByEcosystem()).map((row) => ({
      language: row.ecosystem,
      ...row,
    })),
  // Not `versionKindShares`: nothing called it, and on D1 it counted
  // every artifact row per request, against this store's rule that the
  // aggregates are read, never recomputed (#31).
  licenseShares: (d: DatasetQueries, p: Record<string, unknown>) =>
    d.licenseShares(optionalInt(p, 'limit')),
  adoptionOverTime: (d: DatasetQueries, p: Record<string, unknown>) =>
    d.adoptionOverTime(str(p, 'name')),
  versionSpread: (d: DatasetQueries, p: Record<string, unknown>) =>
    d.versionSpread(str(p, 'name'), optionalInt(p, 'limit')),
  ecosystemsFor: (d: DatasetQueries, p: Record<string, unknown>) =>
    d.ecosystemsFor(str(p, 'name')),
  dependenciesOf: (d: DatasetQueries, p: Record<string, unknown>) =>
    d.dependenciesOf(str(p, 'name'), optionalInt(p, 'limit')),
  pulledInBy: (d: DatasetQueries, p: Record<string, unknown>) =>
    d.pulledInBy(str(p, 'name'), optionalInt(p, 'limit')),
  dependencyTree: (d: DatasetQueries, p: Record<string, unknown>) => {
    const name = str(p, 'name');
    const children = optionalInt(p, 'children');
    const branch = optionalInt(p, 'branch');
    return d.dependencyTree(name, {
      ...(children === undefined ? {} : { children }),
      ...(branch === undefined ? {} : { branch }),
    });
  },
  meta: (d: DatasetQueries) => d.meta(),
});

/**
 * The Worker's cache, where the runtime has one: Node, which runs the
 * tests, does not, and a preview's does nothing. Looked up on
 * `globalThis` because `default` is the Workers runtime's own, and the
 * tests' types also load the DOM's `caches`, which has none.
 */
function workerCache(): Cache | undefined {
  return (globalThis as { caches?: { default?: Cache } }).caches?.default;
}

export async function handleQuery(
  request: Request,
  env: QueryEnv,
  // Where keeping an answer goes on after the response is sent. Without
  // one, as in the tests, it is done before.
  ctx?: Pick<ExecutionContext, 'waitUntil'>,
): Promise<Response> {
  if (request.method !== 'POST') {
    return turnAway(request, 405, 'Method not allowed', { Allow: 'POST' });
  }

  // Before the body is parsed. With no store there is nothing a
  // well-formed request could be answered from, so reporting a
  // malformed one first would send whoever deployed it to debug their
  // JSON instead of their bindings.
  //
  // The only place a concrete store is named. Which one is decided from
  // the bindings; the registry, the endpoint and the browser are all
  // unchanged by the choice, which is what the method-level interface
  // bought.
  const dataset = selectDataset(env);
  if (!dataset) {
    return turnAway(request, 503, 'No database bound to this deployment.');
  }

  // Before the body is parsed, so a flood costs one count each and no
  // parsing, and a malformed request spends the budget like any other.
  // Before the cache too: a hit is still a call.
  const admission = await rateLimit(request, env, 'query', env.QUERY_RATE_LIMIT);
  if (admission !== 'admitted') {
    const [status, error] = OVER_LIMIT[admission];
    return turnAway(request, status, error);
  }

  let method: unknown;
  let params: Record<string, unknown>;
  try {
    const body: unknown = JSON.parse(await readBody(request, MAX_BODY_BYTES));
    if (typeof body !== 'object' || body === null) {
      throw new BadRequest('Expected a JSON object');
    }
    const record = body as Record<string, unknown>;
    method = record['method'];
    const raw = record['params'];
    params =
      typeof raw === 'object' && raw !== null
        ? (raw as Record<string, unknown>)
        : {};
  } catch (error) {
    if (error instanceof BodyError) {
      return json({ error: error.message }, error.status);
    }
    return json(
      { error: error instanceof BadRequest ? error.message : 'Malformed request' },
      400,
    );
  }

  if (typeof method !== 'string') {
    return json({ error: 'A "method" name is required' }, 400);
  }
  const run = METHODS[method];
  if (!run) {
    return json({ error: `Unknown method: ${method}` }, 400);
  }

  // Asked through the cache where there is one (`cache.ts`). Each
  // method reads and checks its arguments before it asks the store, so
  // a malformed call is refused before anything is looked up.
  const cache = workerCache();
  const kept: Kept = {};
  const keeping: Promise<unknown>[] = [];
  const store = cache
    ? cachedDataset(dataset, cache, env, kept, (work) =>
        ctx ? ctx.waitUntil(work) : keeping.push(work),
      )
    : dataset;

  try {
    const answer = await run(store, params);
    await Promise.all(keeping);
    // What the page gets is still `no-store`: it is a POST, which no
    // browser keeps, and the one copy worth keeping is the Worker's.
    // Whether this was it is said, for whoever is looking.
    return json(answer, 200, kept.status ? { 'x-cache': kept.status } : {});
  } catch (error) {
    if (error instanceof BadRequest) {
      return json({ error: error.message }, 400);
    }
    // Database errors carry table names, column names and SQL
    // fragments. Logged, never returned.
    console.error('query failed', method, error);
    return json({ error: 'The query could not be answered.' }, 500);
  }
}

/**
 * A refusal made before the body was read: read it, then answer.
 *
 * Under `wrangler dev`, which serves this under compose, a response
 * sent with the request body unread lost the connection now and then,
 * and its proxy answered 500 in its place: about one 429 in five (#31),
 * and about half the 405s and 503s once the body was large (#32, #42).
 * Read against the cap, as every body here is, so one declared larger
 * than the cap is still refused unread. A body is at most
 * MAX_BODY_BYTES.
 */
async function turnAway(
  request: Request,
  status: number,
  error: string,
  headers: Record<string, string> = {},
): Promise<Response> {
  await readBody(request, MAX_BODY_BYTES).catch(() => undefined);
  return json({ error }, status, headers);
}

function json(
  payload: unknown,
  status = 200,
  headers: Record<string, string> = {},
): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      'content-type': 'application/json; charset=utf-8',
      // Not for the browser or anything between to keep: an answer the
      // Worker keeps is kept under the dataset's version (`cache.ts`),
      // which nothing downstream would know to key on.
      'cache-control': 'no-store',
      ...headers,
    },
  });
}
