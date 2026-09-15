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
 */
import type { DatasetQueries } from '../backend';
import { ClickHouse } from '../clickhouse/client';
import { ClickHouseDataset } from '../clickhouse/queries';
import { D1Binding } from './binding';
import { D1Dataset } from './queries';

/**
 * What the endpoint needs to reach a store.
 *
 * Both are optional and exactly one is expected to be present. The
 * choice is made by which is configured rather than by a mode flag: a
 * flag can disagree with the bindings, and the failure then reads as
 * "the database is empty" rather than "you configured the other one".
 */
export interface QueryEnv {
  /** Cloudflare D1, for a deployment that ships a snapshot. */
  DB?: D1Database;
  /** ClickHouse over HTTP, for a deployment that reads the live data. */
  CLICKHOUSE_URL?: string;
  CLICKHOUSE_DB?: string;
  CLICKHOUSE_USER?: string;
  CLICKHOUSE_PASSWORD?: string;
  /** Reported by `meta()`, since the database cannot know it. */
  GENERATOR?: string;
}

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

function str(params: Record<string, unknown>, key: string): string {
  const value = params[key];
  if (typeof value !== 'string' || value === '') {
    throw new BadRequest(`"${key}" must be a non-empty string`);
  }
  return value;
}

function optionalStr(
  params: Record<string, unknown>,
  key: string,
): string | undefined {
  const value = params[key];
  if (value === undefined || value === null || value === '') return undefined;
  if (typeof value !== 'string') {
    throw new BadRequest(`"${key}" must be a string`);
  }
  return value;
}

function optionalBool(params: Record<string, unknown>, key: string): boolean {
  return params[key] === true;
}

function optionalNum(
  params: Record<string, unknown>,
  key: string,
): number | undefined {
  const value = params[key];
  if (value === undefined || value === null) return undefined;
  if (typeof value !== 'number' || !Number.isFinite(value)) {
    throw new BadRequest(`"${key}" must be a number`);
  }
  return value;
}

/** A dependant query's filters, read from untrusted parameters. */
const dependentQuery: Reader<Parameters<DatasetQueries['dependentsOf']>[0]> = (
  params,
) => ({
  name: str(params, 'name'),
  ...(optionalStr(params, 'type') ? { type: optionalStr(params, 'type')! } : {}),
  ...(optionalStr(params, 'language')
    ? { language: optionalStr(params, 'language')! }
    : {}),
  directOnly: optionalBool(params, 'directOnly'),
  ...(optionalNum(params, 'limit') !== undefined
    ? { limit: optionalNum(params, 'limit')! }
    : {}),
});

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
  relationshipSplit: (d: DatasetQueries, p: Record<string, unknown>) =>
    d.relationshipSplit(optionalStr(p, 'language')),
  totals: (d: DatasetQueries) => d.totals(),
  languageCoverage: (d: DatasetQueries) => d.languageCoverage(),
  topPackages: (d: DatasetQueries, p: Record<string, unknown>) =>
    d.topPackages({
      directOnly: optionalBool(p, 'directOnly'),
      ...(optionalStr(p, 'language')
        ? { language: optionalStr(p, 'language')! }
        : {}),
      ...(optionalNum(p, 'limit') !== undefined
        ? { limit: optionalNum(p, 'limit')! }
        : {}),
    }),
  dependencyDistribution: (d: DatasetQueries) => d.dependencyDistribution(),
  sourceComparison: (d: DatasetQueries) => d.sourceComparison(),
  searchPackages: (d: DatasetQueries, p: Record<string, unknown>) =>
    d.searchPackages(str(p, 'term'), optionalNum(p, 'limit')),
  edgeAmbiguity: (d: DatasetQueries) => d.edgeAmbiguity(),
  relationshipByLanguage: (d: DatasetQueries) => d.relationshipByLanguage(),
  versionKindShares: (d: DatasetQueries) => d.versionKindShares(),
  licenseShares: (d: DatasetQueries, p: Record<string, unknown>) =>
    d.licenseShares(optionalNum(p, 'limit')),
  adoptionOverTime: (d: DatasetQueries, p: Record<string, unknown>) =>
    d.adoptionOverTime(str(p, 'name')),
  versionSpread: (d: DatasetQueries, p: Record<string, unknown>) =>
    d.versionSpread(str(p, 'name'), optionalNum(p, 'limit')),
  ecosystemsFor: (d: DatasetQueries, p: Record<string, unknown>) =>
    d.ecosystemsFor(str(p, 'name')),
  dependenciesOf: (d: DatasetQueries, p: Record<string, unknown>) =>
    d.dependenciesOf(str(p, 'name'), optionalNum(p, 'limit')),
  pulledInBy: (d: DatasetQueries, p: Record<string, unknown>) =>
    d.pulledInBy(str(p, 'name'), optionalNum(p, 'limit')),
  dependencyTree: (d: DatasetQueries, p: Record<string, unknown>) =>
    d.dependencyTree(str(p, 'name'), {
      ...(optionalNum(p, 'children') !== undefined
        ? { children: optionalNum(p, 'children')! }
        : {}),
      ...(optionalNum(p, 'branch') !== undefined
        ? { branch: optionalNum(p, 'branch')! }
        : {}),
    }),
  meta: (d: DatasetQueries) => d.meta(),
});

export async function handleQuery(
  request: Request,
  env: QueryEnv,
): Promise<Response> {
  if (request.method !== 'POST') {
    return json({ error: 'Method not allowed' }, 405, { Allow: 'POST' });
  }

  // Before the body is even read. With no store there is nothing a
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
    return json({ error: 'No database bound to this deployment.' }, 503);
  }

  let method: unknown;
  let params: Record<string, unknown>;
  try {
    const body: unknown = await request.json();
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

  try {
    return json(await run(dataset, params));
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

function json(
  payload: unknown,
  status = 200,
  headers: Record<string, string> = {},
): Response {
  return new Response(JSON.stringify(payload), {
    status,
    headers: {
      'content-type': 'application/json; charset=utf-8',
      // A database behind this is the point; caching would undo it.
      'cache-control': 'no-store',
      ...headers,
    },
  });
}
