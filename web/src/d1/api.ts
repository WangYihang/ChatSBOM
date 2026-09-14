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
import { D1Binding } from './binding';
import { D1Dataset } from './queries';

export interface QueryEnv {
  DB: D1Database;
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
const dependentQuery: Reader<Parameters<D1Dataset['dependentsOf']>[0]> = (
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
 * `Object.create(null)` so the registry has no prototype: a lookup of
 * `constructor` returns undefined rather than a function.
 */
export const METHODS: Record<
  string,
  (dataset: D1Dataset, params: Record<string, unknown>) => Promise<unknown>
> = Object.assign(Object.create(null) as Record<string, never>, {
  dependentsOf: (d: D1Dataset, p: Record<string, unknown>) =>
    d.dependentsOf(dependentQuery(p)),
  countDependents: (d: D1Dataset, p: Record<string, unknown>) =>
    d.countDependents(dependentQuery(p)),
  relationshipSplit: (d: D1Dataset, p: Record<string, unknown>) =>
    d.relationshipSplit(optionalStr(p, 'language')),
  totals: (d: D1Dataset) => d.totals(),
  languageCoverage: (d: D1Dataset) => d.languageCoverage(),
  topPackages: (d: D1Dataset, p: Record<string, unknown>) =>
    d.topPackages({
      directOnly: optionalBool(p, 'directOnly'),
      ...(optionalStr(p, 'language')
        ? { language: optionalStr(p, 'language')! }
        : {}),
      ...(optionalNum(p, 'limit') !== undefined
        ? { limit: optionalNum(p, 'limit')! }
        : {}),
    }),
  dependencyDistribution: (d: D1Dataset) => d.dependencyDistribution(),
  sourceComparison: (d: D1Dataset) => d.sourceComparison(),
  searchPackages: (d: D1Dataset, p: Record<string, unknown>) =>
    d.searchPackages(str(p, 'term'), optionalNum(p, 'limit')),
  licenseShares: (d: D1Dataset, p: Record<string, unknown>) =>
    d.licenseShares(optionalNum(p, 'limit')),
  adoptionOverTime: (d: D1Dataset, p: Record<string, unknown>) =>
    d.adoptionOverTime(str(p, 'name')),
  versionSpread: (d: D1Dataset, p: Record<string, unknown>) =>
    d.versionSpread(str(p, 'name'), optionalNum(p, 'limit')),
  ecosystemsFor: (d: D1Dataset, p: Record<string, unknown>) =>
    d.ecosystemsFor(str(p, 'name')),
  meta: (d: D1Dataset) => d.meta(),
});

export async function handleQuery(
  request: Request,
  env: QueryEnv,
): Promise<Response> {
  if (request.method !== 'POST') {
    return json({ error: 'Method not allowed' }, 405, { Allow: 'POST' });
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

  const dataset = new D1Dataset(new D1Binding(env.DB));
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
