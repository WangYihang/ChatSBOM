/**
 * The questions both stores answer by reading one stored table, said
 * once: which table in each store, what each calls the columns, and
 * what the result names them.
 *
 * Eight of the dashboard's questions are this simple. D1 precomputes
 * them at export time (`agg_*`) and ClickHouse keeps them as rollups
 * (`mv_*`), and past the names the two statements were the same
 * statement, written twice, with `Number(row.x)` written out again per
 * field. The strategies still differ where the stores do — every point
 * lookup, the edges, the tree's second hop, the provenance — and those
 * stay in each backend, written out.
 *
 * Values are bound, never spliced in: a statement here is written with
 * ClickHouse's `{name:Type}` placeholders, and the D1 dialect below
 * turns each into a `?` with its value in order. The only text a
 * statement is built from is this file's.
 */
import { num, type Row } from './shape';
import type {
  AdoptionPoint,
  DependencyBucket,
  EcosystemCoverage,
  LanguageCoverage,
  LicenseShare,
  PackagePopularity,
  SourceComparison,
  Totals,
} from './types';

/** The stores, as the spec names their halves. */
export type Store = 'd1' | 'clickhouse';

/** A value a statement is bound with. */
export type Value = string | number;

/** A column both stores name alike, or what each calls it. */
type Named = string | Readonly<Record<Store, string>>;

interface Column {
  column: Named;
  /** A count comes back as a number, however the store spelled it. */
  count: boolean;
}

const count = (column: Named): Column => ({ column, count: true });
const text = (column: Named): Column => ({ column, count: false });

/** One stored table, read the same way in both stores. */
export interface Read<T> {
  from: Readonly<Record<Store, string>>;
  /** Each field of the result, from the column that holds it. */
  select: { readonly [K in keyof T]: Column };
  /** With `{name:Type}` placeholders, bound from the values given. */
  where?: string;
  /** Of the result's own field names, or the table's columns. */
  order?: string;
  /** Bound as `{limit:UInt32}`. */
  limit?: true;
}

function read<T>(spec: Read<T>): Read<T> {
  return spec;
}

export const READS = {
  totals: read<Totals>({
    from: { d1: 'agg_totals', clickhouse: 'mv_totals' },
    select: {
      repositories: count('repositories'),
      dependencies: count('dependencies'),
      packages: count('packages'),
      classified: count('classified'),
      tracked: count('tracked'),
    },
  }),
  languageCoverage: read<LanguageCoverage>({
    from: { d1: 'agg_language_coverage', clickhouse: 'mv_language_coverage' },
    select: {
      language: text('language'),
      repositories: count('repositories'),
      withSbom: count('with_sbom'),
      withSyft: count('with_syft'),
      withDepgraph: count('with_depgraph'),
      withManifest: count('with_manifest'),
    },
    order: 'repositories DESC, language',
  }),
  ecosystemCoverage: read<EcosystemCoverage>({
    from: { d1: 'agg_ecosystem_coverage', clickhouse: 'mv_ecosystem_coverage' },
    select: {
      ecosystem: text('ecosystem'),
      repositories: count('repositories'),
      withAny: count('with_any'),
      withSyft: count('with_syft'),
      withDepgraph: count('with_depgraph'),
      withManifest: count('with_manifest'),
    },
    order: 'repositories DESC, ecosystem',
  }),
  /**
   * A precomputed rank window: the panel has exactly two controls, so
   * the answers are finite and were enumerated when the table was
   * written. The empty ecosystem is the whole corpus, in both stores,
   * counting each repository once however many ecosystems it has.
   */
  topPackages: read<PackagePopularity>({
    from: { d1: 'agg_top_packages', clickhouse: 'mv_top_packages' },
    select: {
      name: text('name'),
      repositoryCount: count({ d1: 'repository_count', clickhouse: 'repositories' }),
      directCount: count({ d1: 'direct_count', clickhouse: 'direct_repositories' }),
    },
    where:
      'direct_only = {direct:UInt8} AND ecosystem = {ecosystem:String}'
      + ' AND rank <= {limit:UInt32}',
    order: 'rank',
  }),
  /**
   * Ordered by the stored position: the labels are not ordinal, so
   * sorting by them would put '1000+' between '10-24' and '100-249'.
   */
  dependencyDistribution: read<DependencyBucket>({
    from: { d1: 'agg_dependency_buckets', clickhouse: 'mv_dependency_buckets' },
    select: {
      label: text('bucket'),
      repositories: count('repositories'),
    },
    order: 'position',
  }),
  /**
   * The empty ecosystem is a record with no type, not an ecosystem.
   * ClickHouse keeps its row; the export never writes one.
   */
  sourceComparison: read<SourceComparison>({
    from: { d1: 'agg_source_comparison', clickhouse: 'mv_ecosystem_totals' },
    select: {
      ecosystem: text('ecosystem'),
      syft: count({ d1: 'syft', clickhouse: 'syft_records' }),
      depgraph: count({ d1: 'depgraph', clickhouse: 'depgraph_records' }),
      manifest: count({ d1: 'manifest', clickhouse: 'manifest_records' }),
    },
    where: "ecosystem <> ''",
    order: 'syft + depgraph + manifest DESC, ecosystem',
  }),
  /**
   * Unknown is a row like any other. "We do not know" is a finding
   * about SBOM quality — 14,947 repositories are in that row — and
   * filtering it out would overstate coverage. Tied licences by name,
   * so which of two makes the cut does not depend on the store.
   */
  licenseShares: read<LicenseShare>({
    from: { d1: 'licenses', clickhouse: 'mv_licenses' },
    select: {
      license: text('license'),
      repositoryCount: count({ d1: 'repository_count', clickhouse: 'repositories' }),
      packageCount: count({ d1: 'package_count', clickhouse: 'packages' }),
    },
    order: 'repositoryCount DESC, license',
    limit: true,
  }),
  /**
   * The monthly series for one package, per source: every observation,
   * history included, which is what a series over time is for.
   */
  adoptionOverTime: read<AdoptionPoint>({
    from: { d1: 'history', clickhouse: 'mv_package_month' },
    select: {
      source: text('source'),
      month: text('month'),
      repositoryCount: count({ d1: 'repository_count', clickhouse: 'repositories' }),
      directCount: count({ d1: 'direct_count', clickhouse: 'direct_repositories' }),
    },
    where: 'name = {name:String}',
    order: 'source, month',
  }),
} as const;

/** The statement that reads `spec` in `store`. */
export function statement<T>(spec: Read<T>, store: Store): string {
  const columns = Object.entries<Column>(spec.select).map(([field, { column }]) => {
    const name = typeof column === 'string' ? column : column[store];
    return name === field ? name : `${name} AS ${field}`;
  });
  return [
    `SELECT ${columns.join(', ')}`,
    `FROM ${spec.from[store]}`,
    ...(spec.where ? [`WHERE ${spec.where}`] : []),
    ...(spec.order ? [`ORDER BY ${spec.order}`] : []),
    ...(spec.limit ? ['LIMIT {limit:UInt32}'] : []),
  ].join('\n');
}

/** A row of `spec`'s result: its counts as numbers, its text as text. */
export function shapeRead<T>(spec: Read<T>, row: Row): T {
  const shaped: Record<string, number | string> = {};
  for (const [field, column] of Object.entries<Column>(spec.select)) {
    shaped[field] = column.count ? num(row[field]) : String(row[field] ?? '');
  }
  return shaped as T;
}

const PLACEHOLDER = /\{(\w+):\w+\}/g;

/**
 * The D1 dialect: each `{name:Type}` becomes a `?`, its value bound in
 * the order the placeholders appear. A placeholder with no value is a
 * mistake in this file, and fails before anything is sent.
 */
export function positional(
  sql: string,
  values: Readonly<Record<string, Value>>,
): { sql: string; params: Value[] } {
  const params: Value[] = [];
  const text = sql.replace(PLACEHOLDER, (_, name: string) => {
    const value = values[name];
    if (value === undefined) {
      throw new Error(`no value for the placeholder {${name}}`);
    }
    params.push(value);
    return '?';
  });
  return { sql: text, params };
}
