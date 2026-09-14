/**
 * The dashboard's questions, answered by ClickHouse.
 *
 * This is the second implementation of `DatasetQueries`, and it is the
 * reason that interface sits at the method level rather than exposing
 * `query(sql)`. The two stores do not want the same SQL in different
 * dialects; they want different strategies, and the measurements say
 * so.
 *
 * **Point lookups read the fact table directly.** `artifacts` is sorted
 * by `name`, so the sparse index answers `WHERE name = ?` by reading
 * 40,000–150,000 rows of 19,361,638 — 1.8 to 7.0 ms. The D1 backend
 * cannot do this at all: it stores integer references because the
 * strings cost 762.6 MB in SQLite, so every lookup joins through
 * `packages`. Here the names are `LowCardinality` where it helps and
 * plain where it does not, and there is nothing to join.
 *
 * **The overview reads rollups, not `agg_*` tables.** Same idea,
 * different mechanism: refreshable materialized views maintained by the
 * server (`chatsbom/core/rollups.py`) rather than tables written by an
 * export. The numbers are verified against the base tables — 16
 * comparisons, including the 2,433-row dependency histogram.
 *
 * Ranges here are one place a reader can check the claim: every SQL
 * string below binds its values as `{name:Type}` parameters. None
 * interpolates. The page is public.
 */
import type { DatasetQueries } from '../backend';
import type {
  AdoptionPoint,
  DatasetMeta,
  DependencyBucket,
  DependencyTree,
  Dependent,
  DependentQuery,
  EcosystemShare,
  LanguageCoverage,
  LicenseShare,
  PackageEdge,
  PackageMatch,
  PackagePopularity,
  RelationshipSplit,
  SourceComparison,
  Totals,
  VersionShare,
} from '../d1/queries';
import { type Relationship, RELATIONSHIPS } from '../schema';
import type { ClickHouse, Param } from './client';

const DEFAULT_LIMIT = 50;
const MAX_LIMIT = 500;

/** See the note on the same constants in `d1/queries.ts`. */
const TREE_CHILDREN = 14;
const TREE_CHILDREN_MAX = 30;
const TREE_BRANCH = 4;
const TREE_BRANCH_MAX = 12;

function boundedLimit(limit: number | undefined): number {
  if (limit === undefined) return DEFAULT_LIMIT;
  if (!Number.isFinite(limit) || limit < 1) return DEFAULT_LIMIT;
  return Math.min(Math.floor(limit), MAX_LIMIT);
}

function isRelationship(value: string): value is Relationship {
  return (RELATIONSHIPS as readonly string[]).includes(value);
}

/**
 * `WHERE` fragments for "depends on this package", shared by the rows
 * and the count.
 *
 * Shared for the same reason as in the D1 backend: a count computed
 * over different filters than the rows beside it is worse than no
 * count, because it looks authoritative and disagrees with what the
 * reader can see.
 */
function dependentFilters(query: DependentQuery): {
  where: string[];
  params: Record<string, Param>;
} {
  const where = ['a.name = {name:String}'];
  const params: Record<string, Param> = { name: query.name };

  if (query.type) {
    where.push('a.type = {type:String}');
    params['type'] = query.type;
  }
  if (query.language) {
    // Lowercased on both sides: `repositories.language` is capitalised
    // as GitHub spells it and the filter sends lowercase.
    where.push('lower(r.language) = {language:String}');
    params['language'] = query.language.toLowerCase();
  }
  if (query.directOnly) {
    where.push("a.relationship = 'direct'");
  }
  return { where, params };
}

/**
 * The ClickHouse implementation.
 *
 * `implements DatasetQueries` is what makes the seam real rather than
 * aspirational: the interface was written before this existed, and the
 * compiler is what checks that it was written correctly.
 */
export class ClickHouseDataset implements DatasetQueries {
  constructor(private readonly db: ClickHouse) {}

  /* ---------------- point lookups: the fact table ------------------ */

  async dependentsOf(query: DependentQuery): Promise<Dependent[]> {
    const { where, params } = dependentFilters(query);
    params['limit'] = boundedLimit(query.limit);

    const rows = await this.db.rows<{
      owner: string;
      repo: string;
      stars: number;
      version: string;
      url: string;
      relationship: string;
      observed_at: string;
    }>(
      `SELECT r.owner AS owner, r.repo AS repo, r.stars AS stars,
              a.version AS version, r.url AS url,
              a.relationship AS relationship,
              formatDateTime(a.observed_at, '%Y-%m-%d') AS observed_at
       FROM artifacts AS a
       INNER JOIN repositories AS r ON r.id = a.repository_id
       WHERE ${where.join(' AND ')}
       ORDER BY r.stars DESC, r.owner, r.repo
       LIMIT {limit:UInt32}`,
      params,
    );

    return rows.map((row) => ({
      owner: row.owner,
      repo: row.repo,
      stars: Number(row.stars),
      version: row.version,
      url: row.url,
      relationship: isRelationship(row.relationship)
        ? row.relationship
        : 'unknown',
      observedAt: row.observed_at ?? '',
    }));
  }

  /**
   * How many repositories depend on a package, unlimited.
   *
   * `uniqExact`, not `uniq`: this number is printed as "198
   * dependants", and HyperLogLog's half-percent would make that a
   * number nobody can reconcile with the rows below it.
   */
  async countDependents(query: DependentQuery): Promise<number> {
    const { where, params } = dependentFilters(query);
    const row = await this.db.row<{ total: string | number }>(
      `SELECT uniqExact(a.repository_id) AS total
       FROM artifacts AS a
       INNER JOIN repositories AS r ON r.id = a.repository_id
       WHERE ${where.join(' AND ')}`,
      params,
    );
    return Number(row?.total ?? 0);
  }

  async ecosystemsFor(name: string): Promise<EcosystemShare[]> {
    const rows = await this.db.rows<{
      type: string;
      repository_count: string | number;
      direct_count: string | number;
    }>(
      `SELECT type AS type,
              uniqExact(repository_id) AS repository_count,
              uniqExactIf(repository_id, relationship = 'direct')
                  AS direct_count
       FROM artifacts
       WHERE name = {name:String}
       GROUP BY type
       ORDER BY repository_count DESC`,
      { name },
    );
    return rows.map((row) => ({
      type: row.type,
      repositoryCount: Number(row.repository_count),
      directCount: Number(row.direct_count),
    }));
  }

  async versionSpread(name: string, limit = 10): Promise<VersionShare[]> {
    const rows = await this.db.rows<{
      version: string;
      repository_count: string | number;
    }>(
      `SELECT version AS version,
              uniqExact(repository_id) AS repository_count
       FROM artifacts
       WHERE name = {name:String}
       GROUP BY version
       ORDER BY repository_count DESC, version
       LIMIT {limit:UInt32}`,
      { name, limit: boundedLimit(limit) },
    );
    return rows.map((row) => ({
      version: row.version,
      repositoryCount: Number(row.repository_count),
    }));
  }

  /**
   * The monthly series for one package.
   *
   * Computed from `observed_at` rather than read from a `history`
   * table, which the D1 export has to build because SQLite cannot
   * afford this grouping. Here it is 3 ms.
   *
   * `observed_at` now records when the document was *collected* — the
   * dependency graph's own `creationInfo.created`, or the SBOM file's
   * mtime — so these months describe observations rather than the last
   * time someone ran an indexer.
   */
  async adoptionOverTime(name: string): Promise<AdoptionPoint[]> {
    const rows = await this.db.rows<{
      month: string;
      repository_count: string | number;
      direct_count: string | number;
    }>(
      `SELECT formatDateTime(observed_at, '%Y-%m') AS month,
              uniqExact(repository_id) AS repository_count,
              uniqExactIf(repository_id, relationship = 'direct')
                  AS direct_count
       FROM artifacts
       WHERE name = {name:String}
       GROUP BY month
       ORDER BY month`,
      { name },
    );
    return rows.map((row) => ({
      month: row.month,
      repositoryCount: Number(row.repository_count),
      directCount: Number(row.direct_count),
    }));
  }

  /**
   * Package names beginning with a term, ranked by popularity.
   *
   * `startsWith` rather than `LIKE 'term%'`, so there is no pattern for
   * a reader's `%` or `_` to become a wildcard in — the escaping the
   * D1 backend needs has no equivalent problem here.
   *
   * Ranked by repository count, which is the whole point: alphabetical
   * order buries `laravel/framework` under forty `laravel-enso/*`
   * packages with one dependant each. Unlike D1, this needs no stored
   * count — the rollup is already keyed by name.
   */
  async searchPackages(term: string, limit = 20): Promise<PackageMatch[]> {
    if (!term) return [];
    const rows = await this.db.rows<{
      name: string;
      repository_count: string | number;
    }>(
      `SELECT name, sum(repositories) AS repository_count
       FROM mv_package_language
       WHERE startsWith(name, {term:String})
       GROUP BY name
       ORDER BY repository_count DESC, name
       LIMIT {limit:UInt32}`,
      { term, limit: boundedLimit(limit) },
    );
    return rows.map((row) => ({
      name: row.name,
      repositoryCount: Number(row.repository_count),
    }));
  }

  /* ---------------- the edge table, both directions ---------------- */

  async dependenciesOf(name: string, limit = 20): Promise<PackageEdge[]> {
    const rows = await this.db.rows<{
      name: string;
      repositories: string | number;
    }>(
      `SELECT child AS name, sum(repositories) AS repositories
       FROM edges
       WHERE parent = {name:String}
       GROUP BY child
       ORDER BY repositories DESC, child
       LIMIT {limit:UInt32}`,
      { name, limit: boundedLimit(limit) },
    );
    return rows.map((row) => ({
      name: row.name,
      repositories: Number(row.repositories),
    }));
  }

  /**
   * What pulls a package in.
   *
   * `edges` is `ORDER BY (child, parent)` — child first — because this
   * is the more useful direction and it gets the primary-key prefix:
   * 2.6 ms reading 24,576 of 614,221 rows, against 4.8 ms and a full
   * scan for the forward direction. `sum()` because the table is a
   * SummingMergeTree and a pair may sit in more than one unmerged part.
   */
  async pulledInBy(name: string, limit = 20): Promise<PackageEdge[]> {
    const rows = await this.db.rows<{
      name: string;
      repositories: string | number;
    }>(
      `SELECT parent AS name, sum(repositories) AS repositories
       FROM edges
       WHERE child = {name:String}
       GROUP BY parent
       ORDER BY repositories DESC, parent
       LIMIT {limit:UInt32}`,
      { name, limit: boundedLimit(limit) },
    );
    return rows.map((row) => ({
      name: row.name,
      repositories: Number(row.repositories),
    }));
  }

  /**
   * Two hops, bounded at both.
   *
   * One statement rather than the D1 backend's two: ClickHouse will
   * take the first hop as a subquery in the `IN`, and the window
   * function partitions the second hop per parent so a parent whose
   * widest edge points back at the root does not lose a slot.
   *
   * The root is excluded *inside* the window's own SELECT, for the
   * reason the D1 version records: the edges genuinely run both ways —
   * `bytes -> body-parser` in one repository as well as
   * `body-parser -> bytes` in 3,589 — and filtered outside, the row is
   * dropped but its rank is spent.
   */
  async dependencyTree(
    name: string,
    options: { children?: number; branch?: number } = {},
  ): Promise<DependencyTree> {
    const children = await this.dependenciesOf(
      name,
      Math.min(options.children ?? TREE_CHILDREN, TREE_CHILDREN_MAX),
    );
    if (children.length === 0) {
      return { root: name, children: [], grandchildren: [] };
    }

    const branch = Math.min(
      Math.max(Math.floor(options.branch ?? TREE_BRANCH), 1),
      TREE_BRANCH_MAX,
    );
    const rows = await this.db.rows<{
      parent: string;
      child: string;
      repositories: string | number;
    }>(
      // The first hop is re-derived as a subquery rather than passed
      // back as an `Array(String)` parameter. An array parameter is
      // encoded as a bracketed literal in the query string, which means
      // hand-escaping quotes — and a package really can be called
      // `o'reilly`. A subquery has nothing to escape, and the extra
      // work is a second index lookup on a 614,221-row table.
      `SELECT parent, child, repositories
       FROM (
         SELECT parent, child, repositories,
                row_number() OVER (
                  PARTITION BY parent
                  ORDER BY repositories DESC, child
                ) AS branch_rank
         FROM (
           SELECT parent, child, sum(repositories) AS repositories
           FROM edges
           WHERE parent IN (
                   SELECT child FROM edges
                   WHERE parent = {root:String}
                   GROUP BY child
                   ORDER BY sum(repositories) DESC, child
                   LIMIT {children:UInt32}
                 )
             AND child != {root:String}
           GROUP BY parent, child
         )
       )
       WHERE branch_rank <= {branch:UInt32}
       ORDER BY repositories DESC, child`,
      { root: name, children: children.length, branch },
    );

    return {
      root: name,
      children,
      grandchildren: rows.map((row) => ({
        parent: row.parent,
        child: row.child,
        repositories: Number(row.repositories),
      })),
    };
  }

  /* ---------------- the overview: read the rollups ----------------- */

  async totals(): Promise<Totals> {
    const row = await this.db.row<{
      repositories: string | number;
      dependencies: string | number;
      packages: string | number;
      classified: string | number;
    }>(
      `SELECT repositories, dependencies, packages, classified
       FROM mv_totals`,
    );
    return {
      repositories: Number(row?.repositories ?? 0),
      dependencies: Number(row?.dependencies ?? 0),
      packages: Number(row?.packages ?? 0),
      classified: Number(row?.classified ?? 0),
    };
  }

  async relationshipSplit(language?: string): Promise<RelationshipSplit> {
    const filter = language ? 'WHERE language = {language:String}' : '';
    const params: Record<string, Param> = language
      ? { language: language.toLowerCase() }
      : {};
    const row = await this.db.row<{
      direct: string | number;
      transitive: string | number;
      unknown: string | number;
    }>(
      `SELECT sum(direct_records) AS direct,
              sum(transitive_records) AS transitive,
              sum(unknown_records) AS unknown
       FROM mv_package_language ${filter}`,
      params,
    );
    return {
      direct: Number(row?.direct ?? 0),
      transitive: Number(row?.transitive ?? 0),
      unknown: Number(row?.unknown ?? 0),
    };
  }

  async languageCoverage(): Promise<LanguageCoverage[]> {
    const rows = await this.db.rows<{
      language: string;
      repositories: string | number;
      with_sbom: string | number;
    }>(
      // The repository count comes from `repositories` because it must
      // include the 3,736 with no dependencies at all — they are the
      // finding this panel exists to show, and a rollup built from
      // `artifacts` cannot contain them.
      `SELECT lower(r.language) AS language,
              count() AS repositories,
              countIf(d.repository_id != 0) AS with_sbom
       FROM repositories AS r
       LEFT JOIN mv_repository_deps AS d ON d.repository_id = r.id
       GROUP BY language
       ORDER BY repositories DESC, language`,
    );
    return rows.map((row) => ({
      language: row.language,
      repositories: Number(row.repositories),
      withSbom: Number(row.with_sbom),
    }));
  }

  async topPackages(options: {
    directOnly?: boolean;
    language?: string;
    limit?: number;
  }): Promise<PackagePopularity[]> {
    const rows = await this.db.rows<{
      name: string;
      repository_count: string | number;
      direct_count: string | number;
    }>(
      `SELECT name,
              repositories AS repository_count,
              direct_repositories AS direct_count
       FROM mv_top_packages
       WHERE language = {language:String}
         AND direct_only = {direct:UInt8}
         AND rank <= {limit:UInt32}
       ORDER BY rank`,
      {
        // The empty string is the whole-corpus row, the same convention
        // the D1 aggregates use.
        language: options.language ? options.language.toLowerCase() : '',
        direct: options.directOnly ? 1 : 0,
        limit: boundedLimit(options.limit),
      },
    );
    return rows.map((row) => ({
      name: row.name,
      repositoryCount: Number(row.repository_count),
      directCount: Number(row.direct_count),
    }));
  }

  /**
   * Repositories per dependency-count bucket.
   *
   * Bucketed here rather than in the rollup: the rollup stores one row
   * per repository, so the boundaries stay a presentation decision and
   * changing them needs no refresh.
   */
  async dependencyDistribution(): Promise<DependencyBucket[]> {
    const rows = await this.db.rows<{
      bucket: string;
      position: number;
      repositories: string | number;
    }>(
      `SELECT bucket, min(position) AS position, sum(n) AS repositories
       FROM (
         SELECT multiIf(packages < 10, '1-9',
                        packages < 25, '10-24',
                        packages < 100, '25-99',
                        packages < 250, '100-249',
                        packages < 1000, '250-999',
                        '1000+') AS bucket,
                multiIf(packages < 10, 0,
                        packages < 25, 1,
                        packages < 100, 2,
                        packages < 250, 3,
                        packages < 1000, 4,
                        5) AS position,
                1 AS n
         FROM mv_repository_deps
       )
       GROUP BY bucket
       ORDER BY position`,
    );
    return rows.map((row) => ({
      label: row.bucket,
      repositories: Number(row.repositories),
    }));
  }

  async sourceComparison(): Promise<SourceComparison[]> {
    const rows = await this.db.rows<{
      language: string;
      syft: string | number;
      depgraph: string | number;
    }>(
      `SELECT language,
              sum(syft_records) AS syft,
              sum(depgraph_records) AS depgraph
       FROM mv_package_language
       GROUP BY language
       ORDER BY syft + depgraph DESC`,
    );
    return rows.map((row) => ({
      language: row.language,
      syft: Number(row.syft),
      depgraph: Number(row.depgraph),
    }));
  }

  async licenseShares(limit = 12): Promise<LicenseShare[]> {
    const rows = await this.db.rows<{
      license: string;
      repositories: string | number;
      packages: string | number;
    }>(
      `SELECT license, repositories, packages
       FROM mv_licenses
       ORDER BY repositories DESC
       LIMIT {limit:UInt32}`,
      { limit: boundedLimit(limit) },
    );
    return rows.map((row) => ({
      license: row.license,
      repositoryCount: Number(row.repositories),
      packageCount: Number(row.packages),
    }));
  }

  /**
   * Which build produced the data and how fresh it is.
   *
   * The observation span comes from the rows themselves rather than a
   * clock, so it describes the data's age. The generator string is the
   * one thing ClickHouse cannot know — it is the pipeline's version,
   * not the database's — so it is configured.
   */
  async meta(): Promise<DatasetMeta> {
    const row = await this.db.row<{ observed_from: string; observed_to: string }>(
      `SELECT formatDateTime(min(observed_at), '%Y-%m-%d') AS observed_from,
              formatDateTime(max(observed_at), '%Y-%m-%d') AS observed_to
       FROM artifacts`,
    );
    return {
      generator: this.generator,
      schemaVersion: 'clickhouse',
      observedFrom: row?.observed_from ?? '',
      observedTo: row?.observed_to ?? '',
    };
  }

  /** Set by the endpoint from configuration; see `meta`. */
  generator = 'chatsbom/clickhouse';
}
