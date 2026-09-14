/**
 * The query layer, against the normalised D1 schema, running in the
 * Worker.
 *
 * Two things about this file are load-bearing.
 *
 * **The joins are not optional.** Artifact rows carry integer
 * references — `package_id`, `version_id`, `kind_id` — because storing
 * the strings cost 762.6 MB against D1's 500 MB free tier, and
 * interning them brought that to 294.7 MB. So a package lookup joins
 * through `packages`; comparing a name on the fact table is not a
 * slower version of that, it is a column that no longer exists.
 *
 * **The aggregates are read, never recomputed.** The overview's panels
 * measured 3,122 ms and 1,082 ms when aggregated live, because they read
 * every one of 6,062,896 artifact rows by definition — no index helps
 * that, and on D1 it is the bill as well as the latency. They are
 * precomputed at export time into `agg_*` tables; a query here that
 * aggregates them again would put the whole cost straight back.
 */
import type { DatasetQueries } from '../backend';
import { type Relationship, RELATIONSHIPS } from '../schema';

/** The narrow slice of D1 this layer needs, so it is testable. */
export interface D1Queryable {
  all<T>(sql: string, params?: unknown[]): Promise<T[]>;
}

const DEFAULT_LIMIT = 50;
const MAX_LIMIT = 500;

function boundedLimit(limit: number | undefined): number {
  if (limit === undefined) return DEFAULT_LIMIT;
  if (!Number.isFinite(limit) || limit < 1) return DEFAULT_LIMIT;
  return Math.min(Math.floor(limit), MAX_LIMIT);
}

function isRelationship(value: string): value is Relationship {
  return (RELATIONSHIPS as readonly string[]).includes(value);
}

export interface DependentQuery {
  name: string;
  /**
   * Ecosystem to scope to.
   *
   * A package name is not unique across ecosystems: `mail` is a Ruby
   * gem with 118 dependants and a Maven artifactId with 6. Counting
   * them together reports 124 dependants of something that does not
   * exist.
   */
  type?: string;
  language?: string;
  directOnly?: boolean;
  limit?: number;
}

export interface Dependent {
  owner: string;
  repo: string;
  stars: number;
  version: string;
  url: string;
  relationship: Relationship;
  /** When this pipeline last recorded the repository's dependencies. */
  observedAt: string;
}

/**
 * The predicates that define "depends on this package".
 *
 * Shared by the row query and the count so the two cannot diverge: a
 * count computed over different filters than the rows it accompanies is
 * worse than no count, because it looks authoritative and disagrees
 * with what the reader can see.
 */
function dependentFilters(query: DependentQuery): {
  filters: string[];
  params: unknown[];
} {
  const filters = ['p.name = ?'];
  const params: unknown[] = [query.name];

  if (query.type) {
    filters.push('k.type = ?');
    params.push(query.type);
  }
  if (query.language) {
    filters.push('r.language = ?');
    params.push(query.language.toLowerCase());
  }
  if (query.directOnly) {
    filters.push('k.relationship = ?');
    params.push('direct');
  }
  return { filters, params };
}

export interface RelationshipSplit {
  direct: number;
  transitive: number;
  unknown: number;
}

export interface Totals {
  repositories: number;
  dependencies: number;
  packages: number;
  classified: number;
}

export interface LanguageCoverage {
  language: string;
  repositories: number;
  withSbom: number;
}

export interface PackagePopularity {
  name: string;
  repositoryCount: number;
  directCount: number;
}

export interface DependencyBucket {
  label: string;
  repositories: number;
}

export interface PackageMatch {
  name: string;
  repositoryCount: number;
}

export interface LicenseShare {
  license: string;
  repositoryCount: number;
  packageCount: number;
}

export interface AdoptionPoint {
  month: string;
  repositoryCount: number;
  directCount: number;
}

export interface VersionShare {
  version: string;
  repositoryCount: number;
}

export interface EcosystemShare {
  type: string;
  repositoryCount: number;
  directCount: number;
}

export interface DatasetMeta {
  generator: string;
  schemaVersion: string;
  observedFrom: string;
  observedTo: string;
}

export interface SourceComparison {
  language: string;
  syft: number;
  depgraph: number;
}

/**
 * The D1 implementation.
 *
 * `implements DatasetQueries` is load-bearing: it is what makes a second
 * store a compile-time exercise rather than an archaeology exercise.
 */
export class D1Dataset implements DatasetQueries {
  constructor(private readonly db: D1Queryable) {}

  /** Repositories depending on a package, most starred first. */
  async dependentsOf(query: DependentQuery): Promise<Dependent[]> {
    const { filters, params } = dependentFilters(query);
    params.push(boundedLimit(query.limit));

    const rows = await this.db.all<{
      owner: string;
      repo: string;
      stars: number;
      version: string;
      url: string;
      relationship: string;
      observed_at: string;
    }>(
      `SELECT r.owner, r.repo, r.stars, v.version, r.url,
              k.relationship, r.observed_at
       FROM artifacts AS a
       JOIN packages AS p ON p.id = a.package_id
       JOIN versions AS v ON v.id = a.version_id
       JOIN kinds AS k ON k.id = a.kind_id
       JOIN repositories AS r ON r.id = a.repository_id
       WHERE ${filters.join(' AND ')}
       ORDER BY r.stars DESC, r.owner, r.repo
       LIMIT ?`,
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
   * `dependentsOf` is capped, so the length of its result is a display
   * limit rather than a count: at the cap, "100 dependants" reports a
   * truncation as a finding.
   */
  async countDependents(query: DependentQuery): Promise<number> {
    const { filters, params } = dependentFilters(query);
    const rows = await this.db.all<{ total: number }>(
      `SELECT count(DISTINCT a.repository_id) AS total
       FROM artifacts AS a
       JOIN packages AS p ON p.id = a.package_id
       JOIN kinds AS k ON k.id = a.kind_id
       JOIN repositories AS r ON r.id = a.repository_id
       WHERE ${filters.join(' AND ')}`,
      params,
    );
    return Number(rows[0]?.total ?? 0);
  }

  /* ---------------- precomputed: read, never recompute ------------- */

  async relationshipSplit(language?: string): Promise<RelationshipSplit> {
    const rows = await this.db.all<{ relationship: string; records: number }>(
      `SELECT relationship, records
       FROM agg_relationship_split
       WHERE language = ?`,
      [language ? language.toLowerCase() : ''],
    );

    const split: RelationshipSplit = { direct: 0, transitive: 0, unknown: 0 };
    for (const row of rows) {
      if (isRelationship(row.relationship)) {
        split[row.relationship] = Number(row.records);
      }
    }
    return split;
  }

  async totals(): Promise<Totals> {
    const rows = await this.db.all<Totals>(
      `SELECT repositories, dependencies, packages, classified
       FROM agg_totals`,
    );
    const row = rows[0];
    return {
      repositories: Number(row?.repositories ?? 0),
      dependencies: Number(row?.dependencies ?? 0),
      packages: Number(row?.packages ?? 0),
      classified: Number(row?.classified ?? 0),
    };
  }

  async languageCoverage(): Promise<LanguageCoverage[]> {
    const rows = await this.db.all<{
      language: string;
      repositories: number;
      with_sbom: number;
    }>(
      `SELECT language, repositories, with_sbom
       FROM agg_language_coverage
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
    const rows = await this.db.all<{
      name: string;
      repository_count: number;
      direct_count: number;
    }>(
      `SELECT name, repository_count, direct_count
       FROM agg_top_packages
       WHERE direct_only = ? AND language = ? AND rank <= ?
       ORDER BY rank`,
      [
        options.directOnly ? 1 : 0,
        options.language ? options.language.toLowerCase() : '',
        boundedLimit(options.limit),
      ],
    );
    return rows.map((row) => ({
      name: row.name,
      repositoryCount: Number(row.repository_count),
      directCount: Number(row.direct_count),
    }));
  }

  async dependencyDistribution(): Promise<DependencyBucket[]> {
    const rows = await this.db.all<{ bucket: string; repositories: number }>(
      // Ordered by the stored position: the labels are not ordinal, so
      // sorting by them would put '1000+' between '10-24' and '100-249'.
      `SELECT bucket, repositories
       FROM agg_dependency_buckets
       ORDER BY position`,
    );
    return rows.map((row) => ({
      label: row.bucket,
      repositories: Number(row.repositories),
    }));
  }

  /**
   * Provenance: which build produced the data, and how fresh it is.
   *
   * The Parquet path answers this with a manifest, checksums included.
   * D1 has no files, so there is no checksum analogue — but the build,
   * the contract version and the observation span do carry over, and
   * those are what explain a number that looks wrong.
   */
  async meta(): Promise<DatasetMeta> {
    const rows = await this.db.all<{
      generator: string;
      schema_version: string;
      observed_from: string;
      observed_to: string;
    }>(
      `SELECT generator, schema_version, observed_from, observed_to
       FROM meta`,
    );
    const row = rows[0];
    return {
      generator: row?.generator ?? '',
      schemaVersion: row?.schema_version ?? '',
      observedFrom: row?.observed_from ?? '',
      observedTo: row?.observed_to ?? '',
    };
  }

  /**
   * Package names beginning with a term, for the search box.
   *
   * Searches `packages` (141,938 rows) rather than `artifacts`
   * (6,062,896), and anchors the pattern at the start: a leading
   * wildcard cannot use an index, so `%mail%` would scan every name
   * while `mail%` is a range lookup on `idx_packages_name`.
   *
   * The term is escaped before it reaches LIKE. Without that, a `%` or
   * `_` a reader typed becomes a wildcard and the search quietly
   * matches far more than they asked for.
   */
  async searchPackages(term: string, limit = 20): Promise<PackageMatch[]> {
    if (!term) return [];

    const escaped = term.replace(/[\\%_]/g, (c) => `\\${c}`);
    const rows = await this.db.all<{
      name: string;
      repository_count: number;
    }>(
      `SELECT p.name AS name,
              (SELECT count(DISTINCT a.repository_id)
               FROM artifacts AS a
               WHERE a.package_id = p.id) AS repository_count
       FROM packages AS p
       WHERE p.name LIKE ? ESCAPE '\\'
       ORDER BY p.name
       LIMIT ?`,
      [`${escaped}%`, boundedLimit(limit)],
    );
    return rows.map((row) => ({
      name: row.name,
      repositoryCount: Number(row.repository_count),
    }));
  }

  /**
   * Licence shares, precomputed by the export.
   *
   * Unknown is a row like any other. "We do not know" is a finding
   * about SBOM quality — 14,947 repositories are in that row — and
   * filtering it out would overstate coverage.
   */
  async licenseShares(limit = 12): Promise<LicenseShare[]> {
    const rows = await this.db.all<{
      license: string;
      repository_count: number;
      package_count: number;
    }>(
      `SELECT license, repository_count, package_count
       FROM licenses
       ORDER BY repository_count DESC
       LIMIT ?`,
      [boundedLimit(limit)],
    );
    return rows.map((row) => ({
      license: row.license,
      repositoryCount: Number(row.repository_count),
      packageCount: Number(row.package_count),
    }));
  }

  /** The monthly series for one package. */
  async adoptionOverTime(name: string): Promise<AdoptionPoint[]> {
    const rows = await this.db.all<{
      month: string;
      repository_count: number;
      direct_count: number;
    }>(
      `SELECT month, repository_count, direct_count
       FROM history
       WHERE name = ?
       ORDER BY month`,
      [name],
    );
    return rows.map((row) => ({
      month: row.month,
      repositoryCount: Number(row.repository_count),
      directCount: Number(row.direct_count),
    }));
  }

  /** Which resolved versions of a package are in use. */
  async versionSpread(name: string, limit = 10): Promise<VersionShare[]> {
    const rows = await this.db.all<{
      version: string;
      repository_count: number;
    }>(
      `SELECT v.version AS version,
              count(DISTINCT a.repository_id) AS repository_count
       FROM artifacts AS a
       JOIN packages AS p ON p.id = a.package_id
       JOIN versions AS v ON v.id = a.version_id
       WHERE p.name = ?
       GROUP BY v.version
       ORDER BY repository_count DESC, v.version
       LIMIT ?`,
      [name, boundedLimit(limit)],
    );
    return rows.map((row) => ({
      version: row.version,
      repositoryCount: Number(row.repository_count),
    }));
  }

  /**
   * Which ecosystems a package name appears in.
   *
   * Asked before any count is presented as "dependants of X", because a
   * name shared across ecosystems is two different packages: `mail` is
   * a Ruby gem with 118 dependants and a Maven artifactId with 6.
   */
  async ecosystemsFor(name: string): Promise<EcosystemShare[]> {
    const rows = await this.db.all<{
      type: string;
      repository_count: number;
      direct_count: number;
    }>(
      `SELECT k.type AS type,
              count(DISTINCT a.repository_id) AS repository_count,
              count(DISTINCT CASE WHEN k.relationship = 'direct'
                                  THEN a.repository_id END) AS direct_count
       FROM artifacts AS a
       JOIN packages AS p ON p.id = a.package_id
       JOIN kinds AS k ON k.id = a.kind_id
       WHERE p.name = ?
       GROUP BY k.type
       ORDER BY repository_count DESC`,
      [name],
    );
    return rows.map((row) => ({
      type: row.type,
      repositoryCount: Number(row.repository_count),
      directCount: Number(row.direct_count),
    }));
  }

  async sourceComparison(): Promise<SourceComparison[]> {
    const rows = await this.db.all<{
      language: string;
      syft: number;
      depgraph: number;
    }>(
      `SELECT language, syft, depgraph
       FROM agg_source_comparison
       ORDER BY syft + depgraph DESC`,
    );
    return rows.map((row) => ({
      language: row.language,
      syft: Number(row.syft),
      depgraph: Number(row.depgraph),
    }));
  }
}
