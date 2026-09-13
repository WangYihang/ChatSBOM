/**
 * The dashboard's query surface.
 *
 * Every question the UI can ask is a typed function here, and the SQL is
 * built from the generated column names. Nothing takes raw SQL from the
 * caller: when the AI chat is added, it gets these functions as tools
 * rather than a SQL string, so a prompt cannot become a query plan.
 */
import type { ArtifactRow, Relationship, RepositoryRow } from './schema';
import { DATA_FILES, RELATIONSHIPS } from './schema';

/** Minimal surface of a DuckDB-WASM connection, so this is testable. */
export interface Queryable {
  query<T>(sql: string, params?: unknown[]): Promise<T[]>;
}

export interface Dependent {
  owner: string;
  repo: string;
  stars: number;
  version: string;
  url: string;
  relationship: Relationship;
}

export interface PackagePopularity {
  name: string;
  repositoryCount: number;
  directCount: number;
}

export interface LanguageCoverage {
  language: string;
  repositories: number;
  withSbom: number;
}

export interface RelationshipSplit {
  direct: number;
  transitive: number;
  unknown: number;
}

export interface LicenseShare {
  license: string;
  packageCount: number;
  repositoryCount: number;
}

export interface DependencyBucket {
  label: string;
  repositories: number;
}

export interface SourceComparison {
  language: string;
  syft: number;
  depgraph: number;
}

export interface AdoptionPoint {
  month: string;
  repositoryCount: number;
  directCount: number;
}

export interface Totals {
  repositories: number;
  dependencies: number;
  packages: number;
  classified: number;
}

export interface DependentQuery {
  /** Exact package name, as the ecosystem spells it. */
  name: string;
  language?: string;
  /** Only repositories whose own manifest declares the package. */
  directOnly?: boolean;
  limit?: number;
}

const MAX_LIMIT = 500;
const DEFAULT_LIMIT = 50;

function boundedLimit(limit: number | undefined): number {
  if (limit === undefined) return DEFAULT_LIMIT;
  if (!Number.isFinite(limit) || limit < 1) return DEFAULT_LIMIT;
  return Math.min(Math.floor(limit), MAX_LIMIT);
}

/** Table references, so a renamed file is a compile error here. */
const REPOS = (base: string) => `read_parquet('${base}/${DATA_FILES.repositories}')`;
const ARTIFACTS = (base: string) => `read_parquet('${base}/${DATA_FILES.artifacts}')`;
const LICENSES = (base: string) => `read_parquet('${base}/${DATA_FILES.licenses}')`;
const HISTORY = (base: string) => `read_parquet('${base}/${DATA_FILES.history}')`;

export function isRelationship(value: string): value is Relationship {
  return (RELATIONSHIPS as readonly string[]).includes(value);
}

export class Dataset {
  constructor(
    private readonly db: Queryable,
    /** Base URL the Parquet files are served from, e.g. `/data`. */
    private readonly base: string = '/data',
  ) {}

  /** Repositories depending on a package, most starred first. */
  async dependentsOf(query: DependentQuery): Promise<Dependent[]> {
    const filters = ['a.name = ?'];
    const params: unknown[] = [query.name];

    if (query.language) {
      filters.push('r.language = ?');
      params.push(query.language.toLowerCase());
    }
    if (query.directOnly) {
      filters.push('a.relationship = ?');
      params.push('direct' satisfies Relationship);
    }
    params.push(boundedLimit(query.limit));

    const rows = await this.db.query<{
      owner: string;
      repo: string;
      stars: number;
      version: string;
      url: string;
      relationship: string;
    }>(
      `SELECT r.owner, r.repo, r.stars, a.version, r.url, a.relationship
       FROM ${ARTIFACTS(this.base)} AS a
       JOIN ${REPOS(this.base)} AS r ON a.repository_id = r.id
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
    }));
  }

  /** Package names matching a search fragment, ranked by adoption. */
  async searchPackages(
    fragment: string,
    limit?: number,
  ): Promise<PackagePopularity[]> {
    const rows = await this.db.query<{
      name: string;
      repository_count: number;
      direct_count: number;
    }>(
      `SELECT
         name,
         COUNT(DISTINCT repository_id) AS repository_count,
         COUNT(DISTINCT CASE WHEN relationship = 'direct'
                             THEN repository_id END) AS direct_count
       FROM ${ARTIFACTS(this.base)}
       WHERE name ILIKE ?
       GROUP BY name
       ORDER BY repository_count DESC, name
       LIMIT ?`,
      [`%${fragment}%`, boundedLimit(limit)],
    );
    return rows.map(toPopularity);
  }

  /**
   * Most depended-upon packages.
   *
   * `directOnly` is the interesting mode: the unfiltered ranking is
   * dominated by npm micro-packages that no project asks for by name.
   */
  async topPackages(options: {
    language?: string;
    directOnly?: boolean;
    limit?: number;
  } = {}): Promise<PackagePopularity[]> {
    const filters: string[] = [];
    const params: unknown[] = [];

    if (options.language) {
      filters.push('r.language = ?');
      params.push(options.language.toLowerCase());
    }
    if (options.directOnly) {
      filters.push('a.relationship = ?');
      params.push('direct' satisfies Relationship);
    }
    params.push(boundedLimit(options.limit));

    const where = filters.length ? `WHERE ${filters.join(' AND ')}` : '';
    const rows = await this.db.query<{
      name: string;
      repository_count: number;
      direct_count: number;
    }>(
      `SELECT
         a.name AS name,
         COUNT(DISTINCT a.repository_id) AS repository_count,
         COUNT(DISTINCT CASE WHEN a.relationship = 'direct'
                             THEN a.repository_id END) AS direct_count
       FROM ${ARTIFACTS(this.base)} AS a
       JOIN ${REPOS(this.base)} AS r ON a.repository_id = r.id
       ${where}
       GROUP BY a.name
       ORDER BY repository_count DESC, a.name
       LIMIT ?`,
      params,
    );
    return rows.map(toPopularity);
  }

  /** Per-language repository counts and how many yielded an SBOM. */
  async languageCoverage(): Promise<LanguageCoverage[]> {
    const rows = await this.db.query<{
      language: string;
      repositories: number;
      with_sbom: number;
    }>(
      `SELECT
         language,
         COUNT(*) AS repositories,
         COUNT(CASE WHEN total_dependencies > 0 THEN 1 END) AS with_sbom
       FROM ${REPOS(this.base)}
       GROUP BY language
       ORDER BY repositories DESC, language`,
    );
    return rows.map((row) => ({
      language: row.language,
      repositories: Number(row.repositories),
      withSbom: Number(row.with_sbom),
    }));
  }

  /** Version spread for one package, most used first. */
  async versionSpread(
    name: string,
    limit?: number,
  ): Promise<{ version: string; repositoryCount: number }[]> {
    const rows = await this.db.query<{
      version: string;
      repository_count: number;
    }>(
      `SELECT version, COUNT(DISTINCT repository_id) AS repository_count
       FROM ${ARTIFACTS(this.base)}
       WHERE name = ? AND version <> ''
       GROUP BY version
       ORDER BY repository_count DESC, version DESC
       LIMIT ?`,
      [name, boundedLimit(limit)],
    );
    return rows.map((row) => ({
      version: row.version,
      repositoryCount: Number(row.repository_count),
    }));
  }

  /** Headline figures for the stat row. */
  async totals(): Promise<Totals> {
    const [repos] = await this.db.query<{ n: number; classified: number }>(
      `SELECT COUNT(*) AS n, 0 AS classified FROM ${REPOS(this.base)}`,
    );
    const [deps] = await this.db.query<{
      n: number; packages: number; classified: number;
    }>(
      `SELECT
         COUNT(*) AS n,
         COUNT(DISTINCT name) AS packages,
         COUNT(CASE WHEN relationship <> 'unknown' THEN 1 END) AS classified
       FROM ${ARTIFACTS(this.base)}`,
    );
    return {
      repositories: Number(repos?.n ?? 0),
      dependencies: Number(deps?.n ?? 0),
      packages: Number(deps?.packages ?? 0),
      classified: Number(deps?.classified ?? 0),
    };
  }

  /**
   * How dependencies arrived, across the whole corpus.
   *
   * The headline finding: most rows are transitive, so an unfiltered
   * "most used package" ranking measures lockfile size rather than
   * adoption.
   */
  async relationshipSplit(language?: string): Promise<RelationshipSplit> {
    const params: unknown[] = [];
    let where = '';
    if (language) {
      where = `JOIN ${REPOS(this.base)} AS r ON a.repository_id = r.id
               WHERE r.language = ?`;
      params.push(language.toLowerCase());
    }
    const rows = await this.db.query<{ relationship: string; n: number }>(
      `SELECT a.relationship AS relationship, COUNT(*) AS n
       FROM ${ARTIFACTS(this.base)} AS a ${where}
       GROUP BY a.relationship`,
      params,
    );
    const split: RelationshipSplit = { direct: 0, transitive: 0, unknown: 0 };
    for (const row of rows) {
      if (row.relationship === 'direct') split.direct = Number(row.n);
      else if (row.relationship === 'transitive') split.transitive = Number(row.n);
      else split.unknown += Number(row.n);
    }
    return split;
  }

  /** Licence distribution, unknown included rather than hidden. */
  async licenseShares(limit?: number): Promise<LicenseShare[]> {
    const rows = await this.db.query<{
      license: string; package_count: number; repository_count: number;
    }>(
      `SELECT
         license,
         SUM(package_count) AS package_count,
         SUM(repository_count) AS repository_count
       FROM ${LICENSES(this.base)}
       GROUP BY license
       ORDER BY repository_count DESC
       LIMIT ?`,
      [boundedLimit(limit)],
    );
    return rows.map((row) => ({
      license: row.license || '(unknown)',
      packageCount: Number(row.package_count),
      repositoryCount: Number(row.repository_count),
    }));
  }

  /**
   * How many dependencies repositories have, as ordered buckets.
   *
   * Bucketed rather than plotted raw: the distribution spans three orders
   * of magnitude (a Go module with 80, a TypeScript app with 900), so a
   * linear scatter would be unreadable.
   */
  async dependencyDistribution(): Promise<DependencyBucket[]> {
    const rows = await this.db.query<{ bucket: number; n: number }>(
      `SELECT
         CASE
           WHEN total_dependencies = 0 THEN 0
           WHEN total_dependencies < 10 THEN 1
           WHEN total_dependencies < 25 THEN 2
           WHEN total_dependencies < 50 THEN 3
           WHEN total_dependencies < 100 THEN 4
           WHEN total_dependencies < 250 THEN 5
           WHEN total_dependencies < 500 THEN 6
           WHEN total_dependencies < 1000 THEN 7
           ELSE 8
         END AS bucket,
         COUNT(*) AS n
       FROM ${REPOS(this.base)}
       GROUP BY bucket
       ORDER BY bucket`,
    );
    const labels = [
      'none', '1–9', '10–24', '25–49', '50–99',
      '100–249', '250–499', '500–999', '1000+',
    ];
    const byBucket = new Map(rows.map((r) => [Number(r.bucket), Number(r.n)]));
    return labels.map((label, index) => ({
      label,
      repositories: byBucket.get(index) ?? 0,
    }));
  }

  /**
   * Syft against GitHub's dependency graph, per language.
   *
   * The comparison that justifies collecting both: Syft reports zero
   * packages for many Maven projects, which the dependency graph covers.
   */
  async sourceComparison(): Promise<SourceComparison[]> {
    const rows = await this.db.query<{
      language: string; source: string; n: number;
    }>(
      `SELECT r.language AS language, a.source AS source, COUNT(*) AS n
       FROM ${ARTIFACTS(this.base)} AS a
       JOIN ${REPOS(this.base)} AS r ON a.repository_id = r.id
       GROUP BY r.language, a.source`,
    );
    const byLanguage = new Map<string, SourceComparison>();
    for (const row of rows) {
      const entry = byLanguage.get(row.language) ?? {
        language: row.language, syft: 0, depgraph: 0,
      };
      if (row.source === 'github-depgraph') entry.depgraph += Number(row.n);
      else entry.syft += Number(row.n);
      byLanguage.set(row.language, entry);
    }
    return [...byLanguage.values()].sort(
      (a, b) => b.syft + b.depgraph - (a.syft + a.depgraph),
    );
  }

  /** Monthly adoption for one package — the question a snapshot cannot answer. */
  async adoptionOverTime(name: string): Promise<AdoptionPoint[]> {
    const rows = await this.db.query<{
      month: string; repository_count: number; direct_count: number;
    }>(
      `SELECT month, repository_count, direct_count
       FROM ${HISTORY(this.base)}
       WHERE name = ?
       ORDER BY month ASC`,
      [name],
    );
    return rows.map((row) => ({
      month: row.month,
      repositoryCount: Number(row.repository_count),
      directCount: Number(row.direct_count),
    }));
  }

  /** Everything recorded about one repository's dependencies. */
  async repositoryProfile(id: number): Promise<{
    repository: RepositoryRow | null;
    dependencies: Pick<ArtifactRow, 'name' | 'version' | 'relationship'>[];
  }> {
    const [repository] = await this.db.query<RepositoryRow>(
      `SELECT * FROM ${REPOS(this.base)} WHERE id = ? LIMIT 1`,
      [id],
    );
    if (!repository) {
      return { repository: null, dependencies: [] };
    }

    const dependencies = await this.db.query<{
      name: string;
      version: string;
      relationship: string;
    }>(
      `SELECT name, version, relationship
       FROM ${ARTIFACTS(this.base)}
       WHERE repository_id = ?
       ORDER BY relationship, name`,
      [id],
    );

    return {
      repository,
      dependencies: dependencies.map((row) => ({
        name: row.name,
        version: row.version,
        relationship: isRelationship(row.relationship)
          ? row.relationship
          : 'unknown',
      })),
    };
  }
}

function toPopularity(row: {
  name: string;
  repository_count: number;
  direct_count: number;
}): PackagePopularity {
  return {
    name: row.name,
    repositoryCount: Number(row.repository_count),
    directCount: Number(row.direct_count),
  };
}
