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
