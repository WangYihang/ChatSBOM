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
 *
 * The questions answered by reading one such table are declared once,
 * for both stores, in `dataset/reads.ts`. What is here is what only
 * this store does: every lookup of a name through the joins, and the
 * rows of the aggregates that D1 keeps in another shape.
 */
import type { DatasetQueries } from '../backend';
import { SharedDataset } from '../dataset/dataset';
import { positional } from '../dataset/reads';
import {
  boundedLimit,
  boundedOffset,
  num,
  relationshipOf,
  type Row,
  shapeDependant,
  shapeEdge,
  shapeSpread,
} from '../dataset/shape';
import type {
  DatasetMeta,
  Dependent,
  DependentQuery,
  EcosystemRelationship,
  EcosystemShare,
  EdgeAmbiguity,
  PackageEdge,
  PackageMatch,
  RelationshipSplit,
  VersionSpread,
} from '../dataset/types';
import { ecosystemName } from '../ecosystems';

/** The narrow slice of D1 this layer needs, so it is testable. */
export interface D1Queryable {
  all<T>(sql: string, params?: unknown[]): Promise<T[]>;
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
    // Under the name the page shows, which is how `kinds` stores it:
    // the export files each collector's spelling under one name, so
    // Syft's `php-composer`, passed through, matched nothing at all.
    filters.push('k.type = ?');
    params.push(ecosystemName(query.type));
  }
  if (query.language) {
    filters.push('r.language_bucket = ?');
    params.push(query.language.toLowerCase());
  }
  if (query.directOnly) {
    filters.push('k.relationship = ?');
    params.push('direct');
  }
  return { filters, params };
}

/**
 * Where a dependants row is read from.
 *
 * Its date is its own source's observation of the repository
 * (`observations`, one row per repository and source). The
 * repository's `observed_at` is the newest of those, and dating every
 * row by it put September beside a February Syft scan whenever the
 * dependency graph came later (#24). It remains the fallback for a row
 * whose date the export did not write, which would otherwise vanish.
 */
const DEPENDANTS = `FROM artifacts AS a
       JOIN packages AS p ON p.id = a.package_id
       JOIN versions AS v ON v.id = a.version_id
       JOIN kinds AS k ON k.id = a.kind_id
       JOIN repositories AS r ON r.id = a.repository_id
       LEFT JOIN observations AS o
         ON o.repository_id = a.repository_id
        AND o.source = k.source`;

const OBSERVED = 'coalesce(o.observed_at, r.observed_at) AS observed_on';

/**
 * One dependants row per repository, version, relationship, ecosystem
 * and date — what the table shows — whatever number of facts it
 * collapses. The count of rows groups by the same keys, so a page never
 * runs past the end.
 */
const ONE_ROW = 'r.id, v.version, k.relationship, k.type, observed_on';

/**
 * The D1 implementation.
 *
 * `implements DatasetQueries` is load-bearing: it is what makes a second
 * store a compile-time exercise rather than an archaeology exercise.
 */
export class D1Dataset extends SharedDataset implements DatasetQueries {
  constructor(private readonly db: D1Queryable) {
    super('d1', (sql, values) => {
      const bound = positional(sql, values);
      return db.all<Row>(bound.sql, bound.params);
    });
  }

  /** Repositories depending on a package, most starred first. */
  async dependentsOf(query: DependentQuery): Promise<Dependent[]> {
    const { filters, params } = dependentFilters(query);
    params.push(boundedLimit(query.limit));
    // Clamped, so a hand-edited URL cannot ask for a negative
    // offset or a non-finite one.
    params.push(boundedOffset(query.offset));

    const rows = await this.db.all<Row>(
      // `count(*)` collapses the rows the table would show as one. This
      // export keeps one row per dependency fact, so it counts the
      // cataloguers that reported a version rather than the manifests
      // that declare it, which only ClickHouse keeps.
      //
      // Ordered by every key a row is grouped on, ending with the
      // repository, so the order is total: each page is its own
      // statement, and ties in a partial order may fall either way in
      // each of them.
      `SELECT r.owner AS owner, r.repo AS repo, r.stars AS stars,
              v.version AS version, r.url AS url,
              r.github_language AS language, k.type AS ecosystem,
              k.relationship AS relationship, ${OBSERVED},
              count(*) AS manifests
       ${DEPENDANTS}
       WHERE ${filters.join(' AND ')}
       GROUP BY ${ONE_ROW}
       ORDER BY r.stars DESC, r.owner, r.repo, v.version, k.relationship,
                k.type, observed_on, r.id
       LIMIT ? OFFSET ?`,
      params,
    );
    return rows.map(shapeDependant);
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
    const rows = await this.db.all<Row>(
      `SELECT count(DISTINCT a.repository_id) AS total
       FROM artifacts AS a
       JOIN packages AS p ON p.id = a.package_id
       JOIN kinds AS k ON k.id = a.kind_id
       JOIN repositories AS r ON r.id = a.repository_id
       WHERE ${filters.join(' AND ')}`,
      params,
    );
    return num(rows[0]?.['total']);
  }

  async countDependentRows(query: DependentQuery): Promise<number> {
    // The grouped rows, not the repositories.
    const { filters, params } = dependentFilters(query);
    const rows = await this.db.all<Row>(
      `SELECT count(*) AS total FROM (
           SELECT ${OBSERVED}
           ${DEPENDANTS}
           WHERE ${filters.join(' AND ')}
           GROUP BY ${ONE_ROW}
       )`,
      params,
    );
    return num(rows[0]?.['total']);
  }

  /* ---------------- precomputed: read, never recompute ------------- */

  /**
   * Not answered, and answered `null` rather than approximated.
   *
   * The collisions are names with more than one ecosystem among the
   * artifacts, and counting them means grouping every artifact row by
   * name and kind on request, which this store's rule is not to do. The
   * export could store the figure; until it does, a plausible-looking
   * number from the wrong denominator is how the hardcoded caveat went
   * wrong in the first place.
   */
  async edgeAmbiguity(): Promise<EdgeAmbiguity | null> {
    return null;
  }

  async relationshipSplit(ecosystem?: string): Promise<RelationshipSplit> {
    const rows = await this.db.all<Row>(
      `SELECT relationship, records
       FROM agg_relationship_split
       WHERE ecosystem = ?`,
      [ecosystem ? ecosystem.toLowerCase() : ''],
    );

    const split: RelationshipSplit = { direct: 0, transitive: 0, unknown: 0 };
    for (const row of rows) {
      split[relationshipOf(row['relationship'])] += num(row['records']);
    }
    return split;
  }

  async relationshipByEcosystem(): Promise<EcosystemRelationship[]> {
    // `agg_relationship_split` already holds this per ecosystem, a row
    // per relationship; the empty ecosystem is the corpus-wide row and
    // is not an ecosystem.
    const rows = await this.db.all<Row>(
      `SELECT ecosystem, relationship, records
       FROM agg_relationship_split
       WHERE ecosystem <> ''`,
    );
    const byEcosystem = new Map<string, EcosystemRelationship>();
    for (const row of rows) {
      const ecosystem = String(row['ecosystem']);
      const seen = byEcosystem.get(ecosystem) ?? {
        ecosystem,
        direct: 0,
        transitive: 0,
        unknown: 0,
        records: 0,
      };
      const records = num(row['records']);
      seen[relationshipOf(row['relationship'])] += records;
      seen.records += records;
      byEcosystem.set(ecosystem, seen);
    }
    return [...byEcosystem.values()]
      .filter((row) => row.records > 0)
      .sort(
        (a, b) =>
          b.records - a.records
          || (a.ecosystem < b.ecosystem ? -1 : a.ecosystem > b.ecosystem ? 1 : 0),
      );
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
    const rows = await this.db.all<Row>(
      `SELECT generator, schema_version, observed_from, observed_to
       FROM meta`,
    );
    const row = rows[0];
    const version = String(row?.['schema_version'] ?? '');
    return {
      generator: String(row?.['generator'] ?? ''),
      // Prefixed here rather than in the panel. The stored value is a
      // contract number — `5` — and reads as nothing on its own; the
      // ClickHouse backend answers `clickhouse`, which the panel's old
      // `v` prefix turned into "vclickhouse". Whoever knows what the
      // value means adds the prefix.
      schemaVersion: version ? `d1 v${version}` : '',
      observedFrom: String(row?.['observed_from'] ?? ''),
      observedTo: String(row?.['observed_to'] ?? ''),
    };
  }

  /* ---------------- one package, through the joins ----------------- */

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
   *
   * **Ranked by popularity, and reading a stored count to do it.**
   * Alphabetically, `laravel` returns forty `laravel-enso/*` packages
   * with one dependant each — `-` is 0x2D and `/` is 0x2F — and never
   * reaches `laravel/framework`, which has 98. Ranking needs a count
   * for every candidate, not just the ones returned, so the count
   * cannot be a correlated subquery here: at keystroke latency that is
   * one scan of `artifacts` per candidate name. `packages.repositories`
   * is filled once by the export's aggregates instead.
   *
   * **One row per name.** The count is the name's, across its
   * ecosystems: split per ecosystem it would be the artifacts counted
   * on every keystroke, which the stored count exists to avoid. So the
   * row names no ecosystem and stands for all of them, where ClickHouse
   * offers one row per ecosystem.
   */
  async searchPackages(term: string, limit = 20): Promise<PackageMatch[]> {
    if (!term) return [];

    const escaped = term.replace(/[\\%_]/g, (c) => `\\${c}`);
    const rows = await this.db.all<Row>(
      `SELECT p.name AS name, p.repositories AS repository_count
       FROM packages AS p
       WHERE p.name LIKE ? ESCAPE '\\'
       ORDER BY p.repositories DESC, p.name
       LIMIT ?`,
      [`${escaped}%`, boundedLimit(limit)],
    );
    return rows.map((row) => ({
      name: String(row['name']),
      ecosystem: null,
      repositoryCount: num(row['repository_count']),
      nameTotal: num(row['repository_count']),
    }));
  }

  /** Which resolved versions of a package are in use. */
  async versionSpread(name: string, limit = 10): Promise<VersionSpread> {
    // `kinds.version_kind` distinguishes a resolution from a manifest
    // constraint, and the panel needs both: the resolved versions to
    // list, and the rest to count. Unbounded here and sliced by
    // `shapeSpread`, because the top ten *resolved* versions are not
    // the resolved rows among the top ten of everything.
    const rows = await this.db.all<Row>(
      `SELECT k.version_kind AS version_kind,
              v.version AS version,
              count(DISTINCT a.repository_id) AS repository_count
       FROM artifacts AS a
       JOIN packages AS p ON p.id = a.package_id
       JOIN versions AS v ON v.id = a.version_id
       JOIN kinds AS k ON k.id = a.kind_id
       WHERE p.name = ?
       GROUP BY k.version_kind, v.version
       ORDER BY repository_count DESC, v.version`,
      [name],
    );
    return shapeSpread(
      rows.map((row) => ({
        kind: String(row['version_kind']),
        version: String(row['version']),
        repositoryCount: num(row['repository_count']),
      })),
      boundedLimit(limit),
    );
  }

  /**
   * Which ecosystems a package name appears in.
   *
   * Asked before any count is presented as "dependants of X", because a
   * name shared across ecosystems is two different packages: `mail` is
   * a Ruby gem with 118 dependants and a Maven artifactId with 6.
   *
   * `kinds.type` is already the name shown, so a repository holding
   * both collectors' spellings of Composer is counted once.
   */
  async ecosystemsFor(name: string): Promise<EcosystemShare[]> {
    const rows = await this.db.all<Row>(
      `SELECT k.type AS type,
              count(DISTINCT a.repository_id) AS repository_count,
              count(DISTINCT CASE WHEN k.relationship = 'direct'
                                  THEN a.repository_id END) AS direct_count
       FROM artifacts AS a
       JOIN packages AS p ON p.id = a.package_id
       JOIN kinds AS k ON k.id = a.kind_id
       WHERE p.name = ?
       GROUP BY k.type
       ORDER BY repository_count DESC, k.type`,
      [name],
    );
    return rows.map((row) => ({
      type: String(row['type']),
      repositoryCount: num(row['repository_count']),
      directCount: num(row['direct_count']),
    }));
  }

  /* ---------------- the edge table, both directions ---------------- */

  /**
   * What a package pulls in, aggregated across repositories.
   *
   * A lookup on `idx_agg_edges_parent_id`, so the cost is the rows
   * returned rather than the 454,577 in the table — which is the whole
   * reason the edges are stored aggregated by name instead of per
   * repository.
   */
  async dependenciesOf(name: string, limit = 20): Promise<PackageEdge[]> {
    const rows = await this.db.all<Row>(
      `SELECT c.name AS name, e.repositories AS repositories
       FROM agg_edges AS e
       JOIN packages AS p ON p.id = e.parent_id
       JOIN packages AS c ON c.id = e.child_id
       WHERE p.name = ?
       ORDER BY e.repositories DESC, c.name
       LIMIT ?`,
      [name, boundedLimit(limit)],
    );
    return rows.map(shapeEdge);
  }

  /**
   * What pulls a package in — the direction that answers a real
   * question.
   *
   * "Why is `ms` in my lockfile? I never asked for it." The answer is a
   * ranking: `debug` in 7,999 repositories, `send` in 3,853, and a tail
   * down to `connect-timeout` in 40. Served by
   * `idx_agg_edges_child_id`, which exists for exactly this.
   */
  async pulledInBy(name: string, limit = 20): Promise<PackageEdge[]> {
    const rows = await this.db.all<Row>(
      `SELECT p.name AS name, e.repositories AS repositories
       FROM agg_edges AS e
       JOIN packages AS c ON c.id = e.child_id
       JOIN packages AS p ON p.id = e.parent_id
       WHERE c.name = ?
       ORDER BY e.repositories DESC, p.name
       LIMIT ?`,
      [name, boundedLimit(limit)],
    );
    return rows.map(shapeEdge);
  }

  /**
   * The tree's second hop: a statement of its own, after the first.
   *
   * Two statements rather than one, deliberately. The second hop needs
   * the first hop's rows to partition by, and expressing that as a
   * single statement means either a CTE the planner materialises or a
   * correlated subquery per row; two index lookups in the same colo
   * cost less than either and the statement stays readable.
   */
  protected async secondHop(
    root: string,
    children: readonly PackageEdge[],
    branch: number,
  ): Promise<Row[]> {
    const placeholders = children.map(() => '?').join(', ');
    return this.db.all<Row>(
      // Two things about this statement are not stylistic.
      //
      // The window function has to be computed before it can be
      // filtered, hence the subquery: SQLite will not accept a
      // ROW_NUMBER() in a WHERE clause of the same SELECT.
      //
      // And the root is excluded *inside* that subquery, so it never
      // takes a rank. The edges genuinely run both ways — `bytes`
      // pulls in `body-parser` in one repository, as well as the other
      // way round — and drawn as a second hop that reads as the path
      // `body-parser -> bytes -> body-parser`, which is not a claim the
      // data makes. Excluding it in the outer WHERE would drop the row
      // but leave its rank spent, so a parent would show two children
      // where three were asked for.
      `SELECT parent, child, repositories
       FROM (
         SELECT pp.name AS parent,
                cc.name AS child,
                e.repositories AS repositories,
                ROW_NUMBER() OVER (
                  PARTITION BY e.parent_id
                  ORDER BY e.repositories DESC, cc.name
                ) AS branch_rank
         FROM agg_edges AS e
         JOIN packages AS pp ON pp.id = e.parent_id
         JOIN packages AS cc ON cc.id = e.child_id
         WHERE pp.name IN (${placeholders}) AND cc.name <> ?
       )
       WHERE branch_rank <= ?
       ORDER BY repositories DESC, child, parent`,
      [...children.map((child) => child.name), root, branch],
    );
  }
}
