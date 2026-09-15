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
import { ecosystemMembers, ecosystemName } from '../ecosystems';
import type { DatasetQueries } from '../backend';
import { shapeSpread } from '../d1/queries';
import type {
  AdoptionPoint,
  DatasetMeta,
  DependencyBucket,
  DependencyTree,
  EdgeAmbiguity,
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
  VersionSpread,
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
  // `dictHas` is what makes this equivalent to the INNER JOIN it
  // replaced. `dictGet` on a key the dictionary does not hold returns
  // the type's default, so a dependency row pointing at a repository
  // that is not in `repositories` would render as a blank owner with
  // zero stars instead of being dropped. There are none today —
  // measured, zero rows fail this — which is why the guard belongs here
  // rather than in whatever change first creates one.
  const where = [
    'a.name = {name:String}',
    "dictHas('dict_repositories', a.repository_id)",
  ];
  const params: Record<string, Param> = { name: query.name };

  if (query.type) {
    // Expanded, not passed through. `artifacts.type` still holds each
    // collector's own spelling — `composer` from the dependency graph
    // and `php-composer` from Syft for one ecosystem — so sending the
    // shown name straight in matched nothing and read on the page as
    // an ecosystem with no dependants.
    const members = ecosystemMembers(query.type);
    if (members.length === 1) {
      where.push('a.type = {type:String}');
      params['type'] = members[0]!;
    } else {
      // A named parameter per member rather than an Array(String):
      // that type needs its quotes hand-escaped in the wire format,
      // which is a smaller list of things to get wrong than it looks.
      const names = members.map((_, index) => `{type${index}:String}`);
      where.push(`a.type IN (${names.join(', ')})`);
      members.forEach((member, index) => {
        params[`type${index}`] = member;
      });
    }
  }
  if (query.language) {
    // Lowercased on both sides: the stored language is capitalised as
    // GitHub spells it and the filter sends lowercase.
    where.push(
      "lower(dictGet('dict_repositories', 'language', a.repository_id))"
      + ' = {language:String}',
    );
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
      // Repository metadata comes from a dictionary rather than a
      // join. `repositories` is 28,075 rows — a dimension table — and
      // hashed in memory the join becomes a lookup: measured 13.6 ms
      // to 4.3 ms for `ms`, 7.6 ms to 2.9 ms for `laravel/framework`.
      // This is the page's slowest query and the one the rollups cannot
      // touch, because the package name is arbitrary.
      `SELECT dictGet('dict_repositories', 'owner', a.repository_id) AS owner,
              dictGet('dict_repositories', 'repo', a.repository_id) AS repo,
              dictGet('dict_repositories', 'stars', a.repository_id) AS stars,
              a.version AS version,
              dictGet('dict_repositories', 'url', a.repository_id) AS url,
              a.relationship AS relationship,
              formatDateTime(a.observed_at, '%Y-%m-%d') AS observed_at
       FROM artifacts AS a
       WHERE ${where.join(' AND ')}
       ORDER BY stars DESC, owner, repo
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
    // The unfiltered case is the default page load, and `mv_packages`
    // already holds exactly this number per name — one row against
    // 49,152 read from the fact table.
    //
    // Only the unfiltered case. Covering the filtered ones would mean
    // choosing a rollup per filter combination, and `type` with
    // `language` together has no rollup at all, so the branch would
    // have to know which combinations it can serve. A branch that picks
    // wrong returns a confident wrong number under the rows a reader
    // can see, which is the one failure this count must not have.
    if (!query.type && !query.language && !query.directOnly) {
      const row = await this.db.row<{ total: string | number }>(
        `SELECT repositories AS total FROM mv_packages
         WHERE name = {name:String}`,
        { name: query.name },
      );
      return Number(row?.total ?? 0);
    }

    const { where, params } = dependentFilters(query);
    const row = await this.db.row<{ total: string | number }>(
      `SELECT uniqExact(a.repository_id) AS total
       FROM artifacts AS a
       WHERE ${where.join(' AND ')}`,
      params,
    );
    return Number(row?.total ?? 0);
  }

  async edgeAmbiguity(): Promise<EdgeAmbiguity | null> {
    const row = await this.db.row<{
      names: string | number;
      ambiguous_names: string | number;
      edges: string | number;
      ambiguous_edges: string | number;
      largest_repository: string | number;
    }>('SELECT * FROM mv_edge_ambiguity');
    if (!row) return null;
    return {
      names: Number(row.names),
      ambiguousNames: Number(row.ambiguous_names),
      edges: Number(row.edges),
      ambiguousEdges: Number(row.ambiguous_edges),
      largestRepository: Number(row.largest_repository),
    };
  }

  async ecosystemsFor(name: string): Promise<EcosystemShare[]> {
    const rows = await this.db.rows<{
      type: string;
      repository_count: string | number;
      direct_count: string | number;
    }>(
      `SELECT type,
              repositories AS repository_count,
              direct_repositories AS direct_count
       FROM mv_package_type
       WHERE name = {name:String}
       ORDER BY repository_count DESC`,
      { name },
    );
    // Summed under the name shown, because two of these rows can be
    // one ecosystem: `laravel/framework` offered `composer · 183` and
    // `php-composer · 97` as separate choices, each a fraction of the
    // truth.
    //
    // `repositories` is a distinct count per raw type, so adding them
    // overstates any repository holding both spellings. `max` is the
    // floor and never does — and the two spellings come from different
    // collectors, so a repository scanned by both is exactly the case
    // that would have been double counted.
    const merged = new Map<string, EcosystemShare>();
    for (const row of rows) {
      const type = ecosystemName(row.type);
      const repositoryCount = Number(row.repository_count);
      const directCount = Number(row.direct_count);
      const seen = merged.get(type);
      if (!seen) {
        merged.set(type, { type, repositoryCount, directCount });
        continue;
      }
      seen.repositoryCount = Math.max(seen.repositoryCount, repositoryCount);
      seen.directCount = Math.max(seen.directCount, directCount);
    }
    return [...merged.values()].sort(
      (a, b) => b.repositoryCount - a.repositoryCount,
    );
  }

  async versionSpread(name: string, limit = 10): Promise<VersionSpread> {
    // Resolved versions only, and the unresolved totals beside them.
    // One statement, because two would let the panel's list and its
    // caveat come from different reads of a table that is being
    // refreshed.
    const rows = await this.db.rows<{
      version_kind: string;
      version: string;
      repository_count: string | number;
    }>(
      `SELECT version_kind, version, repositories AS repository_count
       FROM mv_package_version
       WHERE name = {name:String}
       ORDER BY
         version_kind = 'resolved' DESC,
         repository_count DESC,
         version
       LIMIT {limit:UInt32} BY version_kind`,
      { name, limit: boundedLimit(limit) },
    );
    return shapeSpread(rows.map((row) => ({
      kind: row.version_kind,
      version: row.version,
      repositoryCount: Number(row.repository_count),
    })), limit);
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
      source: string;
      month: string;
      repository_count: string | number;
      direct_count: string | number;
    }>(
      `SELECT source, month,
              repositories AS repository_count,
              direct_repositories AS direct_count
       FROM mv_package_month
       WHERE name = {name:String}
       ORDER BY source, month`,
      { name },
    );
    return rows.map((row) => ({
      source: row.source,
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
      type: string | null;
      repository_count: string | number | null;
      name_total: string | number;
    }>(
      // `mv_packages` is keyed on name alone, so a prefix is a range
      // scan with no grouping — which matters because this runs on
      // every keystroke.
      //
      // The join is onto the *bounded* result, never the other way
      // round: `mv_package_type` has 267,755 rows and joining it first
      // made a one-word search 40 ms. This way `mail` is 15 ms and a
      // single letter 6 ms.
      //
      // The limit bounds *names*, then each name expands to its
      // ecosystems. A name in three of them is three rows, which is
      // the point — and it means the row count can exceed `limit`.
      `WITH hits AS (
           SELECT name, repositories
           FROM mv_packages
           WHERE startsWith(name, {term:String})
           ORDER BY repositories DESC, name
           LIMIT {limit:UInt32}
       )
       SELECT h.name AS name,
              t.type AS type,
              t.repositories AS repository_count,
              h.repositories AS name_total
       FROM hits h
       LEFT JOIN mv_package_type t ON t.name = h.name
       ORDER BY h.repositories DESC, h.name, t.repositories DESC`,
      { term, limit: boundedLimit(limit) },
    );

    // Canonical names collapse two rows into one — `composer` and
    // `php-composer` are one ecosystem — and `repositories` is a
    // distinct count per raw type, so adding them would overstate any
    // repository carrying both spellings. `max` is the floor and
    // cannot.
    const merged = new Map<string, PackageMatch>();
    const order: string[] = [];
    for (const row of rows) {
      const ecosystem = row.type ? ecosystemName(row.type) : null;
      const key = `${row.name}\u0000${ecosystem ?? ''}`;
      const repositoryCount = Number(row.repository_count ?? 0);
      const seen = merged.get(key);
      if (!seen) {
        merged.set(key, {
          name: row.name,
          ecosystem,
          repositoryCount,
          nameTotal: Number(row.name_total),
        });
        order.push(key);
        continue;
      }
      seen.repositoryCount = Math.max(seen.repositoryCount, repositoryCount);
    }
    return order.map((key) => merged.get(key)!);
  }

  /* ---------------- the edge table, both directions ---------------- */

  async dependenciesOf(name: string, limit = 20): Promise<PackageEdge[]> {
    const rows = await this.db.rows<{
      name: string;
      repositories: string | number;
    }>(
      // `mv_edges_forward` rather than `edges`: the base table is
      // ordered child-first, so this direction had no prefix and
      // scanned all 614,221 rows. A projection is what ClickHouse would
      // normally want here and it refuses one on a SummingMergeTree
      // unless projection upkeep joins every merge — so the second
      // ordering is a rollup, where the rest of the cost already lives.
      `SELECT child AS name, repositories
       FROM mv_edges_forward
       WHERE parent = {name:String}
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
           SELECT parent, child, repositories
           FROM mv_edges_forward
           WHERE parent IN (
                   SELECT child FROM mv_edges_forward
                   WHERE parent = {root:String}
                   ORDER BY repositories DESC, child
                   LIMIT {children:UInt32}
                 )
             AND child != {root:String}
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
      // Nine rows, whether or not a language is named. The
      // per-language rollup is keyed `(name, language)`, so a language
      // filter there could not use the prefix and read all 371,074
      // rows — the same cost as no filter.
      `SELECT sum(direct_records) AS direct,
              sum(transitive_records) AS transitive,
              sum(unknown_records) AS unknown
       FROM mv_language_totals ${filter}`,
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
      // Eleven stored rows. The rollup behind it reads `repositories`
      // rather than `artifacts`, because the 3,736 repositories with no
      // dependency row are the finding this panel exists to show and
      // cannot appear in a rollup over dependencies.
      `SELECT language, repositories, with_sbom
       FROM mv_language_coverage
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
      repositories: string | number;
    }>(
      // Six stored rows. The boundaries were in the query so changing
      // them needed no refresh; a refresh costs 0.3 s, which is not a
      // reason to bucket 24,339 rows on every page load.
      //
      // Ordered by `position`, not by label: '1000+' sorts between
      // '10-24' and '100-249' as a string.
      `SELECT bucket, repositories
       FROM mv_dependency_buckets
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
      `SELECT language, syft_records AS syft, depgraph_records AS depgraph
       FROM mv_language_totals
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
      // Not a version: this store has no export contract to number,
      // because the dashboard reads it live. Naming the store is the
      // useful thing the field can carry.
      schemaVersion: 'clickhouse (live)',
      observedFrom: row?.observed_from ?? '',
      observedTo: row?.observed_to ?? '',
    };
  }

  /** Set by the endpoint from configuration; see `meta`. */
  generator = 'chatsbom/clickhouse';
}
