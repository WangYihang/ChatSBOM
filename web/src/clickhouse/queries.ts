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
 * comparisons, including the 2,433-row dependency histogram. The ones
 * read as they are stored are declared with D1's in `dataset/reads.ts`.
 *
 * Ranges here are one place a reader can check the claim: every SQL
 * string below binds its values as `{name:Type}` parameters. None
 * interpolates a value; the only text spliced in is the ecosystem table
 * of `ecosystems.ts`. The page is public.
 */
import type { DatasetQueries } from '../backend';
import { SharedDataset } from '../dataset/dataset';
import {
  boundedLimit,
  boundedOffset,
  num,
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
import { canonicalSql, ecosystemMembers, ecosystemName } from '../ecosystems';
import type { ClickHouse, Param } from './client';

/**
 * A row's ecosystem, under the name the page shows.
 *
 * `artifacts.type` holds each collector's own spelling — Syft's
 * `php-composer` beside the graph's `composer` — and grouping on it
 * made two rows of what the table shows as one, split before a LIMIT
 * could count them as one.
 */
const ECOSYSTEM = `${canonicalSql('a.type')} AS ecosystem`;

/** A row's date: its own observation, as a UTC day, as the export makes it. */
const OBSERVED = "formatDateTime(a.observed_at, '%Y-%m-%d', 'UTC') AS observed_on";

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
  //
  // The last one keeps each repository's current observations.
  // `artifacts` is append-only, so a repository scanned twice has both
  // scans' rows, and without it a repository that moved from mail
  // 2.7.1 to 2.9.1 was listed at both versions here and at one in the
  // CLI. It is the `current_artifacts` view's condition,
  // `CURRENT_OBSERVATION` in `core/schema.py`, asked of the dictionary:
  // the view rebuilds its join of `repositories FINAL` on every
  // request — 13.6 ms against 4.2 ms for a point lookup, on synthetic
  // data — and this check added 0.4 ms. (`tests/current_state_test.py`
  // parses this list, holds it to that condition and runs it against a
  // database.)
  //
  // A Syft row is current by the commit its repository records, and a
  // dependency-graph row by the graph document it records, by the
  // instant the document states: a graph fetched again while the Syft
  // target stood still carried the same commit, and a package the
  // newer graph dropped stayed listed. 0 is a repository row written
  // before that record existed, which keeps the commit rule. On the
  // same synthetic data this costs 0.9 ms more than the commit check on
  // a lookup of 300 rows (6.9 to 7.8 ms, server time) and 1.4 ms on one
  // of 14,457 (17.8 to 19.1 ms), reading no more rows.
  //
  // `toUnixTimestamp(...) != 0`, not `!= 0` on the date: ClickHouse
  // rewrites a `dictGet` compared with a constant into a set built from
  // the whole dictionary, per request, which read 28,075 rows more and
  // added 3.0 ms.
  const where = [
    'a.name = {name:String}',
    "dictHas('dict_repositories', a.repository_id)",
    "if(a.source = 'github-depgraph'"
      + " AND toUnixTimestamp(dictGet('dict_repositories', 'depgraph_observed_at', a.repository_id)) != 0,"
      + " a.observed_at = dictGet('dict_repositories', 'depgraph_observed_at', a.repository_id),"
      + " a.sbom_commit_sha = dictGet('dict_repositories', 'sbom_commit_sha', a.repository_id))",
  ];
  const params: Record<string, Param> = { name: query.name };

  if (query.type) {
    // Expanded, not passed through. `artifacts.type` still holds each
    // collector's own spelling — `composer` from the dependency graph
    // and `php-composer` from Syft for one ecosystem — so sending the
    // shown name straight in matched nothing and read on the page as
    // an ecosystem with no dependants. From the shown name, whichever
    // spelling arrived: Syft's alone matched Syft's rows alone.
    const members = ecosystemMembers(ecosystemName(query.type));
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
    // The folded language (#55 D7): one of the top twelve, lowercased,
    // `other` or `none`, as the coverage panel lists them and the
    // dictionary computes it with the rollup's own expression.
    where.push(
      "dictGet('dict_repositories', 'language_bucket', a.repository_id)"
      + ' = {language:String}',
    );
    params['language'] = query.language.toLowerCase();
  }
  if (query.directOnly) {
    where.push("a.relationship = 'direct'");
  }
  return { where, params };
}

/** Which rows are current: the dependants' predicates, bar the name. */
const CURRENT = dependentFilters({ name: '' }).where.filter(
  (predicate) => !predicate.startsWith('a.name '),
);

/**
 * One dependants row per repository, version, relationship, ecosystem
 * and day — what the table shows. The repository's columns come out of
 * the dictionary by its id, so they need no grouping of their own.
 */
const ONE_ROW = 'a.repository_id, a.version, a.relationship, ecosystem, observed_on';

/** The observation span `meta` reports, as UTC dates, or empty. */
interface Span {
  from: string;
  to: string;
}

/**
 * How long a span is kept: five minutes, the soonest the dictionary
 * reloads the repositories on its own.
 *
 * It costs a read of every current row (see `meta`), and moves only
 * when `db index` runs, hours apart; the container's healthcheck asks
 * for it every 15 s and its watchdog every 30 s. Kept per isolate, by
 * the server and database it describes.
 */
const SPAN_KEPT_MS = 5 * 60 * 1000;
const SPANS = new Map<string, { until: number; span: Promise<Span> }>();

/**
 * The ClickHouse implementation.
 *
 * `implements DatasetQueries` is what makes the seam real rather than
 * aspirational: the interface was written before this existed, and the
 * compiler is what checks that it was written correctly.
 */
export class ClickHouseDataset extends SharedDataset implements DatasetQueries {
  constructor(private readonly db: ClickHouse) {
    super('clickhouse', (sql, values) => db.rows<Row>(sql, values));
  }

  /* ---------------- point lookups: the fact table ------------------ */

  async dependentsOf(query: DependentQuery): Promise<Dependent[]> {
    const { where, params } = dependentFilters(query);
    params['limit'] = boundedLimit(query.limit);
    // Clamped, so a hand-edited URL cannot ask for a negative
    // offset or a non-finite one, nor one `UInt32` cannot hold.
    params['offset'] = boundedOffset(query.offset);

    const rows = await this.db.rows<Row>(
      // Repository metadata comes from a dictionary rather than a
      // join. `repositories` is 28,075 rows — a dimension table — and
      // hashed in memory the join becomes a lookup: measured 13.6 ms
      // to 4.3 ms for `ms`, 7.6 ms to 2.9 ms for `laravel/framework`.
      // This is the page's slowest query and the one the rollups cannot
      // touch, because the package name is arbitrary.
      //
      // Ordered by every key a row is grouped on, ending with the
      // repository, so the order is total: each page is a statement of
      // its own, and ClickHouse breaks a tie however its threads
      // finish, so a partial order repeated or skipped rows between
      // pages.
      `SELECT dictGet('dict_repositories', 'owner', a.repository_id) AS owner,
              dictGet('dict_repositories', 'repo', a.repository_id) AS repo,
              dictGet('dict_repositories', 'stars', a.repository_id) AS stars,
              a.version AS version,
              dictGet('dict_repositories', 'url', a.repository_id) AS url,
              dictGet('dict_repositories', 'language', a.repository_id)
                AS language,
              ${ECOSYSTEM},
              a.relationship AS relationship,
              ${OBSERVED},
              -- Per-manifest rows collapsed into one, with the count
              -- kept. The dependency graph reports each manifest
              -- separately, so a repository declaring one package in
              -- 80 of them filled 80 of the 100 rows this query
              -- returns, every one identical in the columns the table
              -- shows. (No backticks in here: this is a template
              -- literal, and one closed it early.)
              count() AS manifests
       FROM artifacts AS a
       WHERE ${where.join(' AND ')}
       GROUP BY ${ONE_ROW}
       ORDER BY stars DESC, owner, repo, a.version, a.relationship,
                ecosystem, observed_on, a.repository_id
       LIMIT {limit:UInt32}
       OFFSET {offset:UInt32}`,
      params,
    );
    return rows.map(shapeDependant);
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
      const row = await this.db.row<Row>(
        `SELECT repositories AS total FROM mv_packages
         WHERE name = {name:String}`,
        { name: query.name },
      );
      return num(row?.['total']);
    }

    const { where, params } = dependentFilters(query);
    const row = await this.db.row<Row>(
      `SELECT uniqExact(a.repository_id) AS total
       FROM artifacts AS a
       WHERE ${where.join(' AND ')}`,
      params,
    );
    return num(row?.['total']);
  }

  async countDependentRows(query: DependentQuery): Promise<number> {
    const { where, params } = dependentFilters(query);
    // The grouped rows, not the repositories. Same keys as
    // `dependentsOf`, because paging on a different population is how
    // a "page 4 of 4" comes back empty.
    const row = await this.db.row<Row>(
      `SELECT count() AS total FROM (
           SELECT a.repository_id, a.version, a.relationship,
                  ${ECOSYSTEM}, ${OBSERVED}
           FROM artifacts AS a
           WHERE ${where.join(' AND ')}
           GROUP BY ${ONE_ROW}
       )`,
      params,
    );
    return num(row?.['total']);
  }

  /**
   * Which ecosystems a name is in, from the rollup keyed by the name
   * shown.
   *
   * `mv_package_type` is keyed by each collector's spelling, and this
   * took the larger of the two counts for one ecosystem: a floor, not
   * the count, since the spellings come from different collectors and
   * a repository scanned by only one of them is in only one count.
   * Composer was three repositories of `laravel/framework` where it is
   * five. `mv_package_ecosystem` counts each repository once.
   */
  async ecosystemsFor(name: string): Promise<EcosystemShare[]> {
    const rows = await this.db.rows<Row>(
      `SELECT ecosystem AS type,
              repositories AS repository_count,
              direct_repositories AS direct_count
       FROM mv_package_ecosystem
       WHERE name = {name:String}
       ORDER BY repository_count DESC, type`,
      { name },
    );
    return rows.map((row) => ({
      type: String(row['type']),
      repositoryCount: num(row['repository_count']),
      directCount: num(row['direct_count']),
    }));
  }

  async versionSpread(name: string, limit = 10): Promise<VersionSpread> {
    // Resolved versions, and what was set aside beside them. One
    // statement, because two would let the panel's list and its caveat
    // come from different reads of a table that is being refreshed.
    //
    // Everything that is not a resolution is summed into one row of its
    // kind before the limit, which then cuts only the list. The limit
    // used to apply to every kind, so `constrained` summed only the
    // widest few constraint strings and D1, which sums them all,
    // reported more.
    //
    // One bound for the statement and the slice. The slice took the raw
    // limit, and `slice(0, -1)` dropped the last version without a word.
    const bounded = boundedLimit(limit);
    const rows = await this.db.rows<Row>(
      `SELECT version_kind,
              if(version_kind = 'resolved', version, '') AS listed,
              sum(repositories) AS repository_count
       FROM mv_package_version
       WHERE name = {name:String}
       GROUP BY version_kind, listed
       ORDER BY version_kind = 'resolved' DESC, repository_count DESC, listed
       LIMIT {limit:UInt32} BY version_kind`,
      { name, limit: bounded },
    );
    return shapeSpread(
      rows.map((row) => ({
        kind: String(row['version_kind']),
        version: String(row['listed'] ?? ''),
        repositoryCount: num(row['repository_count']),
      })),
      bounded,
    );
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
    const rows = await this.db.rows<Row>(
      // `mv_packages` is keyed on name alone, so a prefix is a range
      // scan with no grouping — which matters because this runs on
      // every keystroke.
      //
      // The join is onto the *bounded* result, never the other way
      // round: the ecosystem rollup has a quarter of a million rows and
      // joining it first made a one-word search 40 ms. This way `mail`
      // is 15 ms and a single letter 6 ms.
      //
      // The limit bounds *names*, then each name expands to its
      // ecosystems, under the names shown, each repository counted
      // once. A name in three of them is three rows, which is the
      // point — and it means the row count can exceed `limit`.
      `WITH hits AS (
           SELECT name, repositories
           FROM mv_packages
           WHERE startsWith(name, {term:String})
           ORDER BY repositories DESC, name
           LIMIT {limit:UInt32}
       )
       SELECT h.name AS name,
              t.ecosystem AS ecosystem,
              t.repositories AS repository_count,
              h.repositories AS name_total
       FROM hits h
       LEFT JOIN mv_package_ecosystem t ON t.name = h.name
       ORDER BY h.repositories DESC, h.name, t.repositories DESC, t.ecosystem`,
      { term, limit: boundedLimit(limit) },
    );
    return rows.map((row) => ({
      name: String(row['name']),
      ecosystem: row['ecosystem'] ? String(row['ecosystem']) : null,
      repositoryCount: num(row['repository_count']),
      nameTotal: num(row['name_total']),
    }));
  }

  /* ---------------- the edge table, both directions ---------------- */

  async dependenciesOf(name: string, limit = 20): Promise<PackageEdge[]> {
    const rows = await this.db.rows<Row>(
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
    return rows.map(shapeEdge);
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
    const rows = await this.db.rows<Row>(
      `SELECT parent AS name, sum(repositories) AS repositories
       FROM edges
       WHERE child = {name:String}
       GROUP BY parent
       ORDER BY repositories DESC, parent
       LIMIT {limit:UInt32}`,
      { name, limit: boundedLimit(limit) },
    );
    return rows.map(shapeEdge);
  }

  /**
   * The tree's second hop, in one statement.
   *
   * ClickHouse will take the first hop as a subquery in the `IN`, and
   * the window function partitions the second hop per parent so a
   * parent whose widest edge points back at the root does not lose a
   * slot.
   *
   * The root is excluded *inside* the window's own SELECT, for the
   * reason the D1 version records: the edges genuinely run both ways —
   * `bytes -> body-parser` in one repository as well as
   * `body-parser -> bytes` in 3,589 — and filtered outside, the row is
   * dropped but its rank is spent.
   */
  protected secondHop(
    root: string,
    children: readonly PackageEdge[],
    branch: number,
  ): Promise<Row[]> {
    return this.db.rows<Row>(
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
       ORDER BY repositories DESC, child, parent`,
      { root, children: children.length, branch },
    );
  }

  /* ---------------- the overview: read the rollups ----------------- */

  async relationshipByEcosystem(): Promise<EcosystemRelationship[]> {
    const rows = await this.db.rows<Row>(
      // A dozen rows, already aggregated. Records partition by
      // ecosystem, so these add up to the corpus's. The empty
      // ecosystem is a record with no type, not an ecosystem.
      `SELECT ecosystem,
              direct_records AS direct,
              transitive_records AS transitive,
              unknown_records AS unknown,
              records
       FROM mv_ecosystem_totals
       WHERE ecosystem != '' AND records > 0
       ORDER BY records DESC, ecosystem`,
    );
    return rows.map((row) => ({
      ecosystem: String(row['ecosystem']),
      direct: num(row['direct']),
      transitive: num(row['transitive']),
      unknown: num(row['unknown']),
      records: num(row['records']),
    }));
  }

  async edgeAmbiguity(): Promise<EdgeAmbiguity | null> {
    const row = await this.db.row<Row>('SELECT * FROM mv_edge_ambiguity');
    if (!row) return null;
    return {
      names: num(row['names']),
      ambiguousNames: num(row['ambiguous_names']),
      edges: num(row['edges']),
      ambiguousEdges: num(row['ambiguous_edges']),
      largestRepository: num(row['largest_repository']),
    };
  }

  async relationshipSplit(ecosystem?: string): Promise<RelationshipSplit> {
    const filter = ecosystem ? 'WHERE ecosystem = {ecosystem:String}' : '';
    const params: Record<string, Param> = ecosystem
      ? { ecosystem: ecosystem.toLowerCase() }
      : {};
    const row = await this.db.row<Row>(
      // A dozen rows, whether or not an ecosystem is named. Summed
      // unfiltered, because records partition by ecosystem: a record
      // has one type. (A repository count would not sum; there is
      // none here.)
      `SELECT sum(direct_records) AS direct,
              sum(transitive_records) AS transitive,
              sum(unknown_records) AS unknown
       FROM mv_ecosystem_totals ${filter}`,
      params,
    );
    return {
      direct: num(row?.['direct']),
      transitive: num(row?.['transitive']),
      unknown: num(row?.['unknown']),
    };
  }

  /**
   * Which build produced the data and how fresh it is.
   *
   * The span is the one the D1 export writes: of each repository's
   * current observations, the newest, and of those the oldest and the
   * newest (`repository_freshness`). It was the minimum and maximum of
   * every row ever appended, so a scan a later one had replaced set the
   * start of the span, and the two stores disagreed about the data's
   * age by months. The generator string is the one thing ClickHouse
   * cannot know — it is the pipeline's version, not the database's — so
   * it is configured.
   */
  async meta(): Promise<DatasetMeta> {
    const span = await this.span();
    return {
      generator: this.generator,
      // Not a version: this store has no export contract to number,
      // because the dashboard reads it live. Naming the store is the
      // useful thing the field can carry.
      schemaVersion: 'clickhouse (live)',
      observedFrom: span.from,
      observedTo: span.to,
    };
  }

  /**
   * The span, kept a few minutes (`SPAN_KEPT_MS`).
   *
   * The old span came out of part metadata — `observed_at` is the
   * partition key, so its minimum and maximum over the whole table cost
   * 2 ms and read no rows. The current one cannot: no rollup keeps a
   * repository's newest observation, so it reads the current rows of
   * the fact table, every one, with the dependants' check of which are
   * current: 92 ms on 2,000,000 synthetic rows, where the
   * `current_artifacts` view took 83 ms. A probe that finds it kept has
   * still had the Worker answer, and still learns which store it is
   * configured with, which is what the probes assert.
   *
   * Not kept for a client that cannot say which data it reads.
   */
  private span(): Promise<Span> {
    const target = this.db.target;
    if (!target) return this.askSpan();
    const now = Date.now();
    const kept = SPANS.get(target);
    if (kept && kept.until > now) return kept.span;
    const span = this.askSpan();
    SPANS.set(target, { until: now + SPAN_KEPT_MS, span });
    // A failure is not an answer: the next request asks again.
    span.catch(() => {
      if (SPANS.get(target)?.span === span) SPANS.delete(target);
    });
    return span;
  }

  private async askSpan(): Promise<Span> {
    const row = await this.db.row<Row>(
      // A repository with no named dependency has no dependencies to
      // date, as `total_dependencies` counts them in the export.
      `SELECT formatDateTime(min(newest), '%Y-%m-%d', 'UTC') AS observed_from,
              formatDateTime(max(newest), '%Y-%m-%d', 'UTC') AS observed_to,
              count() AS repositories
       FROM (
         SELECT max(a.observed_at) AS newest
         FROM artifacts AS a
         WHERE ${CURRENT.join(' AND ')}
         GROUP BY a.repository_id
         HAVING countIf(a.name != '') > 0
       )`,
    );
    // An empty store's minimum is the epoch, which would read as a real
    // date; none is none, as D1 stores it.
    if (num(row?.['repositories']) === 0) return { from: '', to: '' };
    return {
      from: String(row?.['observed_from'] ?? ''),
      to: String(row?.['observed_to'] ?? ''),
    };
  }

  /** Set by the endpoint from configuration; see `meta`. */
  generator = 'chatsbom/clickhouse';
}
