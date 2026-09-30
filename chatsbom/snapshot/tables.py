"""What each table of a snapshot holds, asked of the warehouse.

The port of `export d1` (`chatsbom/export/d1.py`) from ClickHouse to the
warehouse (#132): its queries (`export/queries.py`), its interning
(`Lookups`) and its aggregate script (`aggregate_sql`), each as one
DuckDB statement whose rows are a table's, column for column, in the
order `export d1` writes them. The warehouse's `facts`,
`current_observations`, `current_scans` and `corpus` are ClickHouse's
`facts`, `current_artifacts` and `corpus`, by one rule
(`warehouse/rollups.py`), and `tests/warehouse/parity_test.py` holds
them equal; so where the two engines agree, a snapshot's rows are
`export d1`'s, id for id.

Where a snapshot is not `export d1` by design, the statement says so:

- **Adoption over time** is `mv_package_month_intervals` (owner
  decision Q9 on #128): a repository counts in every month between two
  scans that both show the package, where `export d1` counts the months
  of the scans alone.
- **A repository with no dependency** is dated by its newest current
  scan, or not at all: `export d1` has the day `db index` wrote its row,
  which says when the indexer ran, and the warehouse keeps no such day.
  No answer reads it.

`aggregate_sql` computes the aggregates in SQLite, from the rows D1 has
just been sent; here DuckDB computes them from the same facts, 7.5 s of
a pass at the documented shape. `tests/snapshot/write_test.py` runs
`aggregate_sql` over a snapshot's own rows and holds every aggregate to
what it gives.

Every statement orders its rows totally: the rows are written, and the
snapshot's id hashed, in that order.
"""
from __future__ import annotations

from chatsbom.export.d1 import TOP_PACKAGES_DEPTH
from chatsbom.models.provenance import DEPGRAPH
from chatsbom.models.provenance import MANIFEST
from chatsbom.models.provenance import SYFT
from chatsbom.models.relationship import DIRECT
from chatsbom.warehouse.rollups import canonical
from chatsbom.warehouse.rollups import language_bucket

#: A fact's ecosystem, under the name the page shows, as `kinds` has it.
ECOSYSTEM = canonical('type')

#: The order `export d1` writes the facts in, and so interns their
#: strings in: `ARTIFACTS_QUERY`'s, every column of a fact, the raw type.
FACT_ORDER = (
    'name', 'repository_id', 'version', 'type', 'found_by', 'relationship',
    'source', 'version_kind',
)

#: A fact as one value that compares as that order does: a struct's
#: fields are compared one after the other.
_FACT = '{' + ', '.join(f"'{column}': {column}" for column in FACT_ORDER) + '}'

# -- made first, in DuckDB, for the tables to read ------------------------

#: The package names, in their order, which is the order `export d1`
#: first meets them in: its artifacts come by name. And how many
#: repositories depend on each, which D1's script sets afterwards.
PACKAGES = """
CREATE TEMP TABLE snapshot_packages AS
SELECT row_number() OVER (ORDER BY name) AS id,
       name,
       count(DISTINCT repository_id) AS repositories
FROM facts
GROUP BY name
""".strip()

#: The versions, in the order `export d1` first meets them: the first
#: fact with a version is the one least by name and then repository,
#: and two versions first met in one repository's facts of one name
#: are met in their own order.
VERSIONS = """
CREATE TEMP TABLE snapshot_versions AS
SELECT row_number() OVER (ORDER BY first, version) AS id, version
FROM (
    SELECT version,
           min({'name': name, 'repository_id': repository_id}) AS first
    FROM facts
    GROUP BY version
)
""".strip()

#: The combinations of a fact's five short columns, the type under its
#: canonical name, in the order `export d1` first meets them: by the
#: least of their facts, every column of it compared.
KINDS = f"""
CREATE TEMP TABLE snapshot_kinds AS
SELECT row_number() OVER (ORDER BY first) AS id,
       ecosystem AS type, found_by, relationship, source, version_kind
FROM (
    SELECT {ECOSYSTEM} AS ecosystem, found_by, relationship, source,
           version_kind,
           min({_FACT}) AS first
    FROM facts
    GROUP BY ALL
)
""".strip()

#: `REPOSITORIES_QUERY`'s: the corpus, every repository of it, scanned
#: or not, with its ecosystems as a list for the coverage below.
#:
#: Its date is the newest of its current scans that saw something,
#: which is ClickHouse's newest current row; for one with no
#: dependency, its newest current scan, or none (the module says why).
#: Its ref and commit are its current Syft scan's, and so are the
#: manifests read for that scan's verdicts, which D1 does not carry and
#: the Parquet export does (`export/warehouse.py`).
REPOSITORIES = f"""
CREATE TEMP TABLE snapshot_repositories AS
WITH counted AS (
    SELECT repository_id,
           count(DISTINCT name) FILTER (
               WHERE name != '' AND relationship = '{DIRECT}'
           ) AS direct_dependencies,
           count(DISTINCT name) FILTER (WHERE name != '')
               AS total_dependencies
    FROM current_observations
    GROUP BY repository_id
),
looked AS (
    SELECT repository_id,
           max(observed_at) FILTER (WHERE observations > 0) AS saw,
           max(observed_at) AS looked
    FROM current_scans
    GROUP BY repository_id
),
syft AS (
    SELECT repository_id, ref, commit_sha, manifest_sources
    FROM current_scans
    WHERE source = '{SYFT}'
),
ecosystems AS (
    SELECT repository_id, list_sort(list(ecosystem)) AS ecosystems
    FROM repository_ecosystems
    GROUP BY repository_id
)
SELECT r.id,
       r.owner,
       r.repo,
       r.stars,
       lower(r.github_language) AS language,
       r.github_language,
       {language_bucket('r.github_language')} AS language_bucket,
       coalesce(e.ecosystems, []::VARCHAR[]) AS ecosystem_list,
       r.url,
       r.description,
       r.license_spdx_id,
       strftime(r.pushed_at, '%Y-%m-%d') AS pushed_at,
       coalesce(strftime(coalesce(l.saw, l.looked), '%Y-%m-%d'), '')
           AS observed_at,
       coalesce(s.ref, '') AS sbom_ref,
       coalesce(s.commit_sha, '') AS sbom_commit_sha,
       coalesce(c.direct_dependencies, 0) AS direct_dependencies,
       coalesce(c.total_dependencies, 0) AS total_dependencies,
       coalesce(s.manifest_sources, []::VARCHAR[]) AS manifest_sources
FROM corpus AS k
JOIN repositories AS r ON r.id = k.id
LEFT JOIN counted AS c ON c.repository_id = r.id
LEFT JOIN looked AS l ON l.repository_id = r.id
LEFT JOIN syft AS s ON s.repository_id = r.id
LEFT JOIN ecosystems AS e ON e.repository_id = r.id
""".strip()

#: Made before any table is read, in this order.
PREPARED: tuple[str, ...] = (PACKAGES, VERSIONS, KINDS, REPOSITORIES)

# -- each table's rows, in its columns' order -----------------------------

#: The facts as four integers, in `ARTIFACTS_QUERY`'s order, so that a
#: package's rows are together in the file.
ARTIFACTS = f"""
SELECT f.repository_id, p.id AS package_id, v.id AS version_id,
       k.id AS kind_id
FROM facts AS f
JOIN snapshot_packages AS p ON p.name = f.name
JOIN snapshot_versions AS v ON v.version = f.version
JOIN snapshot_kinds AS k
  ON k.type = {canonical('f.type')}
 AND k.found_by = f.found_by
 AND k.relationship = f.relationship
 AND k.source = f.source
 AND k.version_kind = f.version_kind
ORDER BY {', '.join(f'f.{column}' for column in FACT_ORDER)}
""".strip()

#: `OBSERVATIONS_QUERY`'s: each source's current scan of a repository,
#: dated, where it saw something.
OBSERVATIONS = """
SELECT repository_id, source, strftime(observed_at, '%Y-%m-%d') AS observed_at
FROM current_scans
WHERE observations > 0
ORDER BY repository_id, source
""".strip()

REPOSITORY_ROWS = """
SELECT id, owner, repo, stars, language, github_language, language_bucket,
       CAST(to_json(ecosystem_list) AS VARCHAR) AS ecosystems, url,
       description, license_spdx_id, pushed_at, observed_at, sbom_ref,
       sbom_commit_sha, direct_dependencies, total_dependencies
FROM snapshot_repositories
ORDER BY id
""".strip()

PACKAGE_ROWS = """
SELECT id, name, repositories FROM snapshot_packages ORDER BY id
""".strip()

VERSION_ROWS = 'SELECT id, version FROM snapshot_versions ORDER BY id'

KIND_ROWS = """
SELECT id, type, found_by, relationship, source, version_kind
FROM snapshot_kinds
ORDER BY id
""".strip()

#: `D1_LICENSES_QUERY`'s: one row a licence, a package with none under
#: the empty one, the widest 500.
LICENSES = """
SELECT license,
       count(DISTINCT repository_id) AS repository_count,
       count(DISTINCT name) AS package_count
FROM (
    SELECT unnest(licenses) AS license, name, repository_id
    FROM current_observations
    WHERE name != ''
    UNION ALL
    SELECT '' AS license, name, repository_id
    FROM current_observations
    WHERE name != '' AND len(licenses) = 0
)
GROUP BY license
ORDER BY repository_count DESC, license
LIMIT 500
""".strip()

#: Adoption over time, by intervals (Q9), per source, of every named
#: package: in `HISTORY_QUERY`'s order.
HISTORY = """
SELECT name, month, source, repositories AS repository_count,
       direct_repositories AS direct_count
FROM mv_package_month_intervals
WHERE name != ''
ORDER BY name, source, month
""".strip()

AGG_TOTALS = """
SELECT
    (SELECT count(*) FROM snapshot_repositories
     WHERE total_dependencies > 0) AS repositories,
    (SELECT count(*) FROM facts) AS dependencies,
    (SELECT count(*) FROM snapshot_packages) AS packages,
    (SELECT count(*) FROM facts WHERE relationship <> 'unknown')
        AS classified,
    (SELECT count(*) FROM snapshot_repositories) AS tracked
""".strip()

AGG_RELATIONSHIP_SPLIT = f"""
SELECT ecosystem, relationship, records
FROM (
    SELECT '' AS ecosystem, relationship, count(*) AS records
    FROM facts
    GROUP BY relationship
    UNION ALL
    SELECT {ECOSYSTEM} AS ecosystem, relationship, count(*) AS records
    FROM facts
    WHERE {ECOSYSTEM} <> ''
    GROUP BY ALL
)
ORDER BY ecosystem, relationship
""".strip()

#: Facts, counted by the source that made them.
_BY_SOURCE = (
    f"       count(*) FILTER (WHERE source = '{SYFT}') AS syft,\n"
    f"       count(*) FILTER (WHERE source = '{DEPGRAPH}') AS depgraph,\n"
    f"       count(*) FILTER (WHERE source = '{MANIFEST}') AS manifest"
)

AGG_LANGUAGE_COVERAGE = f"""
SELECT r.language_bucket AS language,
       count(*) AS repositories,
       count(*) FILTER (WHERE r.total_dependencies > 0) AS with_sbom,
       count(*) FILTER (WHERE s.syft > 0) AS with_syft,
       count(*) FILTER (WHERE s.depgraph > 0) AS with_depgraph,
       count(*) FILTER (WHERE s.manifest > 0) AS with_manifest
FROM snapshot_repositories AS r
LEFT JOIN (
    SELECT repository_id,
{_BY_SOURCE}
    FROM facts
    GROUP BY repository_id
) AS s ON s.repository_id = r.id
GROUP BY r.language_bucket
ORDER BY language
""".strip()

AGG_ECOSYSTEM_COVERAGE = f"""
SELECT e.ecosystem,
       count(*) AS repositories,
       count(*) FILTER (WHERE s.records > 0) AS with_any,
       count(*) FILTER (WHERE s.syft > 0) AS with_syft,
       count(*) FILTER (WHERE s.depgraph > 0) AS with_depgraph,
       count(*) FILTER (WHERE s.manifest > 0) AS with_manifest
FROM (
    SELECT id, unnest(ecosystem_list) AS ecosystem
    FROM snapshot_repositories
) AS e
LEFT JOIN (
    SELECT repository_id, {ECOSYSTEM} AS ecosystem, count(*) AS records,
{_BY_SOURCE}
    FROM facts
    GROUP BY ALL
) AS s ON s.repository_id = e.id AND s.ecosystem = e.ecosystem
GROUP BY e.ecosystem
ORDER BY e.ecosystem
""".strip()

AGG_TOP_PACKAGES = f"""
WITH counted AS (
    SELECT {ECOSYSTEM} AS ecosystem, name,
           count(DISTINCT repository_id) AS repository_count,
           count(DISTINCT repository_id) FILTER (
               WHERE relationship = '{DIRECT}'
           ) AS direct_count
    FROM facts
    GROUP BY ALL
),
overall AS (
    SELECT '' AS ecosystem, name,
           count(DISTINCT repository_id) AS repository_count,
           count(DISTINCT repository_id) FILTER (
               WHERE relationship = '{DIRECT}'
           ) AS direct_count
    FROM facts
    GROUP BY name
),
unioned AS (
    SELECT * FROM counted WHERE ecosystem <> ''
    UNION ALL
    SELECT * FROM overall
),
ranked AS (
    SELECT d.direct_only, u.ecosystem, u.name, u.repository_count,
           u.direct_count,
           row_number() OVER (
               PARTITION BY d.direct_only, u.ecosystem
               ORDER BY CASE WHEN d.direct_only = 1 THEN u.direct_count
                             ELSE u.repository_count END DESC,
                        u.name
           ) AS position
    FROM unioned AS u, (SELECT 0 AS direct_only UNION ALL SELECT 1) AS d
)
SELECT direct_only, ecosystem, position AS rank, name, repository_count,
       direct_count
FROM ranked
WHERE position <= {TOP_PACKAGES_DEPTH}
ORDER BY direct_only, ecosystem, position
""".strip()

#: The buckets' order is the labels', as SQLite groups them in D1's
#: script; the page orders by the position.
AGG_DEPENDENCY_BUCKETS = """
SELECT bucket, position, count(*) AS repositories
FROM (
    SELECT
        CASE
            WHEN total_dependencies = 0 THEN 'none'
            WHEN total_dependencies < 10 THEN '1-9'
            WHEN total_dependencies < 25 THEN '10-24'
            WHEN total_dependencies < 50 THEN '25-49'
            WHEN total_dependencies < 100 THEN '50-99'
            WHEN total_dependencies < 250 THEN '100-249'
            WHEN total_dependencies < 500 THEN '250-499'
            WHEN total_dependencies < 1000 THEN '500-999'
            ELSE '1000+'
        END AS bucket,
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
        END AS position
    FROM snapshot_repositories
)
GROUP BY bucket, position
ORDER BY bucket, position
""".strip()

AGG_SOURCE_COMPARISON = f"""
SELECT {ECOSYSTEM} AS ecosystem,
{_BY_SOURCE}
FROM facts
WHERE {ECOSYSTEM} <> ''
GROUP BY ALL
ORDER BY ecosystem
""".strip()

#: `db edges`' pairs, summed, between two packages of the facts: a pair
#: naming another is left out, as `export d1` leaves it. By name.
AGG_EDGES = """
SELECT p.id AS parent_id, c.id AS child_id, e.repositories
FROM mv_edges_forward AS e
JOIN snapshot_packages AS p ON p.name = e.parent
JOIN snapshot_packages AS c ON c.name = e.child
ORDER BY e.parent, e.child
""".strip()

#: Each table's statement, by the table's name.
ROWS: dict[str, str] = {
    'repositories': REPOSITORY_ROWS,
    'artifacts': ARTIFACTS,
    'observations': OBSERVATIONS,
    'packages': PACKAGE_ROWS,
    'versions': VERSION_ROWS,
    'kinds': KIND_ROWS,
    'licenses': LICENSES,
    'history': HISTORY,
    'agg_totals': AGG_TOTALS,
    'agg_relationship_split': AGG_RELATIONSHIP_SPLIT,
    'agg_language_coverage': AGG_LANGUAGE_COVERAGE,
    'agg_ecosystem_coverage': AGG_ECOSYSTEM_COVERAGE,
    'agg_top_packages': AGG_TOP_PACKAGES,
    'agg_dependency_buckets': AGG_DEPENDENCY_BUCKETS,
    'agg_source_comparison': AGG_SOURCE_COMPARISON,
    'agg_edges': AGG_EDGES,
}

#: The span of the data's age, as `export d1` makes it
#: (`repository_freshness`): the oldest and the newest date of the
#: repositories with dependencies, empty when there are none.
SPAN = """
SELECT coalesce(min(observed_at), ''), coalesce(max(observed_at), '')
FROM snapshot_repositories
WHERE total_dependencies > 0 AND observed_at != ''
""".strip()

#: The search snapshot the pass took for the corpus: empty when the
#: store has none, or the warehouse was made from rows.
CORPUS = "SELECT coalesce(max(corpus), '') FROM build"

#: Edges left out, for the report.
EDGES_LEFT_OUT = """
SELECT count(*)
FROM mv_edges_forward AS e
WHERE e.parent NOT IN (SELECT name FROM snapshot_packages)
   OR e.child NOT IN (SELECT name FROM snapshot_packages)
""".strip()
