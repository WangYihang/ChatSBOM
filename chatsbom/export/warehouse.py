"""The Parquet export's queries, asked of the warehouse (#148).

`queries.py`'s four, ported from ClickHouse to DuckDB over the warehouse
(#131), as `snapshot/tables.py` ported `export d1`'s: each one statement
whose rows are a table's, column for column, in the order `EXPORT_SCHEMA`
declares, and in the total order its ClickHouse query gives them. The
warehouse's `facts`, `current_observations`, `current_scans` and
`corpus` are ClickHouse's `facts`, `current_artifacts` and `corpus`, by
one rule (`warehouse/rollups.py`), which `tests/warehouse/parity_test.py`
holds equal; so where the engines agree, a table is `export parquet`'s,
row for row, and written by the same writer it is the same bytes, under
the same name (`tests/parquet_golden_test.py`).

A repository's row and the adoption series are the snapshot's own
statements (`snapshot/tables.py`): the dataset the site serves and the
one published as files are one dataset, and cannot be two. Where that
is not `export parquet` from ClickHouse, it is by design:

- **Adoption over time** is `mv_package_month_intervals` (owner decision
  Q9 on #128): a repository counts in every month between two scans that
  both show the package, where `HISTORY_QUERY` counts the months of the
  scans alone. The two are the same series wherever no month falls
  between two such scans.
- **A repository with no dependency** is dated by its newest current
  scan, or not at all when it has none: `REPOSITORIES_QUERY` dates it by
  the day `db index` wrote its row, which says when the indexer ran,
  and the warehouse keeps no such day. The manifest's freshness leaves
  these dates out either way (`repository_freshness`).
- **When a commit was scanned** is when the store first had it, the
  earlier of its Syft document and its manifests (`store._first_had`),
  where ClickHouse has the document's instant (`warehouse/parity.py`).
  A repository whose manifests were fetched the day before Syft made
  the document is dated that day, and so is the manifest's span when it
  is the oldest.

`export parquet` reads these (`parquet.export_warehouse`).
"""
from __future__ import annotations

from chatsbom.snapshot import tables

#: Made before any table is read: the snapshot's repository rows.
PREPARED: tuple[str, ...] = (tables.REPOSITORIES,)

#: `REPOSITORIES_QUERY`'s: the corpus, every repository of it, scanned
#: or not, by stars and then id. The ref, commit and manifests are of its
#: current Syft scan.
REPOSITORIES = """
SELECT id, owner, repo, stars, language, github_language, language_bucket,
       ecosystem_list AS ecosystems, url, description, license_spdx_id,
       pushed_at, observed_at, sbom_ref, sbom_commit_sha,
       direct_dependencies, total_dependencies, manifest_sources
FROM snapshot_repositories
ORDER BY stars DESC, id
""".strip()

#: `ARTIFACTS_QUERY`'s: one row a fact, ordered by every column a fact is
#: distinct in, name first, so that no two rows tie.
ARTIFACTS = f"""
SELECT repository_id, name, version, type, found_by, relationship, source,
       version_kind
FROM facts
ORDER BY {', '.join(tables.FACT_ORDER)}
""".strip()

#: `LICENSES_QUERY`'s: every licence a package carries, by the type its
#: collector gave it, and a package with none under the empty licence,
#: which `unnest`, as `ARRAY JOIN`, drops; the widest 500, ties broken by
#: the whole key.
LICENSES = """
SELECT license, type,
       count(DISTINCT name) AS package_count,
       count(DISTINCT repository_id) AS repository_count
FROM (
    SELECT unnest(licenses) AS license, type, name, repository_id
    FROM current_observations
    WHERE name != ''
    UNION ALL
    SELECT '' AS license, type, name, repository_id
    FROM current_observations
    WHERE name != '' AND len(licenses) = 0
)
GROUP BY license, type
ORDER BY repository_count DESC, license, type
LIMIT 500
""".strip()

#: Why a query of the warehouse stops partway, as `queries.whole` says
#: it, beside DuckDB's own error.
STOPPED = (
    'DuckDB stops a query that needs more memory than '
    'CHATSBOM_DUCKDB_MEMORY_LIMIT and cannot spill what does not fit, or '
    'has no disk left to spill it to, beside the warehouse. Raise the '
    'limit, or make room there.'
)

#: Each table's statement, by the table's name.
QUERIES: dict[str, str] = {
    'repositories': REPOSITORIES,
    'artifacts': ARTIFACTS,
    'licenses': LICENSES,
    'history': tables.HISTORY,
}
