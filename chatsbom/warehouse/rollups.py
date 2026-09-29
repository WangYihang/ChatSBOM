"""What a pass derives: the current facts, and the rollups from them.

**Current** is one rule (#128 §2.3): each repository's newest scan of
each source, of the corpus. It takes the place of ClickHouse's
`corpus`, `current_artifacts` and `facts` views and of the pointers a
repository row keeps there (`CURRENT_OBSERVATION` in `core/schema.py`).
A Syft or manifest scan is newer by its commit's document, a graph by
the instant it states, so a graph fetched again while Syft's target
stood still replaces the graph before it, as #22 has it. A scan that
saw nothing is current too, and so the one before it is not.

**The rollups** are `core/rollups.py`'s, by the same names, nearly one
to one, as #120 corrected them: `uniqExact` is `count(DISTINCT)`,
`countIf` a `FILTER`, `ARRAY JOIN` an `UNNEST`, and `transform` the
`CASE` `canonical` spells. Where a ClickHouse aggregate of nothing is 0
and DuckDB's NULL, the port says 0. Each is a table, made again on each
pass, where ClickHouse refreshes a materialized view. `parity.py`
compares every one with ClickHouse's, on the same input.

**Adoption over time**, `mv_package_month_intervals`, is new (Q9): a
repository counts in every month between two consecutive scans that
both show the package. `mv_package_month`, the months of the scans
alone, is kept for the parity check and nothing else.

Strings compare as bytes in both engines, so a tie broken by name
breaks the same way. A grouping by a name the SELECT gives is `GROUP BY
ALL`: DuckDB binds a bare name to a column of the input first, and
`repositories` has a `language` of its own beside the bucket.
ClickHouse's `lower` folds ASCII only and DuckDB's all of Unicode;
GitHub's language names are ASCII.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from chatsbom.core.ecosystems import RENAMES
from chatsbom.core.schema import LANGUAGE_BUCKETS
from chatsbom.models.provenance import DEPGRAPH
from chatsbom.models.provenance import MANIFEST
from chatsbom.models.provenance import SYFT

if TYPE_CHECKING:
    import duckdb


def canonical(column: str = 'type') -> str:
    """`core/ecosystems.canonical_sql`, in SQL DuckDB reads: a raw type
    to its canonical name, and a type the mapping has never seen to
    itself. From the one mapping, `RENAMES`."""
    if not RENAMES:
        return column
    cases = ' '.join(
        f"WHEN '{raw}' THEN '{name}'" for raw, name in RENAMES.items()
    )
    return f'(CASE {column} {cases} ELSE {column} END)'


def language_bucket(column: str) -> str:
    """`core/schema.language_bucket_sql`: the language lowercased when it
    is one of the top twelve, `none` when GitHub names none, `other`
    for the rest (owner decision D7)."""
    return (
        f"(CASE WHEN {column} = '' THEN 'none' "
        f'WHEN lower({column}) IN (SELECT language FROM language_buckets) '
        f"THEN lower({column}) ELSE 'other' END)"
    )


#: A fact's ecosystem.
ECOSYSTEM = canonical('type')

#: The first instant that is a date: an unset one is 1970-01-02
#: (`instants.UNSET`), and a scan dated no later cannot be placed.
DATED = "TIMESTAMP '1970-01-02 00:00:01'"

# -- the current facts --------------------------------------------------

#: The newest scan of each repository and source, of the corpus. A tie
#: in the instant, to the second, goes to the greater input key.
CURRENT_SCANS = """
CREATE TABLE current_scans AS
SELECT s.*
FROM scans AS s
WHERE s.repository_id IN (SELECT id FROM corpus)
QUALIFY row_number() OVER (
    PARTITION BY s.repository_id, s.source
    ORDER BY s.observed_at DESC, s.input_key DESC, s.tool DESC
) = 1
""".strip()

#: `current_artifacts`: what the current scans saw.
CURRENT_OBSERVATIONS = """
CREATE VIEW current_observations AS
SELECT o.*
FROM observations AS o
WHERE o.scan_id IN (SELECT scan_id FROM current_scans)
""".strip()

#: `facts`: one row per dependency fact, however many manifests or
#: catalogers reported it. `artifact_id`, the per-manifest
#: discriminator, is what is left out.
FACTS = """
CREATE TABLE facts AS
SELECT DISTINCT repository_id, name, version, type, found_by,
                relationship, source, version_kind
FROM current_observations
""".strip()

#: A repository's ecosystems: of its current scans' artifacts and
#: manifests, as `db index` writes `repositories.ecosystems` of the
#: scan it reads (`ecosystems_of`). One row per repository and
#: ecosystem, of the corpus.
REPOSITORY_ECOSYSTEMS = """
CREATE TABLE repository_ecosystems AS
SELECT DISTINCT s.repository_id, e.ecosystem
FROM current_scans AS s, UNNEST(s.ecosystems) AS e(ecosystem)
""".strip()

#: `language_buckets`: the twelve GitHub languages with the most
#: repositories in the corpus, lowercased; a tie by name.
LANGUAGE_BUCKETS_TABLE = f"""
CREATE TABLE language_buckets AS
SELECT lower(r.github_language) AS language, count(*) AS repositories
FROM corpus AS c
JOIN repositories AS r ON r.id = c.id
WHERE r.github_language != ''
GROUP BY ALL
ORDER BY repositories DESC, language
LIMIT {LANGUAGE_BUCKETS}
""".strip()

#: Derived before the rollups, in this order.
CURRENT: tuple[tuple[str, str], ...] = (
    ('current_scans', CURRENT_SCANS),
    ('current_observations', CURRENT_OBSERVATIONS),
    ('facts', FACTS),
    ('repository_ecosystems', REPOSITORY_ECOSYSTEMS),
    ('language_buckets', LANGUAGE_BUCKETS_TABLE),
)

# -- the rollups, as `core/rollups.py` declares them ----------------------

PACKAGE_ECOSYSTEM = f"""
CREATE TABLE mv_package_ecosystem AS
SELECT
    {ECOSYSTEM} AS ecosystem,
    name,
    count(DISTINCT repository_id) AS repositories,
    count(DISTINCT repository_id) FILTER (WHERE relationship = 'direct')
        AS direct_repositories,
    count(*) AS records,
    count(*) FILTER (WHERE relationship = 'direct') AS direct_records,
    count(*) FILTER (WHERE relationship = 'transitive') AS transitive_records,
    count(*) FILTER (WHERE relationship = 'unknown') AS unknown_records,
    count(*) FILTER (WHERE source = '{SYFT}') AS syft_records,
    count(*) FILTER (WHERE source = '{DEPGRAPH}') AS depgraph_records,
    count(*) FILTER (WHERE source = '{MANIFEST}') AS manifest_records
FROM facts
GROUP BY ALL
""".strip()

REPOSITORY_DEPS = f"""
CREATE TABLE mv_repository_deps AS
SELECT
    repository_id,
    count(DISTINCT name) AS packages,
    count(DISTINCT name) FILTER (WHERE relationship = 'direct')
        AS direct_packages,
    count(*) AS records,
    count(*) FILTER (WHERE source = '{SYFT}') AS syft_records,
    count(*) FILTER (WHERE source = '{DEPGRAPH}') AS depgraph_records,
    count(*) FILTER (WHERE source = '{MANIFEST}') AS manifest_records
FROM facts
GROUP BY repository_id
""".strip()

#: The unknown bucket, keyed empty, is a row of its own: `UNNEST`, as
#: `ARRAY JOIN`, drops a row whose list is empty. An aggregate with no
#: GROUP BY is one row in both engines, even of nothing.
LICENSES = """
CREATE TABLE mv_licenses AS
SELECT license, count(DISTINCT repository_id) AS repositories,
       count(DISTINCT name) AS packages
FROM (
    SELECT repository_id, name, unnest(licenses) AS license
    FROM current_observations
)
GROUP BY license
UNION ALL
SELECT '' AS license, count(DISTINCT repository_id) AS repositories,
       count(DISTINCT name) AS packages
FROM current_observations
WHERE len(licenses) = 0
""".strip()

ECOSYSTEM_TOTALS = """
CREATE TABLE mv_ecosystem_totals AS
SELECT
    ecosystem,
    CAST(sum(direct_records) AS UBIGINT) AS direct_records,
    CAST(sum(transitive_records) AS UBIGINT) AS transitive_records,
    CAST(sum(unknown_records) AS UBIGINT) AS unknown_records,
    CAST(sum(syft_records) AS UBIGINT) AS syft_records,
    CAST(sum(depgraph_records) AS UBIGINT) AS depgraph_records,
    CAST(sum(manifest_records) AS UBIGINT) AS manifest_records,
    CAST(sum(records) AS UBIGINT) AS records
FROM mv_package_ecosystem
GROUP BY ecosystem
""".strip()

PACKAGES = """
CREATE TABLE mv_packages AS
SELECT
    name,
    count(DISTINCT repository_id) AS repositories,
    count(DISTINCT repository_id) FILTER (WHERE relationship = 'direct')
        AS direct_repositories
FROM facts
GROUP BY name
""".strip()

TOTALS = """
CREATE TABLE mv_totals AS
SELECT
    (SELECT count(*) FROM mv_repository_deps) AS repositories,
    (SELECT CAST(coalesce(sum(records), 0) AS UBIGINT)
     FROM mv_repository_deps) AS dependencies,
    (SELECT count(*) FROM mv_packages) AS packages,
    (SELECT CAST(coalesce(sum(direct_records + transitive_records), 0)
                 AS UBIGINT)
     FROM mv_ecosystem_totals) AS classified,
    (SELECT count(*) FROM corpus) AS tracked
""".strip()

EDGES_FORWARD = """
CREATE TABLE mv_edges_forward AS
SELECT parent, child, CAST(sum(repositories) AS UBIGINT) AS repositories
FROM edges
GROUP BY parent, child
""".strip()

#: The adoption series by the months of the scans: every observation of
#: the corpus's repositories, in the UTC month of its scan. Kept for the
#: parity check alone: the series is `mv_package_month_intervals`.
PACKAGE_MONTH = """
CREATE TABLE mv_package_month AS
SELECT
    o.name,
    o.source,
    strftime(s.observed_at, '%Y-%m') AS month,
    count(DISTINCT o.repository_id) AS repositories,
    count(DISTINCT o.repository_id) FILTER (WHERE o.relationship = 'direct')
        AS direct_repositories
FROM observations AS o
JOIN scans AS s USING (scan_id)
WHERE o.repository_id IN (SELECT id FROM corpus)
GROUP BY ALL
""".strip()

PACKAGE_TYPE = """
CREATE TABLE mv_package_type AS
SELECT
    name,
    type,
    count(DISTINCT repository_id) AS repositories,
    count(DISTINCT repository_id) FILTER (WHERE relationship = 'direct')
        AS direct_repositories
FROM current_observations
GROUP BY name, type
""".strip()

#: What is left out of the resolved versions is one row per kind, with
#: an empty version: `constrained` counts repositories (#120).
PACKAGE_VERSION = """
CREATE TABLE mv_package_version AS
SELECT name, version_kind, listed AS version,
       count(DISTINCT repository_id) AS repositories
FROM (
    SELECT name, version_kind, repository_id,
           CASE WHEN version_kind = 'resolved' THEN version ELSE '' END
               AS listed
    FROM current_observations
)
GROUP BY name, version_kind, listed
""".strip()

DEPENDENCY_BUCKETS = """
CREATE TABLE mv_dependency_buckets AS
SELECT
    CASE WHEN packages < 10 THEN 0 WHEN packages < 25 THEN 1
         WHEN packages < 100 THEN 2 WHEN packages < 250 THEN 3
         WHEN packages < 1000 THEN 4 ELSE 5 END AS position,
    CASE WHEN packages < 10 THEN '1-9' WHEN packages < 25 THEN '10-24'
         WHEN packages < 100 THEN '25-99' WHEN packages < 250 THEN '100-249'
         WHEN packages < 1000 THEN '250-999' ELSE '1000+' END AS bucket,
    count(*) AS repositories
FROM mv_repository_deps
GROUP BY ALL
""".strip()

VERSION_KINDS = """
CREATE TABLE mv_version_kinds AS
SELECT version_kind, count(*) AS records
FROM facts
GROUP BY version_kind
""".strip()

LANGUAGE_COVERAGE = f"""
CREATE TABLE mv_language_coverage AS
SELECT
    {language_bucket('r.github_language')} AS language,
    count(*) AS repositories,
    count(d.repository_id) AS with_sbom,
    count(*) FILTER (WHERE d.syft_records > 0) AS with_syft,
    count(*) FILTER (WHERE d.depgraph_records > 0) AS with_depgraph,
    count(*) FILTER (WHERE d.manifest_records > 0) AS with_manifest
FROM corpus AS c
JOIN repositories AS r ON r.id = c.id
LEFT JOIN mv_repository_deps AS d ON d.repository_id = c.id
GROUP BY ALL
""".strip()

ECOSYSTEM_COVERAGE = f"""
CREATE TABLE mv_ecosystem_coverage AS
SELECT
    e.ecosystem,
    count(*) AS repositories,
    count(*) FILTER (WHERE x.records > 0) AS with_any,
    count(*) FILTER (WHERE x.syft > 0) AS with_syft,
    count(*) FILTER (WHERE x.depgraph > 0) AS with_depgraph,
    count(*) FILTER (WHERE x.manifest > 0) AS with_manifest
FROM repository_ecosystems AS e
LEFT JOIN (
    SELECT
        repository_id,
        {ECOSYSTEM} AS ecosystem,
        count(*) AS records,
        count(*) FILTER (WHERE source = '{SYFT}') AS syft,
        count(*) FILTER (WHERE source = '{DEPGRAPH}') AS depgraph,
        count(*) FILTER (WHERE source = '{MANIFEST}') AS manifest
    FROM facts
    GROUP BY ALL
) AS x ON x.repository_id = e.repository_id AND x.ecosystem = e.ecosystem
GROUP BY e.ecosystem
""".strip()

TOP_PACKAGES = """
CREATE TABLE mv_top_packages AS
WITH by_name AS (
    SELECT '' AS ecosystem, name, repositories, direct_repositories
    FROM mv_packages
    UNION ALL
    SELECT ecosystem, name, repositories, direct_repositories
    FROM mv_package_ecosystem
    WHERE ecosystem != ''
)
SELECT ecosystem, direct_only, name, repositories, direct_repositories, rank
FROM (
    SELECT ecosystem, 0 AS direct_only, name, repositories,
           direct_repositories,
           row_number() OVER (PARTITION BY ecosystem
                              ORDER BY repositories DESC, name) AS rank
    FROM by_name
    UNION ALL
    SELECT ecosystem, 1 AS direct_only, name, repositories,
           direct_repositories,
           row_number() OVER (PARTITION BY ecosystem
                              ORDER BY direct_repositories DESC, name) AS rank
    FROM by_name
)
WHERE rank <= 100
""".strip()

EDGE_AMBIGUITY = f"""
CREATE TABLE mv_edge_ambiguity AS
WITH ambiguous AS (
    SELECT name FROM mv_package_type GROUP BY name
    HAVING count(DISTINCT {canonical('type')}) > 1
)
SELECT
    (SELECT count(*) FROM mv_packages) AS names,
    (SELECT count(*) FROM ambiguous) AS ambiguous_names,
    (SELECT count(*) FROM mv_edges_forward) AS edges,
    (SELECT count(*) FROM mv_edges_forward
     WHERE child IN (SELECT name FROM ambiguous)
        OR parent IN (SELECT name FROM ambiguous)) AS ambiguous_edges,
    (SELECT coalesce(max(packages), 0) FROM mv_repository_deps)
        AS largest_repository
""".strip()

#: Adoption over time by intervals (owner decision Q9 on #128).
#:
#: A repository counts for a package in every month from one scan that
#: shows it to the next scan of the same source, when that one shows it
#: too: the package is known to have held all along. Consecutive scans
#: that all show it are one run, and a run covers every month from its
#: first scan's to its last's. A scan that does not show the package
#: ends the run, so a dependency that disappears counts in the month of
#: the last scan that showed it and in none after, until a scan shows it
#: again. After a repository's newest scan nothing is known, and it
#: counts no further.
#:
#: Per source, as `mv_package_month` is: Syft and the graph measure
#: differently, and a run across both would draw a line between two
#: instruments. `direct_repositories` is the same over the scans that
#: show the package as declared.
#:
#: A scan with no date, one whose document gave none, cannot be placed
#: in a month, and is left out: it neither counts nor ends a run. Only
#: the corpus's repositories count, every scan of them.
PACKAGE_MONTH_INTERVALS = f"""
CREATE TABLE mv_package_month_intervals AS
WITH ordered AS (
    SELECT scan_id, repository_id, source,
           date_trunc('month', observed_at) AS month,
           row_number() OVER (
               PARTITION BY repository_id, source
               ORDER BY observed_at, input_key, tool
           ) AS position
    FROM scans
    WHERE repository_id IN (SELECT id FROM corpus)
      AND observed_at >= {DATED}
),
shown AS (
    SELECT s.repository_id, s.source, o.name, s.position, s.month,
           bool_or(o.relationship = 'direct') AS declared
    FROM observations AS o
    JOIN ordered AS s USING (scan_id)
    GROUP BY ALL
),
runs AS (
    SELECT repository_id, source, name, declared, min(month) AS first,
           max(month) AS last
    FROM (
        SELECT repository_id, source, name, month, false AS declared,
               position - row_number() OVER (
                   PARTITION BY repository_id, source, name
                   ORDER BY position
               ) AS run
        FROM shown
        UNION ALL
        SELECT repository_id, source, name, month, true AS declared,
               position - row_number() OVER (
                   PARTITION BY repository_id, source, name
                   ORDER BY position
               ) AS run
        FROM shown
        WHERE declared
    )
    GROUP BY repository_id, source, name, declared, run
),
months AS (
    SELECT repository_id, source, name, declared,
           unnest(generate_series(first, last, INTERVAL 1 MONTH)) AS month
    FROM runs
)
SELECT
    name,
    source,
    strftime(month, '%Y-%m') AS month,
    count(DISTINCT repository_id) FILTER (WHERE NOT declared)
        AS repositories,
    count(DISTINCT repository_id) FILTER (WHERE declared)
        AS direct_repositories
FROM months
GROUP BY ALL
""".strip()

#: Every rollup, in dependency order: each reads only what is above it.
#: The names are ClickHouse's (`core/rollups.REFRESH_ORDER`), and the
#: last is the warehouse's own.
ROLLUPS: tuple[tuple[str, str], ...] = (
    ('mv_package_ecosystem', PACKAGE_ECOSYSTEM),
    ('mv_repository_deps', REPOSITORY_DEPS),
    ('mv_licenses', LICENSES),
    ('mv_ecosystem_totals', ECOSYSTEM_TOTALS),
    ('mv_packages', PACKAGES),
    ('mv_edges_forward', EDGES_FORWARD),
    ('mv_package_month', PACKAGE_MONTH),
    ('mv_package_type', PACKAGE_TYPE),
    ('mv_package_version', PACKAGE_VERSION),
    ('mv_dependency_buckets', DEPENDENCY_BUCKETS),
    ('mv_version_kinds', VERSION_KINDS),
    ('mv_language_coverage', LANGUAGE_COVERAGE),
    ('mv_ecosystem_coverage', ECOSYSTEM_COVERAGE),
    ('mv_totals', TOTALS),
    ('mv_top_packages', TOP_PACKAGES),
    ('mv_edge_ambiguity', EDGE_AMBIGUITY),
    ('mv_package_month_intervals', PACKAGE_MONTH_INTERVALS),
)


def derive(
    con: duckdb.DuckDBPyConnection,
    timed: dict[str, float] | None = None,
) -> None:
    """The current facts, then every rollup, from the tables as the pass
    left them. With `timed`, how long each took, in seconds, by name."""
    import time

    for name, sql in (*CURRENT, *ROLLUPS):
        started = time.perf_counter()
        con.execute(sql)
        if timed is not None:
            timed[name] = time.perf_counter() - started
