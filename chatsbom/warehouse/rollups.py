"""What a pass derives: the current facts, and the rollups from them.

**Current** is one rule (#128 §2.3): each repository's newest scan of
each source, of the corpus. It takes the place of ClickHouse's
`corpus`, `current_artifacts` and `facts` views and of the pointers a
repository row keeps there (`CURRENT_OBSERVATION` in `core/schema.py`).
A Syft or manifest scan is newer by its commit's document, a graph by
the instant it states, so a graph fetched again while Syft's target
stood still replaces the graph before it, as #22 has it. A scan that
saw nothing is current too, and so the one before it is not.

A grouping by a name the SELECT gives is `GROUP BY ALL`: DuckDB binds
a bare name to a column of the input first, and `repositories` has a
`language` of its own beside the bucket.
"""
from __future__ import annotations

from typing import TYPE_CHECKING

from chatsbom.core.schema import LANGUAGE_BUCKETS

if TYPE_CHECKING:
    import duckdb

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

#: Every rollup, in dependency order: each reads only what is above it.
ROLLUPS: tuple[tuple[str, str], ...] = ()


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
