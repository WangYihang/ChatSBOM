"""Warehouses made from rows, for the snapshots written from them (#132).

A snapshot is written from `warehouse.duckdb` and nothing else, so each
test makes one: ClickHouse-shaped rows loaded as `rows.load` loads the
contract corpus for the golden tests, the current facts and the rollups
derived as a pass derives them, and the `build` row a pass writes last,
which names the corpus.

`SHOP` is the small warehouse the tests read row by row: every case the
snapshot has to get right has a row here, and the comment beside it says
which.

And D1's aggregate script, `aggregate_sql`, which the snapshot's
aggregates are held to.
"""
from __future__ import annotations

from collections.abc import Iterable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

from chatsbom.snapshot.schema import AGG_DEPENDENCY_BUCKETS
from chatsbom.snapshot.schema import AGG_ECOSYSTEM_COVERAGE
from chatsbom.snapshot.schema import AGG_LANGUAGE_COVERAGE
from chatsbom.snapshot.schema import AGG_RELATIONSHIP_SPLIT
from chatsbom.snapshot.schema import AGG_SOURCE_COMPARISON
from chatsbom.snapshot.schema import AGG_TOP_PACKAGES
from chatsbom.snapshot.schema import AGG_TOTALS
from chatsbom.snapshot.tables import TOP_PACKAGES_DEPTH
from chatsbom.warehouse import connect
from chatsbom.warehouse.rollups import derive
from chatsbom.warehouse.rows import load
from chatsbom.warehouse.writer import Scan
from chatsbom.warehouse.writer import Writer
from tests import contract

UTC = timezone.utc


def at(year: int, month: int, day: int, hour: int = 0) -> datetime:
    """An instant in UTC."""
    return datetime(year, month, day, hour, tzinfo=UTC)


def aware(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """The seed's instants, which it writes without a zone, as the UTC
    they are."""
    return [
        {
            key: value.replace(tzinfo=UTC)
            if isinstance(value, datetime) and value.tzinfo is None
            else value
            for key, value in row.items()
        }
        for row in rows
    ]


def repository(
    id: int,
    owner: str,
    repo: str,
    stars: int,
    language: str,
    **fields: Any,
) -> dict[str, Any]:
    """A `repositories` row, as the warehouse keeps one."""
    return {
        'id': id, 'owner': owner, 'repo': repo, 'stars': stars,
        'url': f'https://github.com/{owner}/{repo}',
        'github_language': language, 'language': language,
        'pushed_at': at(2026, 9, 1), 'license_spdx_id': '',
        'description': '', 'snapshot': 'all-2026-09-01',
        **fields,
    }


def artifact(
    repository_id: int,
    name: str,
    version: str,
    type: str,
    *,
    observed_at: datetime,
    relationship: str = 'transitive',
    licenses: Iterable[str] = (),
    found_by: str = 'cataloger',
    source: str = 'syft',
    version_kind: str = 'resolved',
    commit: str = '',
    ref: str = 'main',
) -> dict[str, Any]:
    """An `artifacts` row of one scan: a Syft or manifest scan of
    `commit`, or a dependency graph dated `observed_at`."""
    return {
        'repository_id': repository_id,
        'artifact_id': f'{repository_id}-{name}-{version}-{found_by}',
        'name': name, 'version': version, 'type': type, 'purl': '',
        'found_by': found_by, 'licenses': list(licenses),
        'relationship': relationship, 'source': source,
        'version_kind': version_kind,
        'sbom_ref': ref if commit else '', 'sbom_commit_sha': commit,
        'observed_at': observed_at,
    }


def graph(
    repository_id: int,
    name: str,
    version: str,
    type: str,
    *,
    observed_at: datetime,
    relationship: str = 'direct',
    version_kind: str = 'constraint',
) -> dict[str, Any]:
    """A row of a dependency-graph document."""
    return artifact(
        repository_id, name, version, type, observed_at=observed_at,
        relationship=relationship, found_by='github-dependency-graph',
        source='github-depgraph', version_kind=version_kind,
    )


@dataclass
class Corpus:
    """What a warehouse is made from."""

    repositories: list[dict[str, Any]]
    artifacts: list[dict[str, Any]]
    edges: list[tuple[str, str, int, datetime]] = field(default_factory=list)
    #: The corpus's ids; None for every repository.
    corpus: set[int] | None = None
    #: Scans that saw nothing, which rows cannot say: `(repository,
    #: source, key, instant)`, the key a Syft scan's commit.
    empty: list[tuple[int, str, str, datetime]] = field(
        default_factory=list,
    )
    #: The `build` row's fields; None for no row, as `rows.load` leaves
    #: a warehouse.
    build: dict[str, Any] | None = None


def warehouse(path: Path, corpus: Corpus) -> Path:
    """A warehouse file of `corpus`, derived and closed."""
    with connect(path) as con:
        load(
            con, corpus.repositories, corpus.artifacts, corpus.edges,
            corpus=corpus.corpus,
        )
        with Writer(con) as writer:
            for repository_id, source, key, instant in corpus.empty:
                syft = source == 'syft'
                writer.scan(
                    Scan(
                        repository_id=repository_id, source=source,
                        input_key=key, tool='', observed_at=instant,
                        ref='main' if syft else '',
                        commit_sha=key if syft else '',
                    ),
                )
            if corpus.build is not None:
                writer.add('build', corpus.build)
        derive(con)
        con.execute('CHECKPOINT')
    return path


#: What a pass writes last, but the instant and the store: neither is
#: in a snapshot, so no two passes need agree on them.
BUILD: dict[str, Any] = {
    'version': '0.5.4', 'built_at': at(2026, 9, 29, 12),
    'store': '/srv/chatsbom/data', 'corpus': 'all-2026-09-01',
    'repositories': 4, 'scans': 6, 'observations': 10, 'unreadable': 0,
    'unnamed': 0,
}

JAN = at(2026, 1, 15, 9)
FEB = at(2026, 2, 1, 12)
MAR = at(2026, 3, 10, 9)
APR = at(2026, 4, 2, 23)
SEP = at(2026, 9, 13, 8)


def shop() -> Corpus:
    """Four repositories, three of them the corpus."""
    return Corpus(
        repositories=[
            repository(
                1, 'acme', 'app', 300, 'Ruby', license_spdx_id='MIT',
                description='An app',
            ),
            # Its description is not ASCII: the id is made of the rows'
            # values, whatever their characters.
            repository(
                2, 'acme', 'web', 500, 'JavaScript',
                description='Ünïcode — the web 🕸', pushed_at=at(2026, 8, 1),
            ),
            # No language on GitHub, and scanned once, finding nothing.
            repository(3, 'acme', 'idle', 100, ''),
            # Not in the corpus: in no table of a snapshot.
            repository(4, 'other', 'gone', 50, 'Go', snapshot=''),
        ],
        artifacts=[
            # app: a January scan the March one replaced, which shows
            # `rack` too, so the months between them count (Q9); and the
            # graph in September.
            artifact(
                1, 'rack', '3.0.0', 'gem', observed_at=JAN, commit='c1',
                ref='v1.0.0', licenses=['MIT'],
                found_by='gemspec-cataloger',
            ),
            artifact(
                1, 'rack', '3.1.0', 'gem', observed_at=MAR, commit='c2',
                ref='v2.0.0', relationship='direct', licenses=['MIT'],
                found_by='gemfile-lock-cataloger',
            ),
            artifact(
                1, 'puma', '6.4.0', 'gem', observed_at=MAR, commit='c2',
                ref='v2.0.0', found_by='gemfile-lock-cataloger',
            ),
            graph(1, 'rack', '~> 3.1', 'gem', observed_at=SEP),
            # web: every licence case, a name shared with app, and
            # Syft's spelling of Composer, which a snapshot shows as the
            # page does.
            artifact(
                2, 'lodash', '4.17.21', 'npm', observed_at=FEB, commit='w1',
                licenses=['MIT'], found_by='javascript-lock-cataloger',
            ),
            artifact(
                2, 'left-pad', '1.3.0', 'npm', observed_at=FEB, commit='w1',
                relationship='direct', licenses=['WTFPL'],
                found_by='javascript-lock-cataloger',
            ),
            artifact(
                2, 'rack', '3.1.0', 'gem', observed_at=FEB, commit='w1',
                licenses=['MIT', 'Ruby'], found_by='gemfile-lock-cataloger',
            ),
            artifact(
                2, 'laravel/framework', 'v12.0.0', 'php-composer',
                observed_at=FEB, commit='w1', relationship='direct',
                licenses=['MIT'], found_by='php-composer-lock-cataloger',
            ),
            artifact(
                4, 'cobra', '1.8.0', 'go-module', observed_at=FEB,
                commit='g1', relationship='direct', licenses=['Apache-2.0'],
            ),
        ],
        edges=[
            ('rack', 'puma', 2, SEP),
            ('lodash', 'left-pad', 1, SEP),
            # A package no fact names: left out, as D1's export left it.
            ('mystery', 'rack', 1, SEP),
        ],
        corpus={1, 2, 3},
        empty=[
            (3, 'syft', 'i1', APR),
            # web's graph, fetched after its Syft scan and empty: web is
            # still dated by the scan that saw its dependencies, as
            # D1's export dated it by its newest row, and has no graph
            # date to show beside a row.
            (2, 'github-depgraph', SEP.isoformat(), SEP),
        ],
        build=dict(BUILD),
    )


def contract_corpus() -> Corpus:
    """The contract suite's seed, as `tests/warehouse/golden_test.py`
    loads it: every repository is the corpus, as in ClickHouse while no
    repository named a snapshot."""
    return Corpus(
        repositories=aware(contract.REPOSITORIES_SEED),
        artifacts=aware(contract.ARTIFACTS_SEED),
        edges=[
            (parent, child, count, contract.SEP.replace(tzinfo=UTC))
            for parent, child, count in contract.EDGES_SEED
        ],
        build={**BUILD, 'corpus': ''},
    )


# -- the aggregates, as `export d1` computed them -------------------------
#
# `export d1` wrote the overview's aggregates with a script of its own,
# run in SQLite over the rows it had just written. A snapshot computes
# them in DuckDB, from the warehouse's facts (`snapshot/tables.py`). The
# export is gone (#151), and its script is kept here as the definition
# the snapshot's are held to: `write_test.py` runs it over a snapshot's
# own rows, and every aggregate has to come out the same.

#: The tables `aggregate_sql` fills, all of them computed from the base
#: tables. Not `agg_edges`, which `export d1` filled from ClickHouse.
AGGREGATED = (
    AGG_TOTALS, AGG_RELATIONSHIP_SPLIT, AGG_LANGUAGE_COVERAGE,
    AGG_ECOSYSTEM_COVERAGE, AGG_TOP_PACKAGES, AGG_DEPENDENCY_BUCKETS,
    AGG_SOURCE_COMPARISON,
)


def aggregate_sql() -> str:
    """Fill the precomputed aggregates, inside SQLite, from the base
    tables, as `export d1`'s script did for D1. It empties every table
    it fills before filling it, so it runs over a snapshot's own rows as
    well as over none."""
    emptied = '\n'.join(f'DELETE FROM {table.name};' for table in AGGREGATED)
    return f"""-- ChatSBOM D1 aggregates. Apply after the data, 02-*.sql.

-- Safe to apply again: every table this script fills is emptied first,
-- so a second run replaces what the first wrote rather than adding a
-- second copy of it. The UPDATE of `packages` sets the same numbers.
{emptied}

-- `WHERE total_dependencies > 0`, because the other three numbers
-- here describe the analysed set and this one has to as well.
--
-- The repositories table holds every repository of the current search
-- snapshot, including those with no dependency row, while ClickHouse's
-- `mv_totals` counts the ones that have one. The dashboard reads
-- this field under the label "repositories with dependency data" — a
-- label made true for one backend and false for the other. Two stores
-- answering the same call differently is how a fallback becomes a
-- different dataset. `tracked` is the snapshot, the denominator.
INSERT INTO agg_totals
  (repositories, dependencies, packages, classified, tracked)
SELECT
  (SELECT count(*) FROM repositories WHERE total_dependencies > 0),
  (SELECT count(*) FROM artifacts),
  (SELECT count(*) FROM packages),
  (SELECT count(*) FROM artifacts a JOIN kinds k ON k.id = a.kind_id
   WHERE k.relationship <> 'unknown'),
  (SELECT count(*) FROM repositories);

-- Per ecosystem and, as the '' row, the whole corpus. The overview reads
-- the '' row; the ecosystem filter reads one of the others. Records
-- partition by ecosystem (a record has one type), so the per-ecosystem
-- rows add up to the '' row.
INSERT INTO agg_relationship_split (ecosystem, relationship, records)
SELECT '', k.relationship, count(*)
FROM artifacts a JOIN kinds k ON k.id = a.kind_id
GROUP BY k.relationship;

INSERT INTO agg_relationship_split (ecosystem, relationship, records)
SELECT k.type, k.relationship, count(*)
FROM artifacts a
JOIN kinds k ON k.id = a.kind_id
WHERE k.type <> ''
GROUP BY k.type, k.relationship;

-- Denormalised onto `packages` so the search box can rank by it.
--
-- Ordering the search by popularity is the whole point: alphabetically,
-- `laravel` returns forty `laravel-enso/*` packages with one dependant
-- each ('-' is 0x2D, '/' is 0x2F) and never reaches `laravel/framework`
-- with 98. But computing the count per matching row means a correlated
-- subquery over `artifacts` for every candidate, which is the one thing
-- a keystroke-latency query must not do. Stored once here instead.
-- One grouped pass, joined back. Not a correlated subquery: the index
-- that would make one viable, `idx_artifacts_package_id`, is created by
-- `04-indexes.sql` *after* this script, so the subquery form scans the
-- whole artifacts table once per package. Measured on the real export:
-- 225,400 packages against 16,839,566 rows had not finished in 110
-- seconds and would not have; this form takes 3.2 seconds, which also
-- keeps it inside D1's 30-second statement limit.
--
-- Packages the join finds no rows for keep the column's DEFAULT 0.
UPDATE packages SET repositories = counted.n
FROM (
  SELECT package_id, count(DISTINCT repository_id) AS n
  FROM artifacts GROUP BY package_id
) AS counted
WHERE counted.package_id = packages.id;

-- The denominator is every repository of the snapshot, collected or
-- not, folded by GitHub's language (top twelve, other, none).
INSERT INTO agg_language_coverage
  (language, repositories, with_sbom, with_syft, with_depgraph,
   with_manifest)
SELECT r.language_bucket, count(*),
       count(CASE WHEN r.total_dependencies > 0 THEN 1 END),
       count(CASE WHEN s.syft > 0 THEN 1 END),
       count(CASE WHEN s.depgraph > 0 THEN 1 END),
       count(CASE WHEN s.manifest > 0 THEN 1 END)
FROM repositories r
LEFT JOIN (
  SELECT a.repository_id AS id,
         sum(k.source = 'syft') AS syft,
         sum(k.source = 'github-depgraph') AS depgraph,
         sum(k.source = 'manifest') AS manifest
  FROM artifacts a JOIN kinds k ON k.id = a.kind_id
  GROUP BY a.repository_id
) s ON s.id = r.id
GROUP BY r.language_bucket;

-- A repository counts under every ecosystem it has: its artifacts' or
-- its manifests'. These rows overlap and are not to be summed.
INSERT INTO agg_ecosystem_coverage
  (ecosystem, repositories, with_any, with_syft, with_depgraph,
   with_manifest)
SELECT e.value, count(*),
       count(CASE WHEN s.records > 0 THEN 1 END),
       count(CASE WHEN s.syft > 0 THEN 1 END),
       count(CASE WHEN s.depgraph > 0 THEN 1 END),
       count(CASE WHEN s.manifest > 0 THEN 1 END)
FROM repositories r
JOIN json_each(r.ecosystems) e
LEFT JOIN (
  SELECT a.repository_id AS id,
         k.type AS ecosystem,
         count(*) AS records,
         sum(k.source = 'syft') AS syft,
         sum(k.source = 'github-depgraph') AS depgraph,
         sum(k.source = 'manifest') AS manifest
  FROM artifacts a JOIN kinds k ON k.id = a.kind_id
  GROUP BY a.repository_id, k.type
) s ON s.id = r.id AND s.ecosystem = e.value
GROUP BY e.value;

-- The ranking, per filter combination. The panel has exactly two
-- controls -- declared-only, and ecosystem -- so the answer set is
-- finite and can be enumerated.
--
-- The whole-corpus row counts each name's repositories once. It used
-- to sum the per-language counts, which was exact only while every
-- repository had one language; a repository has as many ecosystems as
-- it has manifests for, and `mail` is a gem and a Maven artifact.
INSERT INTO agg_top_packages
  (direct_only, ecosystem, rank, name, repository_count, direct_count)
WITH counted AS (
  SELECT
    k.type AS ecosystem,
    p.name AS name,
    count(DISTINCT a.repository_id) AS repository_count,
    count(DISTINCT CASE WHEN k.relationship = 'direct'
                        THEN a.repository_id END) AS direct_count
  FROM artifacts a
  JOIN packages p ON p.id = a.package_id
  JOIN kinds k ON k.id = a.kind_id
  GROUP BY k.type, p.name
),
overall AS (
  SELECT
    '' AS ecosystem,
    p.name AS name,
    count(DISTINCT a.repository_id) AS repository_count,
    count(DISTINCT CASE WHEN k.relationship = 'direct'
                        THEN a.repository_id END) AS direct_count
  FROM artifacts a
  JOIN packages p ON p.id = a.package_id
  JOIN kinds k ON k.id = a.kind_id
  GROUP BY p.name
),
unioned AS (
  SELECT * FROM counted WHERE ecosystem <> ''
  UNION ALL SELECT * FROM overall
),
ranked AS (
  SELECT
    direct_only, ecosystem, name, repository_count, direct_count,
    row_number() OVER (
      PARTITION BY direct_only, ecosystem
      ORDER BY CASE WHEN direct_only = 1 THEN direct_count
                    ELSE repository_count END DESC, name ASC
    ) AS rank
  FROM unioned, (SELECT 0 AS direct_only UNION ALL SELECT 1)
)
SELECT direct_only, ecosystem, rank, name, repository_count, direct_count
FROM ranked
WHERE rank <= {TOP_PACKAGES_DEPTH};

INSERT INTO agg_dependency_buckets (bucket, position, repositories)
SELECT bucket, position, count(*) FROM (
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
  FROM repositories
) GROUP BY bucket, position;

INSERT INTO agg_source_comparison (ecosystem, syft, depgraph, manifest)
SELECT k.type,
       sum(CASE WHEN k.source = 'syft' THEN 1 ELSE 0 END),
       sum(CASE WHEN k.source = 'github-depgraph' THEN 1 ELSE 0 END),
       sum(CASE WHEN k.source = 'manifest' THEN 1 ELSE 0 END)
FROM artifacts a
JOIN kinds k ON k.id = a.kind_id
WHERE k.type <> ''
GROUP BY k.type;
"""
