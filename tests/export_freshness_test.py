"""Freshness metadata, for debugging what the page is actually showing.

Three questions a reader or an operator needs answered without opening a
database: which version produced this, how fresh is the dataset as a
whole, and when was *this particular* repository last looked at. The
first was already in the manifest; the other two were not.

Freshness is derived from the data rather than from a clock. That keeps
the manifest reproducible -- byte-identical data yields a byte-identical
manifest -- and it is also the more honest answer: an export can run
long after collection, so a wall-clock reading describes the export
rather than the data, which is exactly the wrong thing to debug against.
"""
from __future__ import annotations

import json
import re
import sqlite3
from contextlib import closing
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest

from chatsbom.core.repository import IngestionRepository
from chatsbom.core.repository import QueryRepository
from chatsbom.core.schema import ARTIFACTS
from chatsbom.core.schema import REPOSITORIES
from chatsbom.export.d1 import export_d1
from chatsbom.export.queries import QUERIES
from chatsbom.export.schema import EXPORT_SCHEMA
from chatsbom.export.schema import REPOSITORIES_TABLE
from chatsbom.models.provenance import CONSTRAINT
from chatsbom.models.provenance import DEPGRAPH
from chatsbom.models.relationship import DIRECT
from tests.conftest import requires_clickhouse
from tests.export_d1_apply_test import apply_scripts
from tests.export_d1_apply_test import seed_edges
from tests.repository_query_test import artifact_row
from tests.repository_query_test import repo_row


class TestObservedAtColumn:

    def test_repositories_carry_when_they_were_last_observed(self) -> None:
        assert 'observed_at' in REPOSITORIES_TABLE.column_names

    def test_observed_at_is_distinct_from_pushed_at(self) -> None:
        """`pushed_at` is upstream's clock, `observed_at` is ours.

        Conflating them is the failure this column exists to prevent: a
        repository can have been pushed to yesterday and last scanned
        six months ago, and only the second explains a stale row.
        """
        observed = REPOSITORIES_TABLE.column('observed_at')
        pushed = REPOSITORIES_TABLE.column('pushed_at')
        assert observed.description != pushed.description
        assert 'scanned' in observed.description.lower()

    def test_schema_version_moved_with_the_contract(self) -> None:
        """A new column and new manifest fields are a contract change.

        6: `history` gained `source`, and the contract stopped naming a
        file per table. 7: `source` may be `manifest`. 8: `repositories`
        gained `github_language`, `language_bucket` and `ecosystems`,
        and the tables cover the current snapshot only.
        """
        assert EXPORT_SCHEMA.version == '8'


class TestManifestFreshness:
    """The manifest's freshness block, derived from data not from a clock."""

    def test_freshness_comes_from_the_rows(self) -> None:
        """min/max of the observation dates actually present."""
        rows = [
            {'observed_at': '2026-09-01'},
            {'observed_at': '2026-09-14'},
            {'observed_at': '2026-08-20'},
        ]
        assert freshness_of(rows) == {
            'observedFrom': '2026-08-20',
            'observedTo': '2026-09-14',
        }

    def test_empty_dataset_has_no_freshness_rather_than_a_fake_one(self) -> None:
        """A default date would read as a real observation."""
        assert freshness_of([]) == {}

    def test_blank_dates_are_not_treated_as_observations(self) -> None:
        """An unscanned row carries '', which sorts below every date."""
        rows = [{'observed_at': ''}, {'observed_at': '2026-09-14'}]
        assert freshness_of(rows) == {
            'observedFrom': '2026-09-14',
            'observedTo': '2026-09-14',
        }

    def test_is_reproducible_for_identical_data(self) -> None:
        """No clock reading: the same rows must give the same answer.

        The manifest is content-addressed by its checksums, so a wall
        time would make byte-identical exports differ — and it would
        describe when the export ran rather than how fresh the data is,
        which is the wrong thing to debug against.
        """
        rows = [{'observed_at': '2026-09-14'}]
        assert freshness_of(rows) == freshness_of(rows)


def freshness_of(rows: list[dict[str, str]]) -> dict[str, str]:
    from chatsbom.export.queries import observed_range
    return observed_range(r['observed_at'] for r in rows)


class TestExportResultFreshness:

    def test_result_carries_a_freshness_span(self) -> None:
        from pathlib import Path

        from chatsbom.export.parquet import ExportResult
        result = ExportResult(directory=Path('.'))
        assert result.freshness == {}

    def test_freshness_is_recorded_per_export(self) -> None:
        from pathlib import Path

        from chatsbom.export.parquet import ExportResult
        result = ExportResult(
            directory=Path('.'),
            freshness={
                'observedFrom': '2026-08-20',
                'observedTo': '2026-09-14',
            },
        )
        assert result.freshness['observedTo'] == '2026-09-14'


# --- observed_at, from what was seen to the exports -------------------------

#: A Syft scan, dated as its document is, in UTC+8: 07:30 on the 1st of
#: February there is 23:30 on the 31st of January in UTC. A date read
#: off the offset's wall clock lands on the 1st.
SCANNED = datetime(2026, 2, 1, 7, 30, tzinfo=timezone(timedelta(hours=8)))

#: When GitHub produced the graph its repository records, as it states.
GRAPHED = datetime(2026, 9, 14, 3, 56, 20, tzinfo=timezone.utc)

#: A later graph document, which its repository no longer records.
FORMERLY = datetime(2026, 9, 21, 3, 56, 20, tzinfo=timezone.utc)

#: A scan that found nothing, after every other.
EMPTY = datetime(2026, 9, 29, 6, tzinfo=timezone.utc)

#: What `db index` records for a repository it read no graph for.
NO_GRAPH = datetime(1970, 1, 2, tzinfo=timezone.utc)

COMMIT = 'c' * 40


def scanned(repository_id: int, name: str) -> dict[str, Any]:
    """A row of the Syft scan each repository records."""
    return artifact_row(
        repository_id=repository_id, artifact_id=name, name=name,
        sbom_commit_sha=COMMIT, observed_at=SCANNED,
    )


def graphed(repository_id: int, name: str, at: datetime) -> dict[str, Any]:
    """A dependency-graph row, of the document that states `at`."""
    return artifact_row(
        repository_id=repository_id, artifact_id=f'SPDXRef-{name}',
        name=name, version='~> 1.0', found_by='github-dependency-graph',
        relationship=DIRECT, source=DEPGRAPH, version_kind=CONSTRAINT,
        sbom_ref='main', sbom_commit_sha=COMMIT, observed_at=at,
    )


def seed_observations(ingest: IngestionRepository) -> None:
    """Four repositories, indexed today, each observed some other day.

    - `lockfile/only`: scanned by Syft, no graph.
    - `both/collectors`: that scan, and the graph it records.
    - `graph/dropped`: that scan, and a later graph that `db index`
      stopped recording: history, and not current.
    - `no/dependencies`: nothing either collector saw.
    """
    ingest.insert_batch(
        REPOSITORIES.name,
        REPOSITORIES.rows([
            repo_row(
                id=21, owner='lockfile', repo='only',
                sbom_commit_sha=COMMIT, depgraph_observed_at=NO_GRAPH,
            ),
            repo_row(
                id=22, owner='both', repo='collectors',
                sbom_commit_sha=COMMIT, depgraph_observed_at=GRAPHED,
            ),
            repo_row(
                id=23, owner='graph', repo='dropped',
                sbom_commit_sha=COMMIT, depgraph_observed_at=NO_GRAPH,
            ),
            repo_row(
                id=24, owner='no', repo='dependencies',
                sbom_commit_sha=COMMIT, depgraph_observed_at=NO_GRAPH,
                manifest_sources=[],
            ),
        ]),
        REPOSITORIES.column_names,
    )
    ingest.insert_batch(
        ARTIFACTS.name,
        ARTIFACTS.rows([
            scanned(21, 'mail'),
            scanned(22, 'mail'),
            graphed(22, 'rails', GRAPHED),
            scanned(23, 'mail'),
            graphed(23, 'puma', FORMERLY),
        ]),
        ARTIFACTS.column_names,
    )


def indexed_on(query: QueryRepository, repository_id: int) -> str:
    """The UTC date `db index` wrote the repository's row."""
    [[day]] = query.client.query(
        "SELECT formatDateTime(updated_at, '%Y-%m-%d', 'UTC') "
        'FROM repositories FINAL WHERE id = {id:UInt64}',
        parameters={'id': repository_id},
    ).result_rows
    return str(day)


class TestObservedAtIsWhenTheDataWasSeen:
    """`observed_at` is when we last looked, and the manifest's span is
    of those days: of the warehouse's current scans, each dated in UTC.

    It was the day `db index` last ran, when the export read ClickHouse
    (below, for D1): every repository read as scanned on the last index
    day, and so did the freshness both manifests reported.
    """

    @pytest.fixture
    def exported(self, tmp_path: Path) -> Path:
        pytest.importorskip('pyarrow')
        from chatsbom.export.parquet import export_warehouse
        from tests.snapshot.conftest import artifact
        from tests.snapshot.conftest import Corpus
        from tests.snapshot.conftest import graph
        from tests.snapshot.conftest import repository
        from tests.snapshot.conftest import warehouse

        path = warehouse(
            tmp_path / 'warehouse.duckdb',
            Corpus(
                repositories=[
                    repository(21, 'lockfile', 'only', 30, 'Ruby'),
                    repository(22, 'both', 'collectors', 20, 'Ruby'),
                    repository(24, 'no', 'dependencies', 10, 'Ruby'),
                ],
                artifacts=[
                    artifact(
                        21, 'mail', '2.9.1', 'gem', observed_at=SCANNED,
                        commit=COMMIT,
                    ),
                    artifact(
                        22, 'mail', '2.9.1', 'gem', observed_at=SCANNED,
                        commit=COMMIT,
                    ),
                    graph(22, 'rails', '~> 7.1', 'gem', observed_at=GRAPHED),
                ],
                # Scanned last, and finding nothing to date it by.
                empty=[(24, 'syft', COMMIT, EMPTY)],
            ),
        )
        export_warehouse(path, tmp_path / 'out')
        return tmp_path / 'out'

    @staticmethod
    def rows(directory: Path, table: str) -> list[dict[str, Any]]:
        import pyarrow.parquet as pq
        [path] = sorted(directory.glob(f'{table}-*.parquet'))
        return list(pq.read_table(path).to_pylist())

    def observed(self, directory: Path) -> dict[str, str]:
        return {
            f"{r['owner']}/{r['repo']}": r['observed_at']
            for r in self.rows(directory, 'repositories')
        }

    def test_a_scan_keeps_its_own_date(self, exported) -> None:
        """Its UTC date, the 31st, not the 1st its offset's clock read."""
        assert self.observed(exported)['lockfile/only'] == '2026-01-31'

    def test_it_is_the_latest_of_its_current_observations(
        self, exported,
    ) -> None:
        """The graph is newer than its scan."""
        assert self.observed(exported)['both/collectors'] == '2026-09-14'

    def test_the_manifest_reports_when_the_data_was_seen(
        self, exported,
    ) -> None:
        """The span of the repositories with dependencies: no/dependencies
        is dated by the scan that saw nothing, the last of all, which
        would make `observedTo` its day."""
        assert self.observed(exported)['no/dependencies'] == '2026-09-29'
        manifest = json.loads((exported / 'manifest.json').read_text())
        assert manifest['freshness'] == {
            'observedFrom': '2026-01-31', 'observedTo': '2026-09-14',
        }

    def test_the_history_is_dated_by_utc_month(self, exported) -> None:
        """January, as the scan was in UTC, where its offset's clock
        read February."""
        assert sorted(
            (r['name'], r['month'], r['source'])
            for r in self.rows(exported, 'history')
        ) == [
            ('mail', '2026-01', 'syft'),
            ('rails', '2026-09', DEPGRAPH),
        ]


@requires_clickhouse
class TestD1ObservedAtIsWhenTheDataWasSeen:
    """`observed_at` was the day `db index` last ran.

    The export took `greatest(max(a.observed_at), r.updated_at)`, and
    `updated_at` is not in the insert list: it defaults to the insert,
    and `db index` writes a fresh row for each repository it indexes. So
    every repository read as scanned on the last index day; the Parquet
    manifest and D1's `meta` reported that day as the data's freshness;
    and D1's dependants view, which reads the repository's date, showed
    a February scan as September's while ClickHouse, reading the row's,
    showed February.

    Each repository here is indexed today and observed another day.
    """

    @pytest.fixture
    def seeded(self, ingest, query) -> QueryRepository:
        seed_observations(ingest)
        return query

    def test_d1_carries_the_same_dates(
        self, ingest, seeded, tmp_path,
    ) -> None:
        """Its repositories, its `meta`, and so the date its dependants
        view shows: for `lockfile/only`'s row the scan's own, as
        ClickHouse shows it."""
        seed_edges(ingest, ('rails', 'mail', 1))
        result = export_d1(seeded, tmp_path / 'd1')
        with closing(
            sqlite3.connect(tmp_path / 'applied.sqlite'),
        ) as connection:
            apply_scripts(result.directory, sorted(result.files), connection)
            observed = dict(
                connection.execute(
                    "SELECT owner || '/' || repo, observed_at "
                    'FROM repositories',
                ).fetchall(),
            )
            meta = connection.execute(
                'SELECT observed_from, observed_to FROM meta',
            ).fetchall()

        assert observed == {
            'lockfile/only': '2026-01-31',
            'both/collectors': '2026-09-14',
            'graph/dropped': '2026-01-31',
            'no/dependencies': indexed_on(seeded, 24),
        }
        assert meta == [('2026-01-31', '2026-09-14')]
        assert result.freshness == {
            'observedFrom': '2026-01-31', 'observedTo': '2026-09-14',
        }
        assert seeded.client.query(
            "SELECT DISTINCT formatDateTime(observed_at, '%Y-%m-%d') "
            'FROM current_artifacts WHERE repository_id = 21',
        ).result_rows == [('2026-01-31',)]


def format_calls(sql: str) -> list[str]:
    """Every `formatDateTime(...)` call in `sql`, comments aside."""
    sql = re.sub(r'--[^\n]*', '', sql)
    calls = []
    start = sql.find('formatDateTime(')
    while start != -1:
        depth = 0
        for end in range(start, len(sql)):
            if sql[end] == '(':
                depth += 1
            elif sql[end] == ')':
                depth -= 1
                if depth == 0:
                    break
        calls.append(' '.join(sql[start:end + 1].split()))
        start = sql.find('formatDateTime(', end)
    return calls


class TestExportedDatesAreUtcDates:
    """A date the export writes is the UTC date of its instant.

    Instants are stored right since `core/instants.py`; turning one into
    a date takes a zone, and `formatDateTime` without one takes the
    server's. On a server in UTC+8 every scan after 16:00 UTC would be
    dated the next day, and a month's last evening the next month. The
    export is a published dataset, so it names its zone rather than
    inheriting whichever server produced it.
    """

    def test_every_date_the_export_writes_is_formatted_in_utc(self) -> None:
        calls = {name: format_calls(sql) for name, sql in QUERIES.items()}
        assert {name for name, found in calls.items() if found} == {
            'repositories', 'history',
        }
        for name, found in calls.items():
            for call in found:
                assert re.search(r",\s*'UTC'\s*\)$", call), (name, call)
