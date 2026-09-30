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
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest

from chatsbom.export.schema import EXPORT_SCHEMA
from chatsbom.export.schema import REPOSITORIES_TABLE
from chatsbom.models.provenance import DEPGRAPH


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
    from chatsbom.export.parquet import observed_range
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


# --- observed_at, from what was seen to the export --------------------------

#: A Syft scan, dated as its document is, in UTC+8: 07:30 on the 1st of
#: February there is 23:30 on the 31st of January in UTC. A date read
#: off the offset's wall clock lands on the 1st.
SCANNED = datetime(2026, 2, 1, 7, 30, tzinfo=timezone(timedelta(hours=8)))

#: When GitHub produced the graph, as its document states.
GRAPHED = datetime(2026, 9, 14, 3, 56, 20, tzinfo=timezone.utc)

#: A scan that found nothing, after every other.
EMPTY = datetime(2026, 9, 29, 6, tzinfo=timezone.utc)

COMMIT = 'c' * 40


class TestObservedAtIsWhenTheDataWasSeen:
    """`observed_at` is when we last looked, and the manifest's span is
    of those days: of the warehouse's current scans, each dated in UTC.

    It was the day `db index` last ran, when the export read ClickHouse:
    `updated_at`, which it took when it was later than every observation,
    is the time of the insert, and `db index` wrote a row for each
    repository it indexed. Every repository read as scanned on the last
    index day, and so did the freshness the manifest reported.
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
