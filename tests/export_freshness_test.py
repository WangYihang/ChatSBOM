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

from chatsbom.export.schema import EXPORT_SCHEMA
from chatsbom.export.schema import REPOSITORIES_TABLE


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
        """A new column and new manifest fields are a contract change."""
        assert EXPORT_SCHEMA.version == '5'


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
        from chatsbom.export.parquet import ExportResult
        from pathlib import Path
        result = ExportResult(directory=Path('.'))
        assert result.freshness == {}

    def test_freshness_is_recorded_per_export(self) -> None:
        from chatsbom.export.parquet import ExportResult
        from pathlib import Path
        result = ExportResult(
            directory=Path('.'),
            freshness={
                'observedFrom': '2026-08-20',
                'observedTo': '2026-09-14',
            },
        )
        assert result.freshness['observedTo'] == '2026-09-14'
