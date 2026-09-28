"""The Parquet writer, handed rows by a stub repository.

`export_dataset` writes whatever each query returns into the columns the
schema declares. A column the schema did not declare, it dropped without
a word. `QUERIES['history']` selects `source` — the series is per
collector, because Syft resolves lockfiles and GitHub's graph parses
manifests, and one series over both reads a change of instrument as a
change in adoption — and `HISTORY_TABLE` did not declare it. So D1 and
the dashboard kept the two series apart and `history.parquet` did not:
two `mail` rows for September, 124 and 149, and nothing in the file to
say which was which. The mixing TODO G fixed for D1.

A stub rather than ClickHouse: the writer is what is under test, and
these are the rows the history query returns for that month.
"""
from __future__ import annotations

from collections.abc import Iterator
from collections.abc import Mapping
from pathlib import Path
from typing import Any
from typing import cast

import pytest

from chatsbom.core.repository import QueryRepository
from chatsbom.export.parquet import export_dataset
from chatsbom.export.parquet import ExportResult
from chatsbom.export.queries import QUERIES
from chatsbom.export.schema import EXPORT_SCHEMA
from chatsbom.models.provenance import DEPGRAPH
from chatsbom.models.provenance import SYFT

pq = pytest.importorskip('pyarrow.parquet')


class StubRepository:
    """Answers each export query with the rows given for its table."""

    def __init__(self, rows: Mapping[str, list[dict[str, Any]]]) -> None:
        self.rows = rows

    def _table(self, sql: str) -> str:
        [name] = [name for name, query in QUERIES.items() if query == sql]
        return name

    def count_rows(
        self,
        sql: str,
        parameters: dict[str, Any] | None = None,
    ) -> int:
        return len(self.rows.get(self._table(sql), []))

    def stream_rows(
        self,
        sql: str,
        parameters: dict[str, Any] | None = None,
    ) -> Iterator[dict[str, Any]]:
        yield from self.rows.get(self._table(sql), [])


#: `mail` in September 2026, once per collector: Syft's lockfile closure
#: and GitHub's manifest graph, as `QUERIES['history']` returns them.
MAIL_IN_SEPTEMBER = [
    {
        'name': 'mail', 'month': '2026-09', 'source': SYFT,
        'repository_count': 124, 'direct_count': 17,
    },
    {
        'name': 'mail', 'month': '2026-09', 'source': DEPGRAPH,
        'repository_count': 149, 'direct_count': 149,
    },
]


def export(
    rows: Mapping[str, list[dict[str, Any]]],
    directory: Path,
) -> ExportResult:
    return export_dataset(
        cast(QueryRepository, StubRepository(rows)), directory,
    )


def history(directory: Path) -> Any:
    [path] = sorted(directory.glob('history-*.parquet'))
    return pq.read_table(path)


class TestTheHistoryKeepsItsSource:

    def test_the_file_carries_the_source(self, tmp_path: Path) -> None:
        export({'history': MAIL_IN_SEPTEMBER}, tmp_path)
        assert 'source' in history(tmp_path).column_names
        assert history(tmp_path).column_names == (
            EXPORT_SCHEMA.table('history').column_names
        )

    def test_the_two_series_are_told_apart(self, tmp_path: Path) -> None:
        """Each row names its collector, so no two rows share a key."""
        export({'history': MAIL_IN_SEPTEMBER}, tmp_path)
        rows = history(tmp_path).to_pylist()
        assert sorted(
            (r['name'], r['month'], r['source'], r['repository_count'])
            for r in rows
        ) == [
            ('mail', '2026-09', DEPGRAPH, 149),
            ('mail', '2026-09', SYFT, 124),
        ]
        keys = [(r['name'], r['month'], r['source']) for r in rows]
        assert len(set(keys)) == len(keys)


class TestTheQueryAndTheContractAgree:
    """A column the query returns and the schema does not declare fails
    the export, as a column the schema declares and the query does not
    already did."""

    def test_an_undeclared_column_fails_the_export(
        self, tmp_path: Path,
    ) -> None:
        rows = [{**row, 'surprise': 1} for row in MAIL_IN_SEPTEMBER]
        with pytest.raises(KeyError, match='surprise'):
            export({'history': rows}, tmp_path)

    def test_a_missing_column_still_fails_the_export(
        self, tmp_path: Path,
    ) -> None:
        rows = [
            {k: v for k, v in row.items() if k != 'direct_count'}
            for row in MAIL_IN_SEPTEMBER
        ]
        with pytest.raises(KeyError, match='direct_count'):
            export({'history': rows}, tmp_path)

    def test_the_error_names_the_table(self, tmp_path: Path) -> None:
        rows = [{**row, 'surprise': 1} for row in MAIL_IN_SEPTEMBER]
        with pytest.raises(KeyError, match='history'):
            export({'history': rows}, tmp_path)
