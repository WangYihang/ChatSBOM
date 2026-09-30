"""The Parquet writer, handed rows by a statement of them.

The export writes whatever each query returns into the columns the
schema declares. A column the schema did not declare, it dropped without
a word. The history query selects `source` — the series is per
collector, because Syft resolves lockfiles and GitHub's graph parses
manifests, and one series over both reads a change of instrument as a
change in adoption — and `HISTORY_TABLE` did not declare it. So D1 and
the dashboard kept the two series apart and `history.parquet` did not:
two `mail` rows for September, 124 and 149, and nothing in the file to
say which was which. The mixing TODO G fixed for D1.

A statement of the rows rather than a warehouse of them: the writer is
what is under test, and these are the rows the history query returns
for that month. What a stream that breaks off leaves is
`parquet_warehouse_test.py`'s.
"""
from __future__ import annotations

from collections.abc import Callable
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pytest

from chatsbom.export.parquet import export_warehouse
from chatsbom.export.parquet import ExportResult
from chatsbom.export.schema import EXPORT_SCHEMA
from chatsbom.export.warehouse import QUERIES
from chatsbom.models.provenance import DEPGRAPH
from chatsbom.models.provenance import SYFT
from chatsbom.warehouse import connect
from chatsbom.warehouse import schema
from chatsbom.warehouse.rollups import derive

pa = pytest.importorskip('pyarrow')
pq = pytest.importorskip('pyarrow.parquet')

#: `mail` in September 2026, once per collector: Syft's lockfile closure
#: and GitHub's manifest graph, as the history query returns them.
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


def literal(value: Any) -> str:
    if isinstance(value, str):
        return "'" + value.replace("'", "''") + "'"
    return str(int(value))


def statement(rows: list[dict[str, Any]]) -> str:
    """A statement whose result is `rows`, their columns in the order
    the first row names them."""
    columns = list(rows[0])
    values = ', '.join(
        '(' + ', '.join(literal(row[column]) for column in columns) + ')'
        for row in rows
    )
    return f"SELECT * FROM (VALUES {values}) AS given({', '.join(columns)})"


Export = Callable[[Mapping[str, list[dict[str, Any]]], Path], ExportResult]


@pytest.fixture
def export(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Export:
    """The export of an empty warehouse, but for the tables given rows:
    each of those is asked a statement of its rows instead."""
    empty = tmp_path / 'empty.duckdb'
    with connect(empty) as con:
        schema.create(con)
        derive(con)

    def exported(
        rows: Mapping[str, list[dict[str, Any]]], directory: Path,
    ) -> ExportResult:
        monkeypatch.setattr(
            'chatsbom.export.parquet.WAREHOUSE_QUERIES',
            {**QUERIES, **{name: statement(r) for name, r in rows.items()}},
        )
        return export_warehouse(empty, directory)

    return exported


def history(directory: Path) -> Any:
    [path] = sorted(directory.glob('history-*.parquet'))
    return pq.read_table(path)


class TestTheHistoryKeepsItsSource:

    def test_the_file_carries_the_source(
        self, export: Export, tmp_path: Path,
    ) -> None:
        export({'history': MAIL_IN_SEPTEMBER}, tmp_path / 'out')
        assert 'source' in history(tmp_path / 'out').column_names
        assert history(tmp_path / 'out').column_names == (
            EXPORT_SCHEMA.table('history').column_names
        )

    def test_the_two_series_are_told_apart(
        self, export: Export, tmp_path: Path,
    ) -> None:
        """Each row names its collector, so no two rows share a key."""
        export({'history': MAIL_IN_SEPTEMBER}, tmp_path / 'out')
        rows = history(tmp_path / 'out').to_pylist()
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
        self, export: Export, tmp_path: Path,
    ) -> None:
        rows = [{**row, 'surprise': 1} for row in MAIL_IN_SEPTEMBER]
        with pytest.raises(KeyError, match='surprise'):
            export({'history': rows}, tmp_path / 'out')

    def test_a_missing_column_still_fails_the_export(
        self, export: Export, tmp_path: Path,
    ) -> None:
        rows = [
            {k: v for k, v in row.items() if k != 'direct_count'}
            for row in MAIL_IN_SEPTEMBER
        ]
        with pytest.raises(KeyError, match='direct_count'):
            export({'history': rows}, tmp_path / 'out')

    def test_the_error_names_the_table(
        self, export: Export, tmp_path: Path,
    ) -> None:
        rows = [{**row, 'surprise': 1} for row in MAIL_IN_SEPTEMBER]
        with pytest.raises(KeyError, match='history'):
            export({'history': rows}, tmp_path / 'out')

    def test_the_columns_are_written_in_the_declared_order(
        self, export: Export, tmp_path: Path,
    ) -> None:
        """Whatever order the query returns them in: they are matched
        by name, as the rows were."""
        rows = [dict(reversed(row.items())) for row in MAIL_IN_SEPTEMBER]
        export({'history': rows}, tmp_path / 'out')
        assert history(tmp_path / 'out').column_names == (
            EXPORT_SCHEMA.table('history').column_names
        )
