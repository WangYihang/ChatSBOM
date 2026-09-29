"""Rows into the warehouse's tables, a batch at a time.

A batch is written as a file of JSON arrays, one line a row, and read
by DuckDB in one statement. Measured on 200,000 observations: DuckDB's
`executemany` inserted 1,179 rows a second, the rows passed as lists of
columns 58,000 a second, and this 130,000 a second, about half of it
Python encoding the lines. There are about 19 million observations, so
the first would take four and a half hours.

The files go in a directory of their own beside the warehouse, and are
deleted once read.
"""
from __future__ import annotations

import json
import tempfile
from collections import Counter
from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from pathlib import Path
from types import TracebackType
from typing import Any
from typing import TYPE_CHECKING

from chatsbom.core.instants import utc
from chatsbom.warehouse import schema

if TYPE_CHECKING:
    import duckdb

#: Rows per file.
BATCH = 100_000


@dataclass(frozen=True)
class Scan:
    """One scan, and the rows the shared parsers made of what it saw.

    `rows` are `artifacts` rows, as `DbService` makes them for
    ClickHouse. What they share with the scan is the scan's
    (`schema.SCAN_COLUMNS`), and the writer checks that it is shared.
    """

    repository_id: int
    source: str
    input_key: str
    tool: str
    observed_at: datetime
    ref: str = ''
    ref_type: str = ''
    commit_sha: str = ''
    document: str = ''
    manifest_sources: Sequence[str] = ()
    ecosystems: Sequence[str] = ()
    rows: Sequence[Mapping[str, Any]] = field(default_factory=tuple)


class Writer:
    """Buffers rows per table and writes them a batch at a time."""

    def __init__(
        self,
        con: duckdb.DuckDBPyConnection,
        scratch: Path | None = None,
        batch: int = BATCH,
    ) -> None:
        self._con = con
        self._batch = batch
        self._directory = tempfile.TemporaryDirectory(
            prefix='warehouse-rows-', dir=scratch,
        )
        self._pending: dict[str, list[str]] = {
            name: [] for name in schema.BY_NAME
        }
        self._next_scan = 1
        #: Rows written, by table.
        self.written: Counter[str] = Counter()

    def __enter__(self) -> Writer:
        return self

    def __exit__(
        self,
        kind: type[BaseException] | None,
        error: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        try:
            if kind is None:
                self.flush()
        finally:
            self._directory.cleanup()

    def add(self, table: str, row: Mapping[str, Any]) -> None:
        columns = schema.BY_NAME[table].columns
        self._pending[table].append(_line(columns, row))
        if len(self._pending[table]) >= self._batch:
            self.flush(table)

    def extend(self, table: str, rows: Iterable[Mapping[str, Any]]) -> None:
        for row in rows:
            self.add(table, row)

    def scan(self, scan: Scan) -> int:
        """Writes `scan` and its observations; its id."""
        scan_id = self._next_scan
        self._next_scan += 1
        self.add(
            'scans', {
                'scan_id': scan_id,
                'repository_id': scan.repository_id,
                'source': scan.source,
                'input_key': scan.input_key,
                'tool': scan.tool,
                'observed_at': scan.observed_at,
                'ref': scan.ref,
                'ref_type': scan.ref_type,
                'commit_sha': scan.commit_sha,
                'document': scan.document,
                'manifest_sources': scan.manifest_sources,
                'ecosystems': scan.ecosystems,
                'observations': len(scan.rows),
            },
        )
        for position, row in enumerate(scan.rows):
            _check_shared(scan, row)
            self.add(
                'observations', {
                    **row,
                    'scan_id': scan_id,
                    'position': position,
                    'repository_id': scan.repository_id,
                    'source': scan.source,
                },
            )
        return scan_id

    def flush(self, table: str | None = None) -> None:
        for name in (table,) if table is not None else tuple(self._pending):
            lines = self._pending[name]
            if not lines:
                continue
            path = Path(self._directory.name) / f'{name}.ndjson'
            path.write_text('\n'.join(lines) + '\n', encoding='utf-8')
            declared = schema.BY_NAME[name]
            selected = ', '.join(
                _read(position, column)
                for position, column in enumerate(declared.columns)
            )
            self._con.execute(
                f'INSERT INTO {name} SELECT {selected} FROM '
                f"read_json_objects({_literal(str(path))}, "
                "format='newline_delimited')",
            )
            path.unlink()
            self.written[name] += len(lines)
            self._pending[name] = []


def _line(columns: Sequence[schema.Column], row: Mapping[str, Any]) -> str:
    """One row as a JSON array, in the table's column order; a column the
    row lacks, or has as None where NULL is not allowed, as its default."""
    values = []
    for column in columns:
        value = row.get(column.name)
        if value is None and not column.nullable:
            value = column.default
        values.append(_json(value))
    return json.dumps(values, separators=(',', ':'))


def _json(value: Any) -> Any:
    if isinstance(value, datetime):
        return _instant(value)
    if isinstance(value, (tuple, list)):
        return list(value)
    return value


def _instant(value: datetime) -> str:
    """An instant as the `TIMESTAMP` it is stored as: its UTC wall time,
    to the second, spelled out.

    A naive value is taken to be UTC already, as `instants.utc` takes
    it: everything upstream computes UTC. Spelled here rather than left
    to DuckDB, which reads an aware value in the session's zone
    (`chatsbom.warehouse.TIMEZONE`), and made from the aware value
    without dropping its zone first (`core/instants.py`).
    """
    return utc(value).strftime('%Y-%m-%d %H:%M:%S')


def _read(position: int, column: schema.Column) -> str:
    """The expression that reads a column from a line's array."""
    if column.type == 'VARCHAR':
        return f'json->>{position}'
    if column.type.endswith('[]'):
        return f'CAST(json->{position} AS {column.type})'
    return f'CAST(json->>{position} AS {column.type})'


def _literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"


def _check_shared(scan: Scan, row: Mapping[str, Any]) -> None:
    """What a row has of its scan's is the scan's: a row that says
    otherwise would be stored under a scan it does not belong to."""
    for column, lifted in schema.SCAN_COLUMNS.items():
        if column not in row:
            continue
        mine = row[column]
        theirs = getattr(scan, lifted)
        if isinstance(mine, datetime):
            mine, theirs = _instant(mine), _instant(theirs)
        if mine != theirs:
            raise ValueError(
                f'a row of scan {scan.source} {scan.input_key} of '
                f'repository {scan.repository_id} has {column}={mine!r}, '
                f'the scan {theirs!r}',
            )
    for column in ('repository_id', 'source'):
        if column in row and row[column] != getattr(scan, column):
            raise ValueError(
                f'a row of scan {scan.source} {scan.input_key} of '
                f'repository {scan.repository_id} has '
                f'{column}={row[column]!r}',
            )
