"""ClickHouse's Arrow stream, as the exports read it.

Both exports that read it stream no more of it: `export d1` went with
the Worker (#151), and the Parquet export reads the warehouse alone
(#153), where how it streams is `parquet_warehouse_test.py`'s. What is
left is the client's own reading, which goes with the client.

The ClickHouse client is faked, underneath the real `QueryRepository`,
so the repository's own reading is part of what is tested. Its rows are
made as they are read and never kept.
"""
from __future__ import annotations

from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Mapping
from types import SimpleNamespace
from typing import Any

import pytest

from chatsbom.core.config import DatabaseConfig
from chatsbom.core.repository import QueryRepository
from chatsbom.export.queries import QUERIES
from chatsbom.export.schema import ColumnType
from chatsbom.export.schema import EXPORT_SCHEMA
from chatsbom.models.provenance import SYFT

pa = pytest.importorskip('pyarrow')

Rows = Callable[[], Iterator[dict[str, Any]]]

#: Each query the export sends, by what it reads.
KNOWN = QUERIES


def _source_schema(table: str) -> Any:
    """The types ClickHouse's Arrow stream sends for an exported table:
    unsigned and not null, where the export declares signed and
    nullable."""
    types = {
        ColumnType.STRING: pa.string(),
        ColumnType.DATE: pa.string(),
        ColumnType.INTEGER: pa.uint64(),
        ColumnType.STRING_LIST: pa.list_(
            pa.field('item', pa.string(), nullable=False),
        ),
    }
    return pa.schema([
        pa.field(column.name, types[column.type], nullable=False)
        for column in EXPORT_SCHEMA.table(table).columns
    ])


class _Stream:
    """A streamed result as the driver hands it over: a context manager
    that iterates, and names its columns once a block has arrived."""

    def __init__(
        self,
        items: Iterator[Any],
        drained: Callable[[], None] = lambda: None,
    ) -> None:
        # The driver's names: `column_names` on a native stream's
        # source, `drain_conn` on an Arrow stream's, the HTTP response.
        self.source = SimpleNamespace(column_names=[], drain_conn=drained)
        self._items = items

    def __enter__(self) -> _Stream:
        return self

    def __exit__(self, *exc_info: object) -> None:
        return None

    def __iter__(self) -> Iterator[Any]:
        return self._items


class FakeClient:
    """ClickHouse, as far as an export sees it.

    Each query's rows come from a factory, in blocks, made as they are
    read. `executed` records every query sent, by the table it reads,
    and `in_flight` how many rows had been read and not yet written
    each time a block was handed over — `written` is the export's to
    keep up to date.
    """

    def __init__(self, tables: Mapping[str, Rows], block: int) -> None:
        self.tables = tables
        self.block = block
        self.executed: list[str] = []
        #: Arrow responses read to their end.
        self.drained = 0
        self.read = 0
        self.written = 0
        self.in_flight: list[int] = []

    def _table(self, sql: str) -> str:
        for name, query in KNOWN.items():
            if query in sql:
                return name
        raise AssertionError(f'an export sent an unexpected query: {sql}')

    def _rows(self, sql: str) -> tuple[str, Iterator[dict[str, Any]]]:
        name = self._table(sql)
        self.executed.append(name)
        return name, self.tables.get(name, lambda: iter(()))()

    def _blocks(
        self, rows: Iterator[dict[str, Any]],
    ) -> Iterator[list[dict[str, Any]]]:
        block: list[dict[str, Any]] = []
        for row in rows:
            block.append(row)
            if len(block) == self.block:
                yield self._handed_over(block)
                block = []
        if block:
            yield self._handed_over(block)

    def _handed_over(
        self, block: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        self.read += len(block)
        self.in_flight.append(self.read - self.written)
        return block

    def query(self, sql: str, **_: Any) -> Any:
        """A count: `SELECT count() AS n FROM ...`."""
        rows = self._rows(sql)[1]
        n = sum(1 for _ in rows)
        return SimpleNamespace(
            named_results=lambda: iter([{'n': n}]),
            result_rows=[(n,)],
        )

    def query_row_block_stream(self, sql: str, **_: Any) -> _Stream:
        rows = self._rows(sql)[1]
        stream: _Stream

        def blocks() -> Iterator[list[tuple[Any, ...]]]:
            for block in self._blocks(rows):
                stream.source.column_names = list(block[0])
                yield [tuple(row.values()) for row in block]

        stream = _Stream(blocks())
        return stream

    def query_arrow_stream(self, sql: str, **_: Any) -> _Stream:
        name, rows = self._rows(sql)
        schema = _source_schema(name)

        def drained() -> None:
            self.drained += 1

        return _Stream(
            (
                pa.RecordBatch.from_pylist(block, schema=schema)
                for block in self._blocks(rows)
            ),
            drained,
        )


@pytest.fixture
def connect(monkeypatch: pytest.MonkeyPatch) -> Callable[..., Any]:
    """A `QueryRepository` whose driver is a `FakeClient`."""
    def make(
        tables: Mapping[str, Rows], block: int = 1_000,
    ) -> tuple[QueryRepository, FakeClient]:
        client = FakeClient(tables, block)
        monkeypatch.setattr(
            'clickhouse_connect.get_client', lambda **_: client,
        )
        return QueryRepository(DatabaseConfig()), client
    return make


def artifacts(count: int) -> Rows:
    """`count` artifact rows over a hundred names and ten versions,
    sorted by name as the query sorts them."""
    def rows() -> Iterator[dict[str, Any]]:
        per_name = max(count // 100, 1)
        for i in range(count):
            yield {
                'repository_id': i % 5_000 + 1,
                'name': f'package-{i // per_name:03d}',
                'version': f'1.{i % 10}.0',
                'type': 'npm',
                'found_by': 'javascript-lock-cataloger',
                'relationship': 'transitive' if i % 3 else 'direct',
                'source': SYFT,
                'version_kind': 'resolved',
            }
    return rows


class TestTheArrowStream:

    def test_is_read_to_the_end_of_the_response(self, connect) -> None:
        """Not only to Arrow's end-of-stream marker, which comes
        before the response ends.

        A response closed with bytes unread takes its connection with
        it, so the next query went out on a new one while the server
        could still be finishing this one in the same session, and now
        and then found it locked: `SESSION_IS_LOCKED`, on the Parquet
        export's next table. Read to its end, the connection goes back
        to the pool and the next query waits behind this one on it.
        """
        repository, client = connect({'artifacts': artifacts(10)})
        batches = repository.stream_arrow(QUERIES['artifacts'])
        assert sum(batch.num_rows for batch in batches) == 10
        assert client.drained == 1
