"""Both exports stream: what they hold is bounded by a row group or a
batch of INSERTs rather than by the table, and each query runs once.

Both held the whole artifacts table in Python lists. For the corpus's
16.8 million rows, D1's `normalise` took about 147 bytes a row, 2.3 GiB,
and the Parquet export's `_columnar` about 241 bytes a row, 3.8 GiB,
before Arrow copied it. The Parquet export also ran each query twice,
the first time only to count the rows.

The ClickHouse client is faked, underneath the real `QueryRepository`,
so the repository's own reading is part of what is tested. Its rows are
made as they are read and never kept: whatever is held is the export's.
"""
from __future__ import annotations

import tracemalloc
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Mapping
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from chatsbom.core.config import DatabaseConfig
from chatsbom.core.repository import QueryRepository
from chatsbom.export.d1 import OBSERVATIONS_QUERY
from chatsbom.export.queries import D1_LICENSES_QUERY
from chatsbom.export.queries import QUERIES
from chatsbom.export.schema import ColumnType
from chatsbom.export.schema import EXPORT_SCHEMA
from chatsbom.models.provenance import SYFT

pa = pytest.importorskip('pyarrow')
pq = pytest.importorskip('pyarrow.parquet')

Rows = Callable[[], Iterator[dict[str, Any]]]

#: Each query an export sends, by what it reads. The D1 export reads its
#: licences grouped by licence alone, each source's date of each
#: repository, and the edges `db edges` stored.
KNOWN = {
    **QUERIES,
    'd1_licenses': D1_LICENSES_QUERY,
    'observations': OBSERVATIONS_QUERY,
    'edges': 'FROM edges',
}


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


def repositories(count: int) -> Rows:
    def rows() -> Iterator[dict[str, Any]]:
        for i in range(1, count + 1):
            yield {
                'id': i, 'owner': f'owner{i}', 'repo': f'repo{i}',
                'stars': 10 * i, 'language': 'javascript',
                'github_language': 'JavaScript',
                'language_bucket': 'javascript', 'ecosystems': ['npm'],
                'url': f'https://github.com/owner{i}/repo{i}',
                'description': f'Repository {i}', 'license_spdx_id': 'MIT',
                'pushed_at': '2026-09-01',
                'observed_at': f'2026-09-{i % 28 + 1:02d}',
                'sbom_ref': 'main', 'sbom_commit_sha': f'{i:040x}',
                'direct_dependencies': i % 7, 'total_dependencies': i % 11,
                'manifest_sources': ['package.json'] * (i % 2),
            }
    return rows


def licences(keyed_by_type: bool) -> Rows:
    def rows() -> Iterator[dict[str, Any]]:
        row: dict[str, Any] = {'license': 'MIT'}
        if keyed_by_type:
            row['type'] = 'npm'
        yield {**row, 'package_count': 3, 'repository_count': 2}
    return rows


def history(count: int) -> Rows:
    def rows() -> Iterator[dict[str, Any]]:
        for i in range(count):
            yield {
                'name': f'package-{i:05d}', 'month': '2026-09',
                'source': SYFT, 'repository_count': 2, 'direct_count': 1,
            }
    return rows


def edges(count: int) -> Rows:
    def rows() -> Iterator[dict[str, Any]]:
        for i in range(count):
            yield {
                'parent': f'package-{i % 100:03d}',
                'child': f'package-{(i + 1) % 100:03d}',
                'repositories': 3,
            }
    return rows


class TestTheParquetExport:

    ROWS = 60_000
    BLOCK = 1_000
    ROW_GROUP = 5_000

    @pytest.fixture
    def tables(self) -> dict[str, Rows]:
        return {
            'repositories': repositories(2_000),
            'artifacts': artifacts(self.ROWS),
            'licenses': licences(keyed_by_type=True),
            'history': history(3_000),
        }

    @pytest.fixture
    def counted(self, monkeypatch: pytest.MonkeyPatch) -> list[FakeClient]:
        """Every row given to a Parquet writer is counted as written, on
        the client in `counted[0]`."""
        clients: list[FakeClient] = []
        real = pq.ParquetWriter.write_table

        def write_table(
            writer: Any, table: Any, row_group_size: Any = None,
        ) -> None:
            clients[0].written += table.num_rows
            real(writer, table, row_group_size=row_group_size)

        monkeypatch.setattr(pq.ParquetWriter, 'write_table', write_table)
        monkeypatch.setattr(
            'chatsbom.export.parquet.ROW_GROUP_SIZE', self.ROW_GROUP,
        )
        return clients

    def test_each_query_runs_once(self, connect, tables, tmp_path) -> None:
        """It ran each twice: a `count()` over the query first, to catch
        a result cap that truncates without an error. The export's
        queries are sent with every overflow mode set to throw now, so
        a cap fails them instead, and the count is not needed."""
        from chatsbom.export.parquet import export_dataset
        repository, client = connect(tables)
        export_dataset(repository, tmp_path)
        assert sorted(client.executed) == sorted(tables)

    def test_batches_are_written_as_they_arrive(
        self, connect, tables, counted, tmp_path,
    ) -> None:
        """Never more than a row group and a block read but not written.

        Every row was collected into Python lists before anything was
        written: at the last block, all 60,000 were in flight.
        """
        from chatsbom.export.parquet import export_dataset
        repository, client = connect(tables, block=self.BLOCK)
        counted.append(client)

        result = export_dataset(repository, tmp_path)

        assert result.row_counts['artifacts'] == self.ROWS
        assert client.read == client.written
        assert max(client.in_flight) <= self.ROW_GROUP + self.BLOCK

    def test_the_rows_arrive_whole_and_as_declared(
        self, connect, tables, counted, tmp_path,
    ) -> None:
        """Every row, in order, in row groups of the declared size, and
        typed as the contract says rather than as ClickHouse sent it:
        signed where it sent unsigned, nullable where it sent not null.
        """
        from chatsbom.export.parquet import _arrow_schema
        from chatsbom.export.parquet import export_dataset
        repository, client = connect(tables, block=self.BLOCK)
        counted.append(client)

        export_dataset(repository, tmp_path)

        for table in EXPORT_SCHEMA.tables:
            [path] = sorted(tmp_path.glob(f'{table.name}-*.parquet'))
            written = pq.ParquetFile(path)
            assert written.schema_arrow == _arrow_schema(table)
            assert written.read().to_pylist() == list(tables[table.name]())
        groups = pq.ParquetFile(
            next(tmp_path.glob('artifacts-*.parquet')),
        ).metadata
        assert [
            groups.row_group(i).num_rows for i in range(groups.num_row_groups)
        ] == [self.ROW_GROUP] * (self.ROWS // self.ROW_GROUP)


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


class TestTheD1Export:

    ROWS = 50_000

    @staticmethod
    def export(repository: QueryRepository, directory: Path) -> Any:
        from chatsbom.export.d1 import export_d1
        return export_d1(repository, directory)

    def test_the_artifacts_are_not_held(
        self, connect, tmp_path, monkeypatch,
    ) -> None:
        """Referenced and written a batch at a time.

        `normalise` returned every artifact as a tuple in one list, and
        the list lived until the last INSERT was written. What has to be
        kept is the lookups, a hundred names and ten versions here.
        """
        # The export once walked `data/09-github-depgraph` under the
        # working directory; there is none here.
        monkeypatch.chdir(tmp_path)

        def run(count: int, directory: Path) -> int:
            repository, _ = connect({
                'repositories': repositories(50),
                'artifacts': artifacts(count),
                'd1_licenses': licences(keyed_by_type=False),
                'history': history(50),
                'edges': edges(50),
            })
            tracemalloc.start()
            try:
                result = self.export(repository, directory)
                peak = tracemalloc.get_traced_memory()[1]
            finally:
                tracemalloc.stop()
            assert result.row_counts['artifacts'] == count
            return peak

        # Once small, so what is imported or cached on first use is not
        # counted against the export.
        run(1_000, tmp_path / 'warm')
        peak = run(self.ROWS, tmp_path / 'd1')

        # Measured: 6.0 MB at the peak when the references were held in
        # a list, 1.2 MB streamed.
        assert peak < 2_500_000, f'{peak:,} bytes at the peak'
