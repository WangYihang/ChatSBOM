"""Writing a snapshot: the warehouse's rows into one SQLite file (#132).

The warehouse is opened read-only, each of the tables asked of it
(`tables.py`) and fed to SQLite through Python's `sqlite3`, a batch of
DuckDB's result at a time: DuckDB's own `sqlite` extension is fetched
from the network when it is first used, and nothing here may be. Then
the warehouse is closed, and the page table, `dependants`, is made in
SQLite from those tables (`schema.add_dependants`), sorted in memory.
The tables are made first and the indexes once every row is in, which
is faster than keeping them up to date row by row, and leaves every
page of the file full: the file is written once, in order, and nothing
in it is updated or deleted, so it has no free page for `VACUUM` to
reclaim. Then `ANALYZE`, so that SQLite plans the page's queries from
what the file holds.

The file is written with no journal (`journal_mode=OFF`): it is nobody's
until it is published, and a write that stops takes it with it. So it
is closed with no `-journal`, `-wal` or `-shm` beside it, and its header
says it is not a WAL file, which a reader could not open read-only
without making a `-shm`. It is made read-only on disk as it is closed.

**The id** is the hash of what the file serves: SHA-256 over each table
the warehouse fills, in the order `SCHEMA` declares them, each as its
name, its columns, every row in the order it is written, and how many
there were, and last the `meta` row but its id. A row is its values as
a JSON array, which spells a string by its code points alone, whatever
Python's Unicode tables say. Every statement orders its rows totally,
so the same warehouse content gives the same bytes, whatever order the
warehouse's own rows are in; and the id is the first sixteen hex digits.
`dependants` is made from those tables by this code, whose version is in
`meta`, so what it holds is in the id without hashing it again. What is
not served is not in it: when or where a pass ran, what the warehouse
counted of the whole store, the indexes. What is served, is: a row of
any table, and the version of the code, which `meta` shows the page, so
the first pass after an upgrade publishes once.
"""
from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
import uuid
from collections.abc import Sequence
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from typing import Protocol
from typing import TYPE_CHECKING

from chatsbom.__version__ import __version__
from chatsbom.dataset.open import SUFFIX
from chatsbom.export.schema import SCHEMA_VERSION
from chatsbom.snapshot import tables
from chatsbom.snapshot.schema import add_dependants
from chatsbom.snapshot.schema import DEPENDANTS
from chatsbom.snapshot.schema import META
from chatsbom.snapshot.schema import SCHEMA
from chatsbom.snapshot.schema import Table
from chatsbom.warehouse import connect

if TYPE_CHECKING:
    import duckdb

#: Rows fetched from DuckDB, inserted and hashed at a time.
BATCH = 50_000

#: A snapshot's name while it is written, before its id is known: in the
#: directory it is published in, so that publishing it is a rename, and
#: hidden, so that nothing takes it for a snapshot until then.
WRITING = '.building-'

#: What the hash starts with: the scheme, so that a change to what is
#: hashed changes every id rather than some.
SCHEME = b'chatsbom snapshot 1\n'

#: SQLite's page cache while the file is written, in KiB: an index is
#: sorted in it, and one of the facts' is some 200 MB at the documented
#: shape.
CACHE_KIB = 256 * 1024

#: How long an id is, in hex digits.
ID_DIGITS = 16


class Digest(Protocol):
    """What the id is hashed into."""

    def update(self, data: bytes, /) -> None:
        ...


@dataclass(frozen=True)
class Written:
    """A snapshot, written, closed and read-only, not yet published."""

    path: Path
    id: str
    #: Rows of each table, by name: `meta.rows`.
    rows: dict[str, int]
    #: Seconds each step took, by name.
    seconds: dict[str, float]


def write(
    warehouse: Path,
    directory: Path,
    *,
    batch: int = BATCH,
) -> Written:
    """A snapshot of the warehouse at `warehouse`, written in `directory`
    under a name of its own. A write that fails takes its file with it."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / f'{WRITING}{uuid.uuid4().hex}{SUFFIX}'
    try:
        return _write(Path(warehouse), path, batch)
    except BaseException:
        path.unlink(missing_ok=True)
        raise


def _write(warehouse: Path, path: Path, batch: int) -> Written:
    seconds: dict[str, float] = {}
    rows: dict[str, int] = {}
    content = hashlib.sha256(SCHEME)
    with closing(sqlite3.connect(path, isolation_level=None)) as target:
        started = time.perf_counter()
        _create(target)
        target.execute('BEGIN')
        with connect(warehouse, read_only=True) as source:
            for statement in tables.PREPARED:
                source.execute(statement)
            seconds['prepare'] = time.perf_counter() - started
            for table in SCHEMA.tables:
                if table.name not in tables.ROWS:
                    continue
                started = time.perf_counter()
                rows[table.name] = _copy(
                    source, target, table, tables.ROWS[table.name],
                    content, batch,
                )
                seconds[table.name] = time.perf_counter() - started
            meta = _meta(source)
        # The warehouse is closed, and what DuckDB held is free for the
        # sort the page table is grouped by.
        started = time.perf_counter()
        rows[DEPENDANTS.name] = add_dependants(target)
        seconds[DEPENDANTS.name] = time.perf_counter() - started
        rows[META.name] = 1
        rows = {table.name: rows[table.name] for table in SCHEMA.tables}
        meta['rows'] = json.dumps(rows, sort_keys=True, separators=(',', ':'))
        _hash(content, META.name, list(meta), [tuple(meta.values())])
        snapshot = content.hexdigest()[:ID_DIGITS]
        meta['snapshot'] = snapshot
        _insert(target, META, [tuple(meta[c] for c in META.column_names)])
        target.execute('COMMIT')

        started = time.perf_counter()
        _index(target)
        seconds['index'] = time.perf_counter() - started
        started = time.perf_counter()
        target.execute('ANALYZE')
        seconds['analyze'] = time.perf_counter() - started
    # Nothing writes it again: not even by mistake.
    os.chmod(path, 0o444)
    return Written(path=path, id=snapshot, rows=rows, seconds=seconds)


def _create(target: sqlite3.Connection) -> None:
    """The tables, and how the file is written: no journal, no sync
    until it is whole (`publish` syncs it), a cache to sort the indexes
    in, and the page table's sort in memory, which is some 1 GB at the
    documented shape: nothing of it lands in a temporary directory. The
    page table is added once the others are written, from their
    rows."""
    for pragma in (
        'journal_mode = OFF', 'synchronous = OFF',
        'locking_mode = EXCLUSIVE', f'cache_size = -{CACHE_KIB}',
        'temp_store = MEMORY',
    ):
        target.execute(f'PRAGMA {pragma}')
    for table in SCHEMA.tables:
        if table is not DEPENDANTS:
            target.execute(table.ddl())


def _copy(
    source: duckdb.DuckDBPyConnection,
    target: sqlite3.Connection,
    table: Table,
    sql: str,
    content: Digest,
    batch: int,
) -> int:
    """One table's rows, from `sql` into `table`, hashed as they go; how
    many."""
    _begin(content, table.name, table.column_names)
    result = source.execute(sql)
    count = 0
    while chunk := result.fetchmany(batch):
        _insert(target, table, chunk)
        _rows(content, chunk, first=count == 0)
        count += len(chunk)
    _end(content, count)
    return count


def _insert(
    target: sqlite3.Connection,
    table: Table,
    rows: Sequence[Sequence[Any]],
) -> None:
    columns = table.column_names
    target.executemany(
        f"INSERT INTO {table.name} ({', '.join(columns)}) "
        f"VALUES ({', '.join('?' for _ in columns)})",
        rows,
    )


def _meta(source: duckdb.DuckDBPyConnection) -> dict[str, Any]:
    """`meta`'s row but its id and its rows, by column. Today's
    generator, contract version and span, the four D1's `meta` had;
    then the version on its own, and the corpus."""
    [(observed_from, observed_to)] = source.execute(tables.SPAN).fetchall()
    [(corpus,)] = source.execute(tables.CORPUS).fetchall()
    return {
        'generator': f'chatsbom/{__version__}',
        'schema_version': SCHEMA_VERSION,
        'observed_from': observed_from,
        'observed_to': observed_to,
        'version': __version__,
        'corpus': corpus,
    }


def _index(target: sqlite3.Connection) -> None:
    for index in SCHEMA.indexes:
        target.execute(index.ddl())


# -- the id ---------------------------------------------------------------


def _begin(content: Digest, name: str, columns: Sequence[str]) -> None:
    content.update(f"{name} {','.join(columns)}\n".encode('ascii'))


def _rows(
    content: Digest,
    chunk: Sequence[Sequence[Any]],
    first: bool,
) -> None:
    """A batch of rows, as the JSON arrays of their values, one after
    another with a comma between: one `dumps` for the batch, and the
    same bytes however the rows were batched."""
    encoded = json.dumps(chunk, separators=(',', ':'))[1:-1]
    content.update(((',' if not first else '') + encoded).encode('ascii'))


def _end(content: Digest, count: int) -> None:
    content.update(f'\n{count}\n'.encode('ascii'))


def _hash(
    content: Digest,
    name: str,
    columns: Sequence[str],
    rows: Sequence[Sequence[Any]],
) -> None:
    _begin(content, name, columns)
    _rows(content, rows, first=True)
    _end(content, len(rows))
