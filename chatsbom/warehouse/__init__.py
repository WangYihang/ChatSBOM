"""The warehouse: an embedded DuckDB file, rebuilt from the store (#131).

Decision Q2 of #128: the analytics move from the ClickHouse server to a
DuckDB file that a pass builds from `data/` alone, and nothing else
writes. It is disposable: delete it and the next pass makes it again,
which is why it is never backed up. Until the cutover it stands beside
ClickHouse, which stays what the dashboard reads, and nothing in the
collector's loop builds it: `chatsbom warehouse build` does, when asked.

What a pass does, in order:

- reads the store with the parsers `db index` uses (`DbService`), so
  one Syft document, manifest or dependency graph is the same rows in
  either engine (`store.py`);
- writes what it read as `scans`, each an input read by one tool, and
  `observations`, what each scan saw, append-only: every scan the store
  holds, not only the newest, with `repositories`, their history,
  `releases` and `edges` (`schema.py`, `writer.py`);
- derives the current facts, the newest scan of each repository and
  source of the corpus, and the rollups from them (`rollups.py`).

It builds into a file of its own and renames it over the last one when
it has finished, so the file is held only for the pass: between passes
the operator's DuckDB CLI can open it, and a reader never sees half a
pass (`build.py`). `parity.py` compares it with ClickHouse on the same
input.

Every connection to DuckDB is `connect`'s: a pass's, the snapshot's
(`chatsbom/snapshot/`) and the Parquet export's (`chatsbom/export/`).
So it is where DuckDB is told how much memory it may hold and how many
threads to run (#148), from `CHATSBOM_DUCKDB_MEMORY_LIMIT` and
`CHATSBOM_DUCKDB_THREADS` (`limits`), and where it spills what does not
fit (`spill`).

duckdb is imported where a connection is made, not here: the CLI
imports every command at start-up, and only this one needs it.
"""
from __future__ import annotations

import functools
import os
import re
import uuid
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import duckdb

#: The zone every connection works in. Every instant is stored as UTC,
#: in `TIMESTAMP` columns that carry no zone, and each month is made
#: from one (#120). An aware datetime inserted is converted to the
#: session's zone, which was the machine's: on a machine in UTC+8, an
#: instant at 20:00 UTC on 31 January was stored as 1 February.
TIMEZONE = 'UTC'

#: How much memory DuckDB may hold, unless CHATSBOM_DUCKDB_MEMORY_LIMIT
#: says. Its own default is 80% of the machine's, and a pass deriving the
#: documented shape held 5.5 GB at its peak (#141): more than the
#: collector's container has, `mem_limit: 4g` (docker-compose.yaml). What
#: does not fit is spilled to disk, beside the file DuckDB opened. The
#: limit is DuckDB's alone, and Python, Arrow's batches and SQLite's page
#: cache are not in it, so it leaves them the rest of the container.
MEMORY_LIMIT = '2GiB'

#: How many threads DuckDB runs, unless CHATSBOM_DUCKDB_THREADS says: the
#: collector's container's CPUs, `cpus: 2.0`. Its own default is a thread
#: per core of the machine, and each holds its own share of a query.
THREADS = 2

#: A memory limit as DuckDB spells one: a number and a unit, of 1000
#: (`KB` to `TB`) or of 1024 (`KiB` to `TiB`), in any case.
_MEMORY = re.compile(r'([0-9]+(?:\.[0-9]+)?) ?([KMGT]i?B)', re.IGNORECASE)


def limits() -> dict[str, str | int]:
    """DuckDB's memory limit and threads, as a connection's config.

    From the environment, read as each connection is made rather than at
    import: the CLI loads `.env` in its root callback, after every module
    is imported. Empty is unset, as a setting `.env.example` shows and a
    copy left as it was. One that is not a limit stops the connection
    before DuckDB is given it, naming itself (`handle_errors` says so as
    a validation error); DuckDB would read `8` as a unit it does not
    know, and `0GB` as memory it cannot run in.
    """
    memory = (os.getenv('CHATSBOM_DUCKDB_MEMORY_LIMIT') or '').strip()
    match = _MEMORY.fullmatch(memory or MEMORY_LIMIT)
    if match is None or not float(match[1]):
        raise ValueError(
            f'CHATSBOM_DUCKDB_MEMORY_LIMIT is {memory!r}: DuckDB\'s memory '
            'limit is a number and a unit, as 2GiB or 1500MB',
        )
    threads = (os.getenv('CHATSBOM_DUCKDB_THREADS') or '').strip()
    if threads and not (
        re.fullmatch('[0-9]+', threads, re.ASCII) and int(threads)
    ):
        raise ValueError(
            f'CHATSBOM_DUCKDB_THREADS is {threads!r}: the threads DuckDB '
            'runs are a whole number, 1 or more',
        )
    return {
        'memory_limit': match[0],
        'threads': int(threads) if threads else THREADS,
    }


#: The suffix of a spill directory's name, after the file's: `spill`.
SPILL = '.tmp-'


@functools.cache
def _process(pid: int) -> str:
    """This process's part of a spill directory's name: random, so that
    two processes never share one, whatever host or container each is
    in; and made again in a process forked from this one."""
    return uuid.uuid4().hex[:16]


def spill(path: str | Path) -> str | None:
    """Where a connection to the file at `path` spills: a directory of
    this process's own beside the file, `<file>.tmp-<id>`. None for a
    database in memory, which spills where DuckDB puts it.

    DuckDB's own is `<file>.tmp`, one for every process that opens the
    file, and two that spill into it at once read each other's blocks:
    two readers of one warehouse, each sorting 6M rows in 120 MB, both
    crashed (SIGSEGV, or "Corrupt temporary file"), where either alone
    was right. The snapshot and the export both read the warehouse, and
    a memory limit is what makes them spill.

    One a process, not one a connection: a process's connections to a
    file share one database, which DuckDB will not open twice with two
    configurations. DuckDB makes the directory when it first spills and
    removes it when the database closes; one a process that was killed
    left behind is nobody's, and can be deleted.
    """
    if str(path) == ':memory:':
        return None
    return f'{path}{SPILL}{_process(os.getpid())}'


def connect(
    path: str | Path,
    read_only: bool = False,
) -> duckdb.DuckDBPyConnection:
    """A connection to the warehouse at `path`, or `:memory:`: in UTC,
    within `limits`, spilling into `spill`."""
    import duckdb

    config: dict[str, str | bool | int | float | list[str]] = {
        'TimeZone': TIMEZONE, **limits(),
    }
    directory = spill(path)
    if directory is not None:
        config['temp_directory'] = directory
    return duckdb.connect(str(path), read_only=read_only, config=config)
