"""One pass: the store in, a new `warehouse.duckdb` out.

The pass is the single writer, and holds the file only while it runs
(#128 §2.3):

- a lock beside the file stops a second pass, which says so rather
  than waits: two passes would read one store twice for one answer;
- the pass writes a file of its own, `<name>.building`, and renames it
  over the warehouse once everything in it is derived. A reader, the
  operator's DuckDB CLI say, opens the last complete file until then,
  and is never refused: DuckDB lets one process write a file or many
  read it, and no pass ever opens the file they read.

Nothing but the store is read (`store.py`), and nothing is kept from
the last pass: a warehouse is thrown away and made again, so a change
of definition needs no migration.
"""
from __future__ import annotations

import fcntl
import os
import time
from collections import Counter
from collections.abc import Callable
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import field
from datetime import date
from datetime import datetime
from datetime import timezone
from pathlib import Path

from chatsbom.__version__ import __version__
from chatsbom.core.config import PathConfig
from chatsbom.warehouse import connect
from chatsbom.warehouse import schema
from chatsbom.warehouse.rollups import derive
from chatsbom.warehouse.store import StoreReader
from chatsbom.warehouse.writer import Writer

#: What a pass writes before it is renamed into place.
BUILDING = '.building'
#: The lock a pass holds, beside the file.
LOCK = '.lock'


class WarehouseBusy(RuntimeError):
    """Another pass holds the lock."""


@dataclass
class BuildReport:
    """What a pass read and wrote."""

    output: Path
    #: The snapshot the corpus is, or '' when the store has none.
    corpus: str
    repositories: int = 0
    corpus_size: int = 0
    #: Scans by source.
    scans: Counter[str] = field(default_factory=Counter)
    observations: int = 0
    unreadable: int = 0
    unnamed: int = 0
    #: Seconds reading the store, and deriving each table, by name.
    read_seconds: float = 0.0
    derived_seconds: dict[str, float] = field(default_factory=dict)
    #: Rows of each derived table.
    derived_rows: dict[str, int] = field(default_factory=dict)
    size: int = 0


def build(
    paths: PathConfig,
    output: Path,
    *,
    today: date | None = None,
    progress: Callable[[int], None] | None = None,
) -> BuildReport:
    """Build the warehouse at `output` from the store at `paths`.

    `today` decides whether today's search snapshot is complete
    (`core/catalog.py`), UTC's by default, as `github search` dates one.
    `progress` is told how many repositories have been read.
    """
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    today = today or datetime.now(timezone.utc).date()
    with _held(output.with_name(output.name + LOCK)):
        building = output.with_name(output.name + BUILDING)
        _remove(building)
        report = _write(paths, building, today, progress)
        os.replace(building, output)
        _sync(output.parent)
    report.output = output
    report.size = output.stat().st_size
    return report


def _write(
    paths: PathConfig,
    building: Path,
    today: date,
    progress: Callable[[int], None] | None,
) -> BuildReport:
    started = time.perf_counter()
    reader = StoreReader(paths, today)
    universe = reader.universe()
    report = BuildReport(output=building, corpus=universe.corpus)
    con = connect(building)
    try:
        schema.create(con)
        known: set[int] = set()
        with Writer(con, scratch=building.parent) as writer:
            writer.extend('repository_history', reader.history())
            for read in reader.repositories(universe):
                writer.add('repositories', read.row)
                # The last of a tag wins, as ClickHouse keeps the row
                # inserted last of those that share its key.
                releases = {r['tag_name']: r for r in read.releases}
                writer.extend('releases', releases.values())
                for scan in read.scans:
                    writer.scan(scan)
                    report.scans[scan.source] += 1
                    report.observations += len(scan.rows)
                known.add(int(read.row['id']))
                report.repositories += 1
                if progress is not None:
                    progress(report.repositories)
            # Of the repositories written: one whose record could not be
            # read has no row, as it has none in ClickHouse.
            corpus = known if universe.ids is None else universe.ids & known
            writer.extend('corpus', ({'id': i} for i in sorted(corpus)))
            writer.extend(
                'edges', (
                    {
                        'parent': parent, 'child': child,
                        'repositories': count,
                        'observed_at': reader.edges.observed_at,
                    }
                    for (parent, child), count in sorted(reader.edges.items())
                ),
            )
        report.read_seconds = time.perf_counter() - started
        report.corpus_size = len(corpus)
        report.unreadable = reader.unreadable
        report.unnamed = reader.unnamed
        derive(con, report.derived_seconds)
        for name in report.derived_seconds:
            (count,), = con.execute(f'SELECT count(*) FROM {name}').fetchall()
            report.derived_rows[name] = int(count)
        with Writer(con, scratch=building.parent) as writer:
            writer.add(
                'build', {
                    'version': __version__,
                    'built_at': datetime.now(timezone.utc),
                    'store': str(paths.base_data_dir),
                    'corpus': report.corpus,
                    'repositories': report.repositories,
                    'scans': sum(report.scans.values()),
                    'observations': report.observations,
                    'unreadable': report.unreadable,
                    'unnamed': report.unnamed,
                },
            )
        con.execute('CHECKPOINT')
    finally:
        con.close()
    return report


@contextmanager
def _held(lock: Path) -> Iterator[None]:
    """The pass's lock, or `WarehouseBusy` at once."""
    with lock.open('a') as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise WarehouseBusy(str(lock)) from error
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def _remove(building: Path) -> None:
    """What a pass that did not finish left: its file and its log."""
    for leftover in (building, building.with_name(building.name + '.wal')):
        leftover.unlink(missing_ok=True)


def _sync(directory: Path) -> None:
    """The rename, made durable: a crash after it keeps the new file."""
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
