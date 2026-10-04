"""One pass: the store in, a new `warehouse.duckdb` out.

The pass is the single writer, and holds the file only while it runs
(#128 §2.3):

- a lock beside the file stops a second pass, which says so rather
  than waits: two passes would read one store twice for one answer;
- the pass writes a file of its own, `<name>.building`, and renames it
  over the warehouse once everything in it is derived. A reader, the
  operator's DuckDB CLI say, opens the last complete file until then,
  and is never refused: DuckDB lets one process write a file or many
  read it, and a pass opens the file they read only as they do, to
  read it.

The store is read (`store.py`), and the last warehouse, for what of
the store has not changed since it was built (`carry.py`): its rows are
copied, and the derived tables made again from everything. A warehouse
made by other code is not read, so a change of definition needs no
migration: the next pass reads the whole store.
"""
from __future__ import annotations

import fcntl
import os
import shutil
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
from typing import Any
from typing import TYPE_CHECKING

from chatsbom.__version__ import __version__
from chatsbom.core.config import PathConfig
from chatsbom.core.instants import UNSET
from chatsbom.core.instants import utc
from chatsbom.warehouse import connect
from chatsbom.warehouse import schema
from chatsbom.warehouse import SPILL
from chatsbom.warehouse.carry import code
from chatsbom.warehouse.carry import FORMAT
from chatsbom.warehouse.carry import identity
from chatsbom.warehouse.carry import PREVIOUS
from chatsbom.warehouse.carry import Previous
from chatsbom.warehouse.carry import State
from chatsbom.warehouse.rollups import derive
from chatsbom.warehouse.store import StoreReader
from chatsbom.warehouse.store import Unit
from chatsbom.warehouse.writer import _instant
from chatsbom.warehouse.writer import Writer

if TYPE_CHECKING:
    import duckdb

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
    #: Repositories read, and carried over unread from the warehouse
    #: before (`carry.py`); why every one was read, '' when not.
    read: int = 0
    carried: int = 0
    full: str = ''
    #: Directories asked whether they changed, and seconds asking.
    checked: int = 0
    check_seconds: float = 0.0
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
    full: bool = False,
) -> BuildReport:
    """Build the warehouse at `output` from the store at `paths`.

    `today` decides whether today's search snapshot is complete
    (`core/catalog.py`), UTC's by default, as `github search` dates one.
    `progress` is told how many repositories have been read or carried.
    Each repository the warehouse at `output` already has, and whose
    store has not changed since, is carried over from it unread
    (`carry.py`); `full` reads every one.
    """
    output = Path(output)
    output.parent.mkdir(parents=True, exist_ok=True)
    today = today or datetime.now(timezone.utc).date()
    with _held(output.with_name(output.name + LOCK)):
        building = output.with_name(output.name + BUILDING)
        _remove(building)
        _remove_abandoned_spill(output)
        try:
            report = _write(
                paths, building, today, progress, None if full else output,
            )
        except BaseException:
            # Nothing of a pass that did not finish is kept: the last
            # warehouse stands, and the next pass starts from the store.
            _remove(building)
            raise
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
    before: Path | None,
) -> BuildReport:
    started = time.perf_counter()
    # The code this pass runs, as it stands when the pass starts.
    made_by = code()
    reader = StoreReader(paths, today)
    universe = reader.universe()
    report = BuildReport(output=building, corpus=universe.corpus)
    con = connect(building)
    try:
        schema.create(con)
        reader.list_roots()
        previous: Previous | None = None
        if before is None:
            report.full = 'asked to read the whole store'
        else:
            previous, report.full = Previous.attach(con, before, paths)
        if previous is not None:
            previous.check()
            report.checked = previous.asked
            report.check_seconds = previous.seconds
        known: set[int] = set()
        seen: datetime | None = None
        inputs: dict[int, dict[str, Any]] = {}
        #: Whether each repository read had settled, by id.
        trusted: dict[int, bool] = {}
        #: The first scan id of each repository carried over.
        carried: dict[int, int] = {}
        with Writer(con, scratch=building.parent) as writer:
            writer.extend('repository_history', reader.history())
            for read in reader.units(
                universe, previous.unchanged if previous else None,
            ):
                if read.carried:
                    assert previous is not None and read.id is not None
                    kept = previous.kept[read.id]
                    # Where a pass that read it would number its scans.
                    carried[read.id] = writer.reserve(kept.scans)
                    if kept.named:
                        known.add(read.id)
                        report.repositories += 1
                        if progress is not None:
                            progress(report.repositories)
                    continue
                report.read += read.row is not None
                report.unreadable += read.unreadable
                report.unnamed += read.unnamed
                if read.id is not None:
                    _add_input(inputs, read)
                    state = reader.states.pop(read.id, None)
                    if state is not None:
                        trusted[read.id] = state.trusted
                        _add_directories(writer, read.id, state)
                for parent, child in sorted(read.edges):
                    writer.add(
                        'graph_edges', {
                            'repository_id': read.id,
                            'parent': parent, 'child': child,
                        },
                    )
                if read.graph_seen is not None:
                    seen = max(seen or read.graph_seen, read.graph_seen)
                if read.row is None:
                    continue
                writer.add('repositories', read.row)
                # The last of a tag wins, as ClickHouse kept the row
                # inserted last of those that shared its key.
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
            for repository_id, entry in inputs.items():
                entry['trusted'] = trusted.get(repository_id, False)
                writer.add('inputs', entry)
            # Of the repositories written: one whose record could not be
            # read has no row, as it had none in ClickHouse.
            corpus = known if universe.ids is None else universe.ids & known
            writer.extend('corpus', ({'id': i} for i in sorted(corpus)))
        if carried:
            assert previous is not None
            seen = _carry(con, carried, report, seen)
            report.carried = len(carried)
        if previous is not None:
            con.execute(f'DETACH {PREVIOUS}')
        # Each pair, and how many repositories' newest graphs show it, as
        # `db edges` counted them; dated by the newest graph counted.
        con.execute(
            'INSERT INTO edges SELECT parent, child, count(*), ? '
            'FROM graph_edges GROUP BY parent, child ORDER BY child, parent',
            [_instant(max(seen or UNSET, UNSET))],
        )
        report.read_seconds = time.perf_counter() - started
        report.corpus_size = len(corpus)
        derive(con, report.derived_seconds)
        for name in report.derived_seconds:
            (count,), = con.execute(f'SELECT count(*) FROM {name}').fetchall()
            report.derived_rows[name] = int(count)
        device, inode = identity(paths)
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
                    'carried': report.carried,
                    'full_reason': report.full,
                    'format': FORMAT,
                    'code': made_by,
                    'store_device': device,
                    'store_inode': inode,
                    'carryable': reader.carryable,
                },
            )
        con.execute('CHECKPOINT')
    finally:
        con.close()
    return report


def _add_input(inputs: dict[int, dict[str, Any]], read: Unit) -> None:
    """What `read` adds to its repository's `inputs` row: a repository
    whose record cannot be read is read in two units."""
    assert read.id is not None
    entry = inputs.setdefault(
        read.id, {
            'repository_id': read.id, 'record': '', 'named': False,
            'scans': 0, 'unreadable': 0, 'unnamed': False,
            'graph_observed_at': None,
        },
    )
    entry['record'] = entry['record'] or read.record
    entry['named'] = entry['named'] or read.row is not None
    entry['scans'] += len(read.scans)
    entry['unreadable'] += read.unreadable
    entry['unnamed'] = entry['unnamed'] or read.unnamed
    if read.graph_seen is not None:
        entry['graph_observed_at'] = read.graph_seen


def _add_directories(writer: Writer, repository_id: int, state: State) -> None:
    """The repository's directories, as they were before it was read."""
    for directory in state.directories:
        writer.add(
            'input_directories', {
                'repository_id': repository_id,
                'path': directory.path,
                'inode': directory.inode,
                'mtime_ns': directory.mtime_ns,
                'ctime_ns': directory.ctime_ns,
            },
        )


def _carry(
    con: duckdb.DuckDBPyConnection,
    carried: dict[int, int],
    report: BuildReport,
    seen: datetime | None,
) -> datetime | None:
    """Every row of each repository in `carried` from the warehouse
    before, its scans numbered from the id given, in the order it had
    them: the ids a pass that read it would have given them. What they
    add to the report; the newest graph `edges` counts, of these and
    `seen`."""
    con.execute(
        'CREATE TEMP TABLE carried AS SELECT '
        'unnest(?::UBIGINT[]) AS repository_id, '
        'unnest(?::UINTEGER[]) AS first_scan',
        [list(carried), list(carried.values())],
    )
    con.execute(
        'CREATE TEMP TABLE scan_ids AS SELECT s.scan_id AS before, '
        'c.first_scan + row_number() OVER ('
        '    PARTITION BY s.repository_id ORDER BY s.scan_id'
        ') - 1 AS now '
        f'FROM {PREVIOUS}.scans AS s JOIN carried AS c USING (repository_id)',
    )
    for table in ('scans', 'observations'):
        con.execute(
            f'INSERT INTO {table} SELECT n.now, t.* EXCLUDE (scan_id) '
            f'FROM {PREVIOUS}.{table} AS t '
            'JOIN scan_ids AS n ON t.scan_id = n.before',
        )
    for table, key in (
        ('repositories', 'id'), ('releases', 'repository_id'),
        ('graph_edges', 'repository_id'), ('inputs', 'repository_id'),
        ('input_directories', 'repository_id'),
    ):
        con.execute(
            f'INSERT INTO {table} SELECT * FROM {PREVIOUS}.{table} '
            f'WHERE {key} IN (SELECT repository_id FROM carried)',
        )
    for source, count, observations in con.execute(
        'SELECT source, count(*), coalesce(sum(observations), 0) '
        'FROM scans WHERE repository_id IN '
        '(SELECT repository_id FROM carried) GROUP BY source',
    ).fetchall():
        report.scans[source] += int(count)
        report.observations += int(observations)
    (unreadable, unnamed, newest), = con.execute(
        'SELECT coalesce(sum(unreadable), 0), count(*) FILTER (unnamed), '
        'max(graph_observed_at) FROM inputs WHERE repository_id IN '
        '(SELECT repository_id FROM carried)',
    ).fetchall()
    report.unreadable += int(unreadable)
    report.unnamed += int(unnamed)
    con.execute('DROP TABLE scan_ids')
    con.execute('DROP TABLE carried')
    if newest is None:
        return seen
    newest = utc(newest)
    return newest if seen is None else max(seen, newest)


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
    """What a pass that did not finish left: its file, its log, and what
    DuckDB spilled for it, if it was killed (`chatsbom.warehouse.spill`).
    Only a pass writes that file, under the lock, so all of it is a
    stopped pass's."""
    for leftover in (building, building.with_name(building.name + '.wal')):
        leftover.unlink(missing_ok=True)
    for spilled in building.parent.iterdir():
        if spilled.name.startswith(building.name + SPILL) and spilled.is_dir():
            shutil.rmtree(spilled)


def _remove_abandoned_spill(output: Path) -> None:
    """What readers of the warehouse spilled beside it and left, when
    none has it open (#150).

    A reader, `snapshot build` or the export, spills into a directory of
    its own beside the file, which DuckDB removes when the reader closes
    it; one killed first, by the collector's stop or by the container's
    memory limit, leaves it, up to the gigabytes it was sorting, each
    time. Whose a directory is cannot be told from it, but a reader
    spills only while it has the file open, and DuckDB holds a lock on
    the file for as long as a process has it open: a read lock for a
    reader, and a write lock for a writer. So when a write lock on the
    file can be had, no process has it open, and every directory found
    before asking is one no reader will use again. One that opens the
    file after that spills into a directory of its own, not among them.

    The lock asked for is on the file this pass is about to replace. A
    reader of an earlier one, which a pass has since replaced, holds none
    on it, and would lose its directory: it would have read through a
    whole pass, a day beside the collector. And it is asked from the pass,
    which never opens that file: a process that had it open could not
    ask, since closing any descriptor of a file drops every lock the
    process holds on it, DuckDB's with the rest.
    """
    prefix = output.name + SPILL
    spilled = [
        path for path in output.parent.iterdir()
        if path.name.startswith(prefix) and path.is_dir()
    ]
    if not spilled:
        return
    try:
        descriptor = os.open(output, os.O_RDWR)
    except FileNotFoundError:
        # No warehouse, and so no reader of one.
        pass
    except OSError:
        # Not this user's to write: whether a reader has it open cannot
        # be asked, and what they spilled stays.
        return
    else:
        try:
            fcntl.lockf(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            # A reader has it open, and may be spilling.
            return
        finally:
            os.close(descriptor)
    for directory in spilled:
        shutil.rmtree(directory, ignore_errors=True)


def _sync(directory: Path) -> None:
    """The rename, made durable: a crash after it keeps the new file."""
    descriptor = os.open(directory, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
