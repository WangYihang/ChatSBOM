"""What a pass carries over from the warehouse before it (#187).

A pass that reads the whole store reads every directory of it, and on a
disk that turns each one is a seek: two hours, of which the derived
tables are three minutes, while a day changes about a tenth of the
repositories. So a pass reads again only the repositories whose store
changed since the warehouse before it was built, and copies every other
one's rows from that file: its row, releases, scans and their
observations, and its part of `edges`. The derived tables are then made
from all of it, as they always were. What it builds is what a pass that
read the whole store would build, row for row, scan ids included; only
the `build` row says which it was.

**How a change is told.** Every writer of the store adds, replaces or
removes a name in a directory: a stage writes a file aside and renames
it into place (`core/fs.atomic_write_bytes`), a decision is linked into
place (`fs.write_once`), `data prune` removes a commit's directories,
and a new directory is a new name in the one above it. Each of those
sets the directory's mtime and ctime. So a pass keeps, of each
repository it reads, every directory under the repository's id in each
stage root the warehouse reads, with its inode, mtime and ctime, as it
found them before reading what is in them (`walk`, `input_directories`);
and the next pass asks each of those directories for its `stat` again
(`Previous.check`). A repository is carried over only when every one is
as it was, its id names the same directories in the stage roots, and
its record, as the lists and the snapshots make it, is the same
(`record_digest`, `inputs`). A `stat` of a directory reads its inode,
which is all a pass asks of a repository that did not change: in inode
order, a sweep of the inode tables, where reading the directories was a
seek apiece.

Nothing else is needed of the writers: what the collector writes, the
SBOM stage writing a document again after an upgrade of Syft, a fetch
of the graph, `data prune`, and a file moved or deleted by hand are all
told the same way. What is not told is a file written in place, by
`open(path, 'w')`, or its mtime set by hand: no writer of the store does
either since #96, and `warehouse build --full` reads everything.

**When nothing is carried.** A pass reads the whole store when asked to
(`--full`), and when the warehouse before it cannot be trusted to be
what this pass would have made of the same store: there is none, or it
cannot be opened, or it was made by other code than this pass's (`code`)
or with other tables (`FORMAT`), or from another store (`identity`), or
by a pass that could not carry (a record whose id is not a number).
A repository is read again, whatever its directories say, when one of
them changed in the `SETTLE_NS` before the pass that read it looked:
a change after that look could fall in the same tick of the clock the
file system stamps with, and leave the stamp as it was.
"""
from __future__ import annotations

import ast
import functools
import hashlib
import json
import os
import stat
import time
from collections.abc import Iterable
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from typing import TYPE_CHECKING

import structlog

import chatsbom
from chatsbom.core.config import PathConfig

if TYPE_CHECKING:
    import duckdb

logger = structlog.get_logger('warehouse')

#: The tables a pass keeps for the next, and how it reads them: another
#: number, and no warehouse before it is carried from.
FORMAT = 1

#: How long a directory must have been left before a pass trusts what
#: it found of it: a change after the look could otherwise share its
#: timestamp. A file system stamps from a clock that ticks every few
#: milliseconds; two seconds leaves room for that, and for a clock set
#: back by a little.
SETTLE_NS = 2 * 10**9

#: The modules whose code makes a repository's rows, as a pass reads
#: the store (`store.py`) and writes what it read (`writer.py`,
#: `build.py`, the tables in `schema.py`); with every module they import.
CODE_ROOTS: tuple[str, ...] = (
    'chatsbom.warehouse.store', 'chatsbom.warehouse.writer',
    'chatsbom.warehouse.build', 'chatsbom.warehouse.carry',
)
#: Of those, what makes no repository's rows: the derived tables, which
#: every pass makes again from all of them.
RECOMPUTED: frozenset[str] = frozenset({'chatsbom.warehouse.rollups'})


@dataclass(frozen=True)
class Directory:
    """A directory of a repository's, as a pass found it."""

    #: Relative to the data directory: `07-sbom/<id>/<sha>`.
    path: str
    inode: int
    mtime_ns: int
    ctime_ns: int


@dataclass
class State:
    """A repository's directories, and whether a pass may trust them to
    say whether it changed."""

    directories: list[Directory]
    trusted: bool = True


def walk(base: Path, tops: Iterable[Path], settle_ns: int | None = None) -> State:
    """Every directory under `tops`, each a repository's directory in a
    stage root, `stat` first and listed after, so that a change made
    while the pass reads it is a change the next pass sees.

    Not trusted when a directory changed within `settle_ns` (`SETTLE_NS`
    by default) of the look, when one is gone or a link where a
    directory was, or when a link is among what they hold: the store
    holds none, and a link's target is not walked.
    """
    settle = SETTLE_NS if settle_ns is None else settle_ns
    prefix = len(str(base)) + 1
    found: list[Directory] = []
    trusted = True
    started = time.time_ns()
    pending = [str(top) for top in tops]
    while pending:
        path = pending.pop()
        try:
            stated = os.stat(path, follow_symlinks=False)
            with os.scandir(path) as entries:
                listed = list(entries)
        except OSError:
            trusted = False
            continue
        if not stat.S_ISDIR(stated.st_mode):
            trusted = False
            continue
        found.append(
            Directory(
                path[prefix:], stated.st_ino, stated.st_mtime_ns,
                stated.st_ctime_ns,
            ),
        )
        if max(stated.st_mtime_ns, stated.st_ctime_ns) >= started - settle:
            trusted = False
        for entry in listed:
            if entry.is_symlink():
                trusted = False
            elif entry.is_dir(follow_symlinks=False):
                pending.append(entry.path)
    found.sort(key=lambda directory: directory.path)
    return State(found, trusted)


def record_digest(record: Mapping[str, Any]) -> str:
    """What a repository's record is, as the lists and the snapshots make
    it (`TrackedRecords`): a digest of every field."""
    text = json.dumps(
        record, sort_keys=True, ensure_ascii=False, default=str,
        separators=(',', ':'),
    )
    return hashlib.sha256(text.encode('utf-8', 'surrogatepass')).hexdigest()


def identity(paths: PathConfig) -> tuple[int, int]:
    """Which store: its directory's device and inode, which a path does
    not say, a relative one or one a container mounts elsewhere."""
    stated = os.stat(paths.base_data_dir)
    return stated.st_dev, stated.st_ino


@functools.cache
def code() -> str:
    """A digest of the source of every module that makes a repository's
    rows (`CODE_ROOTS`, and what they import of this package), less
    `RECOMPUTED`: other code may read the same store otherwise, so a
    warehouse made by other code is never carried from.

    Read from the files, not from what is imported: the CLI imports
    every command, and a change to one that makes no rows would throw
    the last warehouse away for nothing.
    """
    root = Path(chatsbom.__file__).parent
    seen: dict[str, Path] = {}
    pending = list(CODE_ROOTS)
    while pending:
        name = pending.pop()
        if name in seen or name in RECOMPUTED:
            continue
        path = _source(root, name)
        if path is None:
            continue
        seen[name] = path
        package = name if path.name == '__init__.py' else (
            name.rpartition('.')[0]
        )
        pending += _imported(path, package)
        parts = name.split('.')
        pending += ['.'.join(parts[:end]) for end in range(1, len(parts))]
    digest = hashlib.sha256()
    for name in sorted(seen):
        digest.update(name.encode() + b'\0')
        digest.update(hashlib.sha256(seen[name].read_bytes()).digest())
    return digest.hexdigest()


def _source(root: Path, name: str) -> Path | None:
    """The file of module `name` of this package, if it is one."""
    parts = name.split('.')
    if parts[0] != 'chatsbom':
        return None
    base = root.joinpath(*parts[1:])
    for candidate in (base.with_suffix('.py'), base / '__init__.py'):
        if candidate.is_file():
            return candidate
    return None


def _imported(path: Path, package: str) -> list[str]:
    """Each module of this package the source at `path` imports, at any
    depth of it, and each name it imports from one, which may be a
    module too."""
    found: list[str] = []
    for node in ast.walk(ast.parse(path.read_bytes())):
        if isinstance(node, ast.Import):
            found += [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            module = node.module or ''
            if node.level:
                parts = package.split('.')
                parts = parts[:len(parts) - (node.level - 1)]
                module = '.'.join([*parts, *([module] if module else [])])
            found.append(module)
            found += [f'{module}.{alias.name}' for alias in node.names]
    return [name for name in found if name.split('.')[0] == 'chatsbom']


@dataclass(frozen=True)
class Kept:
    """What the warehouse before says of one repository."""

    record: str
    named: bool
    scans: int
    trusted: bool


#: The alias the warehouse before is attached under.
PREVIOUS = 'previous'


class Previous:
    """The warehouse before this pass, attached read-only to its
    connection, and what it says of each repository."""

    def __init__(
        self,
        con: duckdb.DuckDBPyConnection,
        base: Path,
        kept: dict[int, Kept],
        tops: dict[int, frozenset[str]],
    ) -> None:
        self.con = con
        self.base = base
        self.kept = kept
        self.tops = tops
        #: The repositories whose directories changed.
        self.changed: set[int] = set()
        #: Directories asked again, and seconds asking.
        self.asked = 0
        self.seconds = 0.0

    @classmethod
    def attach(
        cls,
        con: duckdb.DuckDBPyConnection,
        path: Path,
        paths: PathConfig,
    ) -> tuple[Previous | None, str]:
        """The warehouse at `path`, if a pass may carry from it; else
        None, and why not."""
        if not path.is_file():
            return None, 'there is no warehouse before it'
        try:
            con.execute(
                f'ATTACH {_literal(str(path))} AS {PREVIOUS} (READ_ONLY)',
            )
        except Exception as error:  # noqa: BLE001 - said, and read whole
            return None, f'the warehouse before it cannot be opened: {error}'
        try:
            made = con.execute(
                f'SELECT format, code, store_device, store_inode, carryable '
                f'FROM {PREVIOUS}.build',
            ).fetchall()
        except Exception:  # noqa: BLE001 - an older warehouse's columns
            return cls._refused(con, 'the warehouse before it keeps no inputs')
        if len(made) != 1:
            return cls._refused(con, 'the warehouse before it has no one build')
        format_, digest, device, inode, carryable = made[0]
        if format_ != FORMAT:
            return cls._refused(
                con, f'the warehouse before it keeps its inputs as {format_}',
            )
        if digest != code():
            return cls._refused(con, 'the warehouse before it was made by other code')
        if (device, inode) != identity(paths):
            return cls._refused(con, 'the warehouse before it is of another store')
        if not carryable:
            return cls._refused(con, 'the pass before it could not carry')
        counted = dict(
            con.execute(
                f'SELECT repository_id, count(*) FROM {PREVIOUS}.scans '
                'GROUP BY repository_id',
            ).fetchall(),
        )
        kept: dict[int, Kept] = {}
        for repository_id, record, named, scans, trusted in con.execute(
            'SELECT repository_id, record, named, scans, trusted '
            f'FROM {PREVIOUS}.inputs',
        ).fetchall():
            if counted.get(repository_id, 0) != scans:
                return cls._refused(
                    con, f'the warehouse before it miscounts the scans of '
                    f'{repository_id}',
                )
            kept[int(repository_id)] = Kept(record, named, scans, trusted)
        tops: dict[int, set[str]] = {}
        for repository_id, top in con.execute(
            f'SELECT repository_id, path FROM {PREVIOUS}.input_directories '
            "WHERE path NOT LIKE '%/%/%'",
        ).fetchall():
            tops.setdefault(int(repository_id), set()).add(top)
        return cls(
            con, paths.base_data_dir, kept,
            {key: frozenset(value) for key, value in tops.items()},
        ), ''

    @staticmethod
    def _refused(
        con: duckdb.DuckDBPyConnection, reason: str,
    ) -> tuple[None, str]:
        con.execute(f'DETACH {PREVIOUS}')
        return None, reason

    def check(self, batch: int = 100_000) -> None:
        """Ask every directory the warehouse before kept for its `stat`,
        in inode order, and note each repository one of whose differs or
        is gone."""
        started = time.perf_counter()
        base = str(self.base)
        cursor = self.con.execute(
            'SELECT repository_id, path, inode, mtime_ns, ctime_ns '
            f'FROM {PREVIOUS}.input_directories ORDER BY inode',
        )
        while found := cursor.fetchmany(batch):
            for repository_id, path, inode, mtime_ns, ctime_ns in found:
                if repository_id in self.changed:
                    continue
                self.asked += 1
                try:
                    stated = os.stat(
                        os.path.join(base, path), follow_symlinks=False,
                    )
                except OSError:
                    self.changed.add(repository_id)
                    continue
                if (
                    not stat.S_ISDIR(stated.st_mode)
                    or stated.st_ino != inode
                    or stated.st_mtime_ns != mtime_ns
                    or stated.st_ctime_ns != ctime_ns
                ):
                    self.changed.add(repository_id)
        self.seconds = time.perf_counter() - started

    def unchanged(
        self, repository_id: int, record: str, tops: frozenset[str],
    ) -> bool:
        """Whether the repository is as the warehouse before read it: its
        record, the directories its id names in the stage roots, and
        every directory under them."""
        kept = self.kept.get(repository_id)
        return (
            kept is not None and kept.trusted and kept.record == record
            and repository_id not in self.changed
            and self.tops.get(repository_id, frozenset()) == tops
        )


def _literal(text: str) -> str:
    return "'" + text.replace("'", "''") + "'"
