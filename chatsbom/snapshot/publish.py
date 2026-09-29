"""Publishing a snapshot: by rename, the last three kept (#132).

The steps, each made durable before the next depends on it:

1. the written file is synced, renamed to `<id>.sqlite`, and the
   directory synced, so the name survives a crash before anything names
   it;
2. `CURRENT` is written aside, synced, renamed over the old one, and the
   directory synced: its first line is the new id, and the lines after
   it the ids published before it that are kept, newest first, up to
   three in all;
3. only then is a snapshot `CURRENT` does not list removed.

A reader opens what `CURRENT` names (`chatsbom/dataset/open.py`), and a
rename is all or nothing, so `CURRENT` names a complete snapshot at
every step: the old one until step 2, the new one after. A snapshot a
reader has open stays readable when it is removed.

Nothing here catches a failure, so a failure at a step leaves on disk
what a crash there would: a written file not yet renamed, a snapshot
renamed and not yet named, a `CURRENT` written aside and not yet
renamed, or snapshots not yet removed. The next pass, which holds the
lock, clears the first and third (`clear`), reuses the second if its
content is the same, and removes the rest.

Content that `CURRENT` already names publishes nothing (Q11 on #128):
the file written again goes, and neither `CURRENT` nor a snapshot is
touched. The retention is `CURRENT`'s own list, and not the files'
times, so it moves in the same rename as what is current, and a copy
or a clock cannot reorder it.
"""
from __future__ import annotations

import fcntl
import os
import re
import uuid
from collections.abc import Iterator
from collections.abc import Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path

from chatsbom.dataset.open import CURRENT
from chatsbom.dataset.open import ID
from chatsbom.dataset.open import SUFFIX
from chatsbom.snapshot.write import WRITING
from chatsbom.snapshot.write import Written

#: How many snapshots are kept: the current one and the two before it.
KEEP = 3

#: The lock a pass holds, beside the snapshots.
LOCK = '.lock'

#: `CURRENT`, while it is written, before it is renamed over the old.
POINTING = '.CURRENT-'

#: What a pass leaves only if it stopped: a snapshot it was writing,
#: and a `CURRENT` it was.
LEFT = re.compile(
    rf'{re.escape(WRITING)}[0-9a-f]{{32}}{re.escape(SUFFIX)}'
    rf'|{re.escape(POINTING)}[0-9a-f]{{32}}',
)


class SnapshotBusy(RuntimeError):
    """Another pass holds the lock."""


@dataclass(frozen=True)
class Published:
    """What publishing a written snapshot did."""

    id: str
    #: The snapshot, `<directory>/<id>.sqlite`.
    path: Path
    #: Whether `CURRENT` moved: not when it named this content already.
    changed: bool
    #: The snapshots removed, which `CURRENT` no longer lists.
    removed: tuple[Path, ...]


@contextmanager
def held(directory: Path) -> Iterator[None]:
    """The pass's lock on `directory`, or `SnapshotBusy` at once: two
    passes would each publish, and each remove what the other kept."""
    lock = directory / LOCK
    with lock.open('a') as handle:
        try:
            fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise SnapshotBusy(str(lock)) from error
        try:
            yield
        finally:
            fcntl.flock(handle, fcntl.LOCK_UN)


def clear(directory: Path) -> list[Path]:
    """What a pass that stopped left, removed: its written file and its
    `CURRENT` written aside, by their names, and nothing else. Only
    under the lock, when no pass is writing either."""
    left = sorted(p for p in directory.iterdir() if LEFT.fullmatch(p.name))
    for path in left:
        os.unlink(path)
    return left


def publish(
    written: Written,
    directory: Path,
    *,
    keep: int = KEEP,
) -> Published:
    """`written`, the snapshot `CURRENT` names, and the `keep` newest
    kept; the file `written` is at is taken."""
    target = directory / f'{written.id}{SUFFIX}'
    if target.exists():
        # This content is here already: published before, or renamed by
        # a pass that stopped before naming it. The file written again
        # is the same rows, and goes.
        os.unlink(written.path)
    else:
        _sync(written.path)
        os.replace(written.path, target)
        _sync(directory)
    kept = listed(directory)
    changed = kept[:1] != [written.id]
    if changed:
        kept = [
            written.id,
            *(
                snapshot for snapshot in kept
                if snapshot != written.id
                and (directory / f'{snapshot}{SUFFIX}').exists()
            ),
        ][:keep]
        _point(directory, kept)
    removed = _prune(directory, kept)
    return Published(
        id=written.id, path=target, changed=changed, removed=tuple(removed),
    )


def listed(directory: Path) -> list[str]:
    """The ids `CURRENT` lists, the current one first; none when it is
    missing, or names nothing, which a pass then writes afresh."""
    try:
        said = (directory / CURRENT).read_bytes().decode('ascii', 'replace')
    except FileNotFoundError:
        return []
    lines = said.splitlines()
    if not lines or not ID.fullmatch(lines[0]):
        return []
    ids: list[str] = []
    for line in lines:
        if ID.fullmatch(line) and line not in ids:
            ids.append(line)
    return ids


def _point(directory: Path, ids: Sequence[str]) -> None:
    """`CURRENT`, written aside and renamed over the old."""
    aside = directory / f'{POINTING}{uuid.uuid4().hex}'
    descriptor = os.open(aside, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    try:
        os.write(descriptor, ''.join(f'{i}\n' for i in ids).encode('ascii'))
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
    os.replace(aside, directory / CURRENT)
    _sync(directory)


def _prune(directory: Path, kept: Sequence[str]) -> list[Path]:
    """Every snapshot `CURRENT` does not list, by its name alone."""
    removed = []
    for path in sorted(directory.iterdir()):
        name = path.name.removesuffix(SUFFIX)
        if (
            path.name.endswith(SUFFIX) and ID.fullmatch(name)
            and name not in kept and path.is_file()
        ):
            os.unlink(path)
            removed.append(path)
    return removed


def _sync(path: Path) -> None:
    """A file's bytes, or a directory's names, on disk."""
    descriptor = os.open(path, os.O_RDONLY)
    try:
        os.fsync(descriptor)
    finally:
        os.close(descriptor)
