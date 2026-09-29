"""One pass: the warehouse in, a snapshot published, if it changed.

It holds the lock on the snapshots' directory throughout, clears what a
pass that stopped left there, writes a snapshot of the warehouse beside
the published ones (`write.py`), and publishes it (`publish.py`), which
does nothing when its content is already current.
"""
from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from chatsbom.snapshot.publish import clear
from chatsbom.snapshot.publish import held
from chatsbom.snapshot.publish import KEEP
from chatsbom.snapshot.publish import publish
from chatsbom.snapshot.publish import Published
from chatsbom.snapshot.write import write
from chatsbom.snapshot.write import Written


@dataclass(frozen=True)
class Report:
    """What a pass wrote and published, and what it cleared first."""

    written: Written
    published: Published
    cleared: tuple[Path, ...]


def build(
    warehouse: Path,
    directory: Path,
    *,
    keep: int = KEEP,
) -> Report:
    """A snapshot of the warehouse at `warehouse`, published in
    `directory` unless its content is current already."""
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    with held(directory):
        cleared = clear(directory)
        written = write(warehouse, directory)
        published = publish(written, directory, keep=keep)
    return Report(
        written=written, published=published, cleared=tuple(cleared),
    )
