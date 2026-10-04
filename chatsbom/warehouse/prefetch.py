"""Reading ahead of a pass, several repositories at once (#187).

A pass reads one repository after another: its directories in six stage
roots, its decisions, Syft documents, manifests and graphs, each a file
wherever the file system put it. On a disk that turns, a file is a seek,
and a pass that reads one at a time waits for each. Given many requests
at once, the disk takes them in the order its head passes them, and the
seeks are short. Measured on the HDD this collector runs on, beside it:
300 repositories' files took 116 s read one repository after another,
24 s sixteen at a time; and the `stat` of 10,000 of their directories
27 s one at a time, 6 s sixteen at a time.

So threads read ahead of the pass, `READERS` repositories at once and at
most `AHEAD` in front of it: every file under each one's directories,
which the pass then reads from the page cache. It only warms the cache:
what the pass reads, it reads itself, so what it builds is the same
with or without it. What cannot be read is passed over: the pass reads
it, and says. Sorting the reads by inode instead, one at a time, was
slower than not reading ahead at all: the file system's inode numbers
do not say where the data is.
"""
from __future__ import annotations

import os
from collections.abc import Sequence
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from types import TracebackType

#: Repositories read at once.
READERS = 16

#: Repositories read ahead of the pass: page cache held for it, about a
#: megabyte a repository.
AHEAD = 64

#: The most of one file read ahead: the parsers read a manifest of a few
#: megabytes, and a Syft document whole.
MOST = 64 * 1024 * 1024

#: How much is read at a time.
CHUNK = 1024 * 1024


class Prefetcher:
    """Reads ahead of a pass whose repositories, in the order it reads
    them, have the directories `plan` lists. The pass says when it has
    read one (`advance`). Files under `stat_only` are asked for their
    `stat` and not read: the pass dates a commit by them, and reads no
    more of them."""

    def __init__(
        self,
        plan: Sequence[Sequence[Path]],
        *,
        readers: int = READERS,
        ahead: int = AHEAD,
        stat_only: Sequence[Path] = (),
    ) -> None:
        self._plan = plan
        self._ahead = ahead
        self._stat_only = tuple(f'{path}{os.sep}' for path in stat_only)
        self._pool = ThreadPoolExecutor(
            readers, thread_name_prefix='warehouse-prefetch',
        )
        self._sent = 0

    def __enter__(self) -> Prefetcher:
        self._send(self._ahead)
        return self

    def __exit__(
        self,
        kind: type[BaseException] | None,
        error: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        self._pool.shutdown(wait=True, cancel_futures=True)

    def advance(self) -> None:
        """The pass has read one more repository of the plan."""
        self._send(1)

    def _send(self, count: int) -> None:
        for _ in range(count):
            if self._sent >= len(self._plan):
                return
            self._pool.submit(self._warm, self._plan[self._sent])
            self._sent += 1

    def _warm(self, tops: Sequence[Path]) -> None:
        for top in tops:
            for directory, _, files in os.walk(top):
                for name in files:
                    path = os.path.join(directory, name)
                    if path.startswith(self._stat_only):
                        try:
                            os.stat(path)
                        except OSError:
                            pass
                    else:
                        _read(path)


def _read(path: str) -> None:
    """The file's first `MOST` bytes, into the page cache."""
    try:
        with open(path, 'rb', buffering=0) as handle:
            left = MOST
            while left > 0 and (chunk := handle.read(min(CHUNK, left))):
                left -= len(chunk)
    except OSError:
        pass
