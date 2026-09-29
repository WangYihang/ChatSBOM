"""Where a snapshot is opened: read-only and immutable, in one place.

#128 §2.4: the web process reads `snapshots/<id>.sqlite`, which nothing
writes once it is published, as `file:<path>?mode=ro&immutable=1`.
Read-only, so what serves the page cannot change what it serves.
Immutable, so SQLite takes no lock and leaves no journal beside it: a
snapshot's directory is its publisher's, and a lock is something to
wait for when nothing will ever write.

#132 adds the helper the web and the CLI will share, and this becomes a
call to it; until then, nothing else in the package opens a file.
"""
from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import closing
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import quote

from chatsbom.dataset.queries import Dataset


def connect(path: Path | str) -> sqlite3.Connection:
    """A connection to the snapshot at `path` that can neither write it
    nor lock it. A missing file is an error, and is not created.

    The path is quoted into the URI: unquoted, a `?` in it would start
    the URI's query, a `#` its fragment, and `%20` would be a space.
    """
    location = quote(str(Path(path).resolve()))
    return sqlite3.connect(
        f'file://{location}?mode=ro&immutable=1', uri=True,
    )


@contextmanager
def open_dataset(path: Path | str) -> Iterator[Dataset]:
    """The dataset API over the snapshot at `path`, closed after.

    Closed, not only committed, which is all a `with` on the connection
    would do: an open connection is a warning from Python 3.13 on, and
    an error in the suite (#135).
    """
    with closing(connect(path)) as connection:
        yield Dataset(connection)
