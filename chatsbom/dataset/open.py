"""Where a snapshot is found and opened: read-only and immutable.

#128 §2.4: the web process reads `snapshots/<id>.sqlite`, which nothing
writes once it is published, as `file:<path>?mode=ro&immutable=1`.
Read-only, so what serves the page cannot change what it serves.
Immutable, so SQLite takes no lock and leaves no journal beside it: a
snapshot's directory is its publisher's, and a lock is something to
wait for when nothing will ever write.

Which snapshot is current is said by one file beside them, `CURRENT`
(#132): its first line is the id of the snapshot to open, and each line
after it the id of one published before it and still kept. The
publisher (`chatsbom/snapshot/publish.py`) replaces it by a rename, so
a reader finds one whole version of it or the other, and it only ever
names a complete file. The web, the chat's tools and the CLI all find
the snapshot here and open it here; nothing else opens one.

Every snapshot `CURRENT` lists is still served (`served`): a page asks
under the id it was told when it started (#144), and one published
since then does not take its answers away while it is kept.
"""
from __future__ import annotations

import re
import sqlite3
from collections.abc import Iterator
from contextlib import closing
from contextlib import contextmanager
from pathlib import Path
from urllib.parse import quote

from chatsbom.dataset.queries import Dataset

#: The file in a directory of snapshots that says which is current.
CURRENT = 'CURRENT'

#: A snapshot's file is named by its id and this.
SUFFIX = '.sqlite'

#: A snapshot's id: sixteen lowercase hexadecimal digits of the hash of
#: what it serves (`chatsbom/snapshot/write.py`).
ID = re.compile(r'[0-9a-f]{16}')


def current(directory: Path | str) -> Path:
    """The snapshot that `directory`'s `CURRENT` names.

    Its first line is checked to be an id before it is joined to the
    directory: it is a name read from a file, and `../` would leave the
    directory. A directory where nothing has been published, and a
    `CURRENT` that names a file no longer there, are said as such.
    """
    first = served(directory)[0]
    path = Path(directory) / f'{first}{SUFFIX}'
    if not path.is_file():
        raise FileNotFoundError(
            f'{Path(directory) / CURRENT} names snapshot {first}, and '
            f'{path} is not there',
        )
    return path


def served(directory: Path | str) -> tuple[str, ...]:
    """The ids of the snapshots `directory`'s `CURRENT` lists: the
    current one, which `current` opens, and then those kept, newest
    first.

    The first line has to be an id, as `current` reads it. A later line
    that is not one, or names one twice, names no other snapshot, and is
    passed over, as the publisher reads its own list.
    """
    pointer = Path(directory) / CURRENT
    try:
        # As bytes, and decoded without failing: whatever is in it is
        # refused below by the one rule, rather than by the codec.
        said = pointer.read_bytes().decode('ascii', 'replace')
    except FileNotFoundError:
        raise FileNotFoundError(
            f'no snapshot has been published in {directory}: there is no '
            f'{pointer}. `chatsbom snapshot build` publishes one.',
        ) from None
    lines = said.splitlines()
    first = lines[0] if lines else ''
    if not ID.fullmatch(first):
        raise ValueError(
            f'{pointer} does not name a snapshot: its first line is '
            f'{first!r}, where an id is sixteen lowercase hexadecimal '
            'digits',
        )
    ids = [first]
    for line in lines[1:]:
        if ID.fullmatch(line) and line not in ids:
            ids.append(line)
    return tuple(ids)


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
    """The dataset API over the snapshot at `path`, closed after:
    `open_dataset(current(directory))` for the one published last.

    Closed, not only committed, which is all a `with` on the connection
    would do: an open connection is a warning from Python 3.13 on, and
    an error in the suite (#135).
    """
    with closing(connect(path)) as connection:
        yield Dataset(connection)
