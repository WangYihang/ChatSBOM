"""web.sqlite: the web service's own small state (#128, section 2.5).

The daily spend ledger lives here (`spend`), and the Worker kept it in
a Durable Object. One file in a directory of its own (`WEB_STATE_DIR`),
which a restart keeps and the nightly backup takes.

It is opened for each operation and closed after it, from whichever
thread or process asks: the operations are a few a question, and a
connection shared between threads would need a lock of its own around
what SQLite already serialises.

  - WAL, so that a reader never waits for the writer, nor the writer
    for readers. Set once, it stays with the file.
  - `synchronous = FULL`, so that a commit is on disk before it
    returns: a hold lost to a power cut would lift the cap by as much,
    for a call that was made.
  - A writer that finds the file locked waits for it, up to
    BUSY_SECONDS, rather than failing at once.
"""
import sqlite3
from collections.abc import Iterator
from contextlib import closing
from contextlib import contextmanager
from pathlib import Path

#: The file, in the state directory.
FILENAME = 'web.sqlite'

#: How long a statement waits for another writer to finish before it
#: fails. A write here takes milliseconds; one still locked after this
#: is a writer that is stuck, and the caller refuses what needed it.
BUSY_SECONDS = 10

SCHEMA = """
-- One row a reservation: a model call's worst case while it is in
-- flight, then what it cost (spend.py).
CREATE TABLE IF NOT EXISTS spend (
    id TEXT PRIMARY KEY,
    -- The UTC day that admitted it, as YYYY-MM-DD.
    day TEXT NOT NULL,
    usd REAL NOT NULL,
    -- 0 while held, 1 once settled.
    settled INTEGER NOT NULL DEFAULT 0 CHECK (settled IN (0, 1))
);
CREATE INDEX IF NOT EXISTS spend_by_day ON spend (day);
"""


class StateError(RuntimeError):
    """web.sqlite cannot be kept as it must be."""


class WebState:
    """web.sqlite, in `directory`, which is made if need be."""

    def __init__(self, directory: Path) -> None:
        directory.mkdir(parents=True, exist_ok=True)
        self.path = directory / FILENAME
        with self.connect() as db:
            (mode,) = db.execute('PRAGMA journal_mode = WAL').fetchone()
            # A file system that cannot share memory between processes,
            # a network mount for one, keeps its old mode.
            if str(mode).lower() != 'wal':
                raise StateError(
                    f'{self.path} cannot be put in WAL mode (it is in '
                    f'{mode!r} mode): put WEB_STATE_DIR on a local disk',
                )
            db.executescript(SCHEMA)

    @contextmanager
    def connect(self) -> Iterator[sqlite3.Connection]:
        """A connection, in autocommit mode, closed after the block.

        `with sqlite3.connect()` alone ends a transaction and leaves the
        connection open.
        """
        with closing(
            sqlite3.connect(
                self.path, timeout=BUSY_SECONDS, isolation_level=None,
            ),
        ) as db:
            db.execute('PRAGMA synchronous = FULL')
            yield db

    @contextmanager
    def transaction(self) -> Iterator[sqlite3.Connection]:
        """A connection in a write transaction, begun before anything is
        read: `BEGIN IMMEDIATE` takes the write lock at once, so nothing
        another writer commits can come between what the block reads
        and what it writes. Committed after the block, rolled back if it
        raises."""
        with self.connect() as db:
            db.execute('BEGIN IMMEDIATE')
            try:
                yield db
            except BaseException:
                # SQLite has already rolled back after some errors.
                if db.in_transaction:
                    db.execute('ROLLBACK')
                raise
            db.execute('COMMIT')
