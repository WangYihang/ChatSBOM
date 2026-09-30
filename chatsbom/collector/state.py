"""collector.sqlite: what the collector keeps between runs (#156, #128
section 2.1).

What saves requests, and what paces them:

- each repository as it was last observed, by its id and its node id
  (renames are free by node id): its full name, stars, whether it is
  archived, `pushedAt`, the default branch and its HEAD, and the latest
  release's tag and date;
- REST validators, ETag and Last-Modified, by the request they answered,
  so that asking again costs nothing while nothing changed;
- `nothing` and failure outcomes, per repository, stage and input key,
  with how many attempts there have been and when the next is due (#100
  Q5);
- the dependency graph's pending reports, and how often each has been
  looked at;
- what detection keeps (#160): the universe, the repositories of the
  newest complete search snapshot by the node ids the sweep asks for
  them by, less those whose node came back null; each sweep, how far it
  got and what it found and cost; and, beside each repository's latest
  observation, when its push, HEAD or latest release last changed and
  which observation it was last collected as of, which 6c reads.

It is never what says a stage is done: the store says that, #147's
decisions and the scans. Deleted, the file costs requests, a document
asked for whole where a validator would have had it free, and a
repository observed again; never a result.

One process writes it, in WAL, so that a reader never waits for it. A
second process that opens it is refused (`InUse`), by a lock on the file
beside it, which the kernel lets go when its holder dies, however it
dies. Its schema has a version, `PRAGMA user_version`: a later collector
brings an earlier file forward, one step at a time and each whole or not
at all (`MIGRATIONS`), and an earlier collector refuses a later file,
untouched (`TooNew`).

SQLite, and synchronous calls: each is a row or two, far under a
millisecond, made from the collector's one event loop.
"""
import fcntl
import os
import sqlite3
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from types import TracebackType
from typing import Any
from typing import Self
from urllib.parse import urlsplit

from chatsbom.core.redact import redact

#: Its name, in `data/`.
STATE_FILE = 'collector.sqlite'

#: `PRAGMA application_id` of a collector.sqlite, `CSBC`: what tells it
#: from another SQLite file named by mistake, the ledger among them.
APPLICATION_ID = 0x43534243

#: How long a write waits for a reader's lock. There is one writer, and
#: a reader of a WAL database holds the writer back only while a
#: checkpoint needs what it reads.
BUSY_TIMEOUT = timedelta(seconds=30)

#: The backoff of an outcome that is not ok: doubling from a quarter of
#: an hour, up to a week, so that what keeps failing is still asked
#: about weekly.
BACKOFF_BASE = timedelta(minutes=15)
BACKOFF_CAP = timedelta(days=7)

#: What an outcome was: a stage that ran and produced nothing for its
#: key, or one that failed.
NOTHING = 'nothing'
FAILED = 'failed'

#: How an instant is written: UTC, to the microsecond, fixed width, so
#: that text order is time order.
_INSTANT = '%Y-%m-%dT%H:%M:%S.%fZ'

#: One step of the schema: brings a file from the version before it.
Migration = Callable[[sqlite3.Connection], None]


def _v1(db: sqlite3.Connection) -> None:
    """The first schema."""
    db.execute('''
        CREATE TABLE repository (
            repository_id   INTEGER PRIMARY KEY,
            node_id         TEXT NOT NULL UNIQUE,
            full_name       TEXT NOT NULL,
            stars           INTEGER,
            archived        INTEGER,
            pushed_at       TEXT,
            default_branch  TEXT,
            head            TEXT,
            release_tag     TEXT,
            release_at      TEXT,
            observed_at     TEXT NOT NULL
        )
    ''')
    db.execute('''
        CREATE TABLE validator (
            request         TEXT PRIMARY KEY,
            etag            TEXT,
            last_modified   TEXT,
            kept_at         TEXT NOT NULL
        )
    ''')
    db.execute(f'''
        CREATE TABLE outcome (
            repository_id   INTEGER NOT NULL,
            stage           TEXT NOT NULL,
            key             TEXT NOT NULL,
            kind            TEXT NOT NULL
                            CHECK (kind IN ('{NOTHING}', '{FAILED}')),
            attempts        INTEGER NOT NULL,
            due_at          TEXT NOT NULL,
            detail          TEXT NOT NULL,
            first_at        TEXT NOT NULL,
            last_at         TEXT NOT NULL,
            PRIMARY KEY (repository_id, stage, key)
        )
    ''')
    db.execute('CREATE INDEX outcome_due ON outcome (stage, due_at)')
    db.execute('''
        CREATE TABLE depgraph_report (
            repository_id   INTEGER PRIMARY KEY,
            url             TEXT NOT NULL,
            head            TEXT,
            requested_at    TEXT NOT NULL,
            attempts        INTEGER NOT NULL,
            due_at          TEXT NOT NULL
        )
    ''')
    db.execute('CREATE INDEX depgraph_report_due ON depgraph_report (due_at)')


def _detection(db: sqlite3.Connection) -> None:
    """Detection's step (#160): the universe and its members' node ids,
    the sweeps, and when a repository last changed and was collected."""
    db.execute('''
        CREATE TABLE universe (
            repository_id   INTEGER PRIMARY KEY,
            node_id         TEXT NOT NULL,
            gone_at         TEXT
        )
    ''')
    db.execute('''
        CREATE TABLE universe_snapshot (
            one             INTEGER PRIMARY KEY CHECK (one = 1),
            snapshot        TEXT NOT NULL,
            stamp           TEXT NOT NULL,
            repositories    INTEGER NOT NULL,
            loaded_at       TEXT NOT NULL
        )
    ''')
    db.execute('''
        CREATE TABLE sweep (
            sweep_id        INTEGER PRIMARY KEY,
            started_at      TEXT NOT NULL,
            finished_at     TEXT,
            position        INTEGER NOT NULL,
            calls           INTEGER NOT NULL,
            cost            INTEGER NOT NULL,
            nodes           INTEGER NOT NULL,
            changed         INTEGER NOT NULL,
            renamed         INTEGER NOT NULL,
            gone            INTEGER NOT NULL,
            unresolved      INTEGER NOT NULL,
            failed          INTEGER NOT NULL
        )
    ''')
    db.execute('ALTER TABLE repository ADD COLUMN changed_at TEXT')
    db.execute('ALTER TABLE repository ADD COLUMN collected_at TEXT')


#: Each step, in order: the one at index i brings a file from version i
#: to i + 1. A step is only ever added: one that shipped is never
#: changed, since a file it ran on keeps what it made.
MIGRATIONS: tuple[Migration, ...] = (_v1, _detection)

#: The version this collector writes.
SCHEMA_VERSION = len(MIGRATIONS)


class StateError(Exception):
    """collector.sqlite cannot be opened, and nothing was changed."""


class InUse(StateError):
    """Another process holds it."""


class TooNew(StateError):
    """A later collector wrote it."""


class Foreign(StateError):
    """It is another SQLite database, not a collector.sqlite."""


def state_path(data_dir: Path) -> Path:
    """Where collector.sqlite is, in the data directory."""
    return Path(data_dir) / STATE_FILE


def backoff(attempts: int) -> timedelta:
    """How long after its latest attempt an outcome is due again."""
    exponent = min(max(attempts, 1) - 1, 30)
    return min(BACKOFF_BASE * (1 << exponent), BACKOFF_CAP)


@dataclass(frozen=True)
class Observed:
    """A repository as it was last observed."""

    repository_id: int
    node_id: str
    full_name: str
    stars: int | None
    archived: bool | None
    pushed_at: datetime | None
    default_branch: str | None
    #: The default branch's HEAD commit.
    head: str | None
    release_tag: str | None
    release_at: datetime | None
    observed_at: datetime


@dataclass(frozen=True)
class Validators:
    """What a REST answer can be asked about again with, for free."""

    etag: str | None
    last_modified: str | None


@dataclass(frozen=True)
class Outcome:
    """A stage that produced nothing for a key, or failed, and when it is
    to be tried again."""

    repository_id: int
    stage: str
    key: str
    #: `NOTHING` or `FAILED`.
    kind: str
    attempts: int
    due_at: datetime
    #: What was said of it, without a credential or a signed URL.
    detail: str
    first_at: datetime
    last_at: datetime

    def backing_off(self, now: datetime) -> bool:
        return now < self.due_at


@dataclass(frozen=True)
class PendingReport:
    """A dependency-graph report GitHub accepted and has not handed over."""

    repository_id: int
    #: Where to look for it, on the API: never the signed link a
    #: finished one is downloaded from.
    url: str
    #: The HEAD it was asked for at, when known.
    head: str | None
    requested_at: datetime
    #: Looks at it so far.
    attempts: int
    due_at: datetime


@dataclass(frozen=True)
class Member:
    """A repository of the universe, as the sweep asks after it."""

    repository_id: int
    node_id: str


@dataclass(frozen=True)
class UniverseSnapshot:
    """The search snapshot the universe was loaded from."""

    #: Its name, `all-<date>`, as the catalog names it.
    snapshot: str
    #: Which writing of it: one written again is loaded again.
    stamp: str
    #: Its repositories with a node id: the members.
    repositories: int
    loaded_at: datetime


@dataclass(frozen=True)
class Sweep:
    """One sweep of the universe: how far it got, and what it found and
    cost, so far or in all."""

    sweep_id: int
    started_at: datetime
    #: None while it is not over.
    finished_at: datetime | None
    #: The last repository id swept: it goes on after it.
    position: int
    #: GraphQL calls answered.
    calls: int
    #: Points, as each answer's `rateLimit { cost }` said.
    cost: int
    #: Repositories observed.
    nodes: int
    #: Observations whose push, HEAD or latest release had changed.
    changed: int
    renamed: int
    #: Nodes that came back null: gone until the next universe.
    gone: int
    #: Nodes GitHub did not resolve to the repository asked after: null
    #: for another reason than its being gone, or another's. Asked after
    #: again the next sweep.
    unresolved: int
    #: Calls that failed every attempt, whose members were skipped.
    failed: int


def _instant(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).strftime(_INSTANT)


def _read_instant(value: str) -> datetime:
    return datetime.strptime(value, _INSTANT).replace(tzinfo=timezone.utc)


def _maybe_instant(value: datetime | None) -> str | None:
    return None if value is None else _instant(value)


def _maybe_read_instant(value: str | None) -> datetime | None:
    return None if value is None else _read_instant(value)


def _hold(path: Path) -> int:
    """The lock on `path`: a file beside it, locked for as long as the
    returned descriptor is open, and so no longer than this process
    lives. Not inherited by a process this one starts, which may outlive
    it. It says which process holds it, for the one refused."""
    lock = path.with_name(f'{path.name}.lock')
    fd = os.open(lock, os.O_RDWR | os.O_CREAT | os.O_CLOEXEC, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        holder = os.pread(fd, 32, 0).decode(errors='replace').strip()
        os.close(fd)
        raise InUse(
            f'{path} is in use: another collector (pid {holder or "?"}) '
            f'holds {lock}. One process writes it.',
        ) from None
    except BaseException:
        os.close(fd)
        raise
    os.ftruncate(fd, 0)
    os.pwrite(fd, f'{os.getpid()}\n'.encode(), 0)
    return fd


def _pragma(db: sqlite3.Connection, name: str) -> int:
    return int(db.execute(f'PRAGMA {name}').fetchone()[0])


def _migrate(
    db: sqlite3.Connection, path: Path, migrations: Sequence[Migration],
) -> int:
    """Brings the file at `path` to the last of `migrations`, and says
    which version that is. Refuses it, writing nothing, if it is not a
    collector.sqlite or is a later one."""
    application = _pragma(db, 'application_id')
    version = _pragma(db, 'user_version')
    schema = db.execute('SELECT count(*) FROM sqlite_master').fetchone()[0]
    if application != APPLICATION_ID and (application or schema or version):
        raise Foreign(
            f'{path} is not a collector.sqlite: another SQLite database '
            f'(application_id {application}, {schema} objects in it). '
            'Nothing was changed.',
        )
    if version > len(migrations):
        raise TooNew(
            f'{path} is at schema version {version}, which a later '
            f'collector wrote; this one reads up to version '
            f'{len(migrations)}. Run the later one, or move the file '
            'aside: it holds no result, only what saves requests.',
        )
    # Only now: a file refused is left as it was, its journal mode too.
    db.execute('PRAGMA journal_mode = WAL')
    for target in range(version + 1, len(migrations) + 1):
        db.execute('BEGIN IMMEDIATE')
        try:
            migrations[target - 1](db)
            if target == 1:
                db.execute(f'PRAGMA application_id = {APPLICATION_ID}')
            db.execute(f'PRAGMA user_version = {target}')
        except BaseException as error:
            db.execute('ROLLBACK')
            if isinstance(error, sqlite3.Error):
                raise StateError(
                    f'{path} could not be brought to schema version '
                    f'{target}, and is left at {target - 1}: {error}',
                ) from None
            raise
        db.execute('COMMIT')
    return len(migrations)


class CollectorState:
    """collector.sqlite, open for the one process that writes it."""

    def __init__(
        self, path: Path, db: sqlite3.Connection, lock: int, version: int,
    ) -> None:
        self.path = path
        #: Its schema version, as opened.
        self.version = version
        self._db = db
        self._lock: int | None = lock

    @classmethod
    def open(
        cls, path: Path, *, migrations: Sequence[Migration] = MIGRATIONS,
    ) -> Self:
        """collector.sqlite at `path`, made if there is none and brought
        forward if it is older, and held until `close`."""
        path = Path(path)
        path.parent.mkdir(parents=True, exist_ok=True)
        lock = _hold(path)
        try:
            db = sqlite3.connect(
                path, timeout=BUSY_TIMEOUT.total_seconds(),
                isolation_level=None,
            )
            try:
                version = _migrate(db, path, migrations)
                # What the WAL keeps survives the process being killed;
                # an outage of the machine may lose its latest writes,
                # which costs requests and nothing else.
                db.execute('PRAGMA synchronous = NORMAL')
            except sqlite3.DatabaseError as error:
                db.close()
                raise StateError(
                    f'{path} cannot be read as a collector.sqlite: {error}',
                ) from None
            except BaseException:
                db.close()
                raise
        except BaseException:
            os.close(lock)
            raise
        return cls(path, db, lock, version)

    def close(self) -> None:
        """Closes the file, then lets the lock go."""
        if self._lock is None:
            return
        try:
            self._db.close()
        finally:
            os.close(self._lock)
            self._lock = None

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """What is written within it, all or none. Within another, it is
        part of that one."""
        if self._db.in_transaction:
            yield
            return
        self._db.execute('BEGIN IMMEDIATE')
        try:
            yield
        except BaseException:
            self._db.execute('ROLLBACK')
            raise
        self._db.execute('COMMIT')

    def _one(self, sql: str, *parameters: Any) -> Any:
        return self._db.execute(sql, parameters).fetchone()

    # -- repositories -----------------------------------------------------

    def observe(self, observed: Observed) -> Observed | None:
        """Keeps `observed` as the repository's latest, and gives back
        what was kept before, if anything was."""
        before = self.observed(observed.repository_id)
        self._db.execute(
            '''
            INSERT INTO repository (
                repository_id, node_id, full_name, stars, archived,
                pushed_at, default_branch, head, release_tag, release_at,
                observed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (repository_id) DO UPDATE SET
                node_id = excluded.node_id,
                full_name = excluded.full_name,
                stars = excluded.stars,
                archived = excluded.archived,
                pushed_at = excluded.pushed_at,
                default_branch = excluded.default_branch,
                head = excluded.head,
                release_tag = excluded.release_tag,
                release_at = excluded.release_at,
                observed_at = excluded.observed_at
            ''',
            (
                observed.repository_id, observed.node_id, observed.full_name,
                observed.stars,
                None if observed.archived is None else int(observed.archived),
                _maybe_instant(observed.pushed_at), observed.default_branch,
                observed.head, observed.release_tag,
                _maybe_instant(observed.release_at),
                _instant(observed.observed_at),
            ),
        )
        return before

    @staticmethod
    def _observed(row: Any) -> Observed:
        (
            repository_id, node_id, full_name, stars, archived, pushed_at,
            default_branch, head, release_tag, release_at, observed_at,
        ) = row
        return Observed(
            repository_id=repository_id, node_id=node_id,
            full_name=full_name, stars=stars,
            archived=None if archived is None else bool(archived),
            pushed_at=_maybe_read_instant(pushed_at),
            default_branch=default_branch, head=head,
            release_tag=release_tag,
            release_at=_maybe_read_instant(release_at),
            observed_at=_read_instant(observed_at),
        )

    _REPOSITORY = '''
        SELECT repository_id, node_id, full_name, stars, archived,
               pushed_at, default_branch, head, release_tag, release_at,
               observed_at
        FROM repository
    '''

    def observed(self, repository_id: int) -> Observed | None:
        row = self._one(
            f'{self._REPOSITORY} WHERE repository_id = ?', repository_id,
        )
        return None if row is None else self._observed(row)

    def observed_node(self, node_id: str) -> Observed | None:
        row = self._one(f'{self._REPOSITORY} WHERE node_id = ?', node_id)
        return None if row is None else self._observed(row)

    def observed_name(self, full_name: str) -> Observed | None:
        """The repository last observed as `owner/name`, matched as
        GitHub matches a name, whatever the case; the one observed last
        where two were (a name taken over after a rename)."""
        row = self._one(
            f'{self._REPOSITORY} WHERE full_name = ? COLLATE NOCASE '
            'ORDER BY observed_at DESC, repository_id LIMIT 1',
            full_name,
        )
        return None if row is None else self._observed(row)

    def observations(self) -> Iterator[Observed]:
        rows = self._db.execute(f'{self._REPOSITORY} ORDER BY repository_id')
        for row in rows:
            yield self._observed(row)

    # -- what 6c reads (#160) ---------------------------------------------

    def _no_earlier(
        self, column: str, repository_id: int, at: datetime,
    ) -> bool:
        """`column` of the repository's row at `at`, unless it holds a
        later instant already; whether there is a row."""
        instant = _instant(at)
        cursor = self._db.execute(
            f'UPDATE repository SET {column} = CASE '
            f'WHEN {column} IS NULL OR {column} < ? THEN ? '
            f'ELSE {column} END WHERE repository_id = ?',
            (instant, instant, repository_id),
        )
        return cursor.rowcount > 0

    def mark_changed(self, repository_id: int, *, at: datetime) -> None:
        """The observation at `at` found the repository's push, HEAD or
        latest release other than the one before it."""
        self._no_earlier('changed_at', repository_id, at)

    def mark_collected(self, repository_id: int, *, as_of: datetime) -> None:
        """6c collected the repository as the observation at `as_of` had
        it: a change observed later makes it changed again."""
        if not self._no_earlier('collected_at', repository_id, as_of):
            raise KeyError(f'no observation of repository {repository_id}')

    _PENDING = '''
        SELECT r.repository_id, r.node_id, r.full_name, r.stars, r.archived,
               r.pushed_at, r.default_branch, r.head, r.release_tag,
               r.release_at, r.observed_at
        FROM repository AS r JOIN universe AS u USING (repository_id)
        WHERE u.gone_at IS NULL
    '''

    def _pending(self, where: str, limit: int | None) -> list[Observed]:
        rows = self._db.execute(
            f'{self._PENDING} {where} LIMIT ?',
            (-1 if limit is None else limit,),
        )
        return [self._observed(row) for row in rows]

    def changed(self, *, limit: int | None = None) -> list[Observed]:
        """The universe's repositories whose push, HEAD or latest release
        an observation found changed after what they were last collected
        as of, as last observed: the longest changed first."""
        return self._pending(
            'AND r.collected_at IS NOT NULL AND r.changed_at > r.collected_at '
            'ORDER BY r.changed_at, r.repository_id',
            limit,
        )

    def never_collected(self, *, limit: int | None = None) -> list[Observed]:
        """The universe's repositories observed and never collected, as
        last observed: the most stars first."""
        return self._pending(
            'AND r.collected_at IS NULL '
            'ORDER BY r.stars DESC, r.repository_id',
            limit,
        )

    # -- the universe (#160) ----------------------------------------------

    def keep_universe(
        self, snapshot: UniverseSnapshot, members: Iterable[Member],
    ) -> None:
        """`members`, which `snapshot` lists, as the universe, in place of
        the last one whole: none of them gone."""
        with self.transaction():
            self._db.execute('DELETE FROM universe')
            self._db.executemany(
                'INSERT OR REPLACE INTO universe (repository_id, node_id) '
                'VALUES (?, ?)',
                ((member.repository_id, member.node_id) for member in members),
            )
            self._db.execute(
                '''
                INSERT OR REPLACE INTO universe_snapshot (
                    one, snapshot, stamp, repositories, loaded_at
                ) VALUES (1, ?, ?, ?, ?)
                ''',
                (
                    snapshot.snapshot, snapshot.stamp, snapshot.repositories,
                    _instant(snapshot.loaded_at),
                ),
            )

    def universe(self) -> UniverseSnapshot | None:
        """The snapshot the universe was loaded from, if one was."""
        row = self._one(
            'SELECT snapshot, stamp, repositories, loaded_at '
            'FROM universe_snapshot',
        )
        if row is None:
            return None
        snapshot, stamp, repositories, loaded_at = row
        return UniverseSnapshot(
            snapshot=snapshot, stamp=stamp, repositories=repositories,
            loaded_at=_read_instant(loaded_at),
        )

    def members(
        self, *, after: int = 0, limit: int | None = None,
    ) -> list[Member]:
        """The universe's members that are not gone, by id, from the
        first after `after`."""
        rows = self._db.execute(
            'SELECT repository_id, node_id FROM universe '
            'WHERE gone_at IS NULL AND repository_id > ? '
            'ORDER BY repository_id LIMIT ?',
            (after, -1 if limit is None else limit),
        )
        return [Member(*row) for row in rows]

    def mark_gone(self, repository_id: int, *, now: datetime) -> None:
        """The member's node came back null: deleted, made private or
        blocked. It is not asked after again until the next universe."""
        self._db.execute(
            'UPDATE universe SET gone_at = ? WHERE repository_id = ?',
            (_instant(now), repository_id),
        )

    # -- sweeps (#160) ----------------------------------------------------

    _SWEEP = '''
        SELECT sweep_id, started_at, finished_at, position, calls, cost,
               nodes, changed, renamed, gone, unresolved, failed
        FROM sweep
    '''

    @staticmethod
    def _sweep(row: Any) -> Sweep:
        (
            sweep_id, started_at, finished_at, position, calls, cost, nodes,
            changed, renamed, gone, unresolved, failed,
        ) = row
        return Sweep(
            sweep_id=sweep_id, started_at=_read_instant(started_at),
            finished_at=_maybe_read_instant(finished_at), position=position,
            calls=calls, cost=cost, nodes=nodes, changed=changed,
            renamed=renamed, gone=gone, unresolved=unresolved, failed=failed,
        )

    def begin_sweep(self, now: datetime) -> Sweep:
        """A sweep from the start of the universe, begun `now`."""
        cursor = self._db.execute(
            '''
            INSERT INTO sweep (
                started_at, position, calls, cost, nodes, changed, renamed,
                gone, unresolved, failed
            ) VALUES (?, 0, 0, 0, 0, 0, 0, 0, 0, 0)
            ''',
            (_instant(now),),
        )
        return self._sweep(
            self._one(f'{self._SWEEP} WHERE sweep_id = ?', cursor.lastrowid),
        )

    def latest_sweep(self) -> Sweep | None:
        """The sweep begun last, over or not."""
        row = self._one(f'{self._SWEEP} ORDER BY sweep_id DESC LIMIT 1')
        return None if row is None else self._sweep(row)

    def keep_sweep(self, sweep: Sweep) -> None:
        """How far `sweep` got, and what it found and cost, as it stands."""
        self._db.execute(
            '''
            UPDATE sweep SET
                finished_at = ?, position = ?, calls = ?, cost = ?,
                nodes = ?, changed = ?, renamed = ?, gone = ?,
                unresolved = ?, failed = ?
            WHERE sweep_id = ?
            ''',
            (
                _maybe_instant(sweep.finished_at), sweep.position, sweep.calls,
                sweep.cost, sweep.nodes, sweep.changed, sweep.renamed,
                sweep.gone, sweep.unresolved, sweep.failed, sweep.sweep_id,
            ),
        )

    # -- validators -------------------------------------------------------

    def validators(self, request: str) -> Validators | None:
        row = self._one(
            'SELECT etag, last_modified FROM validator WHERE request = ?',
            request,
        )
        return None if row is None else Validators(*row)

    def keep_validators(
        self, request: str, validators: Validators,
        now: datetime | None = None,
    ) -> None:
        if validators.etag is None and validators.last_modified is None:
            self.drop_validators(request)
            return
        self._db.execute(
            '''
            INSERT INTO validator (request, etag, last_modified, kept_at)
            VALUES (?, ?, ?, ?)
            ON CONFLICT (request) DO UPDATE SET
                etag = excluded.etag,
                last_modified = excluded.last_modified,
                kept_at = excluded.kept_at
            ''',
            (
                request, validators.etag, validators.last_modified,
                _instant(now or datetime.now(timezone.utc)),
            ),
        )

    def drop_validators(self, request: str) -> None:
        self._db.execute('DELETE FROM validator WHERE request = ?', (request,))

    # -- outcomes ---------------------------------------------------------

    _OUTCOME = '''
        SELECT repository_id, stage, key, kind, attempts, due_at, detail,
               first_at, last_at
        FROM outcome
    '''

    @staticmethod
    def _outcome(row: Any) -> Outcome:
        (
            repository_id, stage, key, kind, attempts, due_at, detail,
            first_at, last_at,
        ) = row
        return Outcome(
            repository_id=repository_id, stage=stage, key=key, kind=kind,
            attempts=attempts, due_at=_read_instant(due_at), detail=detail,
            first_at=_read_instant(first_at), last_at=_read_instant(last_at),
        )

    def record(
        self, repository_id: int, stage: str, key: str, kind: str, *,
        now: datetime, detail: str = '', delay: timedelta | None = None,
    ) -> Outcome:
        """One more attempt at `stage` for `key` that was not ok, and when
        it is due again: after `delay`, or the backoff for as many
        attempts as there have been."""
        if kind not in (NOTHING, FAILED):
            raise ValueError(f'an outcome is {NOTHING} or {FAILED}: {kind!r}')
        before = self.outcome(repository_id, stage, key)
        attempts = 1 if before is None else before.attempts + 1
        outcome = Outcome(
            repository_id=repository_id, stage=stage, key=key, kind=kind,
            attempts=attempts,
            due_at=now + (backoff(attempts) if delay is None else delay),
            detail=redact(detail),
            first_at=now if before is None else before.first_at,
            last_at=now,
        )
        self._db.execute(
            '''
            INSERT INTO outcome (
                repository_id, stage, key, kind, attempts, due_at, detail,
                first_at, last_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT (repository_id, stage, key) DO UPDATE SET
                kind = excluded.kind,
                attempts = excluded.attempts,
                due_at = excluded.due_at,
                detail = excluded.detail,
                last_at = excluded.last_at
            ''',
            (
                repository_id, stage, key, kind, attempts,
                _instant(outcome.due_at), outcome.detail,
                _instant(outcome.first_at), _instant(now),
            ),
        )
        return outcome

    def outcome(
        self, repository_id: int, stage: str, key: str,
    ) -> Outcome | None:
        row = self._one(
            f'{self._OUTCOME} WHERE repository_id = ? AND stage = ? '
            'AND key = ?',
            repository_id, stage, key,
        )
        return None if row is None else self._outcome(row)

    def outcomes(self, stage: str | None = None) -> Iterator[Outcome]:
        order = 'ORDER BY repository_id, stage, key'
        rows = (
            self._db.execute(f'{self._OUTCOME} {order}') if stage is None
            else self._db.execute(
                f'{self._OUTCOME} WHERE stage = ? {order}', (stage,),
            )
        )
        for row in rows:
            yield self._outcome(row)

    def clear(
        self, repository_id: int, stage: str, key: str | None = None,
    ) -> None:
        """Forgets the outcome of `stage` for `key`, or for every key."""
        if key is None:
            self._db.execute(
                'DELETE FROM outcome WHERE repository_id = ? AND stage = ?',
                (repository_id, stage),
            )
        else:
            self._db.execute(
                'DELETE FROM outcome WHERE repository_id = ? AND stage = ? '
                'AND key = ?',
                (repository_id, stage, key),
            )

    # -- dependency-graph reports -----------------------------------------

    _REPORT = '''
        SELECT repository_id, url, head, requested_at, attempts, due_at
        FROM depgraph_report
    '''

    @staticmethod
    def _report(row: Any) -> PendingReport:
        repository_id, url, head, requested_at, attempts, due_at = row
        return PendingReport(
            repository_id=repository_id, url=url, head=head,
            requested_at=_read_instant(requested_at), attempts=attempts,
            due_at=_read_instant(due_at),
        )

    def pend_report(
        self, repository_id: int, url: str, *, head: str | None,
        now: datetime, due_at: datetime,
    ) -> PendingReport:
        """A report GitHub accepted for the repository, in place of any
        before it, to be looked at from `due_at`."""
        if urlsplit(url).query:
            raise ValueError(
                'a report is kept by where the API says to look for it, '
                'which has no query: a signed download link is never kept',
            )
        report = PendingReport(
            repository_id=repository_id, url=url, head=head,
            requested_at=now, attempts=0, due_at=due_at,
        )
        self._db.execute(
            '''
            INSERT OR REPLACE INTO depgraph_report (
                repository_id, url, head, requested_at, attempts, due_at
            ) VALUES (?, ?, ?, ?, 0, ?)
            ''',
            (repository_id, url, head, _instant(now), _instant(due_at)),
        )
        return report

    def report(self, repository_id: int) -> PendingReport | None:
        row = self._one(
            f'{self._REPORT} WHERE repository_id = ?', repository_id,
        )
        return None if row is None else self._report(row)

    def reports_due(self, now: datetime) -> list[PendingReport]:
        """The reports to look at by `now`, the longest due first."""
        rows = self._db.execute(
            f'{self._REPORT} WHERE due_at <= ? ORDER BY due_at, repository_id',
            (_instant(now),),
        )
        return [self._report(row) for row in rows]

    def polled(self, repository_id: int, *, due_at: datetime) -> PendingReport:
        """One more look at the repository's report, which was not ready:
        the next is due at `due_at`."""
        self._db.execute(
            'UPDATE depgraph_report SET attempts = attempts + 1, due_at = ? '
            'WHERE repository_id = ?',
            (_instant(due_at), repository_id),
        )
        report = self.report(repository_id)
        if report is None:
            raise KeyError(f'no pending report for repository {repository_id}')
        return report

    def drop_report(self, repository_id: int) -> None:
        self._db.execute(
            'DELETE FROM depgraph_report WHERE repository_id = ?',
            (repository_id,),
        )
