"""Per-repository collection state, so the pipeline can run continuously.

Batch collection walks a fixed list of repositories through each stage in
turn. That shape has three problems once you want the dataset to stay
fresh rather than be re-collected: a killed process loses the batch, an
unchanged repository costs exactly as much as a changed one, and there is
nowhere to record that a particular repository keeps failing.

The ledger replaces the list with state. Each repository carries what we
last observed (`pushed_at_seen`, per-resource ETags) and whether to leave
it alone for a while (`failure_count`, `next_attempt_at`); each of its
stages, a `stage_state` row: when it last ran, what it consumed and
produced, at which `STAGE_VERSION`, and its own lease and backoff. The
scheduler then asks "which repositories are stalest and due?" instead of
iterating. (`stage_watermarks` is still written, and was what a stage
was judged by before `stage_state`; `adopt_watermarks` carries it over.)

SQLite rather than the analytics store (ClickHouse when this was
written, the DuckDB warehouse since #153): this is small, mutable,
per-row state with frequent single-row updates, which is the opposite
of what a columnar store is for. It also means the queue survives the
database being rebuilt.

Measured against the current corpus, 25.3% of repositories are pushed in
any given week and 41.4% have not been pushed in a year — so most of the
work this structure avoids is work that would have produced identical
results.
"""
import json
import sqlite3
from collections.abc import Iterable
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from enum import Enum
from pathlib import Path
from types import TracebackType
from typing import Any
from typing import Self

import structlog

from chatsbom.core.redact import redact_urls

logger = structlog.get_logger('ledger')


class Stage(str, Enum):
    """Pipeline stages that advance independently per repository.

    `REPO` is different in kind from the rest. It is the *change
    detector*: fetching it is how we learn a repository was pushed. So it
    is due on a clock, not on a comparison against the push it would
    itself discover. The others are *derived* — due only once a newly
    observed push overtakes their watermark.
    """

    REPO = 'repo'
    RELEASE = 'release'
    COMMIT = 'commit'
    TREE = 'tree'
    CONTENT = 'content'
    DEPGRAPH = 'depgraph'
    LOCK = 'lock'
    SBOM = 'sbom'
    INDEX = 'index'

    def __str__(self) -> str:
        return self.value


#: The code version of each stage. A `stage_state` row recorded by an
#: older version is due again, with no push and no manual reset: bumping
#: a stage's number is how a change to what it *does* reaches the
#: corpus. The dependency graph is at 2 since it became its own stage.
#: Content, lock and SBOM are at 2 since manifests are discovered from
#: the tree at any depth and of every ecosystem, and resolved and
#: scanned per directory (PR C of #55): every repository's content root
#: is due to be filled out, and its SBOM regenerated from it. Release
#: is at 2 since only `refs/tags/*` are tags and tags are dated with git
#: (PR F of #55): every stored release history counted branches as
#: tags, so every repository's latest release is chosen again. Commit
#: follows through its input key, and only where the tag chosen changed.
#: Content is at 3 since discovery also takes podspecs and the `buildSrc`
#: sources a Gradle build's constants are in (#55 pilot): a content root
#: filled at 2 lacks them. Its SBOM follows only where the files changed.
STAGE_VERSION: dict[Stage, int] = {
    Stage.REPO: 1,
    Stage.RELEASE: 2,
    Stage.COMMIT: 1,
    Stage.TREE: 1,
    Stage.CONTENT: 3,
    Stage.LOCK: 2,
    Stage.SBOM: 2,
    Stage.DEPGRAPH: 2,
    Stage.INDEX: 1,
}

#: The names of unfiltered search snapshots, `all-<YYYY-MM-DD>`, which
#: sort by date.
UNFILTERED_SNAPSHOT_PREFIX = 'all-'
_OLDER = 'snapshot LIKE ? AND snapshot < ?'

#: What each derived stage consumes. A stage is due when the key it last
#: consumed (`input_key`) is no longer what its upstream produced
#: (`output_key`); for `RELEASE` that is the push `queue sync` saw.
#: `REPO` and `DEPGRAPH` have none: they are due on a clock.
UPSTREAM: dict[Stage, Stage] = {
    Stage.RELEASE: Stage.REPO,
    Stage.COMMIT: Stage.RELEASE,
    Stage.TREE: Stage.COMMIT,
    Stage.CONTENT: Stage.TREE,
    Stage.LOCK: Stage.CONTENT,
    Stage.SBOM: Stage.CONTENT,
}

#: The stages scheduled by input keys, in chain order.
DERIVED_STAGES: tuple[Stage, ...] = tuple(UPSTREAM)

#: `input_key` of a row adopted from a watermark that a later push had
#: already overtaken: never any stage's output, so the row stays due.
STALE_INPUT = 'legacy:stale'


#: How often to re-ask GitHub whether a repository changed. Six hours
#: revalidates the whole corpus four times a day; since an unchanged
#: repository answers 304 and costs no rate limit, the interval is bounded
#: by wall-clock throughput rather than by budget.
DEFAULT_RECHECK = timedelta(hours=6)

#: Exponential backoff, capped so a permanently broken repository is
#: retried weekly rather than abandoned.
BACKOFF_BASE = timedelta(minutes=15)
BACKOFF_CAP = timedelta(days=7)
DEFAULT_LEASE = timedelta(minutes=30)

#: How long a write waits for another's to finish before it fails with
#: "database is locked". A minute, where it was five seconds: `run`
#: workers failed five passes in a row waiting for `queue track` (#98).
#: A claim is cheap and a failed pass is not, so waiting always beats
#: failing. And SQLite queues no one: a waiting writer tries again every
#: 100 ms or so, and can miss the moment another's transactions leave
#: the lock free, so the wait has to outlast a run of them, not one.
BUSY_TIMEOUT = timedelta(minutes=1)

#: When an adopted dependency-graph watermark is due again: the depgraph
#: stage's own refresh (`services/depgraph_stage.DEPGRAPH_REFRESH`).
DEPGRAPH_REFRESH_DAYS = 30


def backoff_for(failures: int) -> timedelta:
    """Delay before retrying a repository that has failed `failures` times."""
    if failures <= 0:
        return timedelta(0)
    # 2**30 minutes already exceeds the cap; clamp the exponent so the
    # multiplication cannot overflow into an unrepresentable timedelta.
    exponent = min(failures - 1, 30)
    return min(BACKOFF_BASE * (2 ** exponent), BACKOFF_CAP)


@dataclass
class RepositoryState:
    """What the ledger knows about one repository."""

    repository_id: int
    owner: str
    repo: str
    language: str = ''

    #: The newest push we have observed, from the repository resource.
    pushed_at_seen: datetime | None = None
    #: When we last asked GitHub anything about it, changed or not.
    last_checked_at: datetime | None = None
    #: Per-stage completion time.
    stage_watermarks: dict[Stage, datetime] = field(default_factory=dict)
    #: Per-resource ETags, for conditional requests.
    etags: dict[str, str] = field(default_factory=dict)

    failure_count: int = 0
    next_attempt_at: datetime | None = None
    last_error: str = ''

    #: When GitHub first answered 404 for it; None while it exists. A 404
    #: is an answer, not an error, so it stays out of `failure_count`,
    #: which should count only what is broken.
    absent_since: datetime | None = None

    claimed_by: str = ''
    claim_expires_at: datetime | None = None

    #: What a search snapshot said of it, read back only: `upsert` never
    #: writes these (`seed` and `observe_default_branch` do). The walk
    #: starts a repository from them, so its record carries its stars
    #: and branch rather than the model's placeholders.
    github_language: str = ''
    stars: int | None = None
    #: '' when no snapshot listed it and no commit stage has looked yet.
    default_branch: str = ''

    @property
    def full_name(self) -> str:
        return f'{self.owner}/{self.repo}'

    def needs(
        self,
        stage: Stage,
        now: datetime | None = None,
        recheck: timedelta = DEFAULT_RECHECK,
    ) -> bool:
        """Whether `stage` has work outstanding.

        For `REPO` — the change detector — this is a clock question: has
        it been longer than `recheck` since we last asked? Gating it on
        `pushed_at_seen` instead would drain the queue and then stop
        noticing pushes: after one successful check the watermark is newer
        than the push forever.

        For every derived stage it is a comparison: a stage never run is
        behind, one run before the last push is stale, one run after it is
        current. That judgement is what lets 74.7% of repositories be
        skipped in a given week.
        """
        if stage is Stage.REPO:
            if self.last_checked_at is None:
                return True
            if now is None:
                return False
            return now - self.last_checked_at >= recheck

        done = self.stage_watermarks.get(stage)
        if done is None:
            return True
        if self.pushed_at_seen is None:
            return False
        return done < self.pushed_at_seen


@dataclass
class StageState:
    """One repository's standing in one stage (`stage_state`)."""

    repository_id: int
    stage: Stage
    done_at: datetime | None = None
    stage_version: int = 0
    input_key: str = ''
    output_key: str = ''
    #: ok | absent | failed | too_large | pending, or '' before any.
    outcome: str = ''
    http_status: int | None = None
    #: Consecutive answers of the kind `outcome` names, when it is not
    #: `ok`: what the next attempt's delay grows with.
    failure_count: int = 0
    next_attempt_at: datetime | None = None
    last_error: str = ''
    claimed_by: str = ''
    claim_expires_at: datetime | None = None


@dataclass(frozen=True, slots=True)
class StageWork:
    """A repository claimed for one stage, with what the stage needs."""

    repository_id: int
    owner: str
    repo: str
    #: From the search snapshot; '' when the ledger has none.
    default_branch: str
    #: Its `stage_state` row before the claim; a fresh one if none.
    state: StageState

    @property
    def full_name(self) -> str:
        return f'{self.owner}/{self.repo}'


@dataclass(frozen=True)
class StageClaim:
    """A repository leased for some of its derived stages."""

    state: RepositoryState
    #: The claimed stages that are due, in chain order.
    due: tuple[Stage, ...]
    #: Stages still backing off after a failure: the walk stops there.
    blocked: frozenset[Stage] = frozenset()
    #: The stages leased: released when the work is done.
    leased: tuple[Stage, ...] = ()


@dataclass(frozen=True, slots=True)
class LedgerHealth:
    """A snapshot of queue health, for `queue status` and metrics."""

    tracked: int
    failing: int
    absent: int
    claimed: int
    never_checked: int
    oldest_check: datetime | None
    due: dict[Stage, int]


_SCHEMA = """
CREATE TABLE IF NOT EXISTS repository_state (
    repository_id     INTEGER PRIMARY KEY,
    owner             TEXT NOT NULL,
    repo              TEXT NOT NULL,
    language          TEXT NOT NULL DEFAULT '',
    pushed_at_seen    TEXT,
    last_checked_at   TEXT,
    stage_watermarks  TEXT NOT NULL DEFAULT '{}',
    etags             TEXT NOT NULL DEFAULT '{}',
    failure_count     INTEGER NOT NULL DEFAULT 0,
    next_attempt_at   TEXT,
    last_error        TEXT NOT NULL DEFAULT '',
    claimed_by        TEXT NOT NULL DEFAULT '',
    claim_expires_at  TEXT,
    absent_since      TEXT,
    snapshot          TEXT NOT NULL DEFAULT '',
    github_language   TEXT NOT NULL DEFAULT '',
    stars             INTEGER,
    default_branch    TEXT NOT NULL DEFAULT ''
);
CREATE INDEX IF NOT EXISTS idx_state_language ON repository_state (language);
CREATE INDEX IF NOT EXISTS idx_state_checked ON repository_state (last_checked_at);
CREATE INDEX IF NOT EXISTS idx_state_attempt ON repository_state (next_attempt_at);

-- Per repository and stage: its own outcome, backoff and lease, so that
-- one stage failing never backs off another. DEPGRAPH is scheduled by
-- `claim_stage`, every derived stage by `claim_stages` (what it consumed
-- against what its upstream produced, and its STAGE_VERSION). REPO stays
-- on `repository_state`: it is the change detector `queue sync` owns.
CREATE TABLE IF NOT EXISTS stage_state (
    repository_id    INTEGER NOT NULL,
    stage            TEXT    NOT NULL,
    done_at          TEXT,
    stage_version    INTEGER NOT NULL DEFAULT 0,
    input_key        TEXT    NOT NULL DEFAULT '',
    output_key       TEXT    NOT NULL DEFAULT '',
    outcome          TEXT    NOT NULL DEFAULT '',
    http_status      INTEGER,
    failure_count    INTEGER NOT NULL DEFAULT 0,
    next_attempt_at  TEXT,
    last_error       TEXT    NOT NULL DEFAULT '',
    claimed_by       TEXT    NOT NULL DEFAULT '',
    claim_expires_at TEXT,
    PRIMARY KEY (repository_id, stage)
);
CREATE INDEX IF NOT EXISTS idx_stage_due ON stage_state (stage, next_attempt_at);
"""

#: Columns declared after ledgers were already in use. `CREATE TABLE IF
#: NOT EXISTS` leaves an existing table as it is, so an older ledger is
#: given these when it is opened; otherwise every write to it would fail.
_ADDED_COLUMNS = {
    'absent_since': 'TEXT',
    # From a search snapshot (`queue track --snapshot`). `language` stays
    # the list a repository was tracked from; it selects no work any more
    # (the content stage reads manifests from the tree) and only names
    # the list a finished record is filed under until the warehouse
    # reads the ledger alone. GitHub's own language is only an attribute.
    'snapshot': "TEXT NOT NULL DEFAULT ''",
    'github_language': "TEXT NOT NULL DEFAULT ''",
    'stars': 'INTEGER',
    'default_branch': "TEXT NOT NULL DEFAULT ''",
}


def _iso(value: datetime | None) -> str | None:
    return value.isoformat() if value else None


def _parse(value: Any) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(str(value))
    # SQLite stores whatever we wrote; normalise so comparisons never mix
    # naive and aware datetimes.
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


class Ledger:
    """Durable per-repository collection state."""

    def __init__(self, path: Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        # Shareable across threads, which callers serialise themselves:
        # the dependency-graph stage runs one thread per token and
        # records each outcome under one lock. `timeout` is SQLite's
        # busy timeout, set before the first statement rather than by a
        # PRAGMA after it: the schema's statements wait for the lock too.
        self._db = sqlite3.connect(
            self.path, timeout=BUSY_TIMEOUT.total_seconds(),
            isolation_level=None, check_same_thread=False,
        )
        self._db.row_factory = sqlite3.Row
        # WAL so a reader (queue status) never blocks the collector.
        self._db.execute('PRAGMA journal_mode=WAL')
        self._db.executescript(_SCHEMA)
        self._reconcile_columns()
        self.adopt_watermarks()

    def _reconcile_columns(self) -> None:
        """Add the columns an older ledger predates. Additive only."""
        existing = {
            row['name']
            for row in self._db.execute('PRAGMA table_info(repository_state)')
        }
        for column, definition in _ADDED_COLUMNS.items():
            if column in existing:
                continue
            self._db.execute(
                f'ALTER TABLE repository_state ADD COLUMN {column} {definition}',
            )
            logger.info('Ledger migrated', added_column=column)

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()

    def close(self) -> None:
        self._db.close()

    @contextmanager
    def transaction(self) -> Iterator[None]:
        """Many writes as one: all of them or none, and one sync.

        Seeding 60,000 repositories one autocommitted statement at a
        time is 60,000 syncs.
        """
        self._db.execute('BEGIN')
        try:
            yield
        except BaseException:
            self._db.execute('ROLLBACK')
            raise
        self._db.execute('COMMIT')

    @classmethod
    def open_readonly(cls, path: Path) -> Self:
        """The ledger at `path`, to be read and never written.

        For a reader beside the workers (`queue due`, #100). None of what
        opening it for work does: no schema script, no columns added to
        an older ledger, no watermarks adopted, no journal mode set. The
        connection is read-only (`mode=ro`) and refuses to write even so
        (`PRAGMA query_only`), so a method that writes raises.

        A ledger in use has its `-wal` beside it, holding the workers'
        latest commits, and is read with them. One nothing has open has
        none, and is read as immutable. Otherwise SQLite makes a `-wal`
        and a `-shm` for a read-only reader of a WAL database and leaves
        them there, owned by whoever read it (after a `sudo`, files the
        collector's user cannot write); and where it may not make them,
        in a directory the reader cannot write, it cannot read at all
        ("attempt to write a readonly database"). An immutable read
        takes no locks, so a reader that must not see a torn page checks
        that no `-wal` appeared meanwhile. Raises FileNotFoundError,
        creating nothing, when there is no ledger.
        """
        path = Path(path)
        if not path.is_file():
            raise FileNotFoundError(f'no ledger at {path}')
        uri = f'{path.resolve().as_uri()}?mode=ro'
        if not Path(f'{path}-wal').exists():
            uri += '&immutable=1'
        ledger = cls.__new__(cls)
        ledger.path = path
        ledger._db = sqlite3.connect(
            uri, uri=True, timeout=BUSY_TIMEOUT.total_seconds(),
            isolation_level=None, check_same_thread=False,
        )
        ledger._db.row_factory = sqlite3.Row
        ledger._db.execute('PRAGMA query_only = ON')
        return ledger

    # -- reads --------------------------------------------------------------

    def count(self) -> int:
        row = self._db.execute(
            'SELECT count(*) FROM repository_state',
        ).fetchone()
        return int(row[0])

    def get(self, repository_id: int) -> RepositoryState | None:
        row = self._db.execute(
            'SELECT * FROM repository_state WHERE repository_id = ?',
            (repository_id,),
        ).fetchone()
        return self._hydrate(row) if row else None

    def all(self) -> list[RepositoryState]:
        rows = self._db.execute('SELECT * FROM repository_state').fetchall()
        return [self._hydrate(r) for r in rows]

    @staticmethod
    def _hydrate(row: sqlite3.Row) -> RepositoryState:
        watermarks = {
            Stage(k): _parse(v)
            for k, v in json.loads(row['stage_watermarks']).items()
            if _parse(v) is not None
        }
        return RepositoryState(
            repository_id=int(row['repository_id']),
            owner=row['owner'],
            repo=row['repo'],
            language=row['language'],
            pushed_at_seen=_parse(row['pushed_at_seen']),
            last_checked_at=_parse(row['last_checked_at']),
            stage_watermarks={k: v for k, v in watermarks.items() if v},
            etags=json.loads(row['etags']),
            failure_count=int(row['failure_count']),
            next_attempt_at=_parse(row['next_attempt_at']),
            last_error=row['last_error'],
            absent_since=_parse(row['absent_since']),
            claimed_by=row['claimed_by'],
            claim_expires_at=_parse(row['claim_expires_at']),
            github_language=row['github_language'] or '',
            stars=row['stars'],
            default_branch=row['default_branch'] or '',
        )

    # -- writes -------------------------------------------------------------

    def upsert(self, state: RepositoryState) -> None:
        """Insert or update one repository, preserving nothing implicitly."""
        self._db.execute(
            """
            INSERT INTO repository_state (
                repository_id, owner, repo, language, pushed_at_seen,
                last_checked_at, stage_watermarks, etags, failure_count,
                next_attempt_at, last_error, claimed_by, claim_expires_at,
                absent_since
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(repository_id) DO UPDATE SET
                owner = excluded.owner,
                repo = excluded.repo,
                language = excluded.language,
                pushed_at_seen = excluded.pushed_at_seen,
                last_checked_at = excluded.last_checked_at,
                stage_watermarks = excluded.stage_watermarks,
                etags = excluded.etags,
                failure_count = excluded.failure_count,
                next_attempt_at = excluded.next_attempt_at,
                last_error = excluded.last_error,
                claimed_by = excluded.claimed_by,
                claim_expires_at = excluded.claim_expires_at,
                absent_since = excluded.absent_since
            """,
            (
                state.repository_id, state.owner, state.repo, state.language,
                _iso(state.pushed_at_seen), _iso(state.last_checked_at),
                json.dumps({
                    str(k): _iso(v) for k, v in state.stage_watermarks.items()
                }),
                json.dumps(state.etags),
                state.failure_count, _iso(state.next_attempt_at),
                state.last_error, state.claimed_by,
                _iso(state.claim_expires_at), _iso(state.absent_since),
            ),
        )

    def track(
        self,
        repository_id: int,
        owner: str,
        repo: str,
        language: str = '',
    ) -> None:
        """Register a repository without disturbing existing progress."""
        self._db.execute(
            """
            INSERT INTO repository_state (repository_id, owner, repo, language)
            VALUES (?, ?, ?, ?)
            ON CONFLICT(repository_id) DO UPDATE SET
                owner = excluded.owner,
                repo = excluded.repo,
                language = excluded.language
            """,
            (repository_id, owner, repo, language),
        )

    def track_all(
        self, repositories: Iterable[tuple[int, str, str, str]],
    ) -> int:
        """`track` for every `(repository_id, owner, repo, language)`, as
        one short write: how many rows it added or changed.

        What is tracked already is read first, and only what differs is
        written, in one transaction. `queue track` runs as the collector
        starts, with the workers already leasing: 65,000 autocommitted
        upserts, one write lock each, kept every worker waiting past its
        busy timeout for five minutes, though none of them changed a row
        (#98).
        """
        known = {
            row['repository_id']: (row['owner'], row['repo'], row['language'])
            for row in self._db.execute(
                'SELECT repository_id, owner, repo, language '
                'FROM repository_state',
            )
        }
        changed = [
            (repository_id, owner, repo, language)
            for repository_id, owner, repo, language in repositories
            if known.get(repository_id) != (owner, repo, language)
        ]
        if not changed:
            return 0
        with self.transaction():
            for repository_id, owner, repo, language in changed:
                self.track(repository_id, owner, repo, language)
        return len(changed)

    def observe_default_branch(self, repository_id: int, branch: str) -> None:
        """Record the branch HEAD points at, as `git ls-remote` said.

        The snapshot's value is what search saw, which a rename makes
        stale, and a repository tracked without a snapshot has none; the
        commit stage asks the remote itself, and what it heard is the
        better answer for the depgraph stamp and the index alike.
        """
        if not branch:
            return
        self._db.execute(
            'UPDATE repository_state SET default_branch = ? '
            'WHERE repository_id = ? AND default_branch != ?',
            (branch, repository_id, branch),
        )

    def seed(
        self,
        repository_id: int,
        owner: str,
        repo: str,
        *,
        snapshot: str,
        github_language: str = '',
        stars: int | None = None,
        default_branch: str = '',
        pushed_at: datetime | None = None,
    ) -> bool:
        """Track a repository listed by a search snapshot.

        Returns whether it was new. A repository already tracked keeps
        its name, `language` and progress — `queue sync` knows its name
        better than a snapshot taken months ago — and gets the snapshot's
        attributes. A new one is tracked with `language = ''`; every
        stage takes it, since none selects by language any more.
        """
        cursor = self._db.execute(
            """
            INSERT INTO repository_state (
                repository_id, owner, repo, language, snapshot,
                github_language, stars, default_branch, pushed_at_seen
            ) VALUES (?, ?, ?, '', ?, ?, ?, ?, ?)
            ON CONFLICT(repository_id) DO NOTHING
            """,
            (
                repository_id, owner, repo, snapshot, github_language,
                stars, default_branch, _iso(pushed_at),
            ),
        )
        if cursor.rowcount:
            return True
        # Stars are the snapshot's, the newest count there is. The push
        # only fills a gap: `queue sync` owns it once it has looked.
        self._db.execute(
            """
            UPDATE repository_state
            SET snapshot = ?, github_language = ?,
                stars = coalesce(?, stars),
                default_branch = CASE WHEN ? != '' THEN ?
                                      ELSE default_branch END,
                pushed_at_seen = coalesce(pushed_at_seen, ?)
            WHERE repository_id = ?
            """,
            (
                snapshot, github_language, stars, default_branch,
                default_branch, _iso(pushed_at), repository_id,
            ),
        )
        return False

    def unlist_older_snapshots(self, snapshot: str) -> int:
        """Mark repositories an older unfiltered snapshot listed and
        `snapshot` does not: their `snapshot` becomes ''.

        Run after seeding every repository `snapshot` lists. Nothing is
        deleted: a repository that fell below the star threshold, or was
        deleted or made private, keeps its row and its data, and the
        rollups count only the current snapshot by default (owner
        decision D2 on #55). Only unfiltered snapshots (`all-<date>`)
        are compared, and only older ones are unlisted, so seeding an
        old snapshot again unlists nothing. Returns how many.
        """
        if not snapshot.startswith(UNFILTERED_SNAPSHOT_PREFIX):
            return 0
        cursor = self._db.execute(
            f"UPDATE repository_state SET snapshot = '' WHERE {_OLDER}",
            (f'{UNFILTERED_SNAPSHOT_PREFIX}%', snapshot),
        )
        return cursor.rowcount

    def listed_only_before(self, snapshot: str) -> int:
        """How many repositories `unlist_older_snapshots` would unlist."""
        if not snapshot.startswith(UNFILTERED_SNAPSHOT_PREFIX):
            return 0
        row = self._db.execute(
            f'SELECT count(*) FROM repository_state WHERE {_OLDER}',
            (f'{UNFILTERED_SNAPSHOT_PREFIX}%', snapshot),
        ).fetchone()
        return int(row[0])

    def record_etag(self, repository_id: int, resource: str, etag: str) -> None:
        """Store an ETag so the next request for `resource` is conditional."""
        state = self._require(repository_id)
        state.etags[resource] = etag
        self.upsert(state)

    def record_unchanged(self, repository_id: int, now: datetime) -> None:
        """A 304: we looked, nothing changed, no stage advanced.

        Deliberately separate from `record_success`: advancing a stage
        watermark here would claim work that was never done.
        """
        state = self._require(repository_id)
        state.last_checked_at = now
        state.failure_count = 0
        state.last_error = ''
        state.next_attempt_at = None
        state.absent_since = None
        state.claimed_by = ''
        state.claim_expires_at = None
        self.upsert(state)

    def record_push(
        self,
        repository_id: int,
        pushed_at: datetime,
        now: datetime,
    ) -> None:
        """Record a newly observed push, which makes later stages due."""
        state = self._require(repository_id)
        state.pushed_at_seen = pushed_at
        state.last_checked_at = now
        self.upsert(state)

    def record_success(
        self,
        repository_id: int,
        stage: Stage,
        now: datetime,
    ) -> None:
        """Advance one stage's watermark and clear any backoff.

        For a derived stage the `stage_state` row says the same: done at
        `now`, and current against what its upstream produced if `now`
        is no older than the last push (`queue backfill` records work
        found on disk this way).
        """
        state = self._require(repository_id)
        if stage in UPSTREAM:
            pushed = state.pushed_at_seen
            current = pushed is None or now >= pushed
            previous = self.stage_state(repository_id, stage)
            self.record_stage(
                StageState(
                    repository_id=repository_id,
                    stage=stage,
                    done_at=now,
                    stage_version=STAGE_VERSION[stage],
                    input_key=(
                        self.upstream_key(repository_id, stage) if current
                        else STALE_INPUT
                    ),
                    output_key=previous.output_key if previous else '',
                    outcome='ok',
                ),
            )
            state = self._require(repository_id)
        state.stage_watermarks[stage] = now
        state.last_checked_at = now
        state.failure_count = 0
        state.last_error = ''
        state.next_attempt_at = None
        state.absent_since = None
        state.claimed_by = ''
        state.claim_expires_at = None
        self.upsert(state)

    def record_absent(
        self,
        repository_id: int,
        now: datetime,
        retry_at: datetime,
    ) -> None:
        """A 404: deleted, made private, or renamed out from under us.

        An answer rather than an error: the request worked and GitHub was
        definite. So it clears the failure count instead of growing it,
        and `chatsbom_queue_failing` keeps counting only what is broken.
        Deferred to `retry_at` rather than untracked, because renames and
        transfers do resolve.
        """
        state = self._require(repository_id)
        if state.absent_since is None:
            state.absent_since = now
        state.last_checked_at = now
        state.failure_count = 0
        state.last_error = ''
        state.next_attempt_at = retry_at
        state.claimed_by = ''
        state.claim_expires_at = None
        self.upsert(state)
        logger.info(
            'Repository absent',
            repo=state.full_name,
            since=_iso(state.absent_since),
            retry_at=_iso(retry_at),
        )

    def record_failure(
        self,
        repository_id: int,
        stage: Stage,
        now: datetime,
        error: str,
    ) -> None:
        """Count a failure and push the repository into backoff.

        The error is kept without the query of any URL it quotes, which
        for a report's download link is the signature: `queue status`
        shows it to whoever runs it.
        """
        state = self._require(repository_id)
        state.failure_count += 1
        state.last_error = redact_urls(f'{stage}: {error}')[:500]
        state.next_attempt_at = now + backoff_for(state.failure_count)
        state.last_checked_at = now
        state.claimed_by = ''
        state.claim_expires_at = None
        self.upsert(state)
        logger.info(
            'Repository deferred',
            repo=state.full_name,
            stage=str(stage),
            failures=state.failure_count,
            retry_at=_iso(state.next_attempt_at),
        )

    def _require(self, repository_id: int) -> RepositoryState:
        state = self.get(repository_id)
        if state is None:
            raise KeyError(f'repository {repository_id} is not tracked')
        return state

    # -- scheduling ---------------------------------------------------------

    def due(
        self,
        stage: Stage,
        now: datetime,
        limit: int | None = None,
        language: str | None = None,
        recheck: timedelta = DEFAULT_RECHECK,
        keyed_only: bool = False,
    ) -> list[RepositoryState]:
        """Repositories needing `stage`, stalest first.

        Ordering by `last_checked_at` with nulls first means never-checked
        repositories are picked up before re-checks, and no repository can
        be starved by a busier one.

        `keyed_only` leaves out repositories tracked with no `language`:
        the ones only a search snapshot listed (`seed`), which the
        language-keyed stages have no path for.
        """
        clauses = ['(next_attempt_at IS NULL OR next_attempt_at <= ?)']
        params: list[Any] = [_iso(now)]

        if language:
            clauses.append('language = ?')
            params.append(language)
        elif keyed_only:
            clauses.append("language != ''")

        sql = f"""
        SELECT * FROM repository_state
        WHERE {' AND '.join(clauses)}
        ORDER BY last_checked_at IS NOT NULL, last_checked_at ASC,
                 repository_id ASC
        """
        rows = self._db.execute(sql, params).fetchall()

        out: list[RepositoryState] = []
        for row in rows:
            state = self._hydrate(row)
            if not state.needs(stage, now, recheck):
                continue
            out.append(state)
            if limit is not None and len(out) >= limit:
                break
        return out

    def claim(
        self,
        stage: Stage,
        now: datetime,
        limit: int,
        worker: str,
        lease: timedelta = DEFAULT_LEASE,
        language: str | None = None,
        recheck: timedelta = DEFAULT_RECHECK,
        keyed_only: bool = False,
    ) -> list[RepositoryState]:
        """Take a slice of due work, leased so a second worker skips it.

        The lease expires rather than being held, so a worker killed
        mid-slice does not strand its repositories.
        """
        expires = now + lease
        claimed: list[RepositoryState] = []
        due = self.due(
            stage, now, limit=None, language=language, recheck=recheck,
            keyed_only=keyed_only,
        )

        # One transaction, one sync, as `claim_stages` (#98).
        with self.transaction():
            for state in due:
                if state.claimed_by and state.claim_expires_at:
                    if state.claim_expires_at > now:
                        continue

                updated = self._db.execute(
                    """
                    UPDATE repository_state
                    SET claimed_by = ?, claim_expires_at = ?
                    WHERE repository_id = ?
                      AND (claimed_by = '' OR claim_expires_at IS NULL
                           OR claim_expires_at <= ?)
                    """,
                    (worker, _iso(expires), state.repository_id, _iso(now)),
                ).rowcount
                if not updated:
                    continue

                state.claimed_by = worker
                state.claim_expires_at = expires
                claimed.append(state)
                if len(claimed) >= limit:
                    break

        return claimed

    def release(self, repository_id: int) -> None:
        """Drop a claim without recording progress."""
        self._db.execute(
            'UPDATE repository_state '
            "SET claimed_by = '', claim_expires_at = NULL "
            'WHERE repository_id = ?',
            (repository_id,),
        )

    # -- per-stage state ----------------------------------------------------

    def stage_state(self, repository_id: int, stage: Stage) -> StageState | None:
        row = self._db.execute(
            'SELECT * FROM stage_state WHERE repository_id = ? AND stage = ?',
            (repository_id, str(stage)),
        ).fetchone()
        return self._hydrate_stage(row) if row else None

    @staticmethod
    def _hydrate_stage(row: sqlite3.Row) -> StageState:
        return StageState(
            repository_id=int(row['repository_id']),
            stage=Stage(row['stage']),
            done_at=_parse(row['done_at']),
            stage_version=int(row['stage_version']),
            input_key=row['input_key'],
            output_key=row['output_key'],
            outcome=row['outcome'],
            http_status=(
                int(row['http_status'])
                if row['http_status'] is not None else None
            ),
            failure_count=int(row['failure_count']),
            next_attempt_at=_parse(row['next_attempt_at']),
            last_error=row['last_error'],
            claimed_by=row['claimed_by'],
            claim_expires_at=_parse(row['claim_expires_at']),
        )

    def claim_stage(
        self,
        stage: Stage,
        now: datetime,
        limit: int | None,
        worker: str,
        lease: timedelta = DEFAULT_LEASE,
        refresh: timedelta = timedelta(days=30),
        repos: Iterable[int] | None = None,
    ) -> list[StageWork]:
        """Lease up to `limit` repositories due for `stage`, in order.

        For a stage that needs nothing from another stage — only the
        dependency graph so far — and so is due for every tracked
        repository that is not deleted (`absent_since`):

        * never asked, or asked with no answer recorded;
        * or its `next_attempt_at` has passed: the refresh of a graph, a
          negative cache expired, a backoff run out.

        A repository whose only record is the legacy watermark in
        `stage_watermarks` — a graph fetched before this table existed —
        is due once that watermark is `refresh` old.

        In this order: never asked, and asked with no answer or a
        failure; then what is due for a refresh; then negative caches
        that expired. Within each, the most starred first.

        The lease is on (repository, stage), never on the repository, so
        another stage's worker can hold the same repository meanwhile.
        """
        expires = _iso(now + lease)
        # Room for rows another worker leases between this read and the
        # update below; -1 is SQLite's "no limit".
        cap = -1 if limit is None else limit * 4 + 50
        rows = self._clock_due(stage, now, refresh, repos, cap)

        claimed: list[StageWork] = []
        for row in rows:
            if limit is not None and len(claimed) >= limit:
                break
            repository_id = int(row['repository_id'])
            if row['has_state'] is None:
                self._db.execute(
                    'INSERT OR IGNORE INTO stage_state '
                    '(repository_id, stage) VALUES (?, ?)',
                    (repository_id, str(stage)),
                )
            updated = self._db.execute(
                """
                UPDATE stage_state
                SET claimed_by = ?, claim_expires_at = ?
                WHERE repository_id = ? AND stage = ?
                  AND (claimed_by = '' OR claim_expires_at IS NULL
                       OR claim_expires_at <= ?)
                """,
                (worker, expires, repository_id, str(stage), _iso(now)),
            ).rowcount
            if not updated:
                continue
            state = self.stage_state(repository_id, stage)
            assert state is not None
            claimed.append(
                StageWork(
                    repository_id=repository_id,
                    owner=row['owner'],
                    repo=row['repo'],
                    default_branch=row['default_branch'] or '',
                    state=state,
                ),
            )
        return claimed

    def _clock_due(
        self,
        stage: Stage,
        now: datetime,
        refresh: timedelta,
        repos: Iterable[int] | None,
        cap: int,
    ) -> list[sqlite3.Row]:
        """The rows `claim_stage` leases from, in the order it leases
        them, at most `cap` (-1 for all): the one statement both it and
        `depgraph_due_ids` read, so that what a reader counts is what a
        worker would take."""
        cutoff = _iso(now - refresh)
        legacy = f"json_extract(r.stage_watermarks, '$.{stage}')"
        sql = f"""
        SELECT r.repository_id, r.owner, r.repo, r.default_branch,
               s.repository_id AS has_state,
               CASE
                   WHEN coalesce(s.outcome, '') IN ('', 'failed', 'pending')
                        AND s.done_at IS NULL AND {legacy} IS NULL THEN 0
                   WHEN coalesce(s.outcome, '') IN ('', 'ok', 'failed', 'pending')
                        THEN 1
                   ELSE 2
               END AS priority
        FROM repository_state AS r
        LEFT JOIN stage_state AS s
            ON s.repository_id = r.repository_id AND s.stage = :stage
        WHERE r.absent_since IS NULL
          AND (s.next_attempt_at IS NULL OR s.next_attempt_at <= :now)
          AND (s.claimed_by IS NULL OR s.claimed_by = ''
               OR s.claim_expires_at IS NULL OR s.claim_expires_at <= :now)
          AND (coalesce(s.outcome, '') != '' OR {legacy} IS NULL
               OR {legacy} <= :cutoff)
          {'AND r.repository_id IN (SELECT value FROM json_each(:repos))' if repos is not None else ''}
        ORDER BY priority ASC, coalesce(r.stars, -1) DESC,
                 r.repository_id ASC
        LIMIT :cap
        """
        return self._db.execute(
            sql,
            {
                'stage': str(stage), 'now': _iso(now), 'cutoff': cutoff,
                'cap': cap,
                'repos': json.dumps(sorted({int(i) for i in repos or ()})),
            },
        ).fetchall()

    def depgraph_due_ids(
        self,
        now: datetime,
        *,
        refresh: timedelta = timedelta(days=DEPGRAPH_REFRESH_DAYS),
        repos: Iterable[int] | None = None,
        limit: int | None = None,
    ) -> list[int]:
        """The repositories the dependency graph is due for, in the order
        `claim_stage` would lease them, and without leasing any.

        `claim_stage`'s own statement (`_clock_due`), so the two cannot
        drift apart: never asked first, then refreshes, then expired
        negative caches, the most starred first within each; never a
        repository that is gone (`absent_since`), one whose backoff or
        negative cache still runs, or one another worker holds. Nothing
        is written, so it can be asked of a ledger the workers are using,
        and of one opened only to be read: `queue due` compares it with
        the due set derived from the store (#100).
        """
        return [
            int(row['repository_id'])
            for row in self._clock_due(
                Stage.DEPGRAPH, now, refresh, repos,
                -1 if limit is None else limit,
            )
        ]

    def count_due_for_stage(
        self,
        stage: Stage,
        now: datetime,
        refresh: timedelta = timedelta(days=30),
    ) -> dict[str, int]:
        """How many are due, by why: `never`, `refresh`, `expired`.

        What `claim_stage` would take with no limit, without taking it.
        """
        cutoff = _iso(now - refresh)
        legacy = f"json_extract(r.stage_watermarks, '$.{stage}')"
        rows = self._db.execute(
            f"""
            SELECT CASE
                       WHEN coalesce(s.outcome, '') IN ('', 'failed', 'pending')
                            AND s.done_at IS NULL AND {legacy} IS NULL
                            THEN 'never'
                       WHEN coalesce(s.outcome, '') IN ('', 'ok', 'failed', 'pending')
                            THEN 'refresh'
                       ELSE 'expired'
                   END AS why,
                   count(*) AS n
            FROM repository_state AS r
            LEFT JOIN stage_state AS s
                ON s.repository_id = r.repository_id AND s.stage = :stage
            WHERE r.absent_since IS NULL
              AND (s.next_attempt_at IS NULL OR s.next_attempt_at <= :now)
              AND (s.claimed_by IS NULL OR s.claimed_by = ''
                   OR s.claim_expires_at IS NULL OR s.claim_expires_at <= :now)
              AND (coalesce(s.outcome, '') != '' OR {legacy} IS NULL
                   OR {legacy} <= :cutoff)
            GROUP BY why
            """,
            {'stage': str(stage), 'now': _iso(now), 'cutoff': cutoff},
        ).fetchall()
        return {row['why']: int(row['n']) for row in rows}

    def stage_outcomes(self, stage: Stage) -> dict[str, int]:
        """Rows of `stage` by outcome, for `queue status`."""
        rows = self._db.execute(
            'SELECT outcome, count(*) AS n FROM stage_state '
            'WHERE stage = ? GROUP BY outcome',
            (str(stage),),
        ).fetchall()
        return {row['outcome'] or 'unanswered': int(row['n']) for row in rows}

    def record_stage(self, state: StageState) -> None:
        """Write one stage outcome, and drop its lease.

        Its error as `record_failure` keeps one, without a URL's query.
        """
        self._db.execute(
            """
            INSERT INTO stage_state (
                repository_id, stage, done_at, stage_version, input_key,
                output_key, outcome, http_status, failure_count,
                next_attempt_at, last_error, claimed_by, claim_expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '', NULL)
            ON CONFLICT(repository_id, stage) DO UPDATE SET
                done_at = excluded.done_at,
                stage_version = excluded.stage_version,
                input_key = excluded.input_key,
                output_key = excluded.output_key,
                outcome = excluded.outcome,
                http_status = excluded.http_status,
                failure_count = excluded.failure_count,
                next_attempt_at = excluded.next_attempt_at,
                last_error = excluded.last_error,
                claimed_by = '',
                claim_expires_at = NULL
            """,
            (
                state.repository_id, str(state.stage), _iso(state.done_at),
                state.stage_version, state.input_key, state.output_key,
                state.outcome, state.http_status, state.failure_count,
                _iso(state.next_attempt_at),
                redact_urls(state.last_error)[:500],
            ),
        )

    def release_stage(self, repository_id: int, stage: Stage) -> None:
        """Drop a stage lease without recording anything."""
        self._db.execute(
            "UPDATE stage_state SET claimed_by = '', claim_expires_at = NULL "
            'WHERE repository_id = ? AND stage = ?',
            (repository_id, str(stage)),
        )

    # -- derived stages -----------------------------------------------------

    def adopt_watermarks(self) -> int:
        """Give every stage watermark a `stage_state` row. Idempotent.

        The watermarks in `stage_watermarks` said when a stage last ran,
        and a stage was due once a newer push overtook that. A row is
        written for each watermark that has none yet, so that the same
        repositories are due, and the same ones are not, as before:

        * `stage_version = 1`, `outcome = ok`, `done_at` the watermark;
        * `input_key` is what the upstream produced if the watermark was
          current, and `STALE_INPUT` if a push had overtaken it;
        * `output_key` is '' (unknown): a stage downstream of an adopted
          one is current exactly when its own watermark was.

        A dependency graph's watermark becomes `ok` due again after
        `DEPGRAPH_REFRESH_DAYS`, as the depgraph stage read it before.

        No stage version is bumped here, so adopting changes nothing that
        is scheduled. Returns the number of rows written.
        """
        rows = self._db.execute(
            'SELECT repository_id, pushed_at_seen, stage_watermarks '
            "FROM repository_state WHERE stage_watermarks != '{}'",
        ).fetchall()
        if not rows:
            return 0
        existing: dict[tuple[int, str], str] = {
            (int(row['repository_id']), row['stage']): row['output_key']
            for row in self._db.execute(
                'SELECT repository_id, stage, output_key FROM stage_state',
            )
        }
        written = 0
        with self.transaction():
            for row in rows:
                repository_id = int(row['repository_id'])
                try:
                    watermarks = json.loads(row['stage_watermarks'])
                except ValueError:
                    continue
                pushed_raw = row['pushed_at_seen'] or ''
                pushed = _parse(pushed_raw)
                outputs: dict[Stage, str] = {}
                for stage in (*DERIVED_STAGES, Stage.DEPGRAPH):
                    key = (repository_id, str(stage))
                    if key in existing:
                        outputs[stage] = existing[key]
                        continue
                    done = _parse(watermarks.get(str(stage)))
                    if done is None:
                        continue
                    if stage is Stage.DEPGRAPH:
                        self._insert_adopted(
                            repository_id, stage, done, '',
                            next_attempt_at=done + timedelta(
                                days=DEPGRAPH_REFRESH_DAYS,
                            ),
                        )
                        written += 1
                        continue
                    upstream = UPSTREAM[stage]
                    produced = (
                        pushed_raw if upstream is Stage.REPO
                        else outputs.get(upstream, '')
                    )
                    current = pushed is None or done >= pushed
                    self._insert_adopted(
                        repository_id, stage, done,
                        produced if current else STALE_INPUT,
                    )
                    outputs[stage] = ''
                    written += 1
        if written:
            logger.info('Ledger watermarks adopted', rows=written)
        return written

    def _insert_adopted(
        self,
        repository_id: int,
        stage: Stage,
        done: datetime,
        input_key: str,
        next_attempt_at: datetime | None = None,
    ) -> None:
        self._db.execute(
            """
            INSERT OR IGNORE INTO stage_state (
                repository_id, stage, done_at, stage_version, input_key,
                output_key, outcome, next_attempt_at
            ) VALUES (?, ?, ?, 1, ?, '', 'ok', ?)
            """,
            (
                repository_id, str(stage), _iso(done), input_key,
                _iso(next_attempt_at),
            ),
        )

    @staticmethod
    def _upstream_sql(stage: Stage) -> tuple[str, str]:
        """(join, expression) for what `stage`'s upstream produced."""
        upstream = UPSTREAM[stage]
        if upstream is Stage.REPO:
            return '', "coalesce(r.pushed_at_seen, '')"
        return (
            'LEFT JOIN stage_state AS u ON u.repository_id = r.repository_id '
            f"AND u.stage = '{upstream}'",
            "coalesce(u.output_key, '')",
        )

    def upstream_key(self, repository_id: int, stage: Stage) -> str:
        """What `stage`'s upstream last produced: its input now."""
        join, expression = self._upstream_sql(stage)
        row = self._db.execute(
            f'SELECT {expression} AS k FROM repository_state AS r {join} '
            'WHERE r.repository_id = ?',
            (repository_id,),
        ).fetchone()
        return str(row['k']) if row else ''

    def _due_ids(
        self,
        stage: Stage,
        now: datetime,
        *,
        keyed_only: bool = False,
        repos: Iterable[int] | None = None,
        language: str | None = None,
    ) -> list[int]:
        """Repositories `stage` is due for, stalest first.

        Due when its row is missing, is from an older `STAGE_VERSION`,
        failed and its backoff ran out, or consumed something other than
        what its upstream produced now. Never while the repository itself
        is deferred (`queue sync`'s backoff, a 404) or the stage's own
        backoff runs; a lease is the claimer's business.
        """
        if stage not in UPSTREAM:
            raise ValueError(f'{stage} is not scheduled by input keys')
        join, upstream = self._upstream_sql(stage)
        clauses = [
            '(r.next_attempt_at IS NULL OR r.next_attempt_at <= :now)',
            '(s.next_attempt_at IS NULL OR s.next_attempt_at <= :now)',
            f"""(s.repository_id IS NULL
                 OR s.stage_version < :version
                 OR s.outcome = 'failed'
                 OR s.input_key != {upstream})""",
        ]
        params: dict[str, Any] = {
            'now': _iso(now), 'stage': str(stage),
            'version': STAGE_VERSION[stage],
        }
        if language:
            clauses.append('r.language = :language')
            params['language'] = language
        elif keyed_only:
            clauses.append("r.language != ''")
        if repos is not None:
            clauses.append(
                'r.repository_id IN (SELECT value FROM json_each(:repos))',
            )
            params['repos'] = json.dumps(sorted({int(i) for i in repos}))
        rows = self._db.execute(
            f"""
            SELECT r.repository_id FROM repository_state AS r
            LEFT JOIN stage_state AS s
                ON s.repository_id = r.repository_id AND s.stage = :stage
            {join}
            WHERE {' AND '.join(clauses)}
            ORDER BY r.last_checked_at IS NOT NULL, r.last_checked_at ASC,
                     r.repository_id ASC
            """,
            params,
        ).fetchall()
        return [int(row['repository_id']) for row in rows]

    def count_due(
        self,
        stage: Stage,
        now: datetime,
        keyed_only: bool = False,
    ) -> int:
        """How many repositories a derived stage is due for."""
        return len(self._due_ids(stage, now, keyed_only=keyed_only))

    def claim_stages(
        self,
        stages: Iterable[Stage],
        now: datetime,
        limit: int,
        worker: str,
        lease: timedelta = DEFAULT_LEASE,
        *,
        lease_stages: Iterable[Stage] | None = None,
        keyed_only: bool = False,
        repos: Iterable[int] | None = None,
        language: str | None = None,
    ) -> list[StageClaim]:
        """Lease up to `limit` repositories due for any of `stages`.

        Stalest first, as `due` orders them. Each repository is leased on
        every stage in `lease_stages` (by default `stages`) — per stage,
        never on the repository, so a worker running another stage can
        hold it meanwhile — and skipped if any of them is leased already.
        """
        wanted = [stage for stage in DERIVED_STAGES if stage in set(stages)]
        leasing = tuple(
            stage for stage in DERIVED_STAGES
            if stage in set(lease_stages if lease_stages is not None else wanted)
        )
        due_by_repo: dict[int, list[Stage]] = {}
        order: list[int] = []
        position: dict[int, int] = {}
        for stage in wanted:
            for index, repository_id in enumerate(
                self._due_ids(
                    stage, now, keyed_only=keyed_only, repos=repos,
                    language=language,
                ),
            ):
                if repository_id not in due_by_repo:
                    due_by_repo[repository_id] = []
                    order.append(repository_id)
                    position[repository_id] = index
                due_by_repo[repository_id].append(stage)
        # One order across stages: the repository's own staleness, which
        # every per-stage list is sorted by already.
        order.sort(key=lambda repository_id: self._staleness(repository_id))

        expires = _iso(now + lease)
        claimed: list[StageClaim] = []
        # One transaction for the whole slice, one sync. A commit per
        # repository was 500 back-to-back write locks, each held through
        # a sync of the ledger's disk: whichever process wanted to record
        # meanwhile waited past its busy timeout and lost its pass (#98).
        with self.transaction():
            for repository_id in order:
                if len(claimed) >= limit:
                    break
                if not self._lease(
                    repository_id, leasing, worker, expires, now,
                ):
                    continue
                state = self.get(repository_id)
                if state is None:
                    self.release_stages(repository_id, leasing)
                    continue
                claimed.append(
                    StageClaim(
                        state=state,
                        due=tuple(due_by_repo[repository_id]),
                        blocked=self._blocked(repository_id, leasing, now),
                        leased=leasing,
                    ),
                )
        return claimed

    def _staleness(self, repository_id: int) -> tuple[int, str, int]:
        row = self._db.execute(
            'SELECT last_checked_at FROM repository_state '
            'WHERE repository_id = ?',
            (repository_id,),
        ).fetchone()
        checked = row['last_checked_at'] if row else None
        return (checked is not None, checked or '', repository_id)

    def _lease(
        self,
        repository_id: int,
        stages: tuple[Stage, ...],
        worker: str,
        expires: str | None,
        now: datetime,
    ) -> bool:
        """Lease every one of `stages`, or none of them.

        Inside the caller's transaction, which `claim_stages` holds for
        the whole slice.
        """
        for stage in stages:
            self._db.execute(
                'INSERT OR IGNORE INTO stage_state '
                '(repository_id, stage) VALUES (?, ?)',
                (repository_id, str(stage)),
            )
            updated = self._db.execute(
                """
                UPDATE stage_state
                SET claimed_by = ?, claim_expires_at = ?
                WHERE repository_id = ? AND stage = ?
                  AND (claimed_by = '' OR claim_expires_at IS NULL
                       OR claim_expires_at <= ?)
                """,
                (worker, expires, repository_id, str(stage), _iso(now)),
            ).rowcount
            if not updated:
                # Held by another worker. Undo this repository's leases
                # so far; the rows inserted stay, and read as never run,
                # which they are.
                for leased in stages[:stages.index(stage)]:
                    self.release_stage(repository_id, leased)
                return False
        return True

    def _blocked(
        self,
        repository_id: int,
        stages: tuple[Stage, ...],
        now: datetime,
    ) -> frozenset[Stage]:
        blocked: set[Stage] = set()
        for stage in stages:
            state = self.stage_state(repository_id, stage)
            if (
                state is not None and state.next_attempt_at is not None
                and state.next_attempt_at > now
            ):
                blocked.add(stage)
        return frozenset(blocked)

    def release_stages(
        self, repository_id: int, stages: Iterable[Stage],
    ) -> None:
        """Drop the leases on `stages` without recording anything."""
        for stage in stages:
            self.release_stage(repository_id, stage)

    def record_stage_success(
        self,
        repository_id: int,
        stage: Stage,
        now: datetime,
        input_key: str,
        output_key: str,
    ) -> None:
        """A derived stage ran: what it consumed and produced, current.

        Also what `record_success` did for the repository, so the
        watermark still says when each stage last ran and `queue sync`
        rechecks on the same clock.
        """
        self.record_stage(
            StageState(
                repository_id=repository_id,
                stage=stage,
                done_at=now,
                stage_version=STAGE_VERSION[stage],
                input_key=input_key,
                output_key=output_key,
                outcome='ok',
            ),
        )
        self._touch(repository_id, now, stage=stage, clear=True)

    def record_stage_failure(
        self,
        repository_id: int,
        stage: Stage,
        now: datetime,
        error: str,
    ) -> StageState:
        """A derived stage failed: back off that stage, and only that one.

        What it last consumed and produced is kept: a stage that failed
        after a success still has that success's outputs on disk.
        """
        previous = self.stage_state(repository_id, stage)
        failures = (previous.failure_count if previous else 0) + 1
        state = StageState(
            repository_id=repository_id,
            stage=stage,
            done_at=previous.done_at if previous else None,
            stage_version=previous.stage_version if previous else 0,
            input_key=previous.input_key if previous else '',
            output_key=previous.output_key if previous else '',
            outcome='failed',
            failure_count=failures,
            next_attempt_at=now + backoff_for(failures),
            last_error=f'{stage}: {error}',
        )
        self.record_stage(state)
        self._touch(repository_id, now)
        logger.info(
            'Stage deferred',
            repository_id=repository_id,
            stage=str(stage),
            failures=failures,
            retry_at=_iso(state.next_attempt_at),
        )
        return state

    def _touch(
        self,
        repository_id: int,
        now: datetime,
        stage: Stage | None = None,
        clear: bool = False,
    ) -> None:
        """The repository-level side of a stage outcome, leases untouched."""
        sets = ['last_checked_at = :now']
        params: dict[str, Any] = {'now': _iso(now), 'id': repository_id}
        if stage is not None:
            sets.append(
                'stage_watermarks = json_set(stage_watermarks, :path, :now)',
            )
            params['path'] = f'$.{stage}'
        if clear:
            sets += [
                'failure_count = 0', "last_error = ''",
                'next_attempt_at = NULL', 'absent_since = NULL',
            ]
        self._db.execute(
            f"UPDATE repository_state SET {', '.join(sets)} "
            'WHERE repository_id = :id',
            params,
        )

    def resolve_repositories(self, names: Iterable[str]) -> tuple[set[int], list[str]]:
        """Repository ids for `owner/repo` names (or ids), for `--repos-file`.

        Matched case-insensitively, as GitHub matches names. Returns the
        ids found and the names that matched nothing tracked.
        """
        by_name: dict[str, int] = {}
        known: set[int] = set()
        for row in self._db.execute(
            'SELECT repository_id, owner, repo FROM repository_state',
        ):
            known.add(int(row['repository_id']))
            by_name[f"{row['owner']}/{row['repo']}".lower()] = int(
                row['repository_id'],
            )
        found: set[int] = set()
        missing: list[str] = []
        for raw in names:
            name = raw.strip()
            if not name or name.startswith('#'):
                continue
            if name.isdigit() and int(name) in known:
                found.add(int(name))
                continue
            repository_id = by_name.get(name.lower())
            if repository_id is None:
                missing.append(name)
            else:
                found.add(repository_id)
        return found, missing

    # -- health -------------------------------------------------------------

    def health(self, now: datetime, stages: Iterable[Stage] | None = None) -> LedgerHealth:
        """Queue metrics: what is tracked, stuck, stale and outstanding."""
        row = self._db.execute(
            """
            SELECT
                count(*) AS tracked,
                sum(failure_count > 0) AS failing,
                sum(absent_since IS NOT NULL) AS absent,
                sum(claimed_by != '' AND claim_expires_at > ?) AS claimed,
                sum(last_checked_at IS NULL) AS never_checked,
                min(last_checked_at) AS oldest_check
            FROM repository_state
            """,
            (_iso(now),),
        ).fetchone()

        return LedgerHealth(
            tracked=int(row['tracked'] or 0),
            failing=int(row['failing'] or 0),
            absent=int(row['absent'] or 0),
            claimed=int(row['claimed'] or 0),
            never_checked=int(row['never_checked'] or 0),
            oldest_check=_parse(row['oldest_check']),
            due={
                stage: self._due_count(stage, now)
                for stage in (stages or list(Stage))
            },
        )

    def _due_count(self, stage: Stage, now: datetime) -> int:
        """Due, by the rule each stage is scheduled by."""
        if stage in UPSTREAM:
            return self.count_due(stage, now)
        if stage is Stage.DEPGRAPH:
            return sum(
                self.count_due_for_stage(
                    stage, now,
                    refresh=timedelta(days=DEPGRAPH_REFRESH_DAYS),
                ).values(),
            )
        return len(self.due(stage, now))


@dataclass(frozen=True, slots=True)
class Tracked:
    """A repository the ledger tracks, as the warehouse masters on it."""

    repository_id: int
    owner: str
    repo: str
    #: GitHub's language, verbatim; '' when no snapshot or resource
    #: said.
    github_language: str = ''
    stars: int | None = None
    default_branch: str = ''
    #: The search snapshot that last listed it; '' when none did.
    snapshot: str = ''

    @property
    def full_name(self) -> str:
        return f'{self.owner}/{self.repo}'


def tracked_repositories(path: Path) -> dict[int, Tracked] | None:
    """Every repository the ledger at `path` tracks, by id.

    Read-only, and without opening a `Ledger`, which migrates and adopts
    on open: `warehouse build` reads the list the collector keeps, and
    must never write to it. None when there is no ledger at all.

    An older ledger may predate the snapshot columns; they read as
    empty there.
    """
    path = Path(path)
    if not path.is_file():
        return None
    db = sqlite3.connect(f'file:{path}?mode=ro', uri=True)
    db.row_factory = sqlite3.Row
    try:
        columns = {
            row['name']
            for row in db.execute('PRAGMA table_info(repository_state)')
        }
        if not columns:
            return {}
        wanted = [
            c for c in (
                'github_language', 'stars', 'default_branch', 'snapshot',
            ) if c in columns
        ]
        rows = db.execute(
            'SELECT repository_id, owner, repo'
            + ''.join(f', {c}' for c in wanted)
            + ' FROM repository_state ORDER BY repository_id',
        ).fetchall()
    finally:
        db.close()
    tracked: dict[int, Tracked] = {}
    for row in rows:
        values = {c: row[c] for c in wanted}
        tracked[int(row['repository_id'])] = Tracked(
            repository_id=int(row['repository_id']),
            owner=row['owner'],
            repo=row['repo'],
            github_language=values.get('github_language') or '',
            stars=values.get('stars'),
            default_branch=values.get('default_branch') or '',
            snapshot=values.get('snapshot') or '',
        )
    return tracked


def resolve_names(
    tracked: dict[int, Tracked],
    names: Iterable[str],
) -> tuple[set[int], list[str]]:
    """`Ledger.resolve_repositories` over a read-only listing."""
    by_name = {t.full_name.lower(): i for i, t in tracked.items()}
    found: set[int] = set()
    missing: list[str] = []
    for raw in names:
        name = raw.strip()
        if not name or name.startswith('#'):
            continue
        if name.isdigit() and int(name) in tracked:
            found.add(int(name))
        elif name.lower() in by_name:
            found.add(by_name[name.lower()])
        else:
            missing.append(name)
    return found, missing
