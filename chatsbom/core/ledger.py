"""Per-repository collection state, so the pipeline can run continuously.

Batch collection walks a fixed list of repositories through each stage in
turn. That shape has three problems once you want the dataset to stay
fresh rather than be re-collected: a killed process loses the batch, an
unchanged repository costs exactly as much as a changed one, and there is
nowhere to record that a particular repository keeps failing.

The ledger replaces the list with state. Each repository carries what we
last observed (`pushed_at_seen`, per-resource ETags), how far each stage
has got (`stage_watermarks`), and whether to leave it alone for a while
(`failure_count`, `next_attempt_at`). The scheduler then asks "which
repositories are stalest and due?" instead of iterating.

SQLite rather than ClickHouse: this is small, mutable, per-row state with
frequent single-row updates, which is the opposite of what a columnar
store is for. It also means the queue survives the database being
rebuilt.

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
-- one stage failing never backs off another. Only DEPGRAPH is scheduled
-- from here so far (see `claim_stage`); the repository-major walk in
-- `chatsbom run` still reads `stage_watermarks` until it moves over too.
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
    # the list a repository was tracked from, which the language-keyed
    # stages still key their paths by; GitHub's own language is only an
    # attribute, and may be one no stage has a handler for.
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
        # records each outcome under one lock.
        self._db = sqlite3.connect(
            self.path, isolation_level=None, check_same_thread=False,
        )
        self._db.row_factory = sqlite3.Row
        # WAL so a reader (queue status) never blocks the collector.
        self._db.execute('PRAGMA journal_mode=WAL')
        self._db.execute('PRAGMA busy_timeout=5000')
        self._db.executescript(_SCHEMA)
        self._reconcile_columns()

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
    ) -> bool:
        """Track a repository listed by a search snapshot.

        Returns whether it was new. A repository already tracked keeps
        its name, `language` and progress — `queue sync` knows its name
        better than a snapshot taken months ago — and gets the snapshot's
        attributes. A new one is tracked with `language = ''`, which the
        language-keyed walk in `chatsbom run` leaves alone: it has no
        list to key its paths by. Stages that need no language, the
        dependency graph first, take it.
        """
        cursor = self._db.execute(
            """
            INSERT INTO repository_state (
                repository_id, owner, repo, language, snapshot,
                github_language, stars, default_branch
            ) VALUES (?, ?, ?, '', ?, ?, ?, ?)
            ON CONFLICT(repository_id) DO NOTHING
            """,
            (
                repository_id, owner, repo, snapshot, github_language,
                stars, default_branch,
            ),
        )
        if cursor.rowcount:
            return True
        self._db.execute(
            """
            UPDATE repository_state
            SET snapshot = ?, github_language = ?,
                stars = coalesce(?, stars),
                default_branch = CASE WHEN ? != '' THEN ?
                                      ELSE default_branch END
            WHERE repository_id = ?
            """,
            (
                snapshot, github_language, stars, default_branch,
                default_branch, repository_id,
            ),
        )
        return False

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
        """Advance one stage's watermark and clear any backoff."""
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
        """Count a failure and push the repository into backoff."""
        state = self._require(repository_id)
        state.failure_count += 1
        state.last_error = f'{stage}: {error}'[:500]
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

        for state in self.due(
            stage, now, limit=None, language=language, recheck=recheck,
            keyed_only=keyed_only,
        ):
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
        cutoff = _iso(now - refresh)
        expires = _iso(now + lease)
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
        ORDER BY priority ASC, coalesce(r.stars, -1) DESC,
                 r.repository_id ASC
        LIMIT :cap
        """
        # Room for rows another worker leases between this read and the
        # update below; -1 is SQLite's "no limit".
        cap = -1 if limit is None else limit * 4 + 50
        rows = self._db.execute(
            sql,
            {
                'stage': str(stage), 'now': _iso(now), 'cutoff': cutoff,
                'cap': cap,
            },
        ).fetchall()

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
        """Write one stage outcome, and drop its lease."""
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
                _iso(state.next_attempt_at), state.last_error[:500],
            ),
        )

    def release_stage(self, repository_id: int, stage: Stage) -> None:
        """Drop a stage lease without recording anything."""
        self._db.execute(
            "UPDATE stage_state SET claimed_by = '', claim_expires_at = NULL "
            'WHERE repository_id = ? AND stage = ?',
            (repository_id, str(stage)),
        )

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
                stage: len(self.due(stage, now))
                for stage in (stages or list(Stage))
            },
        )
