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


@dataclass(frozen=True, slots=True)
class LedgerHealth:
    """A snapshot of queue health, for `queue status` and metrics."""

    tracked: int
    failing: int
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
    claim_expires_at  TEXT
);
CREATE INDEX IF NOT EXISTS idx_state_language ON repository_state (language);
CREATE INDEX IF NOT EXISTS idx_state_checked ON repository_state (last_checked_at);
CREATE INDEX IF NOT EXISTS idx_state_attempt ON repository_state (next_attempt_at);
"""


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
        self._db = sqlite3.connect(self.path, isolation_level=None)
        self._db.row_factory = sqlite3.Row
        # WAL so a reader (queue status) never blocks the collector.
        self._db.execute('PRAGMA journal_mode=WAL')
        self._db.execute('PRAGMA busy_timeout=5000')
        self._db.executescript(_SCHEMA)

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
                next_attempt_at, last_error, claimed_by, claim_expires_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                claim_expires_at = excluded.claim_expires_at
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
                _iso(state.claim_expires_at),
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
        state.claimed_by = ''
        state.claim_expires_at = None
        self.upsert(state)

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
    ) -> list[RepositoryState]:
        """Repositories needing `stage`, stalest first.

        Ordering by `last_checked_at` with nulls first means never-checked
        repositories are picked up before re-checks, and no repository can
        be starved by a busier one.
        """
        clauses = ['(next_attempt_at IS NULL OR next_attempt_at <= ?)']
        params: list[Any] = [_iso(now)]

        if language:
            clauses.append('language = ?')
            params.append(language)

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
    ) -> list[RepositoryState]:
        """Take a slice of due work, leased so a second worker skips it.

        The lease expires rather than being held, so a worker killed
        mid-slice does not strand its repositories.
        """
        expires = now + lease
        claimed: list[RepositoryState] = []

        for state in self.due(
            stage, now, limit=None, language=language, recheck=recheck,
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

    # -- health -------------------------------------------------------------

    def health(self, now: datetime, stages: Iterable[Stage] | None = None) -> LedgerHealth:
        """Queue metrics: what is tracked, stuck, stale and outstanding."""
        row = self._db.execute(
            """
            SELECT
                count(*) AS tracked,
                sum(failure_count > 0) AS failing,
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
            claimed=int(row['claimed'] or 0),
            never_checked=int(row['never_checked'] or 0),
            oldest_check=_parse(row['oldest_check']),
            due={
                stage: len(self.due(stage, now))
                for stage in (stages or list(Stage))
            },
        )
