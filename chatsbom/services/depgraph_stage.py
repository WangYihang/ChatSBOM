"""The dependency-graph stage, scheduled on its own.

GitHub's dependency graph is the second SBOM source, and the one that
sees the Maven and Gradle projects Syft reads nothing from. It used to
be collected only for repositories whose Syft SBOM had succeeded
(`github depgraph` walked the `07-sbom` lists), keyed by language, and
overwritten by every fetch. Its synchronous endpoint closes after
2026-11-13, so none of that could wait for the rest of the
repository-centric redesign (#51, #55): this is its PR A.

What changes:

* **Independent.** Due for every repository the ledger tracks, whatever
  its language and whether or not anything else about it succeeded. It
  needs only `owner/repo`, so `queue track --snapshot` can seed
  repositories no other stage has touched.
* **Scheduled from the ledger**, in `stage_state`, with its own outcome,
  backoff and lease. A depgraph failure no longer backs off Syft, nor
  the other way round.
* **Negative cache.** A 404 — no graph, or the graph switched off — is
  not asked again for 30 days, then 60, then every 90. A 5xx or a
  timeout backs off from 15 minutes, doubling, up to 30 days; after
  `TOO_LARGE_AFTER` in a row the repository is `too_large` (spring-boot
  answers 500 "Request timed out") and is asked monthly.
* **Kept for good.** Every fetch is its own directory
  (`core/depgraph_store.py`), keyed by repository id, stamped with the
  default branch and the HEAD sha `git ls-remote` gave immediately
  before it — the graph's own commit, not the Syft scan's.
* **Several tokens.** GitHub meters the dependency-graph bucket per
  token, at 100 to 200 requests an hour. Each token is a worker thread,
  paced to `rate` requests an hour, and they share the due work.
* **Closing.** `closed_reason`: with `CHATSBOM_DEPGRAPH_API=sync` the
  stage turns itself off from 2026-11-13, saying so once. With the
  default, `auto`, it asks for GitHub's asynchronous report from that
  day instead (#50). Nothing else depends on it.
"""
from __future__ import annotations

import threading
import time
from collections import Counter
from collections import deque
from collections.abc import Callable
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

import structlog

from chatsbom.core import depgraph_store
from chatsbom.core.conditional import ConditionalResult
from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import Stage
from chatsbom.core.ledger import StageState
from chatsbom.core.ledger import StageWork
from chatsbom.services.dependency_graph_service import DependencyGraphService

logger = structlog.get_logger('depgraph_stage')

#: The stage's version in `stage_state`. 1 was the language-keyed,
#: SBOM-gated stage that overwrote its one file.
DEPGRAPH_VERSION = 2

#: How long a stored graph stands before it is fetched again.
DEPGRAPH_REFRESH = timedelta(days=30)

#: The negative cache: 30 days after the first 404, then 60, then 90.
ABSENT_BASE = timedelta(days=30)
ABSENT_CAP = timedelta(days=90)

#: Backoff after a failure: 15 minutes, doubling, up to 30 days.
FAILURE_BASE = timedelta(minutes=15)
FAILURE_CAP = timedelta(days=30)

#: Server errors or timeouts in a row before a repository is taken to
#: be too large for GitHub to answer, and asked monthly.
TOO_LARGE_AFTER = 5

#: When to look again for a report GitHub was still generating.
PENDING_RETRY = timedelta(minutes=15)

#: Requests an hour each token is paced to. GitHub's limit for this
#: bucket was measured at 100 an hour (`commands/run.py`) and later at
#: 200 (#50's probe); 90 stays under both.
DEFAULT_RATE = 90.0

#: Repositories claimed at a time, per token.
CLAIM_BATCH = 5

OK = 'ok'
ABSENT = 'absent'
FAILED = 'failed'
TOO_LARGE = 'too_large'
PENDING = 'pending'


def _server_side(result: ConditionalResult) -> bool:
    """A 5xx, or no answer at all: the session retries 5xx and then
    raises, so a persistent 500 arrives as a transport error."""
    return result.status == 0 or result.status >= 500


def next_state(
    previous: StageState,
    result: ConditionalResult,
    now: datetime,
    output_key: str = '',
) -> StageState | None:
    """What one answer makes of a repository's depgraph state.

    None for a refused token, which says nothing about the repository:
    nothing is recorded, and it stays due.
    """
    if result.rate_limited:
        return None
    base = replace(
        previous,
        stage=Stage.DEPGRAPH,
        stage_version=DEPGRAPH_VERSION,
        http_status=result.status or None,
        claimed_by='',
        claim_expires_at=None,
    )
    if result.changed:
        return replace(
            base,
            outcome=OK,
            done_at=now,
            output_key=output_key,
            failure_count=0,
            next_attempt_at=now + DEPGRAPH_REFRESH,
            last_error='',
        )
    if result.absent:
        streak = previous.failure_count + 1 if previous.outcome == ABSENT else 1
        return replace(
            base,
            outcome=ABSENT,
            failure_count=streak,
            next_attempt_at=now + min(ABSENT_BASE * streak, ABSENT_CAP),
            last_error='',
        )
    if result.pending:
        return replace(
            base,
            outcome=PENDING,
            next_attempt_at=now + PENDING_RETRY,
            last_error='report still being generated',
        )

    streak = (
        previous.failure_count + 1
        if previous.outcome in (FAILED, TOO_LARGE) else 1
    )
    error = result.error or f'HTTP {result.status}'
    if _server_side(result) and streak >= TOO_LARGE_AFTER:
        return replace(
            base,
            outcome=TOO_LARGE,
            failure_count=streak,
            next_attempt_at=now + FAILURE_CAP,
            last_error=error,
        )
    delay = min(FAILURE_BASE * (2 ** min(streak - 1, 30)), FAILURE_CAP)
    return replace(
        base,
        outcome=FAILED,
        failure_count=streak,
        next_attempt_at=now + delay,
        last_error=error,
    )


class Pacer:
    """Keeps one token within `per_hour` requests an hour.

    Spaced rather than burst: a request is sent no sooner than the ones
    before it, times the interval, after the first. A fetch that cost
    more than one request (a report is two or more) pushes the next one
    back by as many intervals.
    """

    def __init__(
        self,
        per_hour: float,
        sleep: Callable[[float], None] = time.sleep,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        if per_hour <= 0:
            raise ValueError('the rate must be positive')
        self.interval = 3600.0 / per_hour
        self._sleep = sleep
        self._monotonic = monotonic
        self._next: float | None = None

    def wait(self) -> None:
        if self._next is None:
            return
        delay = self._next - self._monotonic()
        if delay > 0:
            self._sleep(delay)

    def spent(self, requests: int) -> None:
        now = self._monotonic()
        start = now if self._next is None else max(self._next, now)
        self._next = start + max(requests, 1) * self.interval


@dataclass
class TokenWorker:
    """One token's share of the stage."""

    label: str
    service: DependencyGraphService
    pacer: Pacer
    counts: Counter[str] = field(default_factory=Counter)
    #: The repository GitHub refused this token at, and its answer.
    refusal: tuple[str, ConditionalResult] | None = None


@dataclass
class DepgraphPass:
    """What one pass of the stage did."""

    counts: Counter[str] = field(default_factory=Counter)
    workers: list[TokenWorker] = field(default_factory=list)
    #: Why the stage asked nothing, when it did not.
    closed: str | None = None

    @property
    def refusals(self) -> list[tuple[str, str, ConditionalResult]]:
        return [
            (worker.label, *worker.refusal)
            for worker in self.workers if worker.refusal is not None
        ]

    @property
    def asked(self) -> int:
        return sum(
            self.counts[key]
            for key in (OK, 'unchanged', ABSENT, FAILED, TOO_LARGE, PENDING)
        )

    def summary(self, now: datetime) -> list[str]:
        """Lines for the console, after the pass."""
        if self.closed:
            return [
                f'[yellow]Dependency graph stage disabled:[/] {self.closed}. '
                'Nothing was asked or recorded; `db index` keeps the '
                'documents already stored.',
            ]
        c = self.counts
        lines = [
            f'[dim]Dependency graph: fetched {c[OK]:,} · unchanged '
            f'{c["unchanged"]:,} · no graph {c[ABSENT]:,} · failed '
            f'{c[FAILED]:,} · too large {c[TOO_LARGE]:,} · pending '
            f'{c[PENDING]:,} · tokens {len(self.workers)}[/dim]',
        ]
        for label, name, answer in self.refusals:
            resumes_at = answer.rate_limit.resumes_at(now)
            resumes = (
                f' GitHub accepts it again at '
                f'{resumes_at:%Y-%m-%d %H:%M:%S} UTC.'
                if resumes_at else ''
            )
            lines.append(
                f'[yellow]Dependency graph rate limited:[/] GitHub refused '
                f'{label} at {name} (HTTP {answer.status}); that token '
                'stopped asking. Nothing was recorded for it, so it stays '
                f'due.{resumes}',
            )
        return lines


#: `(owner, repo) -> (default branch, HEAD sha)`, or None when unknown.
HeadResolver = Callable[[str, str], 'tuple[str, str] | None']


class DepgraphStage:
    """Claim due repositories from the ledger and fetch their graphs.

    One thread per token. Each takes the next claimed repository, waits
    for its token's pace, reads the default branch's HEAD, asks GitHub,
    keeps the document and records the outcome. A refused token stops
    its own thread and no other; the repository it was refused at is
    released unrecorded.

    Every ledger call is made under one lock: SQLite serialises writers
    anyway, and one connection is shared.
    """

    def __init__(
        self,
        ledger: Ledger,
        workers: Sequence[TokenWorker],
        root: Path,
        heads: HeadResolver,
        clock: Callable[[], datetime] | None = None,
        worker_name: str = 'depgraph',
        repos: set[int] | None = None,
    ) -> None:
        if not workers:
            raise ValueError('the stage needs at least one token')
        self._ledger = ledger
        self._workers = list(workers)
        self._root = Path(root)
        self._heads = heads
        self._clock = clock or (lambda: datetime.now(timezone.utc))
        self._name = worker_name
        #: `--repos-file`: only these repositories, when given.
        self._repos = repos
        self._lock = threading.Lock()
        self._queue: deque[StageWork] = deque()
        self._claimed = 0
        self._exhausted = False

    def run(self, limit: int | None) -> DepgraphPass:
        """Fetch up to `limit` due repositories; all due if None."""
        self._limit = limit
        result = DepgraphPass(workers=self._workers)
        threads = [
            threading.Thread(
                target=self._work, args=(worker,),
                name=f'depgraph-{position}', daemon=True,
            )
            for position, worker in enumerate(self._workers, start=1)
        ]
        for thread in threads:
            thread.start()
        try:
            for thread in threads:
                # A timeout, so Ctrl-C reaches the main thread.
                while thread.is_alive():
                    thread.join(timeout=0.5)
        finally:
            with self._lock:
                while self._queue:
                    work = self._queue.popleft()
                    self._ledger.release_stage(
                        work.repository_id, Stage.DEPGRAPH,
                    )
        for worker in self._workers:
            result.counts.update(worker.counts)
        return result

    def _next(self, worker: TokenWorker) -> StageWork | None:
        with self._lock:
            if not self._queue and not self._exhausted:
                self._refill(worker)
            return self._queue.popleft() if self._queue else None

    def _refill(self, worker: TokenWorker) -> None:
        want = CLAIM_BATCH
        if self._limit is not None:
            want = min(want, self._limit - self._claimed)
        if want <= 0:
            self._exhausted = True
            return
        # Long enough for this batch at this token's pace, with room:
        # an expired lease would let another worker take it mid-fetch.
        lease = timedelta(
            seconds=worker.pacer.interval * want * 3 + 600,
        )
        claimed = self._ledger.claim_stage(
            Stage.DEPGRAPH, self._clock(), want, self._name, lease=lease,
            refresh=DEPGRAPH_REFRESH, repos=self._repos,
        )
        if not claimed:
            self._exhausted = True
            return
        self._claimed += len(claimed)
        self._queue.extend(claimed)

    def _work(self, worker: TokenWorker) -> None:
        while worker.refusal is None:
            work = self._next(worker)
            if work is None:
                return
            try:
                self._one(worker, work)
            except Exception as error:  # noqa: BLE001 - one repository
                logger.warning(
                    'Dependency graph stage error',
                    repo=work.full_name, token=worker.label,
                    error=str(error)[:300],
                )
                self._record(
                    work,
                    ConditionalResult(status=0, error=str(error)[:300]),
                )
                worker.counts[FAILED] += 1

    def _one(self, worker: TokenWorker, work: StageWork) -> None:
        worker.pacer.wait()
        head = self._heads(work.owner, work.repo)
        before = worker.service.requests
        result = worker.service.fetch(work.owner, work.repo)
        worker.pacer.spent(worker.service.requests - before)

        if result.rate_limited:
            worker.refusal = (work.full_name, result)
            with self._lock:
                self._ledger.release_stage(work.repository_id, Stage.DEPGRAPH)
            return

        output_key = ''
        if result.changed:
            branch, sha = head if head else ('', '')
            stored = depgraph_store.store(
                self._root,
                repository_id=work.repository_id,
                owner=work.owner,
                repo=work.repo,
                payload=result.payload,
                fetched_at=self._clock(),
                ref=branch or work.default_branch,
                head_sha=sha,
                http_status=result.status,
            )
            output_key = stored.fetch.sha256
            worker.counts[OK if stored.written else 'unchanged'] += 1
            logger.info(
                'Dependency graph stored' if stored.written
                else 'Dependency graph unchanged',
                repo=work.full_name, token=worker.label,
                path=str(stored.fetch.document),
                commit_sha=stored.fetch.commit_sha,
            )
        state = self._record(work, result, output_key)
        if state is not None and state.outcome != OK:
            worker.counts[state.outcome] += 1

    def _record(
        self,
        work: StageWork,
        result: ConditionalResult,
        output_key: str = '',
    ) -> StageState | None:
        state = next_state(work.state, result, self._clock(), output_key)
        with self._lock:
            if state is None:
                self._ledger.release_stage(work.repository_id, Stage.DEPGRAPH)
            else:
                self._ledger.record_stage(state)
        if state is not None and state.outcome in (TOO_LARGE, FAILED):
            logger.info(
                'Dependency graph deferred',
                repo=work.full_name, outcome=state.outcome,
                failures=state.failure_count,
                retry_at=(
                    state.next_attempt_at.isoformat()
                    if state.next_attempt_at else None
                ),
            )
        return state
