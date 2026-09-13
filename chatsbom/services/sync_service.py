"""One bounded pass over the stalest due work.

This is what a timer invokes. Three properties matter more than
throughput:

**Bounded.** A slice has a row limit *and* a quota budget, so it cannot
drain the hourly rate allowance. The arithmetic that motivates this: the
6,222 repositories pushed in a typical week cost roughly ten requests
each, about 62,000 in total — 369 per hour if spread across the week, or
12 hours of a saturated token if run as a batch. Spreading it is both
gentler and inherently resumable.

**Free where possible.** Unchanged repositories answer 304, which does
not count against the rate limit, so they do not consume the quota
budget. A slice can therefore revalidate far more repositories than it
could collect.

**Safe to kill.** Every outcome is written to the ledger as it happens,
and claims are leased. Stopping mid-slice loses at most the repository in
flight.
"""
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any

import structlog

from chatsbom.core.conditional import ConditionalResult
from chatsbom.core.ledger import DEFAULT_RECHECK
from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import RepositoryState
from chatsbom.core.ledger import Stage

logger = structlog.get_logger('sync')

#: Resource key under which the repository resource's ETag is stored.
REPO_RESOURCE = 'repo'

#: How long to wait before re-checking a repository GitHub says is gone.
#: Renames and transfers do resolve, so this is a long deferral rather
#: than an untrack.
ABSENT_RETRY = timedelta(days=14)

#: Observes one repository, given its state and the ETag we hold.
Observer = Callable[[RepositoryState, str | None], ConditionalResult]


@dataclass(frozen=True, slots=True)
class RepositoryObservation:
    """The part of the repository resource that drives scheduling."""

    pushed_at: datetime | None

    @classmethod
    def from_payload(cls, payload: Mapping[str, Any]) -> 'RepositoryObservation':
        raw = payload.get('pushed_at')
        if not raw:
            return cls(pushed_at=None)
        try:
            parsed = datetime.fromisoformat(str(raw).replace('Z', '+00:00'))
        except (TypeError, ValueError):
            return cls(pushed_at=None)
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return cls(pushed_at=parsed)


@dataclass(frozen=True, slots=True)
class SyncResult:
    """What one slice did."""

    checked: int = 0
    unchanged: int = 0
    changed: int = 0
    absent: int = 0
    failed: int = 0
    spent_quota: int = 0

    @property
    def unchanged_ratio(self) -> float:
        """Share of checks that were free. The metric to watch."""
        return self.unchanged / self.checked if self.checked else 0.0


class SyncService:
    """Drives revalidation slices against the ledger."""

    def __init__(self, ledger: Ledger, observe: Observer):
        self.ledger = ledger
        self.observe = observe

    def revalidate(
        self,
        now: datetime,
        limit: int,
        quota_budget: int | None = None,
        language: str | None = None,
        worker: str = 'sync',
        recheck: timedelta = DEFAULT_RECHECK,
    ) -> SyncResult:
        """Re-check the stalest repositories and record what changed.

        Only the repository resource is fetched here — it is the one that
        answers "did anything change at all", and its `pushed_at` is what
        makes every later stage due. Collecting those later stages is a
        separate slice, so a cheap revalidation pass cannot be starved by
        an expensive download.
        """
        checked = unchanged = changed = absent = failed = spent = 0

        due = self.ledger.claim(
            Stage.REPO, now, limit=limit, worker=worker, language=language,
            recheck=recheck,
        )

        for state in due:
            if quota_budget is not None and spent >= quota_budget:
                # Out of budget for requests that actually cost something.
                # Release the rest so the next slice picks them up.
                self.ledger.release(state.repository_id)
                continue

            checked += 1
            etag = state.etags.get(REPO_RESOURCE)

            try:
                outcome = self.observe(state, etag)
            except Exception as e:
                failed += 1
                spent += 1
                self.ledger.record_failure(
                    state.repository_id, Stage.REPO, now,
                    f'{type(e).__name__}: {e}',
                )
                continue

            if outcome.spent_quota:
                spent += 1

            if outcome.unchanged:
                unchanged += 1
                if outcome.etag:
                    self.ledger.record_etag(
                        state.repository_id, REPO_RESOURCE, outcome.etag,
                    )
                self.ledger.record_unchanged(state.repository_id, now)

            elif outcome.changed:
                changed += 1
                self._record_change(state, outcome, now)

            elif outcome.absent:
                absent += 1
                self.ledger.record_failure(
                    state.repository_id, Stage.REPO, now,
                    'absent (404): renamed, deleted or made private',
                )
                # Override the exponential backoff: a 404 is not a
                # transient error, so a long fixed deferral is honest.
                self._defer(state.repository_id, now + ABSENT_RETRY)

            else:
                failed += 1
                self.ledger.record_failure(
                    state.repository_id, Stage.REPO, now,
                    outcome.error or f'HTTP {outcome.status}',
                )

        result = SyncResult(
            checked=checked, unchanged=unchanged, changed=changed,
            absent=absent, failed=failed, spent_quota=spent,
        )
        logger.info(
            'Revalidation slice',
            checked=result.checked,
            unchanged=result.unchanged,
            changed=result.changed,
            absent=result.absent,
            failed=result.failed,
            spent_quota=result.spent_quota,
            free_ratio=f'{result.unchanged_ratio:.0%}',
        )
        return result

    def _record_change(
        self,
        state: RepositoryState,
        result: ConditionalResult,
        now: datetime,
    ) -> None:
        if result.etag:
            self.ledger.record_etag(
                state.repository_id, REPO_RESOURCE, result.etag,
            )

        observation = RepositoryObservation.from_payload(result.payload or {})
        if observation.pushed_at:
            # This is what makes the later stages due.
            self.ledger.record_push(
                state.repository_id, observation.pushed_at, now,
            )

        # The repository resource itself is now current.
        self.ledger.record_success(state.repository_id, Stage.REPO, now)

    def _defer(self, repository_id: int, until: datetime) -> None:
        state = self.ledger.get(repository_id)
        if state is None:
            return
        state.next_attempt_at = until
        self.ledger.upsert(state)
