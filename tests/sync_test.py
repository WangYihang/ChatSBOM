"""The sync slice: one bounded pass over the stalest due work.

This is what a timer invokes. It must be safe to kill at any point, must
not spend rate budget on unchanged repositories, and must leave the
ledger describing exactly what it did.
"""
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import pytest

from chatsbom.core.conditional import conditional_get
from chatsbom.core.conditional import ConditionalResult
from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import Stage
from chatsbom.core.metrics import render_prometheus
from chatsbom.services.sync_service import ABSENT_RETRY
from chatsbom.services.sync_service import RepositoryObservation
from chatsbom.services.sync_service import SyncResult
from chatsbom.services.sync_service import SyncService

NOW = datetime(2026, 9, 14, 12, 0, tzinfo=timezone.utc)
PUSHED = datetime(2026, 9, 14, 9, 0, tzinfo=timezone.utc)
OLD = NOW - timedelta(days=400)
RESET = NOW + timedelta(minutes=20)


class FakeObserver:
    """Stands in for the GitHub repository resource."""

    def __init__(self, outcomes: dict[int, ConditionalResult]):
        self.outcomes = outcomes
        self.seen: list[tuple[int, str | None]] = []

    def observe(self, state, etag):
        self.seen.append((state.repository_id, etag))
        return self.outcomes[state.repository_id]


def changed(pushed=PUSHED, etag='W/"new"'):
    return ConditionalResult(
        status=200, etag=etag,
        payload={'pushed_at': pushed.isoformat().replace('+00:00', 'Z')},
    )


UNCHANGED = ConditionalResult(status=304, etag='W/"same"')
ABSENT = ConditionalResult(status=404)
FAILED = ConditionalResult(status=503, error='HTTP 503')
REFUSED = ConditionalResult(status=429, error='HTTP 429')


class Answer:
    """One canned HTTP response, as `conditional_get` reads it."""

    def __init__(self, status_code, headers=None):
        self.status_code = status_code
        self.headers = headers or {}

    def json(self):
        raise ValueError('no body')


class FakeAPI:
    """The repository endpoint, giving every request the same answer.

    It observes through `conditional_get`, so the status and headers are
    read exactly as they are in production.
    """

    def __init__(self, answer: Answer):
        self.answer = answer
        self.urls: list[str] = []

    def get(self, url, **kwargs):
        self.urls.append(url)
        return self.answer

    def observe(self, state, etag):
        return conditional_get(
            self, f'https://api.github.com/repos/{state.full_name}', etag=etag,
        )


#: The primary limit, spent: GitHub answers 403 and says when it resets.
EXHAUSTED = Answer(
    403,
    {
        'X-RateLimit-Remaining': '0',
        'X-RateLimit-Reset': str(int(RESET.timestamp())),
    },
)
#: A secondary limit, which names its own wait.
TOO_MANY = Answer(429, {'Retry-After': '60'})


@pytest.fixture
def ledger(tmp_path):
    with Ledger(tmp_path / 'l.sqlite3') as book:
        book.track(1, 'o', 'a', 'ruby')
        book.track(2, 'o', 'b', 'ruby')
        book.track(3, 'o', 'c', 'go')
        yield book


def service(ledger, outcomes):
    return SyncService(ledger, FakeObserver(outcomes).observe)


# --- the revalidation pass ------------------------------------------------

def test_unchanged_repositories_cost_no_budget(ledger):
    svc = service(ledger, {1: UNCHANGED, 2: UNCHANGED, 3: UNCHANGED})
    result = svc.revalidate(NOW, limit=10)

    assert result == SyncResult(checked=3, unchanged=3)
    assert result.spent_quota == 0


def test_changed_repositories_record_the_new_push(ledger):
    svc = service(ledger, {1: changed(), 2: UNCHANGED, 3: UNCHANGED})
    result = svc.revalidate(NOW, limit=10)

    assert result.changed == 1
    assert result.spent_quota == 1
    assert ledger.get(1).pushed_at_seen == PUSHED


def test_a_changed_repository_becomes_due_for_later_stages(ledger):
    svc = service(ledger, {1: changed(), 2: UNCHANGED, 3: UNCHANGED})
    svc.revalidate(NOW, limit=10)

    assert ledger.get(1).needs(Stage.SBOM)


def test_an_unchanged_repository_does_not_advance_a_stage(ledger):
    ledger.record_success(1, Stage.SBOM, NOW)
    svc = service(ledger, {1: UNCHANGED, 2: UNCHANGED, 3: UNCHANGED})
    svc.revalidate(NOW, limit=10)

    assert not ledger.get(1).needs(Stage.SBOM)


def test_the_stored_etag_is_offered_to_the_observer(ledger):
    ledger.record_etag(1, 'repo', 'W/"stored"')
    observer = FakeObserver({1: UNCHANGED, 2: UNCHANGED, 3: UNCHANGED})
    SyncService(ledger, observer.observe).revalidate(NOW, limit=10)

    assert (1, 'W/"stored"') in observer.seen


def test_a_new_etag_is_stored_for_next_time(ledger):
    svc = service(
        ledger, {
            1: changed(etag='W/"fresh"'),
            2: UNCHANGED, 3: UNCHANGED,
        },
    )
    svc.revalidate(NOW, limit=10)
    assert ledger.get(1).etags['repo'] == 'W/"fresh"'


# --- bounds and resumability ----------------------------------------------

def test_a_slice_is_bounded(ledger):
    svc = service(ledger, {1: UNCHANGED, 2: UNCHANGED, 3: UNCHANGED})
    assert svc.revalidate(NOW, limit=2).checked == 2


def test_no_repository_is_starved_across_slices(ledger):
    """Slices rotate: the least-recently-checked always comes first.

    With three repositories and a slice of two, the never-checked one
    must be picked up by the second slice, and every repository must be
    reached within two slices.
    """
    observer = FakeObserver({1: UNCHANGED, 2: UNCHANGED, 3: UNCHANGED})
    svc = SyncService(ledger, observer.observe)

    svc.revalidate(NOW, limit=2)
    seen_first = {rid for rid, _ in observer.seen}

    svc.revalidate(NOW + timedelta(seconds=1), limit=2)
    seen_all = {rid for rid, _ in observer.seen}

    assert len(seen_first) == 2
    assert seen_all == {1, 2, 3}, 'every repository reached within two slices'


def test_the_stalest_repository_is_always_first(ledger):
    observer = FakeObserver({1: UNCHANGED, 2: UNCHANGED, 3: UNCHANGED})
    svc = SyncService(ledger, observer.observe)

    # Give 1 and 2 a recent check; 3 has never been checked.
    ledger.record_unchanged(1, NOW)
    ledger.record_unchanged(2, NOW)

    svc.revalidate(NOW + timedelta(seconds=1), limit=1)
    assert [rid for rid, _ in observer.seen] == [3]


def test_a_quota_budget_stops_the_slice_early(ledger):
    """A slice must not exhaust the hourly allowance in one go."""
    svc = service(ledger, {1: changed(), 2: changed(), 3: changed()})
    result = svc.revalidate(NOW, limit=10, quota_budget=2)

    assert result.spent_quota == 2
    assert result.checked == 2


def test_unchanged_repositories_do_not_consume_the_quota_budget(ledger):
    svc = service(ledger, {1: UNCHANGED, 2: UNCHANGED, 3: changed()})
    result = svc.revalidate(NOW, limit=10, quota_budget=1)

    assert result.checked == 3, '304s are free, so the budget is untouched'
    assert result.spent_quota == 1


def test_language_can_be_narrowed(ledger):
    svc = service(ledger, {3: UNCHANGED})
    assert svc.revalidate(NOW, limit=10, language='go').checked == 1


# --- failure handling -----------------------------------------------------

def test_a_failure_defers_that_repository_only(ledger):
    svc = service(ledger, {1: FAILED, 2: UNCHANGED, 3: UNCHANGED})
    result = svc.revalidate(NOW, limit=10)

    assert result.failed == 1
    assert result.unchanged == 2
    assert ledger.get(1).next_attempt_at > NOW


def test_a_deleted_repository_is_recorded_and_not_retried_immediately(ledger):
    svc = service(ledger, {1: ABSENT, 2: UNCHANGED, 3: UNCHANGED})
    result = svc.revalidate(NOW, limit=10)

    assert result.absent == 1
    state = ledger.get(1)
    assert state.next_attempt_at == NOW + ABSENT_RETRY
    assert state.absent_since == NOW


def test_a_deleted_repository_is_absent_not_failing(ledger):
    """A 404 is an answer, not an error. Counted as a failure, every
    repository deleted upstream sat in `chatsbom_queue_failing` for good,
    and its count grew with each re-check."""
    later = NOW + ABSENT_RETRY
    for when in (NOW, later):
        result = service(
            ledger, {1: ABSENT, 2: UNCHANGED, 3: UNCHANGED},
        ).revalidate(when, limit=10)
        assert (result.absent, result.failed) == (1, 0)

    assert ledger.get(1).failure_count == 0
    health = ledger.health(later)
    assert health.failing == 0
    assert 'chatsbom_queue_failing 0' in render_prometheus(health, now=later)
    assert health.absent == 1


def test_a_repository_that_comes_back_is_no_longer_absent(ledger):
    service(
        ledger, {1: ABSENT, 2: UNCHANGED, 3: UNCHANGED},
    ).revalidate(NOW, limit=10)

    later = NOW + ABSENT_RETRY
    service(
        ledger, {1: changed(), 2: UNCHANGED, 3: UNCHANGED},
    ).revalidate(later, limit=10)

    assert ledger.get(1).absent_since is None
    assert ledger.health(later).absent == 0


def test_an_exception_in_the_observer_does_not_abort_the_slice(ledger):
    def boom(state, etag):
        if state.repository_id == 1:
            raise RuntimeError('unexpected')
        return UNCHANGED

    result = SyncService(ledger, boom).revalidate(NOW, limit=10)
    assert result.failed == 1
    assert result.unchanged == 2


# --- a refused token ------------------------------------------------------

@pytest.mark.parametrize(
    'answer', [EXHAUSTED, TOO_MANY], ids=['403-no-quota-left', '429'],
)
def test_a_refused_token_stops_the_slice_and_blames_no_repository(
    ledger, answer,
):
    """The refusal is about the token, not the repository it answered,
    and every later request in the slice would be refused the same way.
    Recorded as failures, it put a whole slice into backoff at once."""
    api = FakeAPI(answer)
    result = SyncService(ledger, api.observe).revalidate(NOW, limit=10)

    assert result.failed == 0
    assert [ledger.get(rid).failure_count for rid in (1, 2, 3)] == [0, 0, 0]
    assert ledger.health(NOW).failing == 0
    assert len(api.urls) == 1, 'the slice stops at the first refusal'
    # Released, not left to the lease: the next slice can take them all.
    assert len(ledger.claim(Stage.REPO, NOW, limit=10, worker='next')) == 3
    assert result.rate_limited


def test_a_refusal_mid_slice_keeps_what_was_already_learned(ledger):
    observer = FakeObserver({1: UNCHANGED, 2: REFUSED, 3: UNCHANGED})
    result = SyncService(ledger, observer.observe).revalidate(NOW, limit=10)

    assert [rid for rid, _ in observer.seen] == [1, 2]
    assert (result.checked, result.unchanged) == (1, 1)
    assert ledger.get(1).last_checked_at == NOW
    assert ledger.get(2).last_checked_at is None, 'a refusal learned nothing'
    assert ledger.get(3).last_checked_at is None
    assert result.rate_limited


def test_the_slice_reports_when_the_token_may_be_used_again(ledger):
    api = FakeAPI(EXHAUSTED)
    result = SyncService(ledger, api.observe).revalidate(NOW, limit=10)
    assert result.resumes_at == RESET


# --- reporting ------------------------------------------------------------

def test_result_reports_the_free_ratio(ledger):
    svc = service(ledger, {1: UNCHANGED, 2: UNCHANGED, 3: changed()})
    result = svc.revalidate(NOW, limit=10)
    assert result.unchanged_ratio == pytest.approx(2 / 3)


def test_an_empty_queue_is_not_an_error(tmp_path):
    with Ledger(tmp_path / 'empty.sqlite3') as book:
        svc = SyncService(book, lambda s, e: UNCHANGED)
        assert svc.revalidate(NOW, limit=10) == SyncResult()


# --- observation parsing --------------------------------------------------

def test_observation_reads_pushed_at():
    obs = RepositoryObservation.from_payload(
        {'pushed_at': '2026-09-14T09:00:00Z'},
    )
    assert obs.pushed_at == PUSHED


def test_observation_tolerates_a_missing_pushed_at():
    assert RepositoryObservation.from_payload({}).pushed_at is None


def test_observation_tolerates_an_unparsable_pushed_at():
    assert RepositoryObservation.from_payload(
        {'pushed_at': 'not a date'},
    ).pushed_at is None
