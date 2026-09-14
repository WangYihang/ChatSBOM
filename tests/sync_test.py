"""The sync slice: one bounded pass over the stalest due work.

This is what a timer invokes. It must be safe to kill at any point, must
not spend rate budget on unchanged repositories, and must leave the
ledger describing exactly what it did.
"""
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import pytest

from chatsbom.core.conditional import ConditionalResult
from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import Stage
from chatsbom.services.sync_service import RepositoryObservation
from chatsbom.services.sync_service import SyncResult
from chatsbom.services.sync_service import SyncService

NOW = datetime(2026, 9, 14, 12, 0, tzinfo=timezone.utc)
PUSHED = datetime(2026, 9, 14, 9, 0, tzinfo=timezone.utc)
OLD = NOW - timedelta(days=400)


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
    assert state.next_attempt_at > NOW
    assert 'absent' in state.last_error


def test_an_exception_in_the_observer_does_not_abort_the_slice(ledger):
    def boom(state, etag):
        if state.repository_id == 1:
            raise RuntimeError('unexpected')
        return UNCHANGED

    result = SyncService(ledger, boom).revalidate(NOW, limit=10)
    assert result.failed == 1
    assert result.unchanged == 2


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
