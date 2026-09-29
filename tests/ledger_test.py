"""The work ledger: per-repository state that makes collection resumable.

Batch collection walks a fixed list, so a killed process loses the batch
and an unchanged repository costs the same as a changed one. The ledger
replaces the list with per-repository state — what we last saw, what we
have collected, and when to try again — so the scheduler can pick work by
staleness instead of iterating.
"""
import json
import sqlite3
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import pytest

from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import RepositoryState
from chatsbom.core.ledger import Stage
from chatsbom.core.ledger import StageState

NOW = datetime(2026, 9, 14, 12, 0, tzinfo=timezone.utc)
EARLIER = NOW - timedelta(days=3)
FORTNIGHT = timedelta(days=14)


@pytest.fixture
def ledger(tmp_path):
    with Ledger(tmp_path / 'ledger.sqlite3') as book:
        yield book


def add(ledger, repo_id=1, **over):
    state = RepositoryState(
        repository_id=repo_id,
        owner=over.pop('owner', 'o'),
        repo=over.pop('repo', f'r{repo_id}'),
        language=over.pop('language', 'ruby'),
        **over,
    )
    ledger.upsert(state)
    return state


# --- persistence ----------------------------------------------------------

def test_a_repository_round_trips(ledger):
    add(ledger, 1, owner='mikel', repo='mail', language='ruby')
    state = ledger.get(1)
    assert state is not None
    assert (state.owner, state.repo, state.language) == (
        'mikel', 'mail', 'ruby',
    )


def test_unknown_repository_is_none(ledger):
    assert ledger.get(999) is None


def test_upsert_updates_rather_than_duplicating(ledger):
    add(ledger, 1, repo='before')
    add(ledger, 1, repo='after')
    assert ledger.get(1).repo == 'after'
    assert ledger.count() == 1


def test_state_survives_reopening(tmp_path):
    path = tmp_path / 'ledger.sqlite3'
    with Ledger(path) as book:
        add(book, 7, repo='persisted')
    with Ledger(path) as book:
        assert book.get(7).repo == 'persisted'


# --- concurrency ----------------------------------------------------------

def test_a_write_waits_a_minute_for_another_to_finish(ledger):
    """`run` workers failed five passes in a row with "database is
    locked" while `queue track` held the lock longer than the five
    seconds they waited (#98). A claim is cheap, and waiting for the
    lock beats a failed pass."""
    timeout = ledger._db.execute('PRAGMA busy_timeout').fetchone()[0]
    assert timeout == 60_000


# --- change detection -----------------------------------------------------

def test_a_repository_never_collected_is_due(ledger):
    add(ledger, 1)
    assert [s.repository_id for s in ledger.due(Stage.REPO, NOW)] == [1]


def test_a_repository_whose_push_is_newer_than_our_scan_is_due(ledger):
    add(ledger, 1, pushed_at_seen=NOW, stage_watermarks={Stage.SBOM: EARLIER})
    assert ledger.get(1).needs(Stage.SBOM)


def test_a_repository_scanned_after_its_last_push_is_not_due(ledger):
    add(ledger, 1, pushed_at_seen=EARLIER, stage_watermarks={Stage.SBOM: NOW})
    assert not ledger.get(1).needs(Stage.SBOM)


def test_recording_a_stage_advances_its_watermark(ledger):
    add(ledger, 1, pushed_at_seen=EARLIER)
    ledger.record_success(1, Stage.SBOM, NOW)
    assert not ledger.get(1).needs(Stage.SBOM)


def test_stages_advance_independently(ledger):
    add(ledger, 1, pushed_at_seen=EARLIER)
    ledger.record_success(1, Stage.CONTENT, NOW)
    state = ledger.get(1)
    assert not state.needs(Stage.CONTENT)
    assert state.needs(Stage.SBOM)


# --- etags ----------------------------------------------------------------

def test_etag_is_stored_for_conditional_requests(ledger):
    add(ledger, 1)
    ledger.record_etag(1, 'repo', 'W/"abc123"')
    assert ledger.get(1).etags['repo'] == 'W/"abc123"'


def test_etags_are_kept_per_resource(ledger):
    add(ledger, 1)
    ledger.record_etag(1, 'repo', 'e1')
    ledger.record_etag(1, 'releases', 'e2')
    assert ledger.get(1).etags == {'repo': 'e1', 'releases': 'e2'}


def test_observing_an_unchanged_repository_is_recorded_without_work(ledger):
    """A 304 advances the check time but not the collection watermark."""
    add(ledger, 1, pushed_at_seen=EARLIER)
    ledger.record_unchanged(1, NOW)
    state = ledger.get(1)
    assert state.last_checked_at == NOW
    assert state.stage_watermarks.get(Stage.SBOM) is None


# --- backoff --------------------------------------------------------------

def test_a_failure_schedules_a_retry_in_the_future(ledger):
    add(ledger, 1)
    ledger.record_failure(1, Stage.REPO, NOW, 'boom')
    state = ledger.get(1)
    assert state.failure_count == 1
    assert state.next_attempt_at > NOW


def test_backoff_grows_with_consecutive_failures(ledger):
    add(ledger, 1)
    delays = []
    for _ in range(4):
        ledger.record_failure(1, Stage.REPO, NOW, 'boom')
        delays.append(ledger.get(1).next_attempt_at - NOW)
    assert delays == sorted(delays)
    assert delays[0] < delays[-1]


def test_backoff_is_capped(ledger):
    add(ledger, 1)
    for _ in range(40):
        ledger.record_failure(1, Stage.REPO, NOW, 'boom')
    assert ledger.get(1).next_attempt_at - NOW <= timedelta(days=7)


def test_a_repository_in_backoff_is_not_due(ledger):
    add(ledger, 1)
    ledger.record_failure(1, Stage.REPO, NOW, 'boom')
    assert ledger.due(Stage.REPO, NOW) == []


def test_backoff_expires(ledger):
    add(ledger, 1)
    ledger.record_failure(1, Stage.REPO, NOW, 'boom')
    later = NOW + timedelta(days=30)
    assert [s.repository_id for s in ledger.due(Stage.REPO, later)] == [1]


def test_success_clears_the_failure_count(ledger):
    add(ledger, 1)
    ledger.record_failure(1, Stage.REPO, NOW, 'boom')
    ledger.record_success(1, Stage.REPO, NOW)
    state = ledger.get(1)
    assert state.failure_count == 0
    assert state.last_error == ''


#: requests' error for a download that failed: the request's path and
#: query, and the query of a report's download link is its signature.
SIGNED_ERROR = (
    "ConnectionError: HTTPSConnectionPool(host='sbom-exports.example', "
    'port=443): Max retries exceeded with url: '
    '/a.json?X-Amz-Signature=5ec7e75ec7e7 (Caused by ...)'
)


def test_an_error_is_kept_without_the_signature_it_quotes(ledger):
    """`queue status` shows it, to whoever runs it."""
    add(ledger, 1)

    ledger.record_failure(1, Stage.CONTENT, NOW, SIGNED_ERROR)

    kept = ledger.get(1).last_error
    assert '/a.json?***** (Caused by ...)' in kept
    assert '5ec7e75ec7e7' not in kept


def test_a_stage_error_is_kept_without_the_signature_it_quotes(ledger):
    """Where the dependency-graph stage keeps its failures (#57)."""
    add(ledger, 1)

    ledger.record_stage(
        StageState(
            1, Stage.DEPGRAPH, outcome='failed', last_error=SIGNED_ERROR,
        ),
    )

    kept = ledger.stage_state(1, Stage.DEPGRAPH).last_error
    assert '/a.json?***** (Caused by ...)' in kept
    assert '5ec7e75ec7e7' not in kept


# --- absence --------------------------------------------------------------

def test_a_404_defers_without_counting_a_failure(ledger):
    add(ledger, 1)
    ledger.record_failure(1, Stage.REPO, EARLIER, 'boom')
    ledger.record_absent(1, NOW, retry_at=NOW + FORTNIGHT)

    state = ledger.get(1)
    assert state.failure_count == 0, 'a 404 is an answer, not a failure'
    assert state.last_error == ''
    assert state.absent_since == NOW
    assert state.last_checked_at == NOW
    assert ledger.due(Stage.REPO, NOW + FORTNIGHT - timedelta(days=1)) == []
    assert ledger.due(Stage.REPO, NOW + FORTNIGHT)


def test_absence_is_dated_from_the_first_404(ledger):
    add(ledger, 1)
    ledger.record_absent(1, EARLIER, retry_at=NOW)
    ledger.record_absent(1, NOW, retry_at=NOW + FORTNIGHT)
    assert ledger.get(1).absent_since == EARLIER


def test_seeing_the_repository_again_ends_its_absence(ledger):
    add(ledger, 1)
    ledger.record_absent(1, EARLIER, retry_at=NOW)
    ledger.record_unchanged(1, NOW)
    assert ledger.get(1).absent_since is None


#: `repository_state` as ledgers already in use were created, before
#: absence was recorded.
_BEFORE_ABSENCE = """
CREATE TABLE repository_state (
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
"""


def test_a_ledger_from_before_absence_is_migrated_on_open(tmp_path):
    """`CREATE TABLE IF NOT EXISTS` leaves an existing table as it was,
    so a ledger already in use must gain the column when opened, or every
    write to it fails."""
    path = tmp_path / 'old.sqlite3'
    db = sqlite3.connect(path)
    db.executescript(_BEFORE_ABSENCE)
    db.execute(
        'INSERT INTO repository_state (repository_id, owner, repo, etags) '
        'VALUES (?, ?, ?, ?)',
        (1, 'o', 'r', json.dumps({'repo': 'W/"kept"'})),
    )
    db.commit()
    db.close()

    with Ledger(path) as book:
        book.record_absent(1, NOW, retry_at=NOW + FORTNIGHT)
        state = book.get(1)
        assert state.absent_since == NOW
        assert state.etags == {'repo': 'W/"kept"'}, 'existing state is kept'
        assert book.health(NOW).absent == 1


# --- scheduling -----------------------------------------------------------

def test_due_returns_the_stalest_first(ledger):
    add(ledger, 1, last_checked_at=NOW)       # just checked: not due
    add(ledger, 2, last_checked_at=EARLIER)   # stale
    add(ledger, 3)                            # never checked
    assert [s.repository_id for s in ledger.due(Stage.REPO, NOW)] == [3, 2]


def test_due_respects_a_slice_size(ledger):
    for i in range(1, 6):
        add(ledger, i)
    assert len(ledger.due(Stage.REPO, NOW, limit=2)) == 2


def test_due_can_filter_by_language(ledger):
    add(ledger, 1, language='ruby')
    add(ledger, 2, language='go')
    due = ledger.due(Stage.REPO, NOW, language='go')
    assert [s.repository_id for s in due] == [2]


# --- leases ---------------------------------------------------------------

def test_claiming_hides_a_repository_from_another_worker(ledger):
    add(ledger, 1)
    claimed = ledger.claim(Stage.REPO, NOW, limit=5, worker='a')
    assert [s.repository_id for s in claimed] == [1]
    assert ledger.claim(Stage.REPO, NOW, limit=5, worker='b') == []


def test_an_expired_claim_is_reclaimable(ledger):
    add(ledger, 1)
    ledger.claim(
        Stage.REPO, NOW, limit=5, worker='a',
        lease=timedelta(minutes=5),
    )
    later = NOW + timedelta(minutes=10)
    assert ledger.claim(Stage.REPO, later, limit=5, worker='b')


def test_releasing_a_claim_frees_it_immediately(ledger):
    add(ledger, 1)
    ledger.claim(Stage.REPO, NOW, limit=5, worker='a')
    ledger.release(1)
    assert ledger.claim(Stage.REPO, NOW, limit=5, worker='b')


def test_recording_success_releases_the_claim(ledger):
    add(ledger, 1, pushed_at_seen=NOW)
    ledger.claim(Stage.REPO, NOW, limit=5, worker='a')
    ledger.record_success(1, Stage.REPO, NOW)
    assert ledger.get(1).claimed_by == ''


# --- health ---------------------------------------------------------------

def test_health_summarises_the_queue(ledger):
    add(ledger, 1, pushed_at_seen=NOW)
    add(ledger, 2)
    ledger.record_failure(2, Stage.REPO, NOW, 'boom')
    ledger.record_success(1, Stage.REPO, NOW)

    health = ledger.health(NOW)
    assert health.tracked == 2
    assert health.failing == 1
    assert health.due[Stage.REPO] == 0


def test_health_counts_absent_apart_from_failing(ledger):
    add(ledger, 1)
    add(ledger, 2)
    ledger.record_absent(1, NOW, retry_at=NOW + FORTNIGHT)
    ledger.record_failure(2, Stage.REPO, NOW, 'boom')

    health = ledger.health(NOW)
    assert (health.failing, health.absent) == (1, 1)


# --- the change detector must keep polling --------------------------------

def test_a_checked_repository_is_rechecked_after_the_interval(ledger):
    """REPO is the change detector, so it polls on a clock.

    Gating it on `pushed_at_seen` like the derived stages would drain the
    queue and then stop noticing pushes entirely: after one successful
    check, watermark > pushed_at_seen forever.
    """
    add(ledger, 1, pushed_at_seen=EARLIER)
    ledger.record_success(1, Stage.REPO, NOW)

    assert ledger.due(Stage.REPO, NOW) == [], 'just checked'

    later = NOW + timedelta(hours=7)
    assert [s.repository_id for s in ledger.due(Stage.REPO, later)] == [1]


def test_an_unchanged_check_also_counts_as_a_recheck(ledger):
    add(ledger, 1, pushed_at_seen=EARLIER)
    ledger.record_unchanged(1, NOW)

    assert ledger.due(Stage.REPO, NOW) == []
    assert ledger.due(Stage.REPO, NOW + timedelta(hours=7))


def test_the_recheck_interval_is_configurable(ledger):
    add(ledger, 1, pushed_at_seen=EARLIER)
    ledger.record_success(1, Stage.REPO, NOW)

    soon = NOW + timedelta(minutes=30)
    assert ledger.due(Stage.REPO, soon) == []
    assert ledger.due(Stage.REPO, soon, recheck=timedelta(minutes=10))


def test_derived_stages_still_gate_on_the_observed_push(ledger):
    """Only REPO polls. Re-running syft on an unchanged tree is waste."""
    add(ledger, 1, pushed_at_seen=EARLIER)
    ledger.record_success(1, Stage.SBOM, NOW)

    far_future = NOW + timedelta(days=90)
    assert ledger.due(Stage.SBOM, far_future) == []


def test_a_new_push_makes_derived_stages_due_again(ledger):
    add(ledger, 1, pushed_at_seen=EARLIER)
    ledger.record_success(1, Stage.SBOM, NOW)
    ledger.record_push(1, NOW + timedelta(days=1), NOW + timedelta(days=1))

    assert ledger.due(Stage.SBOM, NOW + timedelta(days=1))
