"""The dependency-graph stage's scheduling: what each answer records, and
what is due when.

The end-to-end behaviour, over a faked GitHub, is in
`depgraph_command_test.py`; this is the policy itself.
"""
from __future__ import annotations

from datetime import date
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import pytest

from chatsbom.core.conditional import ConditionalResult
from chatsbom.core.conditional import RateLimit
from chatsbom.core.github import depgraph_tokens
from chatsbom.core.github import token_label
from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import Stage
from chatsbom.core.ledger import StageState
from chatsbom.services.dependency_graph_service import closed_reason
from chatsbom.services.dependency_graph_service import (
    DEPGRAPH_ENDPOINT_CLOSES,
)
from chatsbom.services.depgraph_stage import DEPGRAPH_VERSION
from chatsbom.services.depgraph_stage import next_state
from chatsbom.services.depgraph_stage import Pacer
from chatsbom.services.depgraph_stage import TOO_LARGE_AFTER
from chatsbom.services.run_service import RunService
from chatsbom.services.run_service import STAGES

NOW = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)

DOCUMENT = ConditionalResult(status=200, payload={'sbom': {}})
NO_GRAPH = ConditionalResult(status=404)
SERVER_ERROR = ConditionalResult(status=500, error='HTTP 500')
TIMED_OUT = ConditionalResult(status=0, error='RetryError: too many 500s')
BAD_REQUEST = ConditionalResult(status=422, error='HTTP 422')
PENDING = ConditionalResult(status=202, pending=True)
REFUSED = ConditionalResult(
    status=429, rate_limit=RateLimit(remaining=0, reset=1),
)


def _fresh() -> StageState:
    return StageState(repository_id=1, stage=Stage.DEPGRAPH)


def _after(*answers: ConditionalResult) -> StageState:
    state = _fresh()
    for answer in answers:
        following = next_state(state, answer, NOW)
        assert following is not None
        state = following
    return state


# --- what each answer records ---------------------------------------------------

def test_a_document_is_due_again_in_30_days():
    state = next_state(_fresh(), DOCUMENT, NOW, output_key='abc')

    assert state is not None
    assert state.outcome == 'ok'
    assert state.done_at == NOW
    assert state.next_attempt_at == NOW + timedelta(days=30)
    assert state.output_key == 'abc'
    assert state.stage_version == DEPGRAPH_VERSION
    assert state.http_status == 200


def test_no_graph_is_cached_for_30_days_then_60_then_90():
    delays = [
        _after(*[NO_GRAPH] * n).next_attempt_at - NOW for n in (1, 2, 3, 4)
    ]

    assert delays == [
        timedelta(days=30), timedelta(days=60),
        timedelta(days=90), timedelta(days=90),
    ]
    assert _after(NO_GRAPH).outcome == 'absent'


def test_a_document_after_no_graph_starts_the_cache_over():
    state = _after(NO_GRAPH, NO_GRAPH, DOCUMENT, NO_GRAPH)

    assert state.next_attempt_at - NOW == timedelta(days=30)


@pytest.mark.parametrize('answer', [SERVER_ERROR, TIMED_OUT])
def test_server_errors_back_off_doubling(answer):
    delays = [_after(*[answer] * n).next_attempt_at - NOW for n in (1, 2, 3)]

    assert delays == [
        timedelta(minutes=15), timedelta(minutes=30), timedelta(minutes=60),
    ]
    assert _after(answer).outcome == 'failed'


@pytest.mark.parametrize('answer', [SERVER_ERROR, TIMED_OUT])
def test_repeated_server_errors_are_too_large_and_asked_monthly(answer):
    """spring-boot answers 500 "Request timed out" every time: after
    enough in a row it is asked once a month, not every few hours."""
    state = _after(*[answer] * TOO_LARGE_AFTER)

    assert state.outcome == 'too_large'
    assert state.next_attempt_at - NOW == timedelta(days=30)
    assert state.last_error

    again = _after(*[answer] * (TOO_LARGE_AFTER + 3))
    assert again.outcome == 'too_large'
    assert again.next_attempt_at - NOW == timedelta(days=30)


def test_other_failures_back_off_but_are_never_too_large():
    state = _after(*[BAD_REQUEST] * 20)

    assert state.outcome == 'failed'
    assert state.next_attempt_at - NOW == timedelta(days=30), 'capped'


def test_a_pending_report_is_looked_at_again_soon_and_is_no_failure():
    state = _after(SERVER_ERROR, PENDING)

    assert state.outcome == 'pending'
    assert state.next_attempt_at - NOW == timedelta(minutes=15)
    assert state.failure_count == 1, 'unchanged'


def test_a_refused_token_records_nothing():
    assert next_state(_fresh(), REFUSED, NOW) is None


def test_a_document_keeps_the_last_success_through_a_failure():
    state = _after(DOCUMENT, SERVER_ERROR)

    assert state.done_at == NOW
    assert state.outcome == 'failed'


# --- pacing ---------------------------------------------------------------------------

class Clock:
    def __init__(self) -> None:
        self.now = 0.0
        self.slept: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


def test_the_first_request_is_sent_at_once_and_the_rest_spaced():
    clock = Clock()
    pacer = Pacer(90, sleep=clock.sleep, monotonic=clock.monotonic)

    for _ in range(3):
        pacer.wait()
        pacer.spent(1)

    assert clock.slept == [40.0, 40.0]


def test_a_fetch_that_cost_two_requests_waits_two_intervals():
    clock = Clock()
    pacer = Pacer(3600, sleep=clock.sleep, monotonic=clock.monotonic)

    pacer.wait()
    pacer.spent(2)
    pacer.wait()

    assert clock.slept == [2.0]


def test_time_already_spent_is_not_waited_again():
    clock = Clock()
    pacer = Pacer(3600, sleep=clock.sleep, monotonic=clock.monotonic)
    pacer.wait()
    pacer.spent(1)
    clock.now += 5

    pacer.wait()

    assert clock.slept == []


def test_a_rate_must_be_positive():
    with pytest.raises(ValueError):
        Pacer(0)


# --- closing ----------------------------------------------------------------------

def test_the_closing_day_is_the_endpoints_removal_date():
    assert DEPGRAPH_ENDPOINT_CLOSES == date(2026, 11, 13)


@pytest.mark.parametrize('setting', [None, '', 'auto', 'async', 'sync'])
def test_the_stage_is_open_before_the_endpoint_closes(setting):
    assert closed_reason(setting, date(2026, 11, 12)) is None


@pytest.mark.parametrize('setting', [None, 'auto', 'async', 'AUTO'])
def test_reports_keep_the_stage_open_after_it_closes(setting):
    """#50: `auto` switches to GitHub's asynchronous report that day."""
    assert closed_reason(setting, date(2026, 11, 13)) is None


def test_sync_closes_the_stage_with_the_endpoint():
    reason = closed_reason('sync', date(2026, 11, 13))

    assert reason is not None and '2026-11-13' in reason


@pytest.mark.parametrize('day', [date(2026, 9, 28), date(2027, 1, 1)])
def test_off_closes_the_stage_on_any_day(day):
    assert closed_reason('off', day) == 'CHATSBOM_DEPGRAPH_API=off'


# --- tokens -----------------------------------------------------------------------

def test_tokens_come_from_the_primary_then_the_list_each_once():
    assert depgraph_tokens('one', 'two, three\nfour  one,,') == [
        'one', 'two', 'three', 'four',
    ]


def test_without_a_list_there_is_the_primary_alone():
    assert depgraph_tokens('one', None) == ['one']
    assert depgraph_tokens('one', '') == ['one']


def test_a_token_is_named_by_position_never_by_value():
    assert token_label(2, 'octocat') == 'token 2 (octocat)'
    assert token_label(1, None) == 'token 1'


# --- the ledger ---------------------------------------------------------------------

@pytest.fixture
def ledger(tmp_path):
    with Ledger(tmp_path / 'ledger.sqlite3') as handle:
        yield handle


def _names(work) -> list[str]:
    return [item.repo for item in work]


def test_the_never_asked_come_first_then_refreshes_then_expired_caches(
    ledger,
):
    for repository_id, name in enumerate(
        ('refresh', 'expired', 'never', 'failed'), start=1,
    ):
        ledger.track(repository_id, 'o', name, 'java')
    old = NOW - timedelta(days=31)
    ledger.record_stage(
        StageState(
            1, Stage.DEPGRAPH, outcome='ok', done_at=old,
            next_attempt_at=NOW - timedelta(days=1),
        ),
    )
    ledger.record_stage(
        StageState(
            2, Stage.DEPGRAPH, outcome='absent',
            next_attempt_at=NOW - timedelta(days=1),
        ),
    )
    ledger.record_stage(
        StageState(
            4, Stage.DEPGRAPH, outcome='failed', failure_count=1,
            next_attempt_at=NOW - timedelta(minutes=1),
        ),
    )

    claimed = ledger.claim_stage(Stage.DEPGRAPH, NOW, None, 'w')

    assert _names(claimed) == ['never', 'failed', 'refresh', 'expired']


def test_a_backoff_or_negative_cache_still_running_is_not_due(ledger):
    ledger.track(1, 'o', 'a', 'java')
    ledger.track(2, 'o', 'b', 'java')
    for repository_id, outcome in ((1, 'absent'), (2, 'failed')):
        ledger.record_stage(
            StageState(
                repository_id, Stage.DEPGRAPH, outcome=outcome,
                next_attempt_at=NOW + timedelta(minutes=1),
            ),
        )

    assert ledger.claim_stage(Stage.DEPGRAPH, NOW, None, 'w') == []
    assert ledger.count_due_for_stage(Stage.DEPGRAPH, NOW) == {}


def test_a_claim_is_leased_per_stage_not_per_repository(ledger):
    """A depgraph worker and the repository-major walk can hold the same
    repository at once: their leases are separate."""
    ledger.track(1, 'o', 'a', 'java')
    ledger.record_push(1, NOW - timedelta(days=1), NOW)

    [work] = ledger.claim_stage(Stage.DEPGRAPH, NOW, 1, 'depgraph')

    assert ledger.claim_stage(Stage.DEPGRAPH, NOW, 1, 'other') == []
    assert ledger.claim(Stage.RELEASE, NOW, 1, 'run'), 'not held by it'
    assert work.state.claimed_by == 'depgraph'


def test_an_expired_lease_is_claimable_again(ledger):
    ledger.track(1, 'o', 'a', 'java')
    ledger.claim_stage(
        Stage.DEPGRAPH, NOW, 1, 'dead', lease=timedelta(minutes=5),
    )

    later = NOW + timedelta(minutes=6)
    assert _names(ledger.claim_stage(Stage.DEPGRAPH, later, 1, 'w')) == ['a']


def test_a_released_claim_is_due_again(ledger):
    ledger.track(1, 'o', 'a', 'java')
    ledger.claim_stage(Stage.DEPGRAPH, NOW, 1, 'w')

    ledger.release_stage(1, Stage.DEPGRAPH)

    assert _names(ledger.claim_stage(Stage.DEPGRAPH, NOW, 1, 'w')) == ['a']
    assert ledger.count_due_for_stage(Stage.DEPGRAPH, NOW) == {}


def test_the_due_counts_say_why(ledger):
    ledger.track(1, 'o', 'a', 'java')
    ledger.track(2, 'o', 'b', 'java')
    state = ledger.get(2)
    assert state is not None
    state.stage_watermarks[Stage.DEPGRAPH] = NOW - timedelta(days=40)
    ledger.upsert(state)

    assert ledger.count_due_for_stage(Stage.DEPGRAPH, NOW) == {
        'never': 1, 'refresh': 1,
    }


def test_an_outcome_is_recorded_and_the_lease_dropped(ledger):
    ledger.track(1, 'o', 'a', 'java')
    [work] = ledger.claim_stage(Stage.DEPGRAPH, NOW, 1, 'w')
    state = next_state(work.state, NO_GRAPH, NOW)
    assert state is not None

    ledger.record_stage(state)

    stored = ledger.stage_state(1, Stage.DEPGRAPH)
    assert stored is not None
    assert stored.outcome == 'absent' and stored.claimed_by == ''
    assert ledger.stage_outcomes(Stage.DEPGRAPH) == {'absent': 1}


# --- seeding, and the walk that leaves the seeded alone ------------------------------

def test_seeding_tracks_a_new_repository_with_no_language(ledger):
    assert ledger.seed(
        7, 'o', 'x', snapshot='all-2026-03-09', github_language='C++',
        stars=1500, default_branch='main',
    )

    state = ledger.get(7)
    assert state is not None and state.language == ''
    row = ledger._db.execute(
        'SELECT snapshot, github_language, stars, default_branch '
        'FROM repository_state WHERE repository_id = 7',
    ).fetchone()
    assert tuple(row) == ('all-2026-03-09', 'C++', 1500, 'main')


def test_seeding_keeps_what_the_queue_knows(ledger):
    """The name `queue sync` saw and the list it was tracked from are
    newer than a snapshot taken months ago."""
    ledger.track(7, 'renamed', 'repo', 'java')
    ledger.record_push(7, NOW, NOW)

    assert not ledger.seed(
        7, 'old', 'name', snapshot='all-2026-03-09', stars=10,
    )

    state = ledger.get(7)
    assert state is not None
    assert (state.owner, state.repo, state.language) == (
        'renamed', 'repo', 'java',
    )
    assert state.pushed_at_seen == NOW


def test_the_repository_walk_leaves_the_seeded_alone(ledger):
    """It keys its paths by language, and they have none."""
    ledger.track(1, 'o', 'keyed', 'java')
    ledger.seed(2, 'o', 'seeded', snapshot='all-2026-03-09')
    walked: list[str] = []

    def runner(repository, carried):
        walked.append(repository.repo)
        return {}

    RunService(ledger, {STAGES[0]: runner}, lambda: 0).advance(
        NOW, limit=10, quota_budget=100,
    )

    assert walked == ['keyed']


def test_the_walk_has_no_depgraph_stage():
    """It is its own stage now, scheduled from `stage_state`."""
    assert Stage.DEPGRAPH not in STAGES
