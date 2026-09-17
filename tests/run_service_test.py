"""What `chatsbom run` schedules, not what a stage does.

The stage callables are injected, so these drive the whole loop with no
GitHub token and no network. That is deliberate: the part worth testing
is which stages run, what gets recorded, and when a pass stops — and
none of that depends on what a stage does with the repository.
"""
from __future__ import annotations

from datetime import datetime
from datetime import timedelta
from datetime import timezone

import pytest

from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import Stage
from chatsbom.services.run_service import content_path
from chatsbom.services.run_service import RunService
from chatsbom.services.run_service import STAGES

NOW = datetime(2026, 9, 17, 12, 0, tzinfo=timezone.utc)
PUSHED = NOW - timedelta(days=1)


@pytest.fixture
def ledger(tmp_path):
    with Ledger(tmp_path / 'ledger.sqlite3') as handle:
        yield handle


def _track(ledger, repository_id=1, pushed=PUSHED):
    ledger.track(repository_id, 'mikel', 'mail', 'ruby')
    if pushed is not None:
        ledger.record_push(repository_id, pushed, NOW)
    return repository_id


class Runners:
    """Stage callables that record what they were asked to do."""

    def __init__(self, produces=None, fails=(), returns_none=()):
        self.calls: list[Stage] = []
        self._produces = produces or {}
        self._fails = set(fails)
        self._none = set(returns_none)
        self.requests = 0

    def table(self):
        return {stage: self._for(stage) for stage in STAGES}

    def _for(self, stage):
        def run(repository, carried):
            self.calls.append(stage)
            self.requests += 1
            if stage in self._fails:
                raise RuntimeError(f'{stage} exploded')
            if stage in self._none:
                return None
            return self._produces.get(stage, {})
        return run


def _service(ledger, runners):
    return RunService(ledger, runners.table(), lambda: runners.requests)


def test_a_pushed_repository_runs_every_stage(ledger):
    """Every derived stage is behind a push it has not seen."""
    _track(ledger)
    runners = Runners()
    result = _service(ledger, runners).advance(NOW, limit=10, quota_budget=100)

    assert result.repositories == 1
    assert runners.calls == list(STAGES)
    assert result.stages_run == len(STAGES)
    assert result.failed == 0


def test_the_chain_is_walked_even_where_it_is_not_due(ledger):
    """A stage needs what the stage before it produced.

    Only `sbom` is behind here, and it cannot run without the directory
    `content` wrote — so the earlier stages are walked (their own caches
    make that nearly free) but not recorded as work.
    """
    repository_id = _track(ledger)
    for stage in STAGES:
        if stage is not Stage.SBOM:
            ledger.record_success(repository_id, stage, NOW)

    runners = Runners()
    result = _service(ledger, runners).advance(NOW, limit=10, quota_budget=100)

    assert runners.calls == list(STAGES), 'the whole chain is walked'
    assert result.completed == {'sbom': 1}, 'only the due stage is recorded'


def test_a_repository_due_for_four_stages_is_one_unit_of_work(ledger):
    """Claimed per stage, collapsed by repository.

    Without the collapse the chain is walked once per due stage and the
    quota pays for it four times.
    """
    _track(ledger)
    runners = Runners()
    result = _service(ledger, runners).advance(NOW, limit=10, quota_budget=100)

    assert result.repositories == 1
    assert runners.calls.count(Stage.RELEASE) == 1


def test_an_unpushed_repository_is_left_alone(ledger):
    """The whole point of the ledger: 74.7% of repositories are not
    pushed in a given week, and re-collecting them produces identical
    results."""
    repository_id = _track(ledger)
    for stage in STAGES:
        ledger.record_success(repository_id, stage, NOW)

    runners = Runners()
    result = _service(ledger, runners).advance(NOW, limit=10, quota_budget=100)

    assert result.repositories == 0
    assert runners.calls == []


def test_a_failing_stage_stops_that_repository_not_the_pass(ledger):
    """The stages after it need its output, and running them against a
    missing input produces rows that look collected."""
    _track(ledger, 1)
    _track(ledger, 2)
    runners = Runners(fails={Stage.COMMIT})
    result = _service(ledger, runners).advance(NOW, limit=10, quota_budget=100)

    assert result.repositories == 2
    assert result.failed == 2, 'both repositories failed at the same stage'
    assert Stage.TREE not in runners.calls, 'nothing ran after the failure'


def test_a_failure_is_recorded_so_it_backs_off(ledger):
    """A repository that keeps failing must stop costing a slot."""
    repository_id = _track(ledger)
    runners = Runners(fails={Stage.RELEASE})
    _service(ledger, runners).advance(NOW, limit=10, quota_budget=100)

    state = ledger.get(repository_id)
    assert state is not None
    assert state.failure_count == 1
    assert state.next_attempt_at is not None
    assert 'exploded' in state.last_error


def test_a_stage_with_nothing_to_do_is_not_a_failure(ledger):
    """A repository with no releases has nothing for `release` to do,
    and backing it off for that would punish it for working."""
    repository_id = _track(ledger)
    runners = Runners(returns_none={Stage.RELEASE})
    result = _service(ledger, runners).advance(NOW, limit=10, quota_budget=100)

    assert result.failed == 0
    state = ledger.get(repository_id)
    assert state is not None and state.failure_count == 0
    assert 'release' not in result.completed, 'no output, no watermark'
    assert result.completed['commit'] == 1, 'the chain continued'


def test_the_quota_stops_the_pass_between_repositories(ledger):
    """Never mid-repository: a half-collected repository whose
    watermarks say it is done is worse than one plainly not done."""
    for repository_id in range(1, 6):
        _track(ledger, repository_id)
    runners = Runners()
    result = _service(ledger, runners).advance(NOW, limit=10, quota_budget=7)

    assert result.stopped_early
    assert result.repositories < 5
    # Whole repositories only: six stages each, so the calls divide.
    assert len(runners.calls) % len(STAGES) == 0


def test_a_repository_the_quota_skipped_is_released_not_leased(ledger):
    """It would expire eventually, but the next pass should see it now."""
    _track(ledger, 1)
    _track(ledger, 2)
    runners = Runners()
    _service(ledger, runners).advance(NOW, limit=10, quota_budget=1)

    unclaimed = [s for s in ledger.all() if not s.claimed_by]
    assert len(unclaimed) == 2, 'both released, whether run or skipped'


def test_the_claim_is_released_even_when_a_stage_raises(ledger):
    """Otherwise a crash strands the repository until the lease expires."""
    repository_id = _track(ledger)
    runners = Runners(fails={Stage.RELEASE})
    _service(ledger, runners).advance(NOW, limit=10, quota_budget=100)

    state = ledger.get(repository_id)
    assert state is not None and not state.claimed_by


def test_the_lock_stage_is_not_in_the_loop(ledger):
    """Generating a lockfile runs a package manager over untrusted
    source, so it belongs in a container and not in a loop holding a
    GitHub token."""
    assert Stage.LOCK not in STAGES


def test_the_repo_stage_is_not_in_the_loop(ledger):
    """`queue sync` owns it — that is the conditional request whose 304
    is free. Doing it here too would spend rate limit to learn what sync
    already knows."""
    assert Stage.REPO not in STAGES


def test_the_content_path_matches_what_content_service_writes():
    """This derivation is what makes the 5.2 GB of JSONL ledgers
    unnecessary to the worker, so it has to agree with the writer."""
    from pathlib import Path
    assert content_path(
        Path('data/06-github-content'), 'ruby', 'mikel', 'mail',
        'v3.2.0', 'abc123',
    ) == Path('data/06-github-content/ruby/mikel/mail/v3.2.0/abc123')
