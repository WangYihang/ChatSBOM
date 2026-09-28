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
    """A repository that keeps failing must stop costing a slot — in the
    stage that failed, and only there."""
    repository_id = _track(ledger)
    runners = Runners(fails={Stage.RELEASE})
    _service(ledger, runners).advance(NOW, limit=10, quota_budget=100)

    state = ledger.stage_state(repository_id, Stage.RELEASE)
    assert state is not None
    assert state.outcome == 'failed'
    assert state.failure_count == 1
    assert state.next_attempt_at is not None
    assert 'exploded' in state.last_error
    repository = ledger.get(repository_id)
    assert repository is not None and repository.failure_count == 0, (
        'the repository itself is not backed off'
    )


def test_a_stage_with_nothing_to_do_is_not_a_failure(ledger):
    """A repository with no releases has nothing for `release` to do,
    and backing it off for that would punish it for working."""
    repository_id = _track(ledger)
    runners = Runners(returns_none={Stage.RELEASE})
    result = _service(ledger, runners).advance(NOW, limit=10, quota_budget=100)

    assert result.failed == 0
    state = ledger.stage_state(repository_id, Stage.RELEASE)
    assert state is None or state.failure_count == 0
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

    for repository_id in (1, 2):
        for stage in STAGES:
            state = ledger.stage_state(repository_id, stage)
            assert state is None or not state.claimed_by, (
                'both released, whether run or skipped'
            )


def test_the_claim_is_released_even_when_a_stage_raises(ledger):
    """Otherwise a crash strands the repository until the lease expires."""
    repository_id = _track(ledger)
    runners = Runners(fails={Stage.RELEASE})
    _service(ledger, runners).advance(NOW, limit=10, quota_budget=100)

    for stage in STAGES:
        state = ledger.stage_state(repository_id, stage)
        assert state is None or not state.claimed_by


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


def test_the_content_root_is_keyed_by_repository_and_commit():
    """A pure function of the repository and its commit, so `sbom` can
    run alone: nothing needs handing over from `content`."""
    from pathlib import Path

    from chatsbom.core.config import PathConfig
    paths = PathConfig(base_data_dir=Path('data'))
    assert paths.content_root(42, 'abc123') == Path(
        'data/06-github-content/42/abc123',
    )


class TestRememberingTheRecord:
    """The record is kept once per repository, at the end of the chain.

    The stage ledgers each append their own copy of the whole record to
    carry it to the next stage, which is why the release list sat on
    disk four times and 21 of the 22 GB of ledgers was that repetition.
    Writing it here is what lets those ledgers keep only a line saying
    which repository reached which stage.
    """

    def test_one_record_per_repository_not_one_per_stage(self, ledger):
        """Six stages run; six records would be five states nothing
        wants to read."""
        _track(ledger, 1)
        _track(ledger, 2)
        kept = []
        runners = Runners()
        RunService(
            ledger, runners.table(), lambda: runners.requests,
            remember=kept.append,
        ).advance(NOW, limit=10, quota_budget=100)

        assert len(kept) == 2
        assert {r['id'] for r in kept} == {1, 2}

    def test_the_record_carries_what_the_stages_produced(self, ledger):
        """It is written after the chain, so it has the whole chain's
        output — that is the point of writing it there."""
        _track(ledger)
        runners = Runners(
            produces={
                Stage.COMMIT: {
                    'download_target': {
                        'ref': 'v1', 'ref_type': 'tag',
                        'commit_sha': 'abc',
                        'commit_sha_short': 'abc',
                    },
                },
                Stage.CONTENT: {'local_content_path': 'data/06/ruby/mikel/mail'},
                Stage.SBOM: {'sbom_path': 'data/07/ruby/mikel/mail/sbom.json'},
            },
        )
        kept = []
        RunService(
            ledger, runners.table(), lambda: runners.requests,
            remember=kept.append,
        ).advance(NOW, limit=10, quota_budget=100)

        assert len(kept) == 1
        record = kept[0]
        assert record['local_content_path'].endswith('mikel/mail')
        assert record['sbom_path'].endswith('sbom.json')
        assert record['download_target']['commit_sha'] == 'abc'

    def test_a_repository_that_failed_is_not_recorded(self, ledger):
        """Its record is half-collected, and storing it would make the
        landing zone claim a state the repository never reached."""
        _track(ledger)
        runners = Runners(fails={Stage.COMMIT})
        kept = []
        RunService(
            ledger, runners.table(), lambda: runners.requests,
            remember=kept.append,
        ).advance(NOW, limit=10, quota_budget=100)

        assert kept == []

    def test_without_a_store_the_pass_still_runs(self, ledger):
        """Collecting must not require a database."""
        _track(ledger)
        runners = Runners()
        result = _service(ledger, runners).advance(
            NOW, limit=10, quota_budget=100,
        )
        assert result.repositories == 1
        assert result.remembered == 0


# --- the repository the walk starts from (#55 pilot) -------------------------

class Seen:
    """Stage callables that keep the repository each stage was handed."""

    def __init__(self, commit_branch: str = '') -> None:
        self.handed: dict[Stage, object] = {}
        self.requests = 0
        self._branch = commit_branch
        self.remembered: list[dict] = []

    def table(self):
        return {stage: self._for(stage) for stage in STAGES}

    def _for(self, stage):
        def run(repository, carried):
            self.handed[stage] = repository.model_copy()
            if stage is Stage.COMMIT and self._branch:
                return {'default_branch': self._branch}
            return {}
        return run


def _seeded(ledger, default_branch='master', stars=4241):
    ledger.seed(
        15648899, 'aporter', 'coursera-android', snapshot='all-2026-09-28',
        github_language='Java', stars=stars, default_branch=default_branch,
        pushed_at=PUSHED,
    )
    return 15648899


def test_the_walk_starts_from_what_the_ledger_knows(ledger):
    """The repository was built from four columns, so the model filled in
    the rest: `default_branch = 'main'` sent the commit stage after a
    branch aporter/coursera-android does not have, and the record filed
    stars 0 and no URL for every repository with no metadata document."""
    _seeded(ledger)
    seen = Seen()
    remembered: list[dict] = []
    RunService(
        ledger, seen.table(), lambda: 0, remember=remembered.append,
    ).advance(NOW, limit=10, quota_budget=100)

    handed = seen.handed[Stage.RELEASE]
    assert handed.default_branch == 'master'
    assert handed.stars == 4241
    assert handed.url == 'https://github.com/aporter/coursera-android'
    [record] = remembered
    assert record['default_branch'] == 'master'
    assert record['stars'] == 4241
    assert record['url'] == 'https://github.com/aporter/coursera-android'


def test_with_no_branch_in_the_ledger_none_is_guessed(ledger):
    """20 ledger rows have no default branch (tracked with no snapshot):
    the repository says so, rather than `'main'`."""
    _seeded(ledger, default_branch='', stars=None)
    seen = Seen()
    _service(ledger, seen).advance(NOW, limit=10, quota_budget=100)

    assert seen.handed[Stage.RELEASE].default_branch == ''
    assert seen.handed[Stage.RELEASE].stars == 0


def test_the_branch_the_commit_stage_heard_is_kept_in_the_ledger(ledger):
    """`ls-remote --symref` says which branch HEAD is. The ledger keeps
    it for the depgraph stamp and `db index`: over none, and over a
    snapshot's name gone stale."""
    empty = _seeded(ledger, default_branch='')
    _service(ledger, Seen(commit_branch='master')).advance(
        NOW, limit=10, quota_budget=100,
    )
    assert ledger.get(empty).default_branch == 'master'


def test_a_renamed_default_branch_replaces_the_snapshots(ledger):
    stale = _seeded(ledger, default_branch='master')
    _service(ledger, Seen(commit_branch='main')).advance(
        NOW, limit=10, quota_budget=100,
    )
    assert ledger.get(stale).default_branch == 'main'
