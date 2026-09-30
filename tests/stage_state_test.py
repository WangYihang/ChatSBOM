"""Every derived stage scheduled from its own `stage_state` row (#55, §4.1).

A stage is due when its row is missing, was written by an older
`STAGE_VERSION`, failed and its backoff ran out, or consumed something
other than what its upstream produced now. Leases and backoff are per
stage, so one stage failing never holds up another.

Adopting the old watermarks must change nothing that is scheduled: the
same repositories are due, and the same ones are not, as under the
push rule the watermarks were read by.
"""
from __future__ import annotations

import itertools
import json
from datetime import datetime
from datetime import timedelta
from datetime import timezone

import pytest

from chatsbom.core import ledger as ledger_module
from chatsbom.core.ledger import DERIVED_STAGES
from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import Stage
from chatsbom.core.ledger import STALE_INPUT
from chatsbom.services.run_service import RunService
from chatsbom.services.run_service import STAGES

NOW = datetime(2026, 9, 28, 12, 0, tzinfo=timezone.utc)
PUSHED = NOW - timedelta(days=2)


@pytest.fixture
def ledger(tmp_path):
    with Ledger(tmp_path / 'ledger.sqlite3') as handle:
        yield handle


def _track(ledger, repository_id=1, pushed=PUSHED, language='ruby'):
    ledger.track(repository_id, 'o', f'r{repository_id}', language)
    if pushed is not None:
        ledger.record_push(repository_id, pushed, NOW)
    return repository_id


def _due(ledger, stage, now=NOW):
    return set(ledger._due_ids(stage, now))


class Runners:
    def __init__(self, fails=(), produces=None):
        self.calls: list[Stage] = []
        self.fails = set(fails)
        self.produces = produces or {}
        self.requests = 0

    def table(self):
        return {stage: self._for(stage) for stage in STAGES}

    def _for(self, stage):
        def run(repository, carried):
            self.calls.append(stage)
            self.requests += 1
            if stage in self.fails:
                raise RuntimeError(f'{stage} exploded')
            return dict(self.produces.get(stage, {}))
        return run


def _service(ledger, runners):
    return RunService(ledger, runners.table(), lambda: runners.requests)


class TestDue:

    def test_a_stage_never_run_is_due(self, ledger):
        _track(ledger)
        for stage in DERIVED_STAGES:
            assert _due(ledger, stage) == {1}

    def test_a_stage_that_consumed_what_its_upstream_produced_is_not(self, ledger):
        _track(ledger)
        _service(ledger, Runners()).advance(NOW, limit=10, quota_budget=100)
        for stage in STAGES:
            assert _due(ledger, stage) == set(), stage

    def test_a_push_makes_the_chain_due_through_its_first_stage(self, ledger):
        _track(ledger)
        _service(ledger, Runners()).advance(NOW, limit=10, quota_budget=100)
        ledger.record_push(1, NOW, NOW)
        assert _due(ledger, Stage.RELEASE) == {1}
        assert _due(ledger, Stage.TREE) == set(), (
            'only what consumed the push; the rest follow what it produces'
        )

    def test_a_new_upstream_output_makes_its_downstream_due(self, ledger):
        """A release re-run that picks another tag makes COMMIT due,
        without any push."""
        _track(ledger)
        _service(ledger, Runners()).advance(NOW, limit=10, quota_budget=100)
        ledger.record_stage_success(1, Stage.RELEASE, NOW, 'x', 'v2.0.0')
        assert _due(ledger, Stage.COMMIT) == {1}
        assert _due(ledger, Stage.TREE) == set()

    def test_a_new_stage_version_makes_every_row_due(self, ledger, monkeypatch):
        """What the push rule could not express: a change to what a stage
        does reaches the corpus with no push and no manual reset."""
        for repository_id in (1, 2, 3):
            _track(ledger, repository_id)
        _service(ledger, Runners()).advance(NOW, limit=10, quota_budget=100)
        assert _due(ledger, Stage.CONTENT) == set()
        monkeypatch.setitem(
            ledger_module.STAGE_VERSION, Stage.CONTENT,
            ledger_module.STAGE_VERSION[Stage.CONTENT] + 1,
        )
        assert _due(ledger, Stage.CONTENT) == {1, 2, 3}
        assert _due(ledger, Stage.SBOM) == set()

    def test_the_stage_versions(self):
        """Manifests discovered from the tree (PR C of #55): every content
        root is due to be filled out, and resolved and scanned again.
        Release histories that counted branches as tags (PR F): every
        release is chosen again. Commit and tree follow by input key.
        Podspecs and `buildSrc` sources discovered too (#55 pilot): every
        content root is filled out again."""
        assert {
            stage: ledger_module.STAGE_VERSION[stage] for stage in DERIVED_STAGES
        } == {
            Stage.RELEASE: 2, Stage.COMMIT: 1, Stage.TREE: 1,
            Stage.CONTENT: 3, Stage.LOCK: 2, Stage.SBOM: 2,
        }

    def test_a_deferred_repository_is_not_due(self, ledger):
        """`queue sync`'s backoff and a 404 still hold the repository."""
        _track(ledger)
        ledger.record_absent(1, NOW, retry_at=NOW + timedelta(days=7))
        assert _due(ledger, Stage.RELEASE) == set()


class TestAdoptingTheWatermarks:
    """The push rule, one last time: due iff never run, or run before the
    newest push. Adoption must agree with it on every combination."""

    COMBINATIONS = list(
        itertools.product(
            (None, PUSHED - timedelta(days=1), PUSHED + timedelta(hours=1)),
            repeat=len(DERIVED_STAGES),
        ),
    )

    def test_every_combination_is_due_exactly_as_before(
        self, tmp_path, monkeypatch,
    ):
        # At the versions the watermarks were written by. A version
        # bumped since (content, lock and SBOM, for discovery) makes
        # every adopted row of that stage due, which is its purpose and
        # not the push rule.
        for stage in DERIVED_STAGES:
            monkeypatch.setitem(ledger_module.STAGE_VERSION, stage, 1)
        path = tmp_path / 'ledger.sqlite3'
        expected: dict[Stage, set[int]] = {s: set() for s in DERIVED_STAGES}
        with Ledger(path) as ledger:
            for repository_id, marks in enumerate(self.COMBINATIONS, start=1):
                for pushed in (PUSHED, None):
                    rid = repository_id * 2 + (pushed is None)
                    ledger.track(rid, 'o', f'r{rid}', 'ruby')
                    if pushed is not None:
                        ledger.record_push(rid, pushed, NOW)
                    watermarks = {
                        str(stage): mark.isoformat()
                        for stage, mark in zip(DERIVED_STAGES, marks)
                        if mark is not None
                    }
                    # Written as a ledger from before `stage_state`.
                    ledger._db.execute(
                        'UPDATE repository_state SET stage_watermarks = ? '
                        'WHERE repository_id = ?',
                        (json.dumps(watermarks), rid),
                    )
                    state = ledger.get(rid)
                    assert state is not None
                    for stage in DERIVED_STAGES:
                        if state.needs(stage, NOW):
                            expected[stage].add(rid)
            ledger._db.execute('DELETE FROM stage_state')
        # Opening it adopts.
        with Ledger(path) as ledger:
            for stage in DERIVED_STAGES:
                assert _due(ledger, stage) == expected[stage], stage
            assert ledger.adopt_watermarks() == 0, 'idempotent'

    def test_an_overtaken_watermark_is_adopted_as_stale(self, tmp_path):
        path = tmp_path / 'ledger.sqlite3'
        with Ledger(path) as ledger:
            _track(ledger)
            ledger._db.execute(
                'UPDATE repository_state SET stage_watermarks = ?',
                (json.dumps({'release': (PUSHED - timedelta(1)).isoformat()}),),
            )
        with Ledger(path) as ledger:
            state = ledger.stage_state(1, Stage.RELEASE)
            assert state is not None
            assert state.input_key == STALE_INPUT
            assert state.outcome == 'ok' and state.stage_version == 1

    def test_a_graph_watermark_is_due_for_its_refresh_as_before(self, tmp_path):
        path = tmp_path / 'ledger.sqlite3'
        fetched = NOW - timedelta(days=10)
        with Ledger(path) as ledger:
            _track(ledger)
            ledger._db.execute(
                'UPDATE repository_state SET stage_watermarks = ?',
                (json.dumps({'depgraph': fetched.isoformat()}),),
            )
        with Ledger(path) as ledger:
            state = ledger.stage_state(1, Stage.DEPGRAPH)
            assert state is not None
            assert state.next_attempt_at == fetched + timedelta(days=30)
            assert not ledger.claim_stage(Stage.DEPGRAPH, NOW, 10, 'w')
            later = fetched + timedelta(days=31)
            assert [
                w.repository_id
                for w in ledger.claim_stage(Stage.DEPGRAPH, later, 10, 'w')
            ] == [1]


class TestLeases:

    def test_two_stages_hold_one_repository_at_once(self, ledger):
        _track(ledger)
        tree = ledger.claim_stages([Stage.TREE], NOW, 10, 'tree-worker')
        sbom = ledger.claim_stages([Stage.SBOM], NOW, 10, 'sbom-worker')
        assert [c.state.repository_id for c in tree] == [1]
        assert [c.state.repository_id for c in sbom] == [1]

    def test_a_slice_is_leased_in_one_commit(self, ledger):
        """A commit per repository was 500 back-to-back write locks, each
        held through a sync of the ledger's disk: whichever process wanted
        to record meanwhile waited past its busy timeout (#98)."""
        for repository_id in range(1, 6):
            _track(ledger, repository_id)
        ledger.claim_stages([Stage.TREE], NOW, 1, 'holder')
        statements: list[str] = []
        ledger._db.set_trace_callback(statements.append)

        claimed = ledger.claim_stages(
            [Stage.TREE, Stage.SBOM], NOW, 3, 'worker',
        )

        ledger._db.set_trace_callback(None)
        assert len(claimed) == 3
        assert statements.count('BEGIN') == 1
        assert statements.count('COMMIT') == 1
        writes = [
            index for index, sql in enumerate(statements)
            if sql.lstrip().split()[0].upper() in {'INSERT', 'UPDATE'}
        ]
        assert writes
        assert statements.index('BEGIN') < writes[0]
        assert writes[-1] < statements.index('COMMIT')

    def test_one_stage_is_held_by_one_worker(self, ledger):
        _track(ledger)
        assert ledger.claim_stages([Stage.TREE], NOW, 10, 'a')
        assert not ledger.claim_stages([Stage.TREE], NOW, 10, 'b')
        later = NOW + timedelta(hours=1)
        assert ledger.claim_stages([Stage.TREE], later, 10, 'b'), (
            'the lease expires rather than being held'
        )

    def test_a_lease_that_ran_out_and_was_taken_is_not_walked(self, ledger):
        """A slice of 500 was walked for an hour and a half on thirty-
        minute leases: the rest of it was claimed again by the next
        worker to start and walked by both. A lease is renewed as its
        repository is reached; one lost meanwhile is skipped, and stays
        with the worker that took it."""
        _track(ledger, 1)
        _track(ledger, 2)
        # The first is reached twenty minutes in, the second an hour in.
        clock = iter([NOW + timedelta(minutes=20), NOW + timedelta(hours=1)])
        walked: list[str] = []

        def run(repository, carried):
            walked.append(repository.repo)
            if repository.repo == 'r1' and len(walked) == 1:
                # While the first is walked, the second's lease runs out
                # and another worker takes it.
                taken = ledger.claim_stages(
                    STAGES, NOW + timedelta(minutes=45), 10, 'b',
                )
                assert [c.state.repository_id for c in taken] == [2]
            return {}

        service = RunService(
            ledger, {stage: run for stage in STAGES}, lambda: 0,
            worker='a', clock=lambda: next(clock),
        )
        result = service.advance(NOW, limit=10, quota_budget=100)

        assert result.repositories == 1
        assert result.taken == 1
        assert set(walked) == {'r1'}
        for stage in STAGES:
            assert ledger.stage_state(2, stage).claimed_by == 'b'
            assert ledger.stage_state(1, stage).claimed_by == ''

    def test_the_walk_skips_a_repository_another_stage_worker_holds(self, ledger):
        """It runs the whole chain, so it needs the whole chain."""
        _track(ledger)
        assert ledger.claim_stages([Stage.SBOM], NOW, 10, 'sbom-worker')
        runners = Runners()
        result = _service(ledger, runners).advance(
            NOW, limit=10, quota_budget=100,
        )
        assert result.repositories == 0
        assert runners.calls == []
        # And it left none of its own leases behind.
        for stage in STAGES:
            state = ledger.stage_state(1, stage)
            if stage is not Stage.SBOM:
                assert state is None or not state.claimed_by


class TestBackoff:

    def test_a_failure_backs_off_that_stage_alone(self, ledger):
        _track(ledger, 1)
        _track(ledger, 2)
        _service(ledger, Runners(fails={Stage.TREE})).advance(
            NOW, limit=10, quota_budget=100,
        )
        tree = ledger.stage_state(1, Stage.TREE)
        assert tree is not None and tree.outcome == 'failed'
        assert tree.next_attempt_at is not None and tree.next_attempt_at > NOW
        assert _due(ledger, Stage.TREE) == set(), 'backing off'
        commit = ledger.stage_state(1, Stage.COMMIT)
        assert commit is not None and commit.outcome == 'ok'

    def test_the_walk_stops_at_a_stage_still_backing_off(self, ledger):
        _track(ledger)
        _service(ledger, Runners(fails={Stage.TREE})).advance(
            NOW, limit=10, quota_budget=100,
        )
        ledger.record_push(1, NOW, NOW)  # RELEASE due again
        runners = Runners()
        result = _service(ledger, runners).advance(
            NOW + timedelta(minutes=1), limit=10, quota_budget=100,
        )
        assert runners.calls == [Stage.RELEASE, Stage.COMMIT]
        assert result.blocked == 1

    def test_a_failed_stage_is_due_again_once_its_backoff_runs_out(self, ledger):
        _track(ledger)
        _service(ledger, Runners(fails={Stage.TREE})).advance(
            NOW, limit=10, quota_budget=100,
        )
        assert _due(ledger, Stage.TREE, NOW + timedelta(hours=1)) == {1}


class TestOneStageAlone:

    def test_only_that_stage_is_claimed_and_recorded(self, ledger):
        _track(ledger)
        runners = Runners(
            produces={
                Stage.COMMIT: {
                    'download_target': {
                        'ref': 'main', 'ref_type': 'branch',
                        'commit_sha': 'a' * 40, 'commit_sha_short': 'aaaaaaa',
                    },
                },
            },
        )
        result = _service(ledger, runners).advance(
            NOW, limit=10, quota_budget=100, stage=Stage.TREE,
        )
        assert result.completed == {'tree': 1}
        # The stages before it walked for their hand-off, not recorded.
        assert runners.calls == [Stage.RELEASE, Stage.COMMIT, Stage.TREE]
        assert ledger.stage_state(1, Stage.RELEASE) is None
        tree = ledger.stage_state(1, Stage.TREE)
        assert tree is not None and tree.input_key == 'a' * 40

    def test_an_upstream_failure_backs_off_the_stage_asked_for(self, ledger):
        _track(ledger)
        _service(ledger, Runners(fails={Stage.COMMIT})).advance(
            NOW, limit=10, quota_budget=100, stage=Stage.SBOM,
        )
        sbom = ledger.stage_state(1, Stage.SBOM)
        assert sbom is not None and sbom.outcome == 'failed'
        assert 'upstream commit' in sbom.last_error
        assert ledger.stage_state(1, Stage.COMMIT) is None

    def test_no_record_is_kept_unless_the_chain_reached_its_end(self, ledger):
        _track(ledger)
        kept: list = []
        runners = Runners()
        RunService(
            ledger, runners.table(), lambda: runners.requests,
            remember=kept.append,
        ).advance(NOW, limit=10, quota_budget=100, stage=Stage.TREE)
        assert kept == []

    def test_lock_does_not_run_here(self, ledger):
        with pytest.raises(ValueError):
            _service(ledger, Runners()).advance(
                NOW, limit=1, quota_budget=1, stage=Stage.LOCK,
            )


class TestRepositoriesFile:

    def test_only_the_named_repositories_are_claimed(self, ledger):
        for repository_id in (1, 2, 3):
            _track(ledger, repository_id)
        ids, missing = ledger.resolve_repositories(
            ['O/R2', '3', 'nobody/here', '# a comment', ''],
        )
        assert ids == {2, 3}
        assert missing == ['nobody/here']
        runners = Runners()
        result = _service(ledger, runners).advance(
            NOW, limit=10, quota_budget=100, repos=ids,
        )
        assert result.repositories == 2
        assert _due(ledger, Stage.RELEASE) == {1}

    def test_the_depgraph_claim_honours_it_too(self, ledger):
        for repository_id in (1, 2):
            _track(ledger, repository_id)
        claimed = ledger.claim_stage(Stage.DEPGRAPH, NOW, 10, 'w', repos={2})
        assert [w.repository_id for w in claimed] == [2]


def test_a_repository_only_a_search_listed_is_walked_too(ledger):
    """Content picks manifests from the tree, so a repository needs no
    language to be collected: a C++ one a snapshot seeded is walked."""
    ledger.seed(9, 'o', 'seeded', snapshot='all', github_language='C++')
    assert _due(ledger, Stage.RELEASE) == {9}
    result = _service(ledger, Runners()).advance(
        NOW, limit=10, quota_budget=100,
    )
    assert result.repositories == 1


def test_health_counts_derived_stages_by_their_rows(ledger):
    _track(ledger, 1)
    _track(ledger, 2)
    _service(ledger, Runners()).advance(NOW, limit=1, quota_budget=100)
    health = ledger.health(NOW)
    assert health.due[Stage.SBOM] == 1


class TestDiscoveryRollout:
    """What PR C of #55 changes about what is due."""

    def test_adopted_content_and_sbom_rows_are_due_again(self, tmp_path):
        """Every repository collected before discovery has a content row
        adopted at version 1: all of them are due to be filled out from
        their trees, with no push. Commit and tree are not; release is,
        since PR F (below)."""
        path = tmp_path / 'ledger.sqlite3'
        with Ledger(path) as ledger:
            _track(ledger)
            done = (PUSHED + timedelta(hours=1)).isoformat()
            ledger._db.execute(
                'UPDATE repository_state SET stage_watermarks = ?',
                (
                    json.dumps({
                        str(stage): done for stage in DERIVED_STAGES
                    }),
                ),
            )
            ledger._db.execute('DELETE FROM stage_state')
        with Ledger(path) as ledger:
            for stage in (Stage.COMMIT, Stage.TREE):
                assert _due(ledger, stage) == set(), stage
            for stage in (Stage.RELEASE, Stage.CONTENT, Stage.LOCK, Stage.SBOM):
                assert _due(ledger, stage) == {1}, stage


class TestReleaseRollout:
    """What PR F of #55 changes about what is due: every stored release
    history counted branches as tags, so every one is chosen again."""

    def test_every_release_row_is_due_again_with_no_push(self, ledger):
        _track(ledger, 1)
        _track(ledger, 2)
        ledger.record_stage_success(
            1, Stage.RELEASE, NOW, PUSHED.isoformat(), 'v1',
        )
        ledger._db.execute(
            'UPDATE stage_state SET stage_version = 1 WHERE stage = ?',
            (str(Stage.RELEASE),),
        )
        assert _due(ledger, Stage.RELEASE) >= {1}

    def test_commit_follows_only_where_the_tag_chosen_changed(self, ledger):
        """Re-choosing the same tag leaves the commit, and all after it,
        alone: only a repository whose "latest release" was a branch
        is collected again."""
        _track(ledger)
        _service(
            ledger,
            Runners(
                produces={
                    Stage.RELEASE: {
                        'latest_stable_release': {'tag_name': 'v1.0.0'},
                    },
                },
            ),
        ).advance(NOW, limit=10, quota_budget=100)
        release = ledger.stage_state(1, Stage.RELEASE)
        assert release is not None
        assert _due(ledger, Stage.COMMIT) == set()

        ledger.record_stage_success(
            1, Stage.RELEASE, NOW, release.input_key, 'v1.0.0',
        )
        assert _due(ledger, Stage.COMMIT) == set()
        ledger.record_stage_success(
            1, Stage.RELEASE, NOW, release.input_key, 'v1.1.0',
        )
        assert _due(ledger, Stage.COMMIT) == {1}

    def test_the_sbom_is_due_when_the_content_digest_changes(self, ledger):
        _track(ledger)
        runners = Runners(produces={Stage.CONTENT: {'content_digest': 'd1'}})
        _service(ledger, runners).advance(NOW, limit=10, quota_budget=100)
        content = ledger.stage_state(1, Stage.CONTENT)
        assert content is not None and content.output_key == 'd1'
        assert _due(ledger, Stage.SBOM) == set()

        ledger.record_stage_success(
            1, Stage.CONTENT, NOW, content.input_key, 'd2',
        )
        assert _due(ledger, Stage.SBOM) == {1}
