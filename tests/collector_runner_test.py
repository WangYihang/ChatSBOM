"""One repository, from a push to its SBOM (#161).

`collect` runs a repository's due stages one after another, asking the
store after each what is due now (`collector/due.py`), until nothing
is. These tests collect synthetic repositories end to end, against the
stand-ins (`tests/fake_github_test.py`, `tests/fake_upstream_test.py`):
git on disk, the API, raw content and Syft. What they hold it to:

- a push is collected to its SBOM, and the store then says it is
  current;
- the early cutoff: a push that comes to a commit already collected
  runs nothing after it;
- waiting upstreams: a stage that fails or finds nothing holds back
  every stage after it, and runs again only after its backoff;
- #100 Q4's stamp: a content root the old pipeline left is fetched
  again, and its SBOM made again;
- rescans after a Syft upgrade, which run Syft and ask GitHub nothing;
- what is not the repository's doing, a refused token, is not kept
  against it;
- #160's contract: a repository is collected as an observation had it,
  for the push it saw, and marked collected as of it once its due
  stages ran, whatever became of them.
"""
from __future__ import annotations

import json
import os
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest

from chatsbom.collector.content import CONTENT_VERSION
from chatsbom.collector.content import VERSION_FIELD
from chatsbom.collector.due import detected
from chatsbom.collector.due import Priority
from chatsbom.collector.due import sbom_key
from chatsbom.collector.due import State
from chatsbom.collector.due import walk_universe
from chatsbom.collector.errors import Unauthorized
from chatsbom.collector.runner import collect
from chatsbom.collector.runner import Collected
from chatsbom.collector.runner import observe_now
from chatsbom.collector.stages import Tools
from chatsbom.collector.state import CollectorState
from chatsbom.collector.state import Member
from chatsbom.collector.state import Observed
from chatsbom.collector.state import UniverseSnapshot
from chatsbom.core.layout import push_instant
from chatsbom.core.ledger import Stage
from tests.collector_stages_test import make_world
from tests.collector_stages_test import World
from tests.fake_github_test import Reply
from tests.fake_upstream_test import Repository


def _observed(
    repository: Repository, at: datetime,
    push: datetime | str | None = None,
) -> Observed:
    """`repository` as the sweep would observe it `at`: pushed `push`, or
    when GitHub says it was."""
    repo = repository.repo
    return Observed(
        repository_id=repo.id, node_id=repo.node_id,
        full_name=repo.full_name, stars=repo.stars, archived=repo.archived,
        pushed_at=push_instant(push or repo.pushed_at),
        default_branch=repo.default_branch, head=repo.head,
        release_tag=None, release_at=None, observed_at=at,
    )


def _collect(
    world: World, repository: Repository,
    push: datetime | str | None = None, *,
    priority: Priority | None = None,
) -> Collected:
    """Collect `repository` as observed now, pushed `push`: the push
    GitHub has for it, unless given."""
    async def collecting(tools: Tools) -> Collected:
        observed = _observed(repository, tools.now(), push)
        tools.state.observe(observed)
        return await collect(tools, observed, priority=priority)

    return world.run(collecting)


def _universe(world: World, *repositories: Repository) -> None:
    """`repositories`, the universe the sweep asks after."""
    with CollectorState.open(world.state_path) as state:
        state.keep_universe(
            UniverseSnapshot(
                snapshot='all-2026-09-28', stamp='all-2026-09-28:1',
                repositories=len(repositories),
                loaded_at=datetime(2026, 9, 28, tzinfo=timezone.utc),
            ),
            [Member(r.id, r.repo.node_id) for r in repositories],
        )


def _ids(found: list[Observed]) -> list[int]:
    return [observed.repository_id for observed in found]


def _ran(collected: Collected) -> list[tuple[str, str]]:
    return [(str(ran.stage), ran.result) for ran in collected.ran]


def _outcomes(world: World) -> list[Any]:
    with CollectorState.open(world.state_path) as state:
        return list(state.outcomes())


def _tagged(repository: Repository) -> str:
    """The commit `one`'s release is at: the one without the README."""
    [tagged] = [
        commit for commit, files in repository.files_at.items()
        if 'README.md' not in files
    ]
    return tagged


@pytest.fixture
def world(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> World:
    return make_world(tmp_path, monkeypatch)


@pytest.fixture
def one(world: World) -> Repository:
    """octo/one: a release, at the commit its tag names, with a
    manifest and a lockfile; and a commit after it."""
    repository = world.upstream.add(1, 'octo/one')
    tagged = repository.commit(
        {
            'package.json': '{"name": "one"}', 'package-lock.json': '{}',
            'src/index.js': 'code',
        },
        date='2026-08-01T00:00:00+00:00',
    )
    repository.tag('v1.0.0', tagged, annotated=True)
    repository.release('v1.0.0', '2026-08-01T00:00:00Z')
    repository.commit(
        {'README.md': 'more'}, date='2026-09-02T00:00:00+00:00',
    )
    return repository


class TestAPushToItsSbom:
    def test_runs_every_stage_in_order(self, world, one):
        collected = _collect(world, one)

        assert _ran(collected) == [
            ('release', 'done'), ('commit', 'done'), ('tree', 'done'),
            ('content', 'done'), ('sbom', 'done'),
        ]
        assert collected.standing is not None
        assert collected.standing.current
        assert _outcomes(world) == []

    def test_leaves_what_todays_stages_leave_in_the_store(self, world, one):
        _collect(world, one)
        paths = world.paths
        tagged = _tagged(one)
        assert paths.tree_file(1, tagged).read_text().splitlines() == [
            'package-lock.json', 'package.json', 'src/index.js',
        ]
        root = paths.content_root(1, tagged)
        assert sorted(p.name for p in root.iterdir()) == [
            'package-lock.json', 'package.json',
        ]
        manifests = json.loads(paths.discovery_file(1, tagged).read_text())
        assert manifests[VERSION_FIELD] == CONTENT_VERSION
        sbom = json.loads(paths.sbom_file(1, tagged).read_text())
        assert sbom['descriptor'] == {'name': 'syft', 'version': '1.52.0'}

    def test_says_what_each_stage_did(self, world, one):
        collected = _collect(world, one)
        said = {str(ran.stage): ran.summary for ran in collected.ran}
        assert said['release'] == 'decided v1.0.0 of 1 release'
        assert said['commit'].startswith('resolved tag:v1.0.0 to ')
        assert said['commit'].endswith('(release v1.0.0)')
        assert said['tree'].startswith('listed 3 paths at ')
        assert said['content'].startswith('2 of 2 manifests stored')
        assert said['sbom'] == 'scanned by Syft 1.52.0: 2 packages'

    def test_keeps_no_validator_it_would_never_read(self, world, one):
        """No stage asks GitHub conditionally: a release list is decided
        whole for each push, and a commit's date asked once. A validator
        kept for their answers would be a row nothing reads."""
        _collect(world, one)
        with CollectorState.open(world.state_path) as state:
            kept = state._db.execute(
                'SELECT count(*) FROM validator',
            ).fetchone()[0]
        assert kept == 0

    def test_then_has_nothing_due(self, world, one):
        _collect(world, one)
        again = _collect(world, one)
        assert again.ran == []
        assert again.standing is not None and again.standing.current
        assert len(world.syft.scans) == 1


class TestTheEarlyCutoff:
    def test_a_push_that_keeps_the_release_runs_the_release_stage_alone(
        self, world, one,
    ):
        _collect(world, one)
        requests = len(world.upstream.raw.requests)
        # A push to another branch: the latest release is the same.
        one.commit({'x.txt': 'x'}, branch='feature')
        one.pushed('2026-09-10T00:00:00+00:00')

        collected = _collect(world, one)

        assert _ran(collected) == [('release', 'done')]
        assert collected.cut_off
        assert len(world.upstream.raw.requests) == requests
        assert len(world.syft.scans) == 1

    def test_a_push_resolved_to_the_same_commit_runs_nothing_after_it(
        self, world,
    ):
        repository = world.upstream.add(2, 'octo/two')
        repository.commit({'go.mod': 'module two\n'})
        _collect(world, repository)
        # Pushed, and the default branch's head did not move.
        repository.commit({'y.txt': 'y'}, branch='feature')
        repository.pushed('2026-09-10T00:00:00+00:00')

        collected = _collect(world, repository)

        assert _ran(collected) == [('release', 'done'), ('commit', 'done')]
        assert collected.cut_off
        assert len(world.syft.scans) == 1

    def test_a_push_to_a_new_commit_with_the_same_manifests_is_not_scanned(
        self, world,
    ):
        """A new commit is listed and fetched; the same files scanned by
        the same Syft come from the cache."""
        repository = world.upstream.add(2, 'octo/two')
        repository.commit({'go.mod': 'module two\n'})
        _collect(world, repository)
        repository.commit(
            {'main.go': 'package main'}, date='2026-09-10T00:00:00+00:00',
        )

        collected = _collect(world, repository)

        assert _ran(collected) == [
            ('release', 'done'), ('commit', 'done'), ('tree', 'done'),
            ('content', 'done'), ('sbom', 'done'),
        ]
        assert not collected.cut_off
        assert 'from the cache' in collected.ran[-1].summary
        assert len(world.syft.scans) == 1


class TestWaitingUpstreams:
    def test_a_failed_stage_holds_the_rest_back_until_its_backoff(
        self, world, one,
    ):
        world.github.script(
            Reply(502, {'message': 'Server Error'}),
            path='/repositories/1/releases', times=2,
        )

        first = _collect(world, one)

        assert _ran(first) == [('release', 'failed')]
        assert first.failed
        assert first.standing is not None
        release = first.standing.verdict(Stage.RELEASE)
        assert release.state is State.BACKING_OFF
        assert release.due_at == first.ran[0].due_at
        assert {
            str(v.stage): str(v.state) for v in first.standing.verdicts
            if v.stage is not Stage.RELEASE
        } == {
            'commit': 'waiting', 'tree': 'waiting', 'content': 'waiting',
            'sbom': 'waiting',
        }
        [kept] = _outcomes(world)
        assert (kept.stage, kept.kind, kept.attempts) == (
            'release', 'failed', 1,
        )
        assert '502' in kept.detail

        # Backing off: nothing runs, and nothing is asked of GitHub.
        asked = len(world.github.requests)
        assert _collect(world, one).ran == []
        assert len(world.github.requests) == asked

        # Past the backoff, it runs again; and fails again, for longer.
        world.github.clock.advance(15 * 60)
        second = _collect(world, one)
        assert _ran(second) == [('release', 'failed')]
        [kept] = _outcomes(world)
        assert kept.attempts == 2
        assert kept.due_at - kept.last_at == timedelta(minutes=30)

        world.github.clock.advance(30 * 60)
        third = _collect(world, one)
        assert third.standing is not None and third.standing.current
        assert _outcomes(world) == []

    def test_a_stage_done_forgets_what_was_kept_of_it_for_older_keys(
        self, world, one,
    ):
        """Only the current key's outcome is ever read: an older push's
        failure is dead weight once a newer push's stage is done."""
        world.github.script(
            Reply(502, {'message': 'Server Error'}),
            path='/repositories/1/releases',
        )
        _collect(world, one)
        [kept] = _outcomes(world)
        assert kept.stage == 'release'
        one.pushed('2026-09-10T00:00:00+00:00')

        collected = _collect(world, one)

        assert collected.standing is not None and collected.standing.current
        assert _outcomes(world) == []

    def test_nothing_is_kept_with_its_backoff_as_well(self, world):
        repository = world.upstream.add(3, 'octo/empty')

        collected = _collect(world, repository)

        assert _ran(collected) == [('release', 'done'), ('commit', 'nothing')]
        [kept] = _outcomes(world)
        assert (kept.stage, kept.kind) == ('commit', 'nothing')
        assert 'no HEAD' in kept.detail
        assert collected.standing is not None
        assert collected.standing.verdict(Stage.TREE).state is State.WAITING

    def test_a_stage_whose_output_stays_stale_is_kept_as_failed(
        self, world, one,
    ):
        """A content file dated in the future is newer than any SBOM made
        of it: the SBOM stage would run for good."""
        _collect(world, one)
        tagged = _tagged(one)
        future = datetime(2100, 1, 1).timestamp()
        os.utime(
            world.paths.content_root(1, tagged) / 'package.json',
            (future, future),
        )

        collected = _collect(world, one)

        assert _ran(collected) == [('sbom', 'failed')]
        assert 'still not current (input-changed)' in collected.ran[0].summary
        assert collected.ran[0].due_at is not None


class TestTheContentStamp:
    def test_a_root_the_old_pipeline_left_is_fetched_again_and_scanned(
        self, world, one,
    ):
        _collect(world, one)
        tagged = _tagged(one)
        index = world.paths.discovery_file(1, tagged)
        document = json.loads(index.read_text())
        del document[VERSION_FIELD]
        index.write_text(json.dumps(document))
        before = len(world.upstream.raw.requests)

        collected = _collect(world, one)

        assert _ran(collected) == [('content', 'done'), ('sbom', 'done')]
        assert sorted(
            r.path for r in world.upstream.raw.requests[before:]
        ) == ['package-lock.json', 'package.json']
        assert json.loads(index.read_text())[VERSION_FIELD] == CONTENT_VERSION
        assert collected.standing is not None and collected.standing.current


class TestARescanAfterASyftUpgrade:
    def test_runs_syft_alone_and_asks_github_nothing(self, world, one):
        _collect(world, one)
        asked = len(world.github.requests)
        fetched = len(world.upstream.raw.requests)
        world.syft.configure(version='1.53.0')

        collected = _collect(world, one)

        assert _ran(collected) == [('sbom', 'done')]
        assert collected.ran[0].key.endswith('syft@1.53.0')
        assert len(world.github.requests) == asked
        assert len(world.upstream.raw.requests) == fetched
        tagged = _tagged(one)
        sbom = json.loads(world.paths.sbom_file(1, tagged).read_text())
        assert sbom['descriptor']['version'] == '1.53.0'
        assert len(world.syft.scans) == 2

    def test_asks_the_pool_at_the_lowest_priority(
        self, world, one, monkeypatch,
    ):
        """A scan waiting for a slot waits behind a changed repository's,
        and a never collected one's, unless its caller says otherwise."""
        _collect(world, one)
        asked = _asked_at(monkeypatch)
        world.syft.configure(version='1.53.0')
        _collect(world, one)
        world.syft.configure(version='1.54.0')
        _collect(world, one, priority=Priority.NEW)
        assert asked == [int(Priority.RESCAN), int(Priority.NEW)]

    def test_is_found_by_walking_the_universe_and_run_at_its_priority(
        self, world, one, monkeypatch,
    ):
        _universe(world, one)
        _collect(world, one)
        asked = _asked_at(monkeypatch)
        world.syft.configure(version='1.53.0')

        async def rescanning(tools: Tools) -> list[Collected]:
            walked = walk_universe(
                tools.state, paths=tools.paths,
                syft_version=await tools.syft.version(), now=tools.now(),
            )
            return [
                await collect(tools, found.observed, priority=found.priority)
                for found in walked.candidates
            ]

        [collected] = world.run(rescanning)
        assert _ran(collected) == [('sbom', 'done')]
        assert asked == [int(Priority.RESCAN)]

    def test_a_failure_under_one_syft_does_not_hold_back_the_next(
        self, world, one,
    ):
        _collect(world, one)
        world.syft.configure(version='1.53.0', exit=1, stderr='boom')
        failed = _collect(world, one)
        assert _ran(failed) == [('sbom', 'failed')]

        [kept] = _outcomes(world)
        assert kept.key == sbom_key(_tagged(one), '1.53.0')

        world.syft.configure(version='1.54.0')
        collected = _collect(world, one)
        assert _ran(collected) == [('sbom', 'done')]
        assert _outcomes(world) == []


def _asked_at(monkeypatch: pytest.MonkeyPatch) -> list[int]:
    """The priority of every scan asked of the pool from now on."""
    from chatsbom.collector.syftpool import SyftPool

    asked: list[int] = []
    real = SyftPool.scan

    async def scan(
        self: SyftPool, directory: Any, *, priority: int = 0,
    ) -> bytes:
        asked.append(priority)
        return await real(self, directory, priority=priority)

    monkeypatch.setattr(SyftPool, 'scan', scan)
    return asked


def _observe_all(world: World, *repositories: Repository) -> None:
    """`repositories`, observed now as the sweep observes them."""
    async def observing(tools: Tools) -> None:
        for repository in repositories:
            await observe_now(
                tools, Member(repository.id, repository.repo.node_id),
            )

    world.run(observing)


def _collect_detected(world: World) -> list[tuple[int, str]]:
    """Every repository detection names, collected in its order and at
    its priority: each id and priority."""
    async def collecting(tools: Tools) -> list[tuple[int, str]]:
        done = []
        for candidate in detected(tools.state, limit=10):
            collected = await collect(
                tools, candidate.observed, priority=candidate.priority,
            )
            assert collected.standing is not None
            assert collected.standing.current
            done.append(
                (candidate.observed.repository_id, candidate.priority.name),
            )
        return done

    return world.run(collecting)


class TestThePriority:
    def test_follows_what_detection_found(self, world, one, monkeypatch):
        """octo/one pushed since it was collected, and octo/two never
        collected: each is collected, the changed first, and scanned at
        its priority; then neither is."""
        _universe(world, one)
        _observe_all(world, one)
        assert _collect_detected(world) == [(1, 'NEW')]
        two = world.upstream.add(2, 'octo/two')
        two.commit({'go.mod': 'module two\n'})
        _universe(world, one, two)
        released = one.commit(
            {'package.json': '{"name": "one", "version": "1.1.0"}'},
            date='2026-09-10T00:00:00+00:00',
        )
        one.tag('v1.1.0', released)
        one.release('v1.1.0', '2026-09-10T00:00:00Z')
        world.github.clock.advance(3_600)
        _observe_all(world, one, two)
        asked = _asked_at(monkeypatch)

        assert _collect_detected(world) == [(1, 'CHANGED'), (2, 'NEW')]
        assert asked == [int(Priority.CHANGED), int(Priority.NEW)]
        assert _collect_detected(world) == []

    def test_of_a_changed_repositorys_scan_is_the_highest(
        self, world, one, monkeypatch,
    ):
        """What its caller says; and, unsaid, a change's, unless the
        store says the scan is a rescan."""
        asked = _asked_at(monkeypatch)
        _collect(world, one)
        repository = world.upstream.add(2, 'octo/two')
        repository.commit({'go.mod': 'module two\n'})
        _collect(world, repository, priority=Priority.NEW)
        assert asked == [int(Priority.CHANGED), int(Priority.NEW)]


class TestWhatIsNotTheRepositorys:
    def test_a_refused_token_stops_it_and_keeps_nothing(self, world, one):
        world.github.accounts.clear()
        with pytest.raises(Unauthorized):
            _collect(world, one)
        assert _outcomes(world) == []


class TestWhatDetectionReads:
    """#160: `changed` and `never_collected` are what is collected for
    what detection found, and a collection says what it collected as of
    (`mark_collected`)."""

    def test_a_repository_collected_is_no_longer_never_collected(
        self, world, one,
    ):
        _universe(world, one)
        _collect(world, one)
        with CollectorState.open(world.state_path) as state:
            assert state.never_collected() == []
            assert state.changed() == []

    def test_it_is_marked_as_of_its_observation_not_when_it_ran(
        self, world, one,
    ):
        """A push the sweep observes while the stages run is not lost:
        the repository is changed again, for the next collection."""
        _universe(world, one)

        async def collecting(tools: Tools) -> list[Observed]:
            seen = _observed(one, tools.now())
            tools.state.observe(seen)
            world.github.clock.advance(3_600)
            assert seen.pushed_at is not None
            later = replace(
                seen, pushed_at=seen.pushed_at + timedelta(hours=1),
                observed_at=tools.now(),
            )
            tools.state.observe(later)
            tools.state.mark_changed(1, at=later.observed_at)
            await collect(tools, seen)
            return tools.state.changed()

        assert _ids(world.run(collecting)) == [1]

    def test_one_whose_stage_failed_is_marked_too(self, world, one):
        """The failure is kept, with its backoff, and found once it has
        passed. Left changed, the repository would come first every
        time, with nothing it may run."""
        _universe(world, one)
        world.github.script(
            Reply(502, {'message': 'Server Error'}),
            path='/repositories/1/releases',
        )
        assert _collect(world, one).failed
        with CollectorState.open(world.state_path) as state:
            assert state.never_collected() == []

    def test_one_stopped_by_a_refused_token_is_not(self, world, one):
        _universe(world, one)
        world.github.accounts.clear()
        with pytest.raises(Unauthorized):
            _collect(world, one)
        with CollectorState.open(world.state_path) as state:
            assert _ids(state.never_collected()) == [1]

    def test_it_collects_for_the_push_its_observation_says(self, world, one):
        """P, in UTC to the second, as a decision keys it (#147)."""
        said = datetime(
            2026, 9, 2, 8, 0, 1, 999, tzinfo=timezone(timedelta(hours=8)),
        )
        collected = _collect(world, one, said)
        push = datetime(2026, 9, 2, 0, 0, 1, tzinfo=timezone.utc)
        assert collected.push == push
        assert collected.ran[0].key == '2026-09-02T00:00:01Z'
        assert collected.standing is not None
        assert collected.standing.push == push


class TestObservingNow:
    def _observe(self, world: World, member: Member) -> Any:
        async def observing(tools: Tools) -> Any:
            return await observe_now(tools, member)

        return world.run(observing)

    def test_keeps_how_the_repository_stands_as_the_sweep_would(
        self, world, one,
    ):
        found = self._observe(world, Member(1, one.repo.node_id))
        assert found is not None
        assert (found.repository_id, found.full_name) == (1, 'octo/one')
        assert found.pushed_at == push_instant('2026-09-02T00:00:00Z')
        assert found.head == world.github.repos[1].head
        assert found.release_tag == 'v1.0.0'
        with CollectorState.open(world.state_path) as state:
            kept = state.observed(1)
        assert kept is not None and kept.head == found.head

    def test_marks_a_change_as_the_sweep_does(self, world, one):
        """Else the sweep, which compares with what was observed last,
        would never see a push this saw first."""
        _universe(world, one)
        member = Member(1, one.repo.node_id)

        async def collecting(tools: Tools) -> None:
            observed = await observe_now(tools, member)
            assert observed is not None
            await collect(tools, observed)

        world.run(collecting)
        world.github.clock.advance(60)
        self._observe(world, member)
        with CollectorState.open(world.state_path) as state:
            assert state.changed() == []

        one.pushed('2026-09-10T00:00:00+00:00')
        world.github.clock.advance(60)
        self._observe(world, member)

        with CollectorState.open(world.state_path) as state:
            assert _ids(state.changed()) == [1]

    def test_says_none_of_a_repository_gone(self, world):
        assert self._observe(world, Member(1, 'R_gone')) is None

    def test_keeps_nothing_of_a_node_that_is_another_repositorys(
        self, world, one,
    ):
        assert self._observe(world, Member(2, one.repo.node_id)) is None
        with CollectorState.open(world.state_path) as state:
            assert state.observed(1) is None
            assert state.observed(2) is None
