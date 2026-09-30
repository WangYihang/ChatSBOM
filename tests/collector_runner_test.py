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
  against it.
"""
from __future__ import annotations

import json
import os
from datetime import datetime
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest

from chatsbom.collector.content import CONTENT_VERSION
from chatsbom.collector.content import VERSION_FIELD
from chatsbom.collector.due import Priority
from chatsbom.collector.due import sbom_key
from chatsbom.collector.due import State
from chatsbom.collector.errors import Unauthorized
from chatsbom.collector.runner import collect
from chatsbom.collector.runner import Collected
from chatsbom.collector.runner import observe_now
from chatsbom.collector.stages import Target
from chatsbom.collector.stages import Tools
from chatsbom.collector.state import CollectorState
from chatsbom.core.layout import push_instant
from chatsbom.core.ledger import Stage
from tests.collector_stages_test import make_world
from tests.collector_stages_test import World
from tests.fake_github_test import Reply
from tests.fake_upstream_test import Repository


def _collect(
    world: World, repository: Repository, push: str | None = None, *,
    priority: Priority | None = None,
) -> Collected:
    """Collect `repository` for `push`: the push GitHub has for it now,
    unless given."""
    at = push_instant(push or world.github.repos[repository.id].pushed_at)

    async def collecting(tools: Tools) -> Collected:
        return await collect(
            tools, Target(repository.id, repository.full_name), at,
            priority=priority,
        )

    return world.run(collecting)


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


class TestThePriority:
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


class TestObservingNow:
    def test_keeps_how_the_repository_stands_as_the_sweep_would(
        self, world, one,
    ):
        node = world.github.repos[1].node_id

        async def observing(tools: Tools) -> Any:
            return await observe_now(tools, node)

        found = world.run(observing)
        assert found is not None
        assert (found.repository_id, found.full_name) == (1, 'octo/one')
        assert found.pushed_at == push_instant('2026-09-02T00:00:00Z')
        assert found.head == world.github.repos[1].head
        assert found.release_tag == 'v1.0.0'
        with CollectorState.open(world.state_path) as state:
            kept = state.observed(1)
        assert kept is not None and kept.head == found.head

    def test_says_none_of_a_repository_gone(self, world):
        async def observing(tools: Tools) -> Any:
            return await observe_now(tools, 'R_gone')

        assert world.run(observing) is None
