"""The collector's stages, one at a time (#161).

Each writes to the store what today's stage writes there: the release
and commit decisions (#147), the tree, the content with
`manifests.json`, and the SBOM. Today's rules are the ones they follow:
the latest stable release of the GitHub releases and bare tags, dated
by git; the release's tag resolved to its commit, and the default
branch's head when it is gone or there is none; the manifests discovery
selects, within its caps; and #110's SBOM, with the Syft cache and the
lockfiles `sbom lock` generated.

What is new is where they ask: the API on the async client, with its
budget; git by `git` (ls-remote, the tag fetch, the tree's clone); raw
content by a client of its own, which carries no token; and Syft in the
pool. Against the stand-ins: `tests/fake_github_test.py` for the API,
and `tests/fake_upstream_test.py` for git, raw content and Syft.
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import Awaitable
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any
from typing import TypeVar

import pytest

from chatsbom.collector.content import CONTENT_VERSION
from chatsbom.collector.content import MAX_FILE_BYTES
from chatsbom.collector.content import VERSION_FIELD
from chatsbom.collector.errors import Failed
from chatsbom.collector.gitremote import GitRemote
from chatsbom.collector.gitremote import resolve_commit
from chatsbom.collector.runner import tools_for
from chatsbom.collector.settings import CollectorSettings
from chatsbom.collector.stages import Nothing
from chatsbom.collector.stages import RepositoryStages
from chatsbom.collector.stages import StageFailed
from chatsbom.collector.stages import Target
from chatsbom.collector.stages import Tools
from chatsbom.collector.state import CollectorState
from chatsbom.collector.state import STATE_FILE
from chatsbom.collector.syftpool import SyftFailed
from chatsbom.collector.syftpool import SyftSettings
from chatsbom.collector.tokens import Token
from chatsbom.core import decisions
from chatsbom.core.config import PathConfig
from chatsbom.core.layout import CommitKey
from chatsbom.services.git_service import RemoteRefs
from tests.fake_github_test import FakeClock
from tests.fake_github_test import FakeGitHub
from tests.fake_github_test import Reply
from tests.fake_upstream_test import FakeSyft
from tests.fake_upstream_test import Repository
from tests.fake_upstream_test import Upstream

TOKEN = 'ghp_stages_token_000000000000000000000000'
UTC = timezone.utc
P1 = datetime(2026, 9, 2, tzinfo=UTC)

Result = TypeVar('Result')


@dataclass
class World:
    """GitHub, as the stages meet it, and the store they write."""

    github: FakeGitHub
    upstream: Upstream
    syft: FakeSyft
    paths: PathConfig
    state_path: Path

    def run(self, use: Callable[[Tools], Awaitable[Result]]) -> Result:
        """`use` of the stages' tools, against the stand-ins."""
        async def running() -> Result:
            with CollectorState.open(self.state_path) as state:
                async with tools_for(
                    self.paths, state,
                    CollectorSettings(
                        tokens=(Token('token 1', TOKEN),), reserve={},
                    ),
                    SyftSettings(
                        slots=2, timeout=30, memory=0,
                        command=str(self.syft.path),
                    ),
                    clock=self.github.clock,
                    sleep=self.github.clock.sleep,
                    github_transport=self.github.transport(),
                    raw_transport=self.upstream.raw.transport(),
                    git_base=self.upstream.git_base,
                ) as tools:
                    return await use(tools)

        return asyncio.run(running())

    def stages(
        self, repository: Repository,
        use: Callable[[RepositoryStages], Awaitable[Result]],
    ) -> Result:
        async def using(tools: Tools) -> Result:
            return await use(
                RepositoryStages(
                    tools, Target(repository.id, repository.full_name),
                ),
            )

        return self.run(using)


def make_world(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> World:
    """The stand-ins, in `tmp_path`, which is the working directory."""
    # The Syft cache is kept in .cache/, under the working directory.
    monkeypatch.chdir(tmp_path)
    # The collector quiets httpx2 as it runs: put back after.
    quiet = logging.getLogger('httpx2')
    monkeypatch.setattr(quiet, 'level', quiet.level)
    github = FakeGitHub(FakeClock())
    github.token(TOKEN, 'alice')
    return World(
        github=github,
        upstream=Upstream(tmp_path / 'github.com', github),
        syft=FakeSyft(tmp_path / 'bin'),
        paths=PathConfig(base_data_dir=tmp_path / 'data'),
        state_path=tmp_path / 'data' / STATE_FILE,
    )


@pytest.fixture
def world(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> World:
    return make_world(tmp_path, monkeypatch)


def _released(world: World, repository_id: int, push: datetime | None = P1):
    assert push is not None
    return decisions.read_release(
        decisions.releases_dir(world.paths, repository_id)
        / push.strftime('%Y%m%dT%H%M%SZ'),
        repository_id,
    )


def _list(world: World, repository_id: int, digest: str) -> list[dict[str, Any]]:
    found = decisions.release_list(world.paths, repository_id, digest)
    assert found is not None
    return found


class TestTheReleaseStage:
    def test_decides_the_latest_stable_release_for_the_push(self, world):
        repository = world.upstream.add(1, 'octo/one')
        first = repository.commit({'package.json': '{}'})
        repository.tag('v1.0.0', first)
        repository.release('v1.0.0', '2026-08-01T00:00:00Z')
        second = repository.commit({'go.mod': 'module x\n'})
        repository.tag('v2.0.0-rc1', second)
        repository.release(
            'v2.0.0-rc1', '2026-08-20T00:00:00Z', prerelease=True,
        )

        done = world.stages(repository, lambda s: s.release(P1))

        decision = _released(world, 1)
        assert decision is not None
        assert decision.tag == 'v1.0.0'
        assert done.output == 'v1.0.0'
        listed = _list(world, 1, decision.releases)
        assert [r['tag_name'] for r in listed] == ['v2.0.0-rc1', 'v1.0.0']

    def test_takes_a_bare_tag_dated_by_git_when_it_is_the_newest(
        self, world,
    ):
        repository = world.upstream.add(1, 'octo/one')
        first = repository.commit(
            {'package.json': '{}'}, date='2026-08-01T00:00:00+00:00',
        )
        repository.tag('v1.0.0', first)
        repository.release('v1.0.0', '2026-08-01T00:00:00Z')
        newer = repository.commit(
            {'go.mod': 'module x\n'}, date='2026-09-01T00:00:00+00:00',
        )
        repository.tag('v1.1.0', newer, annotated=True)

        world.stages(repository, lambda s: s.release(P1))

        decision = _released(world, 1)
        assert decision is not None and decision.tag == 'v1.1.0'
        listed = _list(world, 1, decision.releases)
        bare = next(r for r in listed if r['tag_name'] == 'v1.1.0')
        assert bare['source'] == 'git_tag'
        assert bare['target_commitish'] == newer
        assert datetime.fromisoformat(bare['published_at']) == datetime(
            2026, 9, 1, tzinfo=UTC,
        )

    def test_decides_no_release_for_a_repository_with_none(self, world):
        repository = world.upstream.add(1, 'octo/one')
        repository.commit({'package.json': '{}'})
        done = world.stages(repository, lambda s: s.release(P1))
        decision = _released(world, 1)
        assert decision is not None
        assert decision.tag is None
        assert decision.key == CommitKey.head(P1)
        assert done.output == ''

    def test_carries_each_tags_date_from_the_list_before(self, world):
        """A tag that names the same commit has the same date: the next
        push fetches no tags to date them again."""
        repository = world.upstream.add(1, 'octo/one')
        sha = repository.commit({'package.json': '{}'})
        repository.tag('v1.0.0', sha)
        asked: list[str] = []
        real = GitRemote.tag_dates

        async def counted(self: GitRemote, full_name: str) -> Any:
            asked.append(full_name)
            return await real(self, full_name)

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(GitRemote, 'tag_dates', counted)
            world.stages(repository, lambda s: s.release(P1))
            later = datetime(2026, 9, 3, tzinfo=UTC)
            world.stages(repository, lambda s: s.release(later))

        assert asked == ['octo/one']
        first, second = _released(world, 1), _released(world, 1, later)
        assert first is not None and second is not None
        # The same releases, the same list: written once.
        assert first.releases == second.releases

    def test_asks_the_api_for_a_date_git_could_not_give(self, world):
        repository = world.upstream.add(1, 'octo/one')
        sha = repository.commit({'package.json': '{}'})
        repository.tag('v1.0.0', sha)
        world.github.repos[1].commit_dates[sha] = '2026-07-07T07:07:07Z'

        async def no_dates(self: GitRemote, full_name: str) -> None:
            return None

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(GitRemote, 'tag_dates', no_dates)
            world.stages(repository, lambda s: s.release(P1))

        assert world.github.seen(f'/repositories/1/commits/{sha}')
        decision = _released(world, 1)
        assert decision is not None
        [entry] = _list(world, 1, decision.releases)
        assert entry['published_at'].startswith('2026-07-07T07:07:07')

    def test_reads_every_page_of_releases(self, world):
        repository = world.upstream.add(1, 'octo/one')
        repository.commit({'package.json': '{}'})
        for number in range(150):
            repository.release(
                f'v0.{number}.0', f'2026-01-01T00:{number // 60:02d}:{number % 60:02d}Z',
            )
        world.stages(repository, lambda s: s.release(P1))
        decision = _released(world, 1)
        assert decision is not None
        assert len(_list(world, 1, decision.releases)) == 150
        assert decision.tag == 'v0.149.0'
        assert len(world.github.seen('/repositories/1/releases')) == 2

    def test_that_cannot_read_the_releases_decides_nothing(self, world):
        """A decision is written once for its push: one made from half an
        answer would stand for good."""
        repository = world.upstream.add(1, 'octo/one')
        repository.commit({'package.json': '{}'})
        world.github.script(
            Reply(502, {'message': 'Server Error'}),
            path='/repositories/1/releases',
        )
        with pytest.raises(Failed):
            world.stages(repository, lambda s: s.release(P1))
        assert _released(world, 1) is None

    def test_that_cannot_list_the_refs_decides_nothing(self, world):
        repository = world.upstream.add(1, 'octo/one')
        repository.commit({'package.json': '{}'})
        (world.upstream.root / 'octo' / 'one.git').rename(
            world.upstream.root / 'octo' / 'elsewhere.git',
        )
        with pytest.raises(StageFailed, match='git ls-remote'):
            world.stages(repository, lambda s: s.release(P1))
        assert _released(world, 1) is None


class TestTheCommitStage:
    def _decided(self, world: World, repository: Repository) -> Any:
        world.stages(repository, lambda s: s.release(P1))
        decision = _released(world, repository.id)
        assert decision is not None
        return decision

    def test_resolves_the_releases_tag_to_its_commit(self, world):
        repository = world.upstream.add(1, 'octo/one')
        tagged = repository.commit({'package.json': '{}'})
        repository.tag('v1.0.0', tagged, annotated=True)
        repository.release('v1.0.0')
        repository.commit({'go.mod': 'module x\n'})
        decision = self._decided(world, repository)

        done = world.stages(repository, lambda s: s.commit(decision))

        kept = decisions.commit_decision(
            world.paths, 1, CommitKey.tag('v1.0.0'), P1,
        )
        assert kept is not None
        assert (kept.commit_sha, kept.ref, kept.ref_type) == (
            tagged, 'v1.0.0', 'release',
        )
        assert done.output == tagged

    def test_takes_the_default_branchs_head_without_a_release(self, world):
        repository = world.upstream.add(1, 'octo/one', default_branch='master')
        head = repository.commit({'package.json': '{}'})
        decision = self._decided(world, repository)

        world.stages(repository, lambda s: s.commit(decision))

        kept = decisions.commit_decision(
            world.paths, 1, CommitKey.head(P1), P1,
        )
        assert kept is not None
        assert (kept.commit_sha, kept.ref, kept.ref_type) == (
            head, 'master', 'branch',
        )

    def test_reuses_the_listing_the_release_stage_made(self, world):
        """One `git ls-remote` a collection, made after the push (#100
        Q6): the release stage's tags and the commit stage's refs."""
        repository = world.upstream.add(1, 'octo/one')
        repository.commit({'package.json': '{}'})
        listed: list[str] = []
        real = GitRemote.list_remote

        async def counted(self: GitRemote, full_name: str) -> RemoteRefs:
            listed.append(full_name)
            return await real(self, full_name)

        async def both(stages: RepositoryStages) -> None:
            await stages.release(P1)
            decision = _released(world, 1)
            assert decision is not None
            await stages.commit(decision)

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(GitRemote, 'list_remote', counted)
            world.stages(repository, both)
        assert listed == ['octo/one']

    def test_finds_nothing_in_a_repository_with_no_commits(self, world):
        repository = world.upstream.add(1, 'octo/empty')
        decision = self._decided(world, repository)
        with pytest.raises(Nothing, match='no commit'):
            world.stages(repository, lambda s: s.commit(decision))


class TestResolvingACommit:
    """The commit stage's rule (`CommitService`'s), on one listing."""

    LISTING = RemoteRefs(
        refs={
            'HEAD': 'a' * 40, 'refs/heads/master': 'a' * 40,
            'master': 'a' * 40, 'refs/heads/develop': 'b' * 40,
            'develop': 'b' * 40, 'refs/tags/v1.0.0': 'c' * 40,
            'v1.0.0': 'c' * 40,
        },
        head='master',
    )

    def test_a_tag_is_its_commit(self):
        found = resolve_commit(self.LISTING, 'v1.0.0')
        assert found is not None
        assert (found.ref, found.ref_type, found.commit_sha) == (
            'v1.0.0', 'release', 'c' * 40,
        )

    def test_a_tag_that_is_gone_is_the_default_branchs_head(self):
        found = resolve_commit(self.LISTING, 'v9.9.9')
        assert found is not None
        assert (found.ref, found.ref_type, found.commit_sha) == (
            'master', 'branch', 'a' * 40,
        )

    def test_no_tag_is_the_branch_head_points_at(self):
        found = resolve_commit(self.LISTING, None)
        assert found is not None
        assert (found.ref, found.commit_sha) == ('master', 'a' * 40)

    def test_a_head_without_a_branch_is_still_collected(self):
        found = resolve_commit(RemoteRefs(refs={'HEAD': 'd' * 40}), None)
        assert found is not None
        assert (found.ref, found.commit_sha) == ('HEAD', 'd' * 40)

    def test_nothing_resolves_in_an_empty_listing(self):
        assert resolve_commit(RemoteRefs(), 'v1.0.0') is None


class TestTheTreeStage:
    def test_lists_every_file_at_the_commit(self, world):
        repository = world.upstream.add(1, 'octo/one')
        sha = repository.commit({
            'package.json': '{}', 'app/go.mod': 'module x\n', 'README.md': 'hi',
        })
        repository.commit({'later.txt': 'not at the commit'})

        done = world.stages(repository, lambda s: s.tree(sha))

        stored = world.paths.tree_file(1, sha).read_text()
        assert stored.splitlines() == [
            'README.md', 'app/go.mod', 'package.json',
        ]
        assert stored.endswith('\n')
        assert '3 paths' in done.summary

    def test_finds_nothing_at_a_commit_with_no_files(self, world):
        repository = world.upstream.add(1, 'octo/one')
        sha = repository.commit({})
        with pytest.raises(Nothing, match='no files'):
            world.stages(repository, lambda s: s.tree(sha))
        assert not world.paths.tree_file(1, sha).exists()

    def test_fails_for_a_commit_git_cannot_fetch(self, world):
        repository = world.upstream.add(1, 'octo/one')
        repository.commit({'package.json': '{}'})
        with pytest.raises(StageFailed, match='git'):
            world.stages(repository, lambda s: s.tree('f' * 40))


class TestTheContentStage:
    def _tree(self, world: World, files: dict[str, str]) -> tuple[Repository, str]:
        repository = world.upstream.add(1, 'octo/one')
        sha = repository.commit(files)
        world.stages(repository, lambda s: s.tree(sha))
        return repository, sha

    def test_fetches_the_manifests_at_the_commit_and_stamps_them(
        self, world,
    ):
        repository, sha = self._tree(
            world, {
                'package.json': '{"name": "one"}', 'app/go.mod': 'module x\n',
                'src/main.go': 'package main',
            },
        )

        done = world.stages(repository, lambda s: s.content(sha))

        root = world.paths.content_root(1, sha)
        assert (root / 'package.json').read_text() == '{"name": "one"}'
        assert (root / 'app' / 'go.mod').read_text() == 'module x\n'
        assert not (root / 'src' / 'main.go').exists()
        document = json.loads(world.paths.discovery_file(1, sha).read_text())
        assert document[VERSION_FIELD] == CONTENT_VERSION
        assert document['commit_sha'] == sha
        assert {e['path']: e['status'] for e in document['selected']} == {
            'package.json': 'ok', 'app/go.mod': 'ok',
        }
        assert done.output == document['digest']

    def test_asks_raw_content_with_no_token(self, world):
        repository, sha = self._tree(world, {'package.json': '{}'})
        world.stages(repository, lambda s: s.content(sha))
        [request] = world.upstream.raw.requests
        assert request.path == 'package.json'
        assert 'authorization' not in request.headers
        assert TOKEN not in json.dumps(request.headers)

    def test_fetches_a_root_the_old_pipeline_left_again_whole(self, world):
        """#100 Q4, strict: no stamp, so every file is fetched again,
        whatever is on disk."""
        repository, sha = self._tree(
            world, {
                'package.json': '{"v": 2}', 'go.mod': 'module x\n',
            },
        )
        root = world.paths.content_root(1, sha)
        root.mkdir(parents=True)
        (root / 'package.json').write_text('{"v": "cut')
        (root / 'go.mod').write_text('module x\n')
        index = world.paths.discovery_file(1, sha)
        index.write_text(json.dumps({'commit_sha': sha, 'selected': []}))

        world.stages(repository, lambda s: s.content(sha))

        assert sorted(r.path for r in world.upstream.raw.requests) == [
            'go.mod', 'package.json',
        ]
        assert (root / 'package.json').read_text() == '{"v": 2}'
        assert json.loads(index.read_text())[VERSION_FIELD] == CONTENT_VERSION

    def test_asks_again_only_for_what_may_yet_be_fetched(self, world):
        repository, sha = self._tree(
            world, {
                'package.json': '{}', 'go.mod': 'module x\n',
            },
        )
        world.upstream.raw.fail('go.mod', 502, 502, 502)
        with pytest.raises(StageFailed, match='1 of 2 files'):
            world.stages(repository, lambda s: s.content(sha))
        document = json.loads(world.paths.discovery_file(1, sha).read_text())
        assert {e['path']: e['status'] for e in document['selected']} == {
            'package.json': 'ok', 'go.mod': 'http-502',
        }
        assert document[VERSION_FIELD] == CONTENT_VERSION
        before = len(world.upstream.raw.requests)

        world.stages(repository, lambda s: s.content(sha))

        assert [r.path for r in world.upstream.raw.requests[before:]] == [
            'go.mod',
        ]

    def test_retries_a_passing_server_error_before_giving_up(self, world):
        repository, sha = self._tree(world, {'package.json': '{}'})
        world.upstream.raw.fail('package.json', 503)
        world.stages(repository, lambda s: s.content(sha))
        assert [r.status for r in world.upstream.raw.requests] == [503, 200]

    def test_leaves_out_a_file_over_the_per_file_cap(self, world):
        repository, sha = self._tree(
            world, {
                'package.json': '{"big": "' + 'x' * MAX_FILE_BYTES + '"}',
                'go.mod': 'm\n',
            },
        )
        world.stages(repository, lambda s: s.content(sha))
        document = json.loads(world.paths.discovery_file(1, sha).read_text())
        assert {e['path']: e['status'] for e in document['selected']} == {
            'package.json': 'over-file-byte-cap', 'go.mod': 'ok',
        }


class TestTheSbomStage:
    def _content(
        self, world: World, files: dict[str, str],
    ) -> tuple[Repository, str]:
        repository = world.upstream.add(1, 'octo/one')
        sha = repository.commit(files)

        async def through(stages: RepositoryStages) -> None:
            await stages.tree(sha)
            await stages.content(sha)

        world.stages(repository, through)
        return repository, sha

    def test_scans_the_content_root_in_the_pool(self, world):
        repository, sha = self._content(world, {'package.json': '{}'})
        done = world.stages(repository, lambda s: s.sbom(sha, '1.52.0'))
        document = json.loads(world.paths.sbom_file(1, sha).read_text())
        assert document['descriptor']['version'] == '1.52.0'
        assert [a['name'] for a in document['artifacts']] == ['package.json']
        [scan] = world.syft.scans
        assert scan['scan'] == str(world.paths.content_root(1, sha).absolute())
        assert '1 package' in done.summary

    def test_reuses_a_scan_of_the_same_content_from_the_cache(self, world):
        """A push that changes no manifest has the same content at a new
        commit: Syft is not run for it again."""
        repository, sha = self._content(world, {'package.json': '{}'})
        world.stages(repository, lambda s: s.sbom(sha, '1.52.0'))
        later = repository.commit({'src/app.js': 'code'})

        async def through(stages: RepositoryStages) -> Any:
            await stages.tree(later)
            await stages.content(later)
            return await stages.sbom(later, '1.52.0')

        done = world.stages(repository, through)
        assert len(world.syft.scans) == 1
        assert world.paths.sbom_file(1, later).read_bytes() == (
            world.paths.sbom_file(1, sha).read_bytes()
        )
        assert 'cache' in done.summary

    def test_scans_with_the_lockfile_sbom_lock_generated(self, world):
        repository, sha = self._content(world, {'composer.json': '{}'})
        locks = world.paths.generated_lock_path(1, sha)
        locks.mkdir(parents=True)
        (locks / 'composer.lock').write_text('{"packages": []}')

        world.stages(repository, lambda s: s.sbom(sha, '1.52.0'))

        document = json.loads(world.paths.sbom_file(1, sha).read_text())
        assert sorted(a['name'] for a in document['artifacts']) == [
            'composer.json', 'composer.lock',
        ]
        # A copy was scanned, and the content root was left as it was.
        root = world.paths.content_root(1, sha)
        assert not (root / 'composer.lock').exists()

    def test_that_fails_keeps_no_sbom(self, world):
        repository, sha = self._content(world, {'package.json': '{}'})
        world.syft.configure(exit=1, stderr='could not determine source')
        with pytest.raises(SyftFailed):
            world.stages(repository, lambda s: s.sbom(sha, '1.52.0'))
        assert not world.paths.sbom_file(1, sha).exists()


def test_a_content_file_newer_than_its_sbom_is_left_to_the_due_set(world):
    """The stage writes; whether what it wrote is current is the due
    set's to say (`collector/due.py`), from the times."""
    repository = world.upstream.add(1, 'octo/one')
    sha = repository.commit({'package.json': '{}'})

    async def through(stages: RepositoryStages) -> None:
        await stages.tree(sha)
        await stages.content(sha)
        await stages.sbom(sha, '1.52.0')

    world.stages(repository, through)
    stored = world.paths.content_root(1, sha) / 'package.json'
    sbom = world.paths.sbom_file(1, sha)
    assert os.stat(sbom).st_mtime >= os.stat(stored).st_mtime
