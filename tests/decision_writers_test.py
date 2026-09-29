"""Who writes the release and commit decisions (#147).

`chatsbom run`'s release and commit stages, and the stage-major `github
release` and `github commit`, each keep what their stage decided in the
store, beside what they wrote before: the record `RecordStore` lands in
`raw_documents` at the end of `run`'s chain, and the per-language JSONL
lists, are written as they were.

The services are faked; the commands, the walk, the ledger and the
files are real, under a fresh data directory.
"""
from __future__ import annotations

import json
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.commands import run as run_command
from chatsbom.commands.github import commit as commit_command
from chatsbom.commands.github import release as release_command
from chatsbom.core import decisions
from chatsbom.core.config import PathConfig
from chatsbom.core.container import Container
from chatsbom.core.decisions import Outcome
from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import Stage
from chatsbom.models.repository import Repository

UTC = timezone.utc
NOW = datetime(2026, 9, 29, 13, 0, tzinfo=UTC)
PUSHED = datetime(2026, 9, 29, 12, 28, 14, tzinfo=UTC)
TAGGED = 'c' * 40
HEAD = 'd' * 40

V2 = {
    'id': 2, 'tag_name': 'v2.0.0', 'name': 'v2.0.0',
    'published_at': '2026-09-01T00:00:00Z', 'is_prerelease': False,
    'is_draft': False, 'created_at': '2026-09-01T00:00:00Z', 'assets': [],
    'source': 'github_release', 'target_commitish': 'main',
}

runner = CliRunner()


class FakeReleases:
    """The release stage, as `ReleaseService` leaves a repository: every
    release, and the latest stable one. None when the fetch failed."""

    def __init__(
        self,
        releases: list[dict[str, Any]] | None = None,
        failed: bool = False,
    ) -> None:
        self.releases = [V2] if releases is None else releases
        self.failed = failed
        self.asked: list[int] = []

    def process_repo(
        self, repository: Repository, stats: Any, language: str,
    ) -> dict[str, Any] | None:
        self.asked.append(repository.id)
        if self.failed:
            return None
        return {
            **repository.model_dump(mode='json'),
            'has_releases': bool(self.releases),
            'total_releases': len(self.releases),
            'all_releases': self.releases,
            'latest_stable_release': self.releases[0] if self.releases else None,
        }


class FakeCommits:
    """The commit stage: the tag's commit, or the default branch's."""

    def __init__(self) -> None:
        self.asked: list[int] = []

    def process_repo(
        self, repository: Repository, stats: Any, language: str,
    ) -> dict[str, Any] | None:
        self.asked.append(repository.id)
        release = repository.latest_stable_release
        target = (
            {'ref': release.tag_name, 'ref_type': 'release', 'commit_sha': TAGGED}
            if release else
            {'ref': 'main', 'ref_type': 'branch', 'commit_sha': HEAD}
        )
        return {
            **repository.model_dump(mode='json'),
            'download_target': {
                **target, 'commit_sha_short': target['commit_sha'][:7],
            },
        }


def files(root: Path) -> list[str]:
    return sorted(
        str(path.relative_to(root)) for path in root.rglob('*')
        if path.is_file()
    )


# -- chatsbom run -----------------------------------------------------------


@pytest.fixture
def paths(tmp_path: Path) -> PathConfig:
    return PathConfig(base_data_dir=tmp_path / 'data')


def walk(
    paths: PathConfig,
    releases: FakeReleases,
    commits: FakeCommits | None = None,
    pushed: datetime = PUSHED,
    stage: Stage = Stage.COMMIT,
) -> Any:
    """`run --stage commit` for repository 7, pushed at `pushed`: the
    walk up to the commit stage, as `chatsbom run` makes it."""
    paths.base_data_dir.mkdir(parents=True, exist_ok=True)
    with Ledger(paths.ledger_path) as ledger:
        ledger.track(7, 'acme', 'app', 'python')
        ledger.record_push(7, pushed, pushed + timedelta(minutes=5))
    container = SimpleNamespace(
        config=SimpleNamespace(paths=paths),
        get_release_service=lambda token: releases,
        get_commit_service=lambda token: commits or FakeCommits(),
    )
    return run_command.advance(
        container, 'token', limit=10, quota=100, stage=stage,
    )


class TestRun:

    def test_the_walk_keeps_both_decisions(self, paths: PathConfig) -> None:
        result = walk(paths, FakeReleases())

        assert result.failed == 0
        [listing] = (paths.release_dir / '7' / 'releases').iterdir()
        assert files(paths.release_dir) == [
            '7/20260929T122814Z/release@2.json', f'7/releases/{listing.name}',
        ]
        assert files(paths.commit_dir) == ['7/tag-v2.0.0/commit@1.json']
        chain = decisions.newest(paths, 7)
        assert chain is not None and chain.commit is not None
        assert chain.release.tag == 'v2.0.0'
        assert chain.commit.commit_sha == TAGGED

    def test_with_no_release_the_head_is_decided(self, paths: PathConfig) -> None:
        walk(paths, FakeReleases([]))

        assert files(paths.commit_dir) == [
            '7/head-20260929T122814Z/commit@1.json',
        ]

    def test_a_new_push_with_the_same_releases_adds_only_its_decision(
        self, paths: PathConfig,
    ) -> None:
        walk(paths, FakeReleases())
        walk(
            paths, FakeReleases(), pushed=PUSHED + timedelta(days=1),
            stage=Stage.RELEASE,
        )

        assert len(list((paths.release_dir / '7' / 'releases').iterdir())) == 1
        assert sorted(
            p.name for p in (paths.release_dir / '7').iterdir()
        ) == ['20260929T122814Z', '20260930T122814Z', 'releases']

    def test_releases_that_could_not_be_fetched_decide_nothing(
        self, paths: PathConfig,
    ) -> None:
        """The walk goes on to the commit stage, which takes the default
        branch: no decision, for a push whose release is not known."""
        commits = FakeCommits()

        walk(paths, FakeReleases(failed=True), commits)

        assert commits.asked == [7]
        assert not paths.release_dir.exists()
        assert not paths.commit_dir.exists()

    def test_a_decision_kept_already_stands_and_the_stage_succeeds(
        self, paths: PathConfig,
    ) -> None:
        other = {**V2, 'tag_name': 'v1.0.0', 'name': 'v1.0.0'}
        decisions.keep_release(
            paths, {
                'id': 7, 'pushed_at': PUSHED.isoformat(),
                'all_releases': [other], 'latest_stable_release': other,
            },
        )
        stored = paths.release_dir / '7' / '20260929T122814Z' / 'release@2.json'
        before = stored.read_bytes()

        result = walk(paths, FakeReleases(), stage=Stage.RELEASE)

        assert result.failed == 0
        assert stored.read_bytes() == before
        with Ledger(paths.ledger_path) as ledger:
            state = ledger.stage_state(7, Stage.RELEASE)
        assert state is not None and state.outcome == 'ok'

    def test_a_store_that_cannot_be_written_fails_the_stage(
        self, paths: PathConfig,
    ) -> None:
        """The decision is the stage's output: not kept, it did not run."""
        paths.base_data_dir.mkdir(parents=True)
        paths.release_dir.write_text('not a directory')

        result = walk(paths, FakeReleases(), stage=Stage.RELEASE)

        assert result.failed == 1
        with Ledger(paths.ledger_path) as ledger:
            state = ledger.stage_state(7, Stage.RELEASE)
        assert state is not None and state.outcome == 'failed'


# -- github release and github commit ------------------------------------


@pytest.fixture
def stage_major(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> SimpleNamespace:
    """A working directory with `data/`, and the two commands' services
    faked."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr('chatsbom.core.config._config', None)
    monkeypatch.setattr(Container, '_instance', None)
    for module in (release_command, commit_command):
        monkeypatch.setattr(
            module, 'verify_github_token', lambda *a, **k: 'octocat',
        )
    world = SimpleNamespace(releases=FakeReleases(), commits=FakeCommits())
    monkeypatch.setattr(
        Container, 'get_release_service',
        lambda self, token=None: world.releases,
    )
    monkeypatch.setattr(
        Container, 'get_commit_service',
        lambda self, token=None: world.commits,
    )
    world.paths = PathConfig(base_data_dir=Path('data'))
    return world


def listed(path: Path, *records: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(''.join(json.dumps(r) + '\n' for r in records))


REPO: dict[str, Any] = {
    'id': 7, 'owner': 'acme', 'name': 'app', 'stargazers_count': 1000,
    'pushed_at': '2026-09-29T12:28:14Z',
}


def command(*arguments: str) -> None:
    result = runner.invoke(
        app, ['github', *arguments, '--language', 'python', '--token', 't'],
    )
    assert result.exit_code == 0, result.output


class TestGitHubRelease:

    def test_it_keeps_the_release_decision(
        self, stage_major: SimpleNamespace,
    ) -> None:
        paths = stage_major.paths
        listed(paths.get_repo_list_path('python'), REPO)

        command('release')

        assert decisions.has_release(paths, 7, REPO['pushed_at'])
        assert stage_major.releases.asked == [7]
        # The list it wrote before, as it was.
        [line] = paths.get_release_list_path('python').read_text().splitlines()
        latest = json.loads(line)['latest_stable_release']
        assert latest['tag_name'] == 'v2.0.0'

    def test_a_push_decided_already_is_not_asked_again(
        self, stage_major: SimpleNamespace,
    ) -> None:
        paths = stage_major.paths
        listed(paths.get_repo_list_path('python'), REPO)
        command('release')

        command('release')

        assert stage_major.releases.asked == [7]

    def test_a_new_push_is_decided_though_its_list_has_the_repository(
        self, stage_major: SimpleNamespace,
    ) -> None:
        """The list was deduplicated by repository id, so a repository
        collected again never had its new releases kept (README's known
        gap). The store is keyed by the push."""
        paths = stage_major.paths
        listed(paths.get_repo_list_path('python'), REPO)
        command('release')
        listed(
            paths.get_repo_list_path('python'),
            {**REPO, 'pushed_at': '2026-10-03T08:15:00Z'},
        )

        command('release')

        assert stage_major.releases.asked == [7, 7]
        assert decisions.has_release(paths, 7, '2026-10-03T08:15:00Z')
        lines = paths.get_release_list_path('python').read_text().splitlines()
        assert len(lines) == 1

    def test_force_asks_again_and_the_decision_stands(
        self, stage_major: SimpleNamespace,
    ) -> None:
        paths = stage_major.paths
        listed(paths.get_repo_list_path('python'), REPO)
        command('release')
        before = files(paths.release_dir)

        command('release', '--force')

        assert stage_major.releases.asked == [7, 7]
        assert files(paths.release_dir) == before


class TestGitHubCommit:

    def test_it_keeps_the_commit_decision(
        self, stage_major: SimpleNamespace,
    ) -> None:
        paths = stage_major.paths
        listed(paths.get_repo_list_path('python'), REPO)
        command('release')

        command('commit')

        assert files(paths.commit_dir / '7') == ['tag-v2.0.0/commit@1.json']
        assert stage_major.commits.asked == [7]
        # The list it wrote before, beside the repositories' directories.
        assert paths.get_commit_list_path('python').is_file()

    def test_a_key_decided_already_is_not_asked_again(
        self, stage_major: SimpleNamespace,
    ) -> None:
        paths = stage_major.paths
        listed(paths.get_repo_list_path('python'), REPO)
        command('release')
        command('commit')

        command('commit')

        assert stage_major.commits.asked == [7]

    def test_a_record_without_the_release_stages_output_decides_nothing(
        self, stage_major: SimpleNamespace,
    ) -> None:
        paths = stage_major.paths
        listed(paths.get_release_list_path('python'), REPO)

        command('commit')

        assert stage_major.commits.asked == [7]
        assert not paths.commit_dir.joinpath('7').exists()


def test_keeping_says_what_it_did(paths: PathConfig) -> None:
    """The outcomes the backfill reports, and the stages log."""
    record = {
        'id': 7, 'pushed_at': '2026-09-29T12:28:14Z', 'all_releases': [V2],
        'latest_stable_release': V2, 'has_releases': True,
        'download_target': {
            'ref': 'v2.0.0', 'ref_type': 'release', 'commit_sha': TAGGED,
            'commit_sha_short': TAGGED[:7],
        },
    }
    assert decisions.keep_release(paths, record).decision is Outcome.WRITTEN
    assert decisions.keep_commit(paths, record) is Outcome.WRITTEN
    assert decisions.keep_commit(paths, record) is Outcome.KEPT
