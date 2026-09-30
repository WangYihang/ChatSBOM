"""Retention for the release and commit decisions (#147).

A release decision is written for every push the collector sees, and a
commit decision for every tag it resolves and every head of a
repository with no release: two inodes each, and a block for each on
ext4, however small. Kept for good, they would outgrow the rest of the
store's inodes (README, "The repository-keyed layout").

`data prune` keeps what the current scan descends from, always (#100
Q13): the newest push whose key is resolved, its commit decision, the
list it names, and the scan itself. Beside those it keeps the `--keep`
newest release decisions of each repository, as it keeps that many
scans, the commit decisions those lead to or whose scan it keeps, and
the lists they name.
"""
from __future__ import annotations

import os
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.core import decisions
from chatsbom.core.config import PathConfig
from chatsbom.core.container import Container
from chatsbom.core.prune import current_scans
from chatsbom.core.prune import DecisionReport
from chatsbom.core.prune import prune_decisions

UTC = timezone.utc
S1 = '1' * 40
S2 = '2' * 40
S3 = '3' * 40
S4 = '4' * 40
#: Past the day a list no decision names is given.
LATER = datetime.now(UTC) + timedelta(days=2)

runner = CliRunner()


def release(tag: str, day: int) -> dict[str, Any]:
    published = f'2026-0{day}-01T00:00:00Z'
    return {
        'id': day, 'tag_name': tag, 'name': tag, 'published_at': published,
        'created_at': published, 'is_prerelease': False, 'is_draft': False,
        'assets': [], 'source': 'github_release', 'target_commitish': 'main',
    }


V1 = release('v1', 1)
V2 = release('v2', 2)
V3 = release('v3', 3)


def push(day: int) -> str:
    return f'2026-09-{day:02}T12:00:00Z'


def decide(
    paths: PathConfig,
    day: int,
    releases: list[dict[str, Any]],
    commit: str | None,
    repository_id: int = 1,
) -> None:
    """The release stage's decision for the push on `day`, and, with a
    `commit`, the commit stage's for its key."""
    tag = releases[0]['tag_name'] if releases else None
    record = {
        'id': repository_id, 'pushed_at': push(day),
        'all_releases': releases, 'has_releases': bool(releases),
        'latest_stable_release': releases[0] if releases else None,
        'download_target': {
            'ref': tag or 'main', 'ref_type': 'release' if tag else 'branch',
            'commit_sha': commit, 'commit_sha_short': (commit or '')[:7],
        } if commit else None,
    }
    assert decisions.keep_release(paths, record).decision is (
        decisions.Outcome.WRITTEN
    )
    if commit:
        decisions.keep_commit(paths, record)


def pushed(paths: PathConfig, repository_id: int = 1) -> list[str]:
    return [d.name for d in decisions.pushes(paths, repository_id)]


def keyed(paths: PathConfig, repository_id: int = 1) -> list[str]:
    return [d.name for d in decisions.keys(paths, repository_id)]


def lists(paths: PathConfig, repository_id: int = 1) -> set[str]:
    directory = decisions.releases_dir(paths, repository_id) / 'releases'
    return {p.stem for p in directory.iterdir()} if directory.is_dir() else set()


def digest(paths: PathConfig, day: int, repository_id: int = 1) -> str:
    directory = decisions.releases_dir(paths, repository_id) / (
        f'202609{day:02}T120000Z'
    )
    decision = decisions.read_release(directory, repository_id)
    assert decision is not None
    return decision.releases


@pytest.fixture
def paths(tmp_path: Path) -> PathConfig:
    return PathConfig(base_data_dir=tmp_path / 'data')


class TestTheRule:

    def test_the_newest_are_kept_with_what_they_name(
        self, paths: PathConfig,
    ) -> None:
        decide(paths, 1, [V1], S1)
        decide(paths, 2, [V1], S1)
        decide(paths, 3, [V2, V1], S2)
        decide(paths, 4, [V2, V1], S2)
        decide(paths, 5, [V3, V2, V1], S3)
        kept = {digest(paths, 4), digest(paths, 5)}

        report = prune_decisions(paths, keep=2, scans={}, now=LATER)

        assert pushed(paths) == ['20260904T120000Z', '20260905T120000Z']
        assert keyed(paths) == ['tag-v2', 'tag-v3']
        assert lists(paths) == kept
        assert report == DecisionReport(
            releases_kept=2, releases_removed=3,
            commits_kept=2, commits_removed=1,
            lists_kept=2, lists_removed=1,
            bytes_freed=report.bytes_freed,
        )
        assert report.bytes_freed > 0

    def test_what_the_current_scan_descends_from_is_never_removed(
        self, paths: PathConfig,
    ) -> None:
        """Three pushes since a new release whose commit is not decided
        yet: the scan in the store is still the first push's chain."""
        decide(paths, 1, [V1], S1)
        for day in (2, 3, 4):
            decide(paths, day, [V2, V1], None)

        prune_decisions(paths, keep=2, scans={}, now=LATER)

        assert pushed(paths) == [
            '20260901T120000Z', '20260903T120000Z', '20260904T120000Z',
        ]
        assert keyed(paths) == ['tag-v1']
        assert lists(paths) == {digest(paths, 1), digest(paths, 4)}

    def test_a_commit_decision_whose_scan_is_kept_is_kept(
        self, paths: PathConfig,
    ) -> None:
        decide(paths, 1, [V1], S1)
        decide(paths, 2, [V2, V1], S2)

        prune_decisions(paths, keep=1, scans={1: {S1, S2}}, now=LATER)

        assert pushed(paths) == ['20260902T120000Z']
        assert keyed(paths) == ['tag-v1', 'tag-v2']

    def test_heads_go_with_their_pushes(self, paths: PathConfig) -> None:
        """A repository with no release has a key per push."""
        for day, commit in ((1, S1), (2, S2), (3, S3)):
            decide(paths, day, [], commit)

        prune_decisions(paths, keep=2, scans={1: {S3}}, now=LATER)

        assert keyed(paths) == [
            'head-20260902T120000Z', 'head-20260903T120000Z',
        ]

    def test_a_list_no_decision_names_is_given_a_day(
        self, paths: PathConfig,
    ) -> None:
        """Its writer writes it before the decision that names it, and
        may be between the two."""
        decide(paths, 1, [V1], S1)
        decide(paths, 2, [V2, V1], S2)
        first, second = digest(paths, 1), digest(paths, 2)

        prune_decisions(paths, keep=1, scans={}, now=datetime.now(UTC))

        assert pushed(paths) == ['20260902T120000Z']
        assert lists(paths) == {first, second}

        prune_decisions(paths, keep=1, scans={}, now=LATER)

        assert lists(paths) == {second}

    def test_a_dry_run_removes_nothing_and_says_what_it_would(
        self, paths: PathConfig,
    ) -> None:
        decide(paths, 1, [V1], S1)
        decide(paths, 2, [V2, V1], S2)
        before = sorted(p for p in paths.base_data_dir.rglob('*'))

        report = prune_decisions(
            paths, keep=1, scans={}, now=LATER, dry_run=True,
        )

        assert sorted(p for p in paths.base_data_dir.rglob('*')) == before
        assert (report.releases_removed, report.commits_removed) == (1, 1)
        assert report.lists_removed == 1
        assert report.dry_run

    def test_repositories_are_pruned_on_their_own(
        self, paths: PathConfig,
    ) -> None:
        decide(paths, 1, [V1], S1, repository_id=1)
        decide(paths, 2, [V1], S1, repository_id=1)
        decide(paths, 1, [V1], S1, repository_id=2)

        prune_decisions(paths, keep=1, scans={}, now=LATER)

        assert pushed(paths, 1) == ['20260902T120000Z']
        assert pushed(paths, 2) == ['20260901T120000Z']

    def test_keep_must_be_positive(self, paths: PathConfig) -> None:
        with pytest.raises(ValueError, match='keep'):
            prune_decisions(paths, keep=0, scans={})


class TestWhatItCannotRead:

    def test_an_unreadable_push_is_left_and_so_are_the_lists(
        self, paths: PathConfig,
    ) -> None:
        """A version of the stage this code does not know may have written
        it, and may name a list: older code never deletes what newer code
        wrote."""
        decide(paths, 1, [V1], S1)
        decide(paths, 2, [V2, V1], S2)
        decide(paths, 3, [V3, V2, V1], S3)
        stranger = decisions.releases_dir(paths, 1) / '20260920T120000Z'
        stranger.mkdir()
        (stranger / 'release@9.json').write_text('{"a later shape": true}')
        before = lists(paths)

        report = prune_decisions(paths, keep=1, scans={}, now=LATER)

        assert stranger.is_dir()
        assert pushed(paths) == ['20260903T120000Z', '20260920T120000Z']
        assert lists(paths) == before
        assert report.unreadable == 1

    def test_commit_decisions_with_no_release_decision_to_go_by_are_kept(
        self, paths: PathConfig,
    ) -> None:
        decide(paths, 1, [V1], S1)
        decide(paths, 2, [V2, V1], S2)
        for directory in decisions.pushes(paths, 1):
            for file in directory.iterdir():
                file.unlink()
            directory.rmdir()

        prune_decisions(paths, keep=1, scans={}, now=LATER)

        assert keyed(paths) == ['tag-v1', 'tag-v2']


class TestTheCurrentScans:

    def test_each_repositorys_is_what_its_newest_resolved_chain_names(
        self, paths: PathConfig,
    ) -> None:
        decide(paths, 1, [V1], S1, repository_id=1)
        decide(paths, 2, [V2, V1], None, repository_id=1)
        decide(paths, 1, [], S2, repository_id=2)
        decide(paths, 1, [V1], None, repository_id=3)

        assert current_scans(paths) == {1: S1, 2: S2}


def resolutions(paths: PathConfig, repository_id: int = 1) -> list[str]:
    """Every commit decision of a repository: each key's first, and the
    later ones filed under their push."""
    directory = decisions.commits_dir(paths, repository_id)
    return sorted(
        str(path.parent.relative_to(directory))
        for path in directory.rglob('commit@*.json')
    )


class TestAMovedTag:
    """One tag chosen for three pushes and moved each time, as a `latest`
    or `nightly` tag is: its key is resolved again for each push, and
    the first resolution stands only for the pushes before the next."""

    @pytest.fixture
    def moved(self, paths: PathConfig) -> PathConfig:
        for day, commit in ((1, S1), (2, S2), (3, S3)):
            decide(paths, day, [V1], commit)
        return paths

    def test_the_current_scan_is_its_latest_resolution(
        self, moved: PathConfig,
    ) -> None:
        assert current_scans(moved) == {1: S3}

    def test_a_later_resolution_goes_with_its_push(
        self, moved: PathConfig,
    ) -> None:
        """The key's first stays while its directory does."""
        report = prune_decisions(moved, keep=1, scans={1: {S3}}, now=LATER)

        assert pushed(moved) == ['20260903T120000Z']
        assert resolutions(moved) == ['tag-v1', 'tag-v1/20260903T120000Z']
        assert (report.commits_kept, report.commits_removed) == (2, 1)

    def test_a_key_none_of_whose_resolutions_is_kept_goes_whole(
        self, moved: PathConfig,
    ) -> None:
        decide(moved, 4, [V2, V1], S4)

        report = prune_decisions(moved, keep=1, scans={1: {S4}}, now=LATER)

        assert resolutions(moved) == ['tag-v2']
        assert (report.commits_kept, report.commits_removed) == (1, 3)

    def test_a_resolution_whose_scan_is_kept_is_kept(
        self, moved: PathConfig,
    ) -> None:
        prune_decisions(moved, keep=1, scans={1: {S2, S3}}, now=LATER)

        assert resolutions(moved) == [
            'tag-v1', 'tag-v1/20260902T120000Z', 'tag-v1/20260903T120000Z',
        ]


# -- data prune ------------------------------------------------------------


def scan(root: Path, repository_id: int, sha: str, mtime: int) -> Path:
    directory = root / str(repository_id) / sha
    directory.mkdir(parents=True)
    (directory / 'sbom.json').write_text('{}')
    os.utime(directory, (mtime, mtime))
    return directory


@pytest.fixture
def store(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> PathConfig:
    """`data/` in a working directory of its own: a repository whose
    release was withdrawn, so its head was scanned, and then published
    again, so the current scan is the older one."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr('chatsbom.core.config._config', None)
    monkeypatch.setattr(Container, '_instance', None)
    paths = PathConfig(base_data_dir=Path('data'))
    decide(paths, 1, [V1], S1)
    decide(paths, 2, [], S2)
    decide(paths, 3, [V1], S1)
    for root in (paths.sbom_dir, paths.content_dir, paths.tree_dir):
        scan(root, 1, S1, 1000)
        scan(root, 1, S2, 2000)
    # The decisions are old enough for their lists to go.
    for path in paths.release_dir.rglob('*.json'):
        os.utime(path, (1000, 1000))
    return paths


def test_data_prune_reports_the_decisions_and_removes_nothing(
    store: PathConfig,
) -> None:
    before = sorted(p for p in store.base_data_dir.rglob('*'))

    result = runner.invoke(app, ['data', 'prune', '--keep', '1'])

    assert result.exit_code == 0, result.output
    assert 'Decisions' in result.stdout
    assert 'release decisions' in result.stdout
    assert sorted(p for p in store.base_data_dir.rglob('*')) == before


def test_data_prune_keeps_the_current_scan_and_what_it_descends_from(
    store: PathConfig,
) -> None:
    """The current scan beside the `--keep` newest, not in place of the
    newest: the head's scan stays, and so does the commit decision it
    was made for."""
    result = runner.invoke(app, ['data', 'prune', '--keep', '1', '--apply'])

    assert result.exit_code == 0, result.output
    for root in (store.sbom_dir, store.content_dir, store.tree_dir):
        assert sorted(p.name for p in (root / '1').iterdir()) == [S1, S2]
    assert pushed(store) == ['20260903T120000Z']
    assert keyed(store) == ['head-20260902T120000Z', 'tag-v1']
    assert lists(store) == {digest(store, 3)}


def test_data_prune_keeps_a_moved_tags_newest_scan(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A tag moved at every push, each commit scanned: the current scan
    is the tag's newest commit, not the first it was resolved to, and
    `--keep 1` keeps it."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr('chatsbom.core.config._config', None)
    monkeypatch.setattr(Container, '_instance', None)
    paths = PathConfig(base_data_dir=Path('data'))
    for day, commit in ((1, S1), (2, S2), (3, S3)):
        decide(paths, day, [V1], commit)
        for root in (paths.sbom_dir, paths.content_dir, paths.tree_dir):
            scan(root, 1, commit, 1000 * day)

    result = runner.invoke(app, ['data', 'prune', '--keep', '1', '--apply'])

    assert result.exit_code == 0, result.output
    for root in (paths.sbom_dir, paths.content_dir, paths.tree_dir):
        assert sorted(p.name for p in (root / '1').iterdir()) == [S3]
    assert resolutions(paths) == ['tag-v1', 'tag-v1/20260903T120000Z']
