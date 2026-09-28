"""The commit stage: which commit a repository is collected at.

Driven through a real `GitService` whose `git ls-remote` answers from a
string, so what is tested is the resolution itself, not a mock's echo.
"""
import json
from unittest.mock import patch

import pytest

from chatsbom.models.github_release import GitHubRelease
from chatsbom.models.repository import Repository
from chatsbom.services.commit_service import CommitService
from chatsbom.services.commit_service import CommitStats
from chatsbom.services.git_service import GitService
from chatsbom.services.git_service import parse_ls_remote

MASTER = 'a' * 40
DEVELOP = 'b' * 40
V1 = 'c' * 40
V1_TAG_OBJECT = 'd' * 40

#: `git ls-remote --symref` of a repository whose default branch is
#: `master`, as most of the corpus's is (36,692 of 65,457 in the ledger).
ON_MASTER = '\n'.join([
    'ref: refs/heads/master\tHEAD',
    f'{MASTER}\tHEAD',
    f'{DEVELOP}\trefs/heads/develop',
    f'{MASTER}\trefs/heads/master',
    f'{V1_TAG_OBJECT}\trefs/tags/v1.0.0',
    f'{V1}\trefs/tags/v1.0.0^{{}}',
])


class FakeGit:
    """`git ls-remote`, answered from a string instead of github.com."""

    def __init__(self, output: str) -> None:
        self.output = output
        self.calls: list[dict[str, object]] = []

    def ls_remote(self, url: str, **kwargs: object) -> str:
        self.calls.append(kwargs)
        return self.output


@pytest.fixture
def cache(tmp_path):
    return tmp_path / 'git' / 'refs' / 'index.json'


def service_over(output: str, cache) -> tuple[CommitService, FakeGit]:
    git = FakeGit(output)
    service = GitService()
    # A stand-in that answers `ls_remote` alone, not a GitPython Git.
    service.g = git  # type: ignore[assignment]
    with patch('chatsbom.services.commit_service.get_config') as config:
        config.return_value.paths.get_git_refs_cache_path.return_value = cache
        return CommitService(service), git


def repository(**fields) -> Repository:
    return Repository.model_validate(
        {'id': 1, 'owner': 'owner', 'name': 'repo', **fields},
    )


def test_no_release_collects_the_branch_head_points_at(cache):
    """G-Joker/WeaponApp, aporter/coursera-android, SpringAll: no
    release, default branch `master`. The ledger row gave no branch, the
    model said `'main'`, and "Failed to resolve commit … ref='main'"
    stopped the repository after its release stage."""
    service, git = service_over(ON_MASTER, cache)
    repo = repository()
    stats = CommitStats(total=1)

    result = service.process_repo(repo, stats, '')

    assert result is not None
    target = repo.download_target
    assert (target.ref, target.ref_type, target.commit_sha) == (
        'master', 'branch', MASTER,
    )
    assert repo.default_branch == 'master'
    assert result['default_branch'] == 'master'
    assert stats.enriched == 1
    # One listing, which named HEAD's branch because it was asked to.
    assert [call.get('symref') for call in git.calls] == [True]


def test_a_stale_branch_name_is_not_believed(cache):
    """The snapshot's default branch goes stale on a rename: HEAD is
    asked, not the name the repository arrived with."""
    service, _ = service_over(ON_MASTER, cache)
    repo = repository(default_branch='main')

    service.process_repo(repo, CommitStats(total=1), '')

    assert repo.download_target.ref == 'master'
    assert repo.download_target.commit_sha == MASTER
    assert repo.default_branch == 'master'


def test_a_release_is_collected_at_its_tag(cache):
    service, _ = service_over(ON_MASTER, cache)
    repo = repository(
        latest_stable_release=GitHubRelease(tag_name='v1.0.0', id=1),
    )

    service.process_repo(repo, CommitStats(total=1), '')

    target = repo.download_target
    assert (target.ref, target.ref_type, target.commit_sha) == (
        'v1.0.0', 'release', V1,
    )


def test_a_tag_that_is_gone_falls_back_to_heads_branch(cache):
    service, git = service_over(ON_MASTER, cache)
    repo = repository(
        latest_stable_release=GitHubRelease(tag_name='v9.9.9', id=1),
    )

    service.process_repo(repo, CommitStats(total=1), '')

    target = repo.download_target
    assert (target.ref, target.ref_type, target.commit_sha) == (
        'master', 'branch', MASTER,
    )
    # The fallback read the listing the tag lookup had just cached.
    assert len(git.calls) == 1


def test_the_listing_is_cached_with_heads_branch(cache):
    service, git = service_over(ON_MASTER, cache)
    service.process_repo(repository(), CommitStats(total=1), '')

    stored = json.loads(cache.read_text(encoding='utf-8'))
    assert stored['head'] == 'master'

    again, offline = service_over('', cache)
    stats = CommitStats(total=1)
    repo = repository()
    again.process_repo(repo, stats, '')
    assert repo.download_target.ref == 'master'
    assert offline.calls == []
    assert stats.cache_hits == 1
    # `git ls-remote` spends no REST quota.
    assert stats.api_requests == 0


def test_a_cache_from_before_the_branch_was_kept_is_listed_again(cache):
    """Every refs cache on disk predates `head`: it names no branch, so
    it cannot answer "which branch", and is listed again."""
    cache.parent.mkdir(parents=True)
    refs, _ = parse_ls_remote(ON_MASTER, GitService()._get_short_name)
    cache.write_text(
        json.dumps({
            'url': 'https://github.com/owner/repo.git',
            'updated_at': '2999-01-01T00:00:00+00:00',
            'data': refs,
        }),
    )
    service, git = service_over(ON_MASTER, cache)
    repo = repository()

    service.process_repo(repo, CommitStats(total=1), '')

    assert len(git.calls) == 1
    assert repo.download_target.ref == 'master'


def test_an_empty_repository_is_not_collected(cache):
    service, _ = service_over('', cache)
    stats = CommitStats(total=1)

    assert service.process_repo(repository(), stats, '') is None
    assert stats.failed == 1


def test_a_head_without_a_named_branch_is_still_collected(cache):
    service, _ = service_over(f'{MASTER}\tHEAD\n{MASTER}\trefs/heads/x', cache)
    repo = repository()

    service.process_repo(repo, CommitStats(total=1), '')

    assert repo.download_target.ref == 'HEAD'
    assert repo.download_target.commit_sha == MASTER
    assert repo.default_branch == ''


def test_parse_ls_remote_reads_the_symref_apart_from_the_refs():
    refs, head = parse_ls_remote(ON_MASTER, GitService()._get_short_name)

    assert head == 'master'
    assert refs['HEAD'] == MASTER
    assert refs['master'] == refs['refs/heads/master'] == MASTER
    assert refs['v1.0.0'] == V1
    assert not any(key.startswith('ref:') for key in refs)
    assert not any('\t' in key for key in refs)
