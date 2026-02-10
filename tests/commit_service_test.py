from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from chatsbom.models.github_release import GitHubRelease
from chatsbom.models.repository import Repository
from chatsbom.services.commit_service import CommitService
from chatsbom.services.commit_service import CommitStats


@pytest.fixture
def mock_git():
    return MagicMock()


@pytest.fixture
def commit_service(mock_git, tmp_path):
    with patch('chatsbom.services.commit_service.get_config') as mock_config:
        mock_config.return_value.paths.get_git_refs_cache_path.return_value = tmp_path / 'index.json'
        return CommitService(mock_git)


def test_process_repo_via_git_remote(commit_service, mock_git):
    repo = Repository(
        id=1,
        owner='owner',
        repo='repo',
        url='https://github.com/owner/repo',
        stars=100,
        default_branch='main',
    )

    # Return (sha, is_cached, num_refs)
    mock_git.resolve_ref.return_value = ('git_sha_12345', False, 10)
    stats = CommitStats(total=1)

    result = commit_service.process_repo(repo, stats, 'go')

    assert result is not None
    assert repo.download_target.commit_sha == 'git_sha_12345'
    assert stats.enriched == 1
    assert stats.api_requests == 1
    assert stats.cache_hits == 0


def test_process_repo_via_git_cache(commit_service, mock_git):
    repo = Repository(
        id=1,
        owner='owner',
        repo='repo',
        url='https://github.com/owner/repo',
        stars=100,
        default_branch='main',
    )

    # Return (sha, is_cached, num_refs)
    mock_git.resolve_ref.return_value = ('git_sha_cache', True, 5)
    stats = CommitStats(total=1)

    result = commit_service.process_repo(repo, stats, 'go')

    assert result is not None
    assert repo.download_target.commit_sha == 'git_sha_cache'
    assert stats.enriched == 1
    assert stats.api_requests == 0
    assert stats.cache_hits == 1


def test_process_repo_fallback_logic(commit_service, mock_git):
    repo = Repository(
        id=1,
        owner='owner',
        repo='repo',
        url='https://github.com/owner/repo',
        stars=100,
        default_branch='main',
        latest_stable_release=GitHubRelease(tag_name='v1.0.0', id=123),
    )

    # First call (tag) fails, second call (branch) succeeds
    mock_git.resolve_ref.side_effect = [
        (None, False, 10), ('branch_sha', False, 10),
    ]
    stats = CommitStats(total=1)

    result = commit_service.process_repo(repo, stats, 'go')

    assert result is not None
    assert repo.download_target.ref == 'main'
    assert repo.download_target.commit_sha == 'branch_sha'
    assert mock_git.resolve_ref.call_count == 2
    # In the current implementation, it updates stats based on the last call's is_cached
    assert stats.api_requests == 1
