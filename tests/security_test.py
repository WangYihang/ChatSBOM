from unittest.mock import patch

import git
from structlog.testing import capture_logs

from chatsbom.services.git_service import GitService


@patch('chatsbom.services.git_service.git.cmd.Git')
def test_git_service_token_masking(mock_git_class):
    token = 'secret_github_pat_12345'
    mock_git_instance = mock_git_class.return_value
    service = GitService(token=token)

    # We need to make sure service.g is the mock instance
    service.g = mock_git_instance

    with capture_logs() as captured:
        # Mock ls_remote on the mock instance
        mock_git_instance.ls_remote.side_effect = git.GitCommandError(
            ['git', 'ls-remote'], 128, stderr=f"fatal: unable to access 'https://{token}@github.com/owner/repo.git/': 403",
        )

        service.get_repo_refs('owner', 'repo')

    # Check that the token is not in the logs
    for event in captured:
        if event['event'] == 'Git ls-remote failed':
            assert token not in event['url']
            assert '*****' in event['url']
            assert token not in event['error']
            assert '*****' in event['error']


def test_git_service_mask_url_directly():
    token = 'my-secret-token'
    service = GitService(token=token)
    url = f"https://{token}@github.com/owner/repo.git"
    masked = service._mask_url(url)
    assert token not in masked
    assert 'https://*****@github.com/owner/repo.git' == masked


def test_github_config_repr():
    from chatsbom.core.config import GitHubConfig
    config = GitHubConfig(token='secret-token')
    assert 'secret-token' not in repr(config)
    assert '*****' in repr(config)


def test_database_config_repr():
    from chatsbom.core.config import DatabaseConfig
    config = DatabaseConfig(password='secret-password')
    assert 'secret-password' not in repr(config)
    assert '*****' in repr(config)
