from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from chatsbom.models.download_target import DownloadTarget
from chatsbom.models.language import Language
from chatsbom.models.repository import Repository
from chatsbom.services.content_service import ContentService
from chatsbom.services.content_service import ContentStats


@pytest.fixture
def content_service(tmp_path):
    with patch('chatsbom.services.content_service.get_config') as mock_config:
        mock_config.return_value.paths.content_dir = tmp_path
        service = ContentService('fake_token')
        return service


def test_content_service_init(content_service):
    assert content_service.session.headers['Authorization'] == 'Bearer fake_token'


def test_content_service_timeout():
    """Test custom timeout."""
    service = ContentService('token', timeout=30)
    assert service.timeout == 30


def test_process_repo_success(tmp_path):
    """Test successful download creates files."""
    with patch('chatsbom.services.content_service.get_config') as mock_config:
        mock_config.return_value.paths.content_dir = tmp_path
        service = ContentService('fake_token')

        # Mock the session's get method
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'module github.com/owner/repo'
        mock_response.from_cache = False
        service.session.get = MagicMock(return_value=mock_response)

        repo = Repository(
            id=1, owner='owner', repo='repo',
            stargazers_count=10, default_branch='main',
        )
        # Setup download target to avoid "unknown"
        repo.download_target = MagicMock()
        repo.download_target.ref = 'v1.0'
        repo.download_target.commit_sha = 'a1b2c3d'

        lang = Language.GO

        result_dict = service.process_repo(repo, lang)

        assert result_dict is not None
        assert result_dict['owner'] == 'owner'
        assert result_dict['repo'] == 'repo'
        assert 'local_content_path' in result_dict

        # Check file was created
        # Structure: base / lang / owner / name / ref / commit_sha / filename
        target_file = tmp_path / 'go' / 'owner' / \
            'repo' / 'v1.0' / 'a1b2c3d' / 'go.mod'
        assert target_file.exists()
        assert target_file.read_bytes() == b'module github.com/owner/repo'


def test_a_release_is_downloaded_by_commit_not_by_tag(tmp_path):
    """The files are stored under the commit the commit stage resolved.

    Release targets were fetched by tag name instead. A tag that had
    moved on since (`v1`, `latest`, `nightly`) stored another commit's
    files under the resolved sha, and a "tag" that was really a branch
    was fetched from `refs/tags/<branch>`, which does not exist.
    """
    sha = '0123456789abcdef0123456789abcdef01234567'
    with patch('chatsbom.services.content_service.get_config') as mock_config:
        mock_config.return_value.paths.content_dir = tmp_path
        service = ContentService('fake_token')

        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b'module github.com/owner/repo'
        service.session.get = MagicMock(return_value=mock_response)

        repo = Repository(
            id=1, owner='owner', repo='repo',
            stargazers_count=10, default_branch='main',
        )
        repo.download_target = DownloadTarget(
            ref='v1.0.0', ref_type='release',
            commit_sha=sha, commit_sha_short=sha[:7],
        )

        assert service.process_repo(repo, Language.GO) is not None

    urls = [call.args[0] for call in service.session.get.call_args_list]
    assert urls, 'nothing was requested'
    for url in urls:
        assert url.startswith(
            f'https://raw.githubusercontent.com/owner/repo/{sha}/',
        ), url
        assert '/refs/tags/' not in url
    assert (
        tmp_path / 'go' / 'owner' / 'repo' /
        'v1.0.0' / sha / 'go.mod'
    ).exists()


class TestContentStats:
    """Tests for ContentStats dataclass."""

    def test_default_values(self):
        """Test default values are correct."""
        result = ContentStats(repo='test/repo')
        assert result.downloaded_files == 0
        assert result.missing_files == 0
        assert result.failed == 0
        assert result.skipped == 0
        assert result.cache_hits == 0
