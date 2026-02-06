from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from chatsbom.commands.download import DownloadResult
from chatsbom.commands.download import SBOMDownloader
from chatsbom.models.language import Language


@pytest.fixture
def downloader(tmp_path):
    return SBOMDownloader('fake_token', str(tmp_path))


def test_downloader_init(downloader):
    assert downloader.session.headers['Authorization'] == 'Bearer fake_token'


def test_downloader_base_dir(tmp_path):
    """Test base_dir is set correctly."""
    downloader = SBOMDownloader('token', str(tmp_path))
    assert downloader.base_dir == tmp_path


def test_downloader_timeout():
    """Test custom timeout."""
    downloader = SBOMDownloader('token', '/tmp', timeout=30)
    assert downloader.timeout == 30


def test_download_repo_success(tmp_path):
    """Test successful download creates files."""
    # Create a downloader with mocked session
    downloader = SBOMDownloader('fake_token', str(tmp_path))

    # Mock the session's get method
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b'module github.com/owner/repo'
    mock_response.from_cache = False
    mock_response.url = 'https://raw.githubusercontent.com/owner/repo/main/go.mod'
    downloader.session.get = MagicMock(return_value=mock_response)

    repo = {'full_name': 'owner/repo', 'default_branch': 'main'}
    lang = Language.GO

    result = downloader.download_repo(repo, lang)

    # Result is now a DownloadResult object
    assert isinstance(result, DownloadResult)
    assert result.repo == 'owner/repo'
    assert result.downloaded_files >= 1

    # Check file was created
    target_file = tmp_path / 'go' / 'owner' / 'repo' / 'main' / 'go.mod'
    assert target_file.exists()
    assert target_file.read_bytes() == b'module github.com/owner/repo'


@patch('requests.Session.get')
def test_download_repo_404(mock_get, downloader):
    repo = {'full_name': 'owner/repo', 'default_branch': 'main'}
    lang = Language.GO

    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.from_cache = False
    mock_get.return_value = mock_response

    result = downloader.download_repo(repo, lang)

    # Result is now a DownloadResult object
    assert isinstance(result, DownloadResult)
    assert result.missing_files > 0


def test_download_repo_mixed_results(tmp_path):
    """Test repo with some files found, some 404."""
    downloader = SBOMDownloader('fake_token', str(tmp_path))

    repo = {'full_name': 'owner/repo', 'default_branch': 'main'}
    lang = Language.GO

    # First call succeeds, rest fail with 404
    mock_response_success = MagicMock()
    mock_response_success.status_code = 200
    mock_response_success.content = b'module test'
    mock_response_success.from_cache = False
    mock_response_success.url = 'test'

    mock_response_404 = MagicMock()
    mock_response_404.status_code = 404
    mock_response_404.from_cache = False

    downloader.session.get = MagicMock(
        side_effect=[
            mock_response_success,
            mock_response_404, mock_response_404,
        ],
    )

    result = downloader.download_repo(repo, lang)
    assert isinstance(result, DownloadResult)
    assert result.downloaded_files >= 1
    assert result.missing_files >= 1


class TestDownloadResult:
    """Tests for DownloadResult dataclass."""

    def test_default_values(self):
        """Test default values are correct."""
        result = DownloadResult(repo='test/repo', status_msg='ok')
        assert result.downloaded_files == 0
        assert result.missing_files == 0
        assert result.failed_files == 0
        assert result.skipped_files == 0
        assert result.cache_hits == 0

    def test_increment_values(self):
        """Test values can be incremented."""
        result = DownloadResult(repo='test/repo', status_msg='ok')
        result.downloaded_files += 3
        result.cache_hits += 2
        assert result.downloaded_files == 3
        assert result.cache_hits == 2
