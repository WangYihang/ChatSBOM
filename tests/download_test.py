from pathlib import Path
from unittest.mock import MagicMock

import pytest

from chatsbom.models.language import Language
from chatsbom.models.repository import Repository
from chatsbom.services.downloader_service import DownloaderService
from chatsbom.services.downloader_service import DownloadResult


@pytest.fixture
def downloader(tmp_path):
    return DownloaderService('fake_token', str(tmp_path))


def test_downloader_init(downloader):
    assert downloader.session.headers['Authorization'] == 'Bearer fake_token'


def test_downloader_base_dir(tmp_path):
    """Test base_dir is set correctly."""
    downloader = DownloaderService('token', str(tmp_path))
    assert downloader.base_dir == Path(tmp_path)


def test_downloader_timeout():
    """Test custom timeout."""
    downloader = DownloaderService('token', '/tmp', timeout=30)
    assert downloader.timeout == 30


def test_download_repository_assets_success(tmp_path):
    """Test successful download creates files."""
    # Create a downloader with mocked session
    downloader = DownloaderService('fake_token', str(tmp_path))

    # Mock the session's get method
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b'module github.com/owner/repo'
    mock_response.from_cache = False
    mock_response.url = 'https://raw.githubusercontent.com/owner/repo/main/go.mod'
    downloader.session.get = MagicMock(return_value=mock_response)

    repo = Repository(
        id=1, full_name='owner/repo',
        stargazers_count=10, default_branch='main',
    )
    lang = Language.GO

    result = downloader.download_repository_assets(repo, lang)

    # Result is now a DownloadResult object
    assert isinstance(result, DownloadResult)
    assert result.repo_full_name == 'owner/repo'
    assert result.downloaded_files >= 1

    # Check file was created
    # Structure: base / lang / owner / name / ref / commit_sha / filename
    # Our mocked Repository has no download_target, so it uses default_branch 'main' and commit_sha 'unknown'
    target_file = tmp_path / 'go' / 'owner' / \
        'repo' / 'main' / 'unknown' / 'go.mod'
    assert target_file.exists()
    assert target_file.read_bytes() == b'module github.com/owner/repo'


class TestDownloadResult:
    """Tests for DownloadResult dataclass."""

    def test_default_values(self):
        """Test default values are correct."""
        result = DownloadResult(repo_full_name='test/repo')
        assert result.downloaded_files == 0
        assert result.missing_files == 0
        assert result.failed_files == 0
        assert result.skipped_files == 0
        assert result.cache_hits == 0
