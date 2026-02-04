from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from sbom_insight.downloader import SBOMDownloader
from sbom_insight.models.language import Language


@pytest.fixture
def downloader(tmp_path):
    return SBOMDownloader('fake_token', str(tmp_path))


def test_downloader_init(downloader):
    assert downloader.session.headers['Authorization'] == 'Bearer fake_token'


@patch('requests.Session.get')
def test_download_repo_success(mock_get, downloader, tmp_path):
    repo = {'full_name': 'owner/repo', 'default_branch': 'main'}
    lang = Language.GO

    # Mock successful response for go.mod
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.content = b'module github.com/owner/repo'
    mock_get.return_value = mock_response

    # We expect it to try to download go.mod, go.sum etc.
    # We will just verify it creates the directory and file for the first success

    result = downloader.download_repo(repo, lang)

    target_file = tmp_path / 'go' / 'owner' / 'repo' / 'main' / 'go.mod'
    assert target_file.exists()
    assert target_file.read_bytes() == b'module github.com/owner/repo'
    assert 'go.mod' in result


@patch('requests.Session.get')
def test_download_repo_404(mock_get, downloader):
    repo = {'full_name': 'owner/repo', 'default_branch': 'main'}
    lang = Language.GO

    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_get.return_value = mock_response

    result = downloader.download_repo(repo, lang)
    assert 'no go.mod' in result
