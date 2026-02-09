import json
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from chatsbom.core.storage import Storage
from chatsbom.services.github_service import GitHubService
from chatsbom.services.search_service import SearchStats


@pytest.fixture
def mock_storage(tmp_path):
    f = tmp_path / 'test_output.jsonl'
    return Storage(f)


def test_storage_save(mock_storage):
    item = {
        'id': 123,
        'full_name': 'owner/repo',
        'stargazers_count': 100,
        'html_url': 'http://github.com/owner/repo',
        'created_at': '2020-01-01T00:00:00Z',
    }
    assert mock_storage.save(item) is True
    # Save again should return False (deduplication)
    assert mock_storage.save(item) is False

    # Verify content
    with open(mock_storage.filepath) as f:
        data = json.loads(f.read())
        assert data['id'] == 123
        assert data['full_name'] == 'owner/repo'


def test_github_service_init():
    service = GitHubService('fake_token')
    assert service.session.headers['Authorization'] == 'Bearer fake_token'


@patch('chatsbom.services.github_service.get_http_client')
def test_search_repositories(mock_get_client):
    """Test search_repositories returns results correctly."""
    # Setup mock session
    mock_session = MagicMock()
    mock_session.headers = {}
    mock_get_client.return_value = mock_session

    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        'items': [{'id': 1, 'full_name': 'a/b', 'stargazers_count': 10}],
    }
    mock_response.from_cache = False
    mock_response.url = 'test'
    # Mock request method as _make_request uses session.request
    mock_session.request.return_value = mock_response
    mock_session.get.return_value = mock_response  # Keep for safety

    service = GitHubService('fake_token')
    service.session = mock_session

    results = service.search_repositories('query')
    assert len(results['items']) == 1
    assert results['items'][0]['id'] == 1


class TestSearchStats:
    """Tests for SearchStats dataclass."""

    def test_default_values(self):
        """Test default values are set correctly."""
        stats = SearchStats()
        assert stats.api_requests == 0
        assert stats.cache_hits == 0
        assert stats.repos_found == 0
        assert stats.repos_saved == 0
