import json
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from sbom_insight.crawl_repos import GitHubClient
from sbom_insight.crawl_repos import Storage


@pytest.fixture
def mock_storage(tmp_path):
    f = tmp_path / 'test_output.jsonl'
    return Storage(str(f))


def test_storage_save(mock_storage):
    item = {
        'id': 123,
        'full_name': 'owner/repo',
        'stargazers_count': 100,
        'html_url': 'http://github.com/owner/repo',
        'created_at': '2020-01-01',
    }
    assert mock_storage.save(item) is True
    # Save again should return False (deduplication)
    assert mock_storage.save(item) is False

    # Verify content
    with open(mock_storage.filepath) as f:
        data = json.loads(f.read())
        assert data['id'] == 123
        assert data['full_name'] == 'owner/repo'


def test_github_client_init():
    client = GitHubClient('fake_token')
    assert client.session.headers['Authorization'] == 'Bearer fake_token'


@patch('requests.Session.get')
def test_search_repositories(mock_get):
    client = GitHubClient('fake_token')
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        'items': [{'id': 1, 'full_name': 'a/b', 'stargazers_count': 10}],
    }
    mock_get.return_value = mock_response

    task_id = MagicMock()
    progress = MagicMock()

    results = list(client.search_repositories('query', task_id, progress))
    assert len(results) == 1
    assert results[0]['id'] == 1
