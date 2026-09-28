import json
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest
from rich.progress import Progress

from chatsbom.core.storage import Storage
from chatsbom.services.github_service import GitHubService
from chatsbom.services.search_service import SearchService
from chatsbom.services.search_service import SearchStats
from tests.repository_model_test import repos_payload


@pytest.fixture(autouse=True)
def scratch_directory(tmp_path, monkeypatch):
    """`GitHubService` opens the cached client, whose database is under
    the working directory, which is the checkout when the suite runs."""
    monkeypatch.chdir(tmp_path)


@pytest.fixture
def mock_storage(tmp_path):
    f = tmp_path / 'test_output.jsonl'
    return Storage(f)


def test_storage_save(mock_storage):
    item = {
        'id': 123,
        'owner': {'login': 'owner'},
        'name': 'repo',
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
        assert data['owner'] == 'owner'
        assert data['repo'] == 'repo'


def test_the_search_stage_stores_the_licence(tmp_path):
    """`github search` saves GitHub's items through `Repository` too, so
    they are read the way `/repos/{owner}/{repo}` is (#11)."""
    class FakeGitHub:
        def search_repositories(self, query, page=1):
            items = [{**repos_payload(), 'score': 1.0}] if page == 1 else []
            return {'items': items}

    output = tmp_path / 'ruby.jsonl'
    search = SearchService(FakeGitHub(), None, 1000, str(output))
    progress = Progress()
    search.run(progress, progress.add_task('search', stars='', status=''))

    stored = json.loads(output.read_text())
    assert stored.get('license_spdx_id') == 'MIT'
    assert stored.get('license_name') == 'MIT License'


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
        'items': [{'id': 1, 'owner': {'login': 'a'}, 'name': 'b', 'stargazers_count': 10}],
    }
    # Mocking _is_cached to avoid error
    with patch('chatsbom.services.github_service.GitHubService._is_cached', return_value=False):
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


class FakeSearch:
    """GitHub's search API over a fixed set of repositories: `stars:`,
    `created:` and `language:` qualifiers, most stars first, 100 a page
    and at most 1,000 results a query, as GitHub answers it."""

    def __init__(self, repositories: list[dict]) -> None:
        self.repositories = repositories
        self.queries: list[str] = []

    @staticmethod
    def _stars(expression: str, stars: int) -> bool:
        if expression.startswith('>='):
            return stars >= int(expression[2:])
        if expression.startswith('>'):
            return stars > int(expression[1:])
        if '..' in expression:
            low, high = expression.split('..')
            return int(low) <= stars <= int(high)
        return stars == int(expression)

    def search_repositories(self, query: str, page: int = 1) -> dict:
        self.queries.append(query)
        qualifiers = dict(part.split(':', 1) for part in query.split())
        matches = [
            r for r in self.repositories
            if self._stars(qualifiers['stars'], r['stargazers_count'])
            and (
                'language' not in qualifiers
                or (r['language'] or '').lower() == qualifiers['language']
            )
            and (
                'created' not in qualifiers
                or qualifiers['created'].split('..')[0]
                <= r['created_at'][:10]
                <= qualifiers['created'].split('..')[1]
            )
        ]
        matches.sort(key=lambda r: -r['stargazers_count'])
        matches = matches[:1000]
        return {'items': matches[(page - 1) * 100:page * 100]}


def _repository(repository_id: int, stars: int, created: str, language='Go') -> dict:
    return {
        'id': repository_id, 'owner': {'login': 'o'},
        'name': f'r{repository_id}', 'stargazers_count': stars,
        'created_at': created, 'language': language,
        'pushed_at': '2026-09-01T00:00:00Z', 'default_branch': 'main',
    }


def _dense_corpus(language=None) -> list[dict]:
    """1,200 repositories with exactly 1,000 stars, a star wall only a
    `created:` slice gets through, and 100 above it."""
    wall = [
        _repository(
            i, 1000, f'{2010 + i % 12}-0{1 + i % 9}-15T00:00:00Z',
            language=language or ('Go' if i % 2 else None),
        )
        for i in range(1200)
    ]
    top = [
        _repository(10_000 + i, 5000 + i, '2015-01-01T00:00:00Z')
        for i in range(100)
    ]
    return wall + top


def _run(search: SearchService) -> SearchStats:
    progress = Progress()
    return search.run(progress, progress.add_task('s', stars='', status=''))


class TestUnfilteredSearch:
    """`github search` with no language: the snapshot the queue is seeded
    from (design #55, §4.14)."""

    def test_no_query_names_a_language(self, tmp_path):
        """The time slices sent `language:None` (F20), which matches
        nothing: every repository behind a dense star count was lost."""
        api = FakeSearch(_dense_corpus())
        search = SearchService(api, None, 1000, str(tmp_path / 'all.jsonl'))
        _run(search)

        assert any('created:' in query for query in api.queries)
        assert not any('language:' in query for query in api.queries)
        assert len(search.storage.visited_ids) == 1300

    def test_a_language_is_still_filtered_in_its_time_slices(self, tmp_path):
        api = FakeSearch(_dense_corpus(language='Go'))
        _run(SearchService(api, 'go', 1000, str(tmp_path / 'go.jsonl')))
        sliced = [q for q in api.queries if 'created:' in q]
        assert sliced
        assert all(
            q.startswith('language:go stars:1000 created:')
            for q in sliced
        )

    def test_the_threshold_is_inclusive(self, tmp_path):
        """A snapshot of repositories with at least 1,000 stars: `>` left
        out the ones with exactly 1,000."""
        api = FakeSearch([_repository(1, 1000, '2020-01-01T00:00:00Z')])
        search = SearchService(api, None, 1000, str(tmp_path / 'all.jsonl'))
        _run(search)
        assert api.queries[0] == 'stars:>=1000'
        assert search.storage.visited_ids == {1}

    def test_an_interrupted_search_resumes_from_its_fewest_stars(self, tmp_path):
        corpus = [
            _repository(i, 1000 + i, '2020-01-01T00:00:00Z') for i in range(50)
        ]
        output = tmp_path / 'all-2026-10-01.jsonl'
        Storage(output).save(corpus[-1])  # 1,049 stars, then interrupted

        api = FakeSearch(corpus)
        search = SearchService(api, None, 1000, str(output))
        _run(search)

        assert api.queries[0] == 'stars:1000..1049'
        assert len(search.storage.visited_ids) == 50

    def test_requests_are_counted_as_sent(self, tmp_path):
        api = FakeSearch(_dense_corpus())
        stats = _run(SearchService(api, None, 1000, str(tmp_path / 'a.jsonl')))
        assert stats.api_requests == len(api.queries)


def test_the_snapshot_is_dated():
    from datetime import date

    from chatsbom.core.config import PathConfig

    assert PathConfig().search_snapshot(date(2026, 10, 1)).as_posix() == (
        'data/01-github-search/all-2026-10-01.jsonl'
    )


def test_search_query():
    from chatsbom.services.search_service import search_query

    assert search_query(None, '1000', '2008-01-01..2010-01-01') == (
        'stars:1000 created:2008-01-01..2010-01-01'
    )
    assert search_query('go', '>=1000') == 'language:go stars:>=1000'
