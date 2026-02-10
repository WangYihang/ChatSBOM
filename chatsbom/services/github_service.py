import time
from typing import Any

import requests
import structlog
from ratelimit import limits
from ratelimit import sleep_and_retry

from chatsbom.core.client import get_http_client

logger = structlog.get_logger('github_service')

# GitHub API limits (per token)
# Core: 5000/hour -> 4500 for safety
# Search: 30/minute -> 25 for safety
CORE_CALLS = 4500
CORE_PERIOD = 3600
SEARCH_CALLS = 25
SEARCH_PERIOD = 60


class GitHubService:
    """Service for interacting with GitHub REST API with proactive and reactive rate limiting."""

    def __init__(self, token: str, delay: float = 0.0):
        self.session = get_http_client()
        self.session.headers.update({
            'Authorization': f"Bearer {token}",
            'Accept': 'application/vnd.github.v3+json',
            'User-Agent': 'ChatSBOM',
        })

    def _is_cached(self, method: str, url: str, params: dict | None = None) -> bool:
        """Check if a request is already in the local cache."""
        try:
            request = requests.Request(
                method, url, params=params, headers=self.session.headers,
            )
            prepared = self.session.prepare_request(request)
            key = self.session.cache.create_key(prepared)
            return self.session.cache.contains(key)
        except Exception:
            return False

    def _handle_api_rate_limit(self, response: requests.Response):
        """Handle 403 (Rate Limit) and 429 (Too Many Requests) from GitHub."""
        reset_time = response.headers.get('X-RateLimit-Reset')
        retry_after = response.headers.get('Retry-After')

        wait_seconds = 60.0  # Default fallback

        if reset_time:
            wait_seconds = float(reset_time) - time.time() + 1.0
        elif retry_after:
            wait_seconds = float(retry_after) + 1.0

        if wait_seconds < 0:
            wait_seconds = 1.0

        # Circuit Breaker: If wait time is > 1 hour, abort.
        if wait_seconds > 3600:
            logger.error(
                'Rate limit reset too far in future',
                wait_seconds=wait_seconds,
            )
            raise requests.RequestException(
                'Rate limit exceeded and reset time is too long (circuit breaker).',
            )

        logger.warning(
            'API Rate limit hit (Reactive)',
            status=response.status_code,
            wait_seconds=f"{wait_seconds:.2f}s",
        )
        time.sleep(wait_seconds)

    @sleep_and_retry
    @limits(calls=CORE_CALLS, period=CORE_PERIOD)
    def _make_core_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Rate-limited core API request."""
        return self._make_request(method, url, **kwargs)

    @sleep_and_retry
    @limits(calls=SEARCH_CALLS, period=SEARCH_PERIOD)
    def _make_search_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """Rate-limited search API request."""
        return self._make_request(method, url, **kwargs)

    def _make_request(self, method: str, url: str, **kwargs) -> requests.Response:
        """
        Base wrapper for requests with reactive handling for GitHub Rate Limits.
        """
        while True:
            response = self.session.request(method, url, **kwargs)

            # Handle Rate Limits (403 or 429)
            if response.status_code == 429:
                self._handle_api_rate_limit(response)
                continue

            if response.status_code == 403:
                if 'rate limit' in response.text.lower():
                    self._handle_api_rate_limit(response)
                    continue

            return response

    def search_repositories(self, query: str, page: int = 1, per_page: int = 100) -> dict[str, Any]:
        url = 'https://api.github.com/search/repositories'
        params = {
            'q': query,
            'sort': 'stars',
            'order': 'desc',
            'per_page': str(per_page),
            'page': str(page),
        }

        # Check cache manually to avoid consuming ratelimit tokens for cached data
        if self._is_cached('GET', url, params=params):
            response = self.session.get(url, params=params, timeout=20)
        else:
            response = self._make_search_request(
                'GET', url, params=params, timeout=20,
            )

        response.raise_for_status()
        return response.json()

    def get_repository_metadata(self, owner: str, repo: str) -> dict[str, Any] | None:
        """Fetch detailed repository metadata."""
        url = f"https://api.github.com/repos/{owner}/{repo}"

        try:
            if self._is_cached('GET', url):
                response = self.session.get(url, timeout=20)
            else:
                response = self._make_core_request('GET', url, timeout=20)

            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                return None
            else:
                response.raise_for_status()
        except requests.RequestException as e:
            logger.error(
                'Failed to fetch repository metadata',
                repo=f"{owner}/{repo}", error=str(e),
            )
            return None
        return None

    def get_repository_releases(self, owner: str, repo: str, max_pages: int = 5) -> list[dict[str, Any]]:
        """Fetch all releases for a repository."""
        all_releases = []
        page = 1

        while page <= max_pages:
            url = f"https://api.github.com/repos/{owner}/{repo}/releases"
            params = {'per_page': '100', 'page': str(page)}

            try:
                if self._is_cached('GET', url, params=params):
                    response = self.session.get(url, params=params, timeout=20)
                else:
                    response = self._make_core_request(
                        'GET', url, params=params, timeout=20,
                    )

                if response.status_code == 200:
                    releases = response.json()
                    if not releases:
                        break
                    all_releases.extend(releases)
                    if len(releases) < 100:
                        break
                    page += 1
                elif response.status_code == 404:
                    break
                else:
                    response.raise_for_status()
            except requests.RequestException as e:
                logger.error(
                    'Failed to fetch releases',
                    repo=f"{owner}/{repo}", error=str(e),
                )
                break

        return all_releases

    def get_commit_metadata(self, owner: str, repo: str, ref: str) -> dict[str, Any] | None:
        """Fetch commit information for a given ref."""
        url = f"https://api.github.com/repos/{owner}/{repo}/commits/{ref}"
        try:
            if self._is_cached('GET', url):
                response = self.session.get(url, timeout=20)
            else:
                response = self._make_core_request(
                    'GET', url, timeout=20,
                )

            if response.status_code == 200:
                data = response.json()
                sha = data['sha']
                return {
                    'sha': sha,
                    'sha_short': sha[:7],
                    'date': data['commit']['author']['date'],
                    'message': data['commit']['message'],
                }
            else:
                return None
        except requests.RequestException as e:
            logger.error(
                'Failed to fetch commit metadata',
                repo=f"{owner}/{repo}", ref=ref, error=str(e),
            )
            return None
