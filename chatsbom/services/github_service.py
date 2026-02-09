import time
from typing import Any

import requests
import structlog

from chatsbom.core.client import get_http_client

logger = structlog.get_logger('github_service')


class GitHubService:
    """Service for interacting with GitHub REST API."""

    def __init__(self, token: str, delay: float = 2.0):
        self.session = get_http_client()
        self.session.headers.update({
            'Authorization': f"Bearer {token}",
            'Accept': 'application/vnd.github.v3+json',
            'User-Agent': 'ChatSBOM',
        })
        self.delay = delay
        self.last_request_time = 0.0

    def _is_cached(self, method: str, url: str, params: dict | None = None) -> bool:
        """Check if a request is already in the local cache."""
        try:
            # Create a prepared request to match how requests-cache generates keys
            request = requests.Request(
                method, url, params=params, headers=self.session.headers,
            )
            prepared = self.session.prepare_request(request)
            key = self.session.cache.create_key(prepared)
            return self.session.cache.contains(key)
        except Exception:
            return False

    def search_repositories(self, query: str, page: int = 1, per_page: int = 100) -> dict[str, Any]:
        url = 'https://api.github.com/search/repositories'
        params = {
            'q': query,
            'sort': 'stars',
            'order': 'desc',
            'per_page': str(per_page),
            'page': str(page),
        }

        # Check cache manually to avoid delay
        if not self._is_cached('GET', url, params=params):
            self._wait_for_local_rate_limit()

        response = self.session.get(url, params=params, timeout=20)

        is_cached = getattr(response, 'from_cache', False)
        if not is_cached:
            self.last_request_time = time.time()

        response.raise_for_status()
        return response.json()

    def get_repository_metadata(self, owner: str, repo: str) -> dict[str, Any] | None:
        """Fetch detailed repository metadata."""
        url = f"https://api.github.com/repos/{owner}/{repo}"

        try:
            response = self.session.get(url, timeout=20)

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
                response = self.session.get(url, params=params, timeout=20)

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
            response = self.session.get(
                url, params={'per_page': '1'}, timeout=20,
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

    def _wait_for_local_rate_limit(self):
        """Simple local rate limiting to avoid hitting GitHub API too fast."""
        gap = time.time() - self.last_request_time
        if gap < self.delay:
            time.sleep(self.delay - gap)
