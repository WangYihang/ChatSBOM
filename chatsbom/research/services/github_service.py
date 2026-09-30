"""The README fetch `readme` and `classify` share (#167).

It was the pipeline's GitHub client's (chatsbom/services/github_service.py),
and moved with the research tools with what it needs of that client: the
session, which keeps GitHub's answers on disk, the headers it sends, and
the waits GitHub's rate limits ask for. The rest of that client is the
pipeline's, and can go with it (#155, 6e).
"""
import time
from typing import TypeAlias

import requests
import structlog
from requests_cache import AnyResponse

from chatsbom.core.client import get_http_client
from chatsbom.core.config import get_config

logger = structlog.get_logger('github_service')

# requests-cache returns OriginalResponse or CachedResponse depending on
# whether the request was served from disk. Both subclass requests.Response,
# but the union is what the session actually hands back.
GitHubResponse: TypeAlias = AnyResponse


class GitHubService:
    """READMEs from GitHub's REST API, kept on disk, with reactive rate limiting."""

    def __init__(self, token: str):
        self.config = get_config()
        headers = {
            'Authorization': f"Bearer {token}",
            'Accept': 'application/vnd.github.v3+json',
            'User-Agent': 'ChatSBOM',
        }
        self.session = get_http_client(
            expire_after=self.config.github.cache_ttl,
        )
        self.session.headers.update(headers)

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
            status_code=response.status_code,
            wait_seconds=f"{wait_seconds:.2f}s",
        )
        time.sleep(wait_seconds)

    def _make_request(self, method: str, url: str, **kwargs) -> GitHubResponse:
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

            # Proactive Rate Limit Handling
            remaining = response.headers.get('X-RateLimit-Remaining')
            if remaining and int(remaining) < 10:
                self._handle_api_rate_limit(response)

            return response

    def get_readme(self, owner: str, repo: str) -> str | None:
        """Fetch README content from GitHub and cache it locally."""
        cache_path = self.config.paths.get_readme_cache_path(owner, repo)
        if cache_path.exists():
            return cache_path.read_text(encoding='utf-8')

        url = f"https://api.github.com/repos/{owner}/{repo}/readme"
        try:
            # Use raw media type to get content directly
            headers = dict(self.session.headers)
            headers['Accept'] = 'application/vnd.github.v3.raw'

            if self._is_cached('GET', url):
                response = self.session.get(url, headers=headers, timeout=20)
            else:
                response = self._make_request(
                    'GET', url, headers=headers, timeout=20,
                )

            if response.status_code == 200:
                content = response.text
                # Save to local cache
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_path.write_text(content, encoding='utf-8')
                return content
        except Exception as e:
            logger.debug(f"Failed to fetch README for {owner}/{repo}: {e}")
        return None
