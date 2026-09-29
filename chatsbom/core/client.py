from collections.abc import Iterable
from datetime import datetime
from datetime import timedelta
from pathlib import Path

import requests
import requests_cache
import structlog
from requests.adapters import HTTPAdapter
from requests_cache import CachedResponse
from requests_cache import SQLiteCache
from requests_cache.cache_keys import redact_response
from urllib3.util.retry import Retry

from chatsbom.core.redact import redact_url

logger = structlog.get_logger('client')


def _log_response(response, *args, **kwargs):
    if getattr(response, '_logged', False):
        return
    response._logged = True

    is_cached = getattr(response, 'from_cache', False)
    method = response.request.method
    url = response.url
    status_code = response.status_code
    if kwargs.get('stream'):
        # Reading `.content` here would read a streamed body whole
        # before the caller could stop it (the content stage's byte
        # caps), so a stream is logged by what it declares.
        declared = response.headers.get('Content-Length') or ''
        content_length = int(declared) if declared.isdigit() else 0
    else:
        content_length = len(response.content) if response.content else 0
    elapsed = response.elapsed.total_seconds()

    # Log via structlog, letting RichConsoleRenderer handle the styling.
    # Nothing from the request's headers: `Authorization` is the token.
    log_kwargs = {
        'method': method,
        'status_code': status_code,
        'content_length': content_length,
        'elapsed': f"{elapsed:.3f}s",
        'cached': is_cached,
    }

    # Add GitHub Rate Limit Info if present
    remaining = response.headers.get('X-RateLimit-Remaining')
    limit = response.headers.get('X-RateLimit-Limit')
    if remaining and limit:
        log_kwargs['ratelimit'] = f"{remaining}/{limit}"

    # Add URL at the end for better alignment, without what could fetch
    # it again: a report's download link is signed in its query.
    log_kwargs['url'] = redact_url(url)

    if is_cached:
        logger.info('HTTP Request', _style='dim', **log_kwargs)
    else:
        logger.info('HTTP Request', **log_kwargs)


def _unvary_authorization(response, *args, **kwargs):
    """Takes `Authorization` out of the headers an answer varies by.

    GitHub varies every answer by it (`Vary: Accept, Authorization,
    Cookie, X-GitHub-OTP`), and the cache keeps no token: `Authorization`
    is one of the headers requests-cache leaves out of its keys and
    redacts from what it stores. From 1.3.2 an answer that varies by
    such a header never matches a request that carries it, so every call
    went to GitHub, and spent quota, while the answer sat in the cache
    (#88). No setting changes that: the check comes before `match_headers`
    or a `key_fn` is read, and taking `Authorization` out of
    `ignored_parameters` would write the token to disk.

    The cost is that a cached answer can be served for any token. That
    is how the cache behaved before 1.3.2, and a deployment uses one
    token.
    """
    vary = response.headers.get('Vary')
    if vary:
        response.headers['Vary'] = ', '.join(
            header.strip() for header in vary.split(',')
            if header.strip().lower() != 'authorization'
        )


def _redacted_redirect(
    hop: CachedResponse, ignored: Iterable[str],
) -> CachedResponse:
    """One of the redirects an answer came through, as requests-cache
    keeps the answer itself: `ignored` redacted from its URL, its headers
    and its request, by requests-cache's own `redact_response`.

    A copy: the answer being saved may be one read from the cache, which
    the caller is handed back. Without `next`, which requests sets on
    every redirect but the first: it is the request sent next, which the
    redirect or the answer after it keeps as its own.
    """
    copy = CachedResponse.from_response(
        hop, request=hop.request.copy(), next=None,
    )
    return redact_response(copy, ignored)


class _RedactingCache(SQLiteCache):
    """requests-cache's SQLite cache, keeping no token in a redirect.

    GitHub answers a renamed repository's old name with a redirect on the
    same host, and requests keeps `Authorization` for that. requests-cache
    redacts `ignored_parameters` from the request the answer came from,
    and from nothing else it stores: each redirect kept its request as it
    was sent, and the token was in the file (1.3.0 to 1.3.3 at least).

    Redacted as the answer is saved, once every redirect has been
    followed. A hook, called on each redirect as it comes, would have to
    edit the request requests copies for the next one, which would then
    go out without the token.

    `save_response` is overridden, not copied: the answer is made as
    requests-cache makes it, its redirects are redacted, and it is handed
    to requests-cache's own, which takes an answer already made as it
    does to save one again after a 304. That still redacts and stores the
    answer, and records the redirects it came through, however a later
    version does those; one that redacts redirects itself finds nothing
    left to redact.
    """

    def save_response(
        self,
        response: requests.Response,
        cache_key: str | None = None,
        expires: datetime | None = None,
    ) -> None:
        kept = CachedResponse.from_response(response)
        ignored = self._settings.ignored_parameters
        kept.history = [
            _redacted_redirect(hop, ignored) for hop in kept.history
        ]
        super().save_response(kept, cache_key, expires)


def _mount_retrying_adapter(
    session: requests.Session,
    retries: int,
    pool_size: int,
    respect_retry_after: bool = True,
) -> None:
    """Robust connection pooling and retry configuration.

    With `respect_retry_after`, urllib3 also retries a 429 that carries
    `Retry-After`, sleeping for as long as it asks before each attempt.
    """
    retry_strategy = Retry(
        total=retries,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        respect_retry_after_header=respect_retry_after,
    )

    adapter = HTTPAdapter(
        pool_connections=pool_size,
        pool_maxsize=pool_size,
        max_retries=retry_strategy,
    )

    session.mount('https://', adapter)
    session.mount('http://', adapter)


def get_http_client(
    cache_name: str = '.requests-cache/db.sqlite3',
    expire_after: int = 604800,
    retries: int = 3,
    pool_size: int = 50,
) -> requests_cache.CachedSession:
    """
    Returns a requests session with caching and retry logic.
    """

    # Ensure the data directory exists
    cache_path = Path(cache_name)
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    # Configure Caching
    # We want to cache 200 OK and 404 Not Found (negative caching)
    session = requests_cache.CachedSession(
        backend=_RedactingCache(cache_name),
        expire_after=timedelta(seconds=expire_after),
        allowable_codes=[200, 404],
        uwsgi_enabled=True,  # For thread safety if needed, though sqlite is generally thread-safe
    )
    # First, so that every hook after it, and the cache, which stores the
    # answer once the hooks have run, see it without `Authorization` in
    # `Vary`.
    session.hooks['response'].insert(0, _unvary_authorization)
    session.hooks['response'].append(_log_response)
    _mount_retrying_adapter(session, retries, pool_size)

    logger.debug(
        'Initialized Cached HTTP Client',
        cache_name=cache_name,
        expire_after=expire_after,
    )

    return session


def get_plain_client(
    retries: int = 3,
    pool_size: int = 50,
    respect_retry_after: bool = False,
) -> requests.Session:
    """The same retries and logging as `get_http_client`, and no cache.

    For conditional requests, which are their own cache: the ETag is kept
    in the ledger and GitHub answers a match with a free 304. An HTTP
    cache in front of them answers in GitHub's place — from disk while
    its copy is fresh, and once stale by swapping the 304 for the 200 it
    stored — so no re-check is ever seen as unchanged.

    A refused token comes straight back rather than being slept through:
    the caller would rather stop than keep asking while rate limited.
    `respect_retry_after` is for a caller that would rather wait: the
    content stage, fetching from `raw.githubusercontent.com`, which
    spends no API quota.
    """
    session = requests.Session()
    session.hooks['response'].append(_log_response)
    _mount_retrying_adapter(
        session, retries, pool_size, respect_retry_after=respect_retry_after,
    )
    return session
