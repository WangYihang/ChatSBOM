from datetime import timedelta
from pathlib import Path

import requests
import requests_cache
import structlog
from requests.adapters import HTTPAdapter
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
        cache_name=cache_name,
        backend='sqlite',
        expire_after=timedelta(seconds=expire_after),
        allowable_codes=[200, 404],
        uwsgi_enabled=True,  # For thread safety if needed, though sqlite is generally thread-safe
    )
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
