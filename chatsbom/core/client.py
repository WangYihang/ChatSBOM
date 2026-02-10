from datetime import timedelta
from pathlib import Path

import requests_cache
import structlog
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

logger = structlog.get_logger('client')


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

    def logging_hook(response, *args, **kwargs):
        if getattr(response, '_logged', False):
            return
        response._logged = True

        is_cached = getattr(response, 'from_cache', False)
        method = response.request.method
        url = response.url
        status = response.status_code
        content_length = len(response.content) if response.content else 0
        elapsed = response.elapsed.total_seconds()

        # Log via structlog, letting RichConsoleRenderer handle the styling
        log_kwargs = {
            'method': method,
            'url': url,
            'status': status,
            'content_length': content_length,
            'elapsed': f"{elapsed:.3f}s",
            'cached': is_cached,
        }

        # Add GitHub Rate Limit Info if present
        remaining = response.headers.get('X-RateLimit-Remaining')
        limit = response.headers.get('X-RateLimit-Limit')
        if remaining and limit:
            log_kwargs['ratelimit'] = f"{remaining}/{limit}"

        if is_cached:
            logger.info('HTTP Request', _style='dim', **log_kwargs)
        else:
            logger.info('HTTP Request', **log_kwargs)
    session.hooks['response'].append(logging_hook)

    # Robust connection pooling and retry configuration
    retry_strategy = Retry(
        total=retries,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
    )

    adapter = HTTPAdapter(
        pool_connections=pool_size,
        pool_maxsize=pool_size,
        max_retries=retry_strategy,
    )

    session.mount('https://', adapter)
    session.mount('http://', adapter)

    logger.debug(
        'Initialized Cached HTTP Client',
        cache_name=cache_name,
        expire_after=expire_after,
    )

    return session
