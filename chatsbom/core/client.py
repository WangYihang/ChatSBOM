import os
import shutil
import sqlite3
from collections.abc import Iterable
from contextlib import closing
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


#: `PRAGMA user_version` of a cache file `_RedactingCache.scrub` has
#: scrubbed. requests-cache leaves it at 0.
_SCRUBBED = 1


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

    def scrub(self) -> int:
        """Redacts the redirects a client before this one kept: how many
        answers held something to redact.

        As the file is opened. An answer asked for again is saved again,
        redacted, and one that never is would keep the token for good.
        Once for each file, as `PRAGMA user_version` says, which
        requests-cache leaves at 0: it goes with the file, so a copy made
        before is scrubbed again when it is opened. Nothing is deleted:
        every answer would cost quota to fetch again.

        The answers read are the ones requests-cache records in
        `redirects`, as it records every answer it saves with the
        redirects it came through. Each is written back redacted as
        `save_response` redacts it, with `secure_delete` on, so that the
        copy it replaces is overwritten. Then VACUUM rebuilds the file
        without the copies earlier refreshes left in its free pages:
        SQLite overwrites what it frees only where it is built to.
        """
        ignored = self._settings.ignored_parameters
        responses = self.responses.table_name
        redirected = (
            f'SELECT key FROM {responses} WHERE key IN'
            f' (SELECT value FROM {self.redirects.table_name})'
        )
        path = self.responses.db_path
        scrubbed = 0
        # A minute, where the cache waits five seconds for a lock: another
        # process may be scrubbing the same file. What raises before the
        # COMMIT is rolled back as the connection closes.
        with closing(
            sqlite3.connect(path, timeout=60, isolation_level=None),
        ) as db:
            if _user_version(db) >= _SCRUBBED:
                return 0
            db.execute('PRAGMA secure_delete = ON')
            db.execute('BEGIN IMMEDIATE')
            # Again, now that no one else can write: another process may
            # have finished first.
            if _user_version(db) < _SCRUBBED:
                for (key,) in db.execute(redirected).fetchall():
                    (value,) = db.execute(
                        f'SELECT value FROM {responses} WHERE key = ?',
                        (key,),
                    ).fetchone()
                    kept = self.responses.deserialize(key, value)
                    if kept is None:
                        continue
                    kept.history = [
                        _redacted_redirect(hop, ignored)
                        for hop in kept.history
                    ]
                    redacted = bytes(self.responses.serialize(kept))
                    if redacted != value:
                        db.execute(
                            f'UPDATE {responses} SET value = ? WHERE key = ?',
                            (redacted, key),
                        )
                        scrubbed += 1
                db.execute(f'PRAGMA user_version = {_SCRUBBED}')
            db.execute('COMMIT')
            if scrubbed:
                logger.warning(
                    'The HTTP cache held the GitHub token and is rebuilt '
                    'without it, once; a backup of it made before still '
                    'holds the token',
                    answers=scrubbed, cache=str(path),
                )
                short = _vacuum_short_of(Path(path))
                if short:
                    logger.warning(
                        'The HTTP cache is not rebuilt: VACUUM needs room '
                        'for two more copies of it; its free pages may '
                        'hold the token until `sqlite3 <cache> VACUUM` is '
                        'run, with nothing else using it and the room free',
                        cache=str(path), short_by_bytes=short,
                    )
                    return scrubbed
                try:
                    db.execute('VACUUM')
                except sqlite3.Error as error:
                    logger.warning(
                        'The HTTP cache could not be rebuilt; its free '
                        'pages may hold the token until `sqlite3 <cache> '
                        'VACUUM` is run, with nothing else using it',
                        cache=str(path), error=str(error),
                    )
        return scrubbed


def _sqlite_temp_dir() -> Path:
    """Where SQLite builds VACUUM's copy: `unix_tempdir`'s order."""
    for value in (
        os.environ.get('SQLITE_TMPDIR'), os.environ.get('TMPDIR'),
        '/var/tmp', '/usr/tmp', '/tmp',
    ):
        if value and os.path.isdir(value) and os.access(value, os.W_OK):
            return Path(value)
    return Path('.')


def _vacuum_short_of(path: Path) -> int:
    """How many bytes VACUUM of `path` would lack; 0 when it has room.

    VACUUM writes the whole database twice before it frees anything: a
    copy in SQLite's temporary directory, then that copy back over the
    file, with a rollback journal of the old pages beside it. A 33 GB
    cache opened first by a worker on a disk with 30 GB free filled the
    disk under every other process on it; this asks first.
    """
    size = path.stat().st_size
    here = path.resolve().parent
    temp = _sqlite_temp_dir().resolve()
    need: dict[int, int] = {}
    free: dict[int, int] = {}
    for directory in (here, temp):
        device = os.stat(directory).st_dev
        need[device] = need.get(device, 0) + size
        free[device] = shutil.disk_usage(directory).free
    return max(0, *(need[device] - free[device] for device in need))


def _user_version(db: sqlite3.Connection) -> int:
    return int(db.execute('PRAGMA user_version').fetchone()[0])


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
    cache = _RedactingCache(cache_name)
    session = requests_cache.CachedSession(
        backend=cache,
        expire_after=timedelta(seconds=expire_after),
        allowable_codes=[200, 404],
        uwsgi_enabled=True,  # For thread safety if needed, though sqlite is generally thread-safe
    )
    # Once the session has given the cache its settings, which name what
    # the scrub redacts.
    try:
        cache.scrub()
    except Exception as error:
        # A cache not scrubbed still answers, and the next client to open
        # it tries again.
        logger.warning(
            'Could not take the GitHub token out of the HTTP cache',
            cache=cache_name, error=str(error),
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
