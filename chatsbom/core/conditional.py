"""Conditional GETs against the GitHub API.

This is the lever the continuous-collection design rests on. From
GitHub's REST best practices:

    Making a conditional request does not count against your primary
    rate limit if a 304 response is returned and the request was made
    while correctly authorized with an Authorization header.

Measured on the current corpus, 74.7% of repositories are not pushed in a
given week. Revalidating those with `If-None-Match` costs nothing, so the
whole 28k set can be re-checked continuously while the rate budget is
spent only on the 25.3% that actually changed.

`requests-cache` already stores responses, but it expires them by TTL and
does not send the ETag, so every re-check paid full price. This module
makes the condition explicit and reports the four outcomes a caller has
to distinguish — unchanged, changed, absent, failed — instead of raising
on three of them.
"""
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any
from typing import Protocol

import requests
import structlog

logger = structlog.get_logger('conditional')

NOT_MODIFIED = 304
NOT_FOUND = 404

DEFAULT_TIMEOUT = 30


class Session(Protocol):
    """The slice of `requests.Session` this module uses."""

    def get(self, url: str, **kwargs: Any) -> Any:
        ...


@dataclass(frozen=True, slots=True)
class ConditionalResult:
    """The outcome of one conditional request.

    Exactly one of `unchanged`, `changed`, `absent` and `failed` is true,
    so a caller cannot forget a case.
    """

    status: int
    etag: str | None = None
    payload: Any = None
    error: str = ''

    @property
    def unchanged(self) -> bool:
        """304: the resource is as we last saw it, and this was free."""
        return self.status == NOT_MODIFIED

    @property
    def changed(self) -> bool:
        """2xx with a usable body."""
        return 200 <= self.status < 300 and not self.error

    @property
    def absent(self) -> bool:
        """404: gone, renamed, or never had the resource."""
        return self.status == NOT_FOUND

    @property
    def failed(self) -> bool:
        """Anything we cannot act on — transport error, 5xx, bad body."""
        if self.unchanged or self.absent:
            return False
        return bool(self.error) or not (200 <= self.status < 300)

    @property
    def spent_quota(self) -> bool:
        """Whether this request consumed primary rate limit.

        A 304 does not; everything that reached the server does.
        """
        return not self.unchanged and self.status != 0


def conditional_get(
    session: Session,
    url: str,
    etag: str | None = None,
    headers: Mapping[str, str] | None = None,
    timeout: int = DEFAULT_TIMEOUT,
    **kwargs: Any,
) -> ConditionalResult:
    """GET `url`, sending `etag` as `If-None-Match` when we have one.

    Never raises: in a loop over tens of thousands of repositories, a
    missing or unavailable resource is ordinary, and the caller needs to
    record it in the ledger rather than abort the slice.
    """
    request_headers: dict[str, str] = dict(headers or {})
    if etag:
        request_headers['If-None-Match'] = etag

    try:
        response = session.get(
            url, headers=request_headers, timeout=timeout, **kwargs,
        )
    except requests.RequestException as e:
        logger.debug('Conditional request failed', url=url, error=str(e))
        return ConditionalResult(status=0, error=f'{type(e).__name__}: {e}')

    status = int(response.status_code)
    new_etag = response.headers.get('ETag') or response.headers.get('etag')

    if status == NOT_MODIFIED:
        # Keep the ETag we already had: a 304 may omit it.
        return ConditionalResult(status=status, etag=new_etag or etag)

    if status == NOT_FOUND:
        return ConditionalResult(status=status)

    if not 200 <= status < 300:
        return ConditionalResult(status=status, error=f'HTTP {status}')

    try:
        payload = response.json()
    except (ValueError, requests.RequestException) as e:
        return ConditionalResult(
            status=status, etag=new_etag, error=f'unparsable body: {e}',
        )

    return ConditionalResult(status=status, etag=new_etag, payload=payload)
