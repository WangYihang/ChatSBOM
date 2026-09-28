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
does not send the ETag, so every re-check paid full price. Worse, put in
front of a conditional request it answers in GitHub's place, so the 304
never arrives (see `conditional_get`). This module makes the condition
explicit and reports the five outcomes a caller has to distinguish —
unchanged, changed, absent, rate-limited, failed — instead of raising.
"""
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from typing import Protocol

import requests
import structlog

logger = structlog.get_logger('conditional')

NOT_MODIFIED = 304
FORBIDDEN = 403
NOT_FOUND = 404
TOO_MANY_REQUESTS = 429

DEFAULT_TIMEOUT = 30


class Session(Protocol):
    """The slice of `requests.Session` this module uses."""

    def get(
        self,
        url: str,
        *,
        headers: Mapping[str, str],
        timeout: int,
    ) -> Any:
        ...


def _int_header(headers: Mapping[str, str], name: str) -> int | None:
    """An integer header, or None when it is missing or not a number."""
    raw = headers.get(name) or headers.get(name.lower())
    if raw is None:
        return None
    try:
        return int(raw)
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True, slots=True)
class RateLimit:
    """GitHub's rate-limit headers, as they came with one answer.

    They are what tells a refused token from a failing resource: GitHub
    answers both an exhausted token and a repository it blocks with 403,
    and only these headers differ. A header that is missing or unreadable
    is None, so garbage is never read as an exhausted token.
    """

    #: `X-RateLimit-Remaining`: requests left in the current window.
    remaining: int | None = None
    #: `X-RateLimit-Reset`: when the window resets, in UTC epoch seconds.
    reset: int | None = None
    #: `Retry-After`: seconds to wait. Sent with secondary limits.
    retry_after: int | None = None

    @classmethod
    def from_headers(cls, headers: Mapping[str, str]) -> 'RateLimit':
        return cls(
            remaining=_int_header(headers, 'X-RateLimit-Remaining'),
            reset=_int_header(headers, 'X-RateLimit-Reset'),
            retry_after=_int_header(headers, 'Retry-After'),
        )

    def resumes_at(self, now: datetime) -> datetime | None:
        """When GitHub said a refused token may ask again, if it said.

        GitHub's documented order: `Retry-After` when present, otherwise
        the reset of a spent token.
        """
        if self.retry_after is not None:
            return now + timedelta(seconds=self.retry_after)
        if self.remaining == 0 and self.reset is not None:
            try:
                return datetime.fromtimestamp(self.reset, timezone.utc)
            except (OverflowError, OSError, ValueError):
                return None
        return None


@dataclass(frozen=True, slots=True)
class ConditionalResult:
    """The outcome of one conditional request.

    Exactly one of `unchanged`, `changed`, `absent`, `rate_limited`,
    `pending` and `failed` is true, so a caller cannot forget a case.
    """

    status: int
    etag: str | None = None
    payload: Any = None
    error: str = ''
    rate_limit: RateLimit = field(default_factory=RateLimit)
    #: Accepted, and not ready yet: GitHub is still producing the answer,
    #: as its asynchronous SBOM report says with 202 while it is being
    #: generated. Not a document, and not a failure. `conditional_get`
    #: never says this — a 202 means "not yet" only to a caller that
    #: knows its resource is produced asynchronously — so it is only
    #: ever set, with the 2xx that said it, by one that does.
    pending: bool = False

    @property
    def unchanged(self) -> bool:
        """304: the resource is as we last saw it, and this was free."""
        return self.status == NOT_MODIFIED

    @property
    def changed(self) -> bool:
        """2xx with a usable body."""
        return 200 <= self.status < 300 and not self.error and not self.pending

    @property
    def absent(self) -> bool:
        """404: gone, renamed, or never had the resource.

        Unless the caller has said why it is not (`error`). A 404 for the
        SBOM report GitHub accepted a moment before is an expired or lost
        report, which says nothing about whether the repository has a
        graph, and is a failure.
        """
        return self.status == NOT_FOUND and not self.error

    @property
    def rate_limited(self) -> bool:
        """The token was refused, not the resource.

        A 429; a 403 with no quota left; or a 403 carrying `Retry-After`,
        which is how GitHub marks a secondary limit while quota remains.
        It says nothing about the resource asked for, and every later
        request with the same token would be refused the same way.
        """
        if self.status == TOO_MANY_REQUESTS:
            return True
        if self.status != FORBIDDEN:
            return False
        return (
            self.rate_limit.remaining == 0
            or self.rate_limit.retry_after is not None
        )

    @property
    def failed(self) -> bool:
        """Anything we cannot act on — transport error, 5xx, bad body."""
        if self.unchanged or self.absent or self.rate_limited or self.pending:
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

    `session` must not cache: use `get_plain_client`. A requests-cache
    session answers from disk while its copy is fresh, so the condition
    never reaches GitHub; once the copy is stale it revalidates with its
    own ETag and hands back the stored 200 in place of GitHub's 304.
    Either way nothing is ever `unchanged`, and every check is billed.

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
    # Every answer says where the token stands, refusals included.
    rate_limit = RateLimit.from_headers(response.headers)

    if status == NOT_MODIFIED:
        # Keep the ETag we already had: a 304 may omit it.
        return ConditionalResult(
            status=status, etag=new_etag or etag, rate_limit=rate_limit,
        )

    if status == NOT_FOUND:
        return ConditionalResult(status=status, rate_limit=rate_limit)

    if not 200 <= status < 300:
        return ConditionalResult(
            status=status, error=f'HTTP {status}', rate_limit=rate_limit,
        )

    try:
        payload = response.json()
    except (ValueError, requests.RequestException) as e:
        return ConditionalResult(
            status=status, etag=new_etag, error=f'unparsable body: {e}',
            rate_limit=rate_limit,
        )

    return ConditionalResult(
        status=status, etag=new_etag, payload=payload, rate_limit=rate_limit,
    )
