"""Raw content, by a client of its own that carries no token (#161).

The content stage downloads each manifest from
`raw.githubusercontent.com`, at the commit, which spends no API quota
and needs no token: the repositories are public. So this client has
none to send, whatever the collector holds, and a file is asked for as
anyone would ask for it.

A file is read up to the room it has (`collector/content.Wanted.room`),
by what its `Content-Length` declares and then by what arrives, and no
further. A server error or a rate limit is asked again, twice, after
`Retry-After` or a second, then two, before it is left to the walk,
which records it as one that may pass (`collector/content.walk`). A
connection that fails is `Lost`.
"""
from __future__ import annotations

import asyncio
from collections.abc import Awaitable
from collections.abc import Callable
from types import TracebackType
from typing import Self
from urllib.parse import quote

import httpx2
import structlog

from chatsbom.__version__ import __version__
from chatsbom.collector.content import Answer
from chatsbom.collector.content import Got
from chatsbom.collector.content import Lost
from chatsbom.collector.content import TooLarge

logger = structlog.get_logger('collector.raw')

#: Where raw content is.
RAW = 'https://raw.githubusercontent.com'

#: How long a download may take: a manifest is 16 MiB at most.
TIMEOUT = httpx2.Timeout(30.0, connect=10.0)

#: Downloads at once, across every repository.
IN_FLIGHT = 16

#: A connection that could not be made is made again this often.
CONNECT_RETRIES = 2

#: An answer that may pass, asked again this often.
RETRIES = 2

#: The statuses that may pass.
PASSING = frozenset({429, 500, 502, 503, 504})

#: The longest a `Retry-After` is waited for, in seconds.
LONGEST_WAIT = 60.0

_CHUNK = 1 << 16


def _retry_after(headers: httpx2.Headers) -> float | None:
    value = headers.get('retry-after', '').strip()
    return float(value) if value.isdigit() else None


class RawClient:
    """Raw content at `base`, asked with no token."""

    def __init__(
        self,
        *,
        base: str = RAW,
        transport: httpx2.AsyncBaseTransport | None = None,
        timeout: httpx2.Timeout = TIMEOUT,
        in_flight: int = IN_FLIGHT,
        retries: int = RETRIES,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self._base = base.rstrip('/')
        self._retries = retries
        self._sleep = sleep
        self._slots = asyncio.Semaphore(max(1, in_flight))
        # No `Authorization`, and nothing that would add one.
        self._http = httpx2.AsyncClient(
            transport=(
                transport if transport is not None
                else httpx2.AsyncHTTPTransport(retries=CONNECT_RETRIES)
            ),
            timeout=timeout,
            follow_redirects=True,
            max_redirects=5,
            headers={'User-Agent': f'chatsbom/{__version__}'},
        )

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._http.aclose()

    def url(self, full_name: str, sha: str, path: str) -> str:
        """One file at one commit, each segment of its path quoted.

        Always the commit, for releases too: the files are stored under
        it, and a tag can have moved on since the commit stage resolved
        it (`content_service.raw_url`)."""
        quoted = '/'.join(
            quote(segment, safe='') for segment in path.split('/')
        )
        return f'{self._base}/{full_name}/{sha}/{quoted}'

    async def fetch(
        self, full_name: str, sha: str, path: str, room: int,
    ) -> Answer:
        """`path` at `sha`, its body read up to `room` bytes."""
        url = self.url(full_name, sha, path)
        async with self._slots:
            attempt = 0
            while True:
                try:
                    answer, wait = await self._once(url, room)
                except httpx2.HTTPError as error:
                    return Lost(f'{type(error).__name__}: {error}')
                passing = isinstance(answer, Got) and answer.status in PASSING
                if not passing or attempt >= self._retries:
                    logger.debug(
                        'Raw content', url=url,
                        status=getattr(answer, 'status', None),
                    )
                    return answer
                attempt += 1
                await self._sleep(
                    min(
                        wait if wait is not None else float(
                            attempt,
                        ), LONGEST_WAIT,
                    ),
                )

    async def _once(self, url: str, room: int) -> tuple[Answer, float | None]:
        async with self._http.stream('GET', url) as response:
            if response.status_code != 200:
                return (
                    Got(response.status_code), _retry_after(response.headers),
                )
            declared = response.headers.get('content-length', '')
            if declared.isdigit() and int(declared) > room:
                return TooLarge(int(declared)), None
            chunks: list[bytes] = []
            size = 0
            async for chunk in response.aiter_bytes(_CHUNK):
                size += len(chunk)
                if size > room:
                    return TooLarge(size), None
                chunks.append(chunk)
            return Got(200, b''.join(chunks)), None
