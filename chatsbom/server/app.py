"""The web service's routes: the page, the chat, and /healthz (#134).

One FastAPI app, which `web serve` runs on uvicorn (`server`):

  /api/meta           the current snapshot's id and provenance, counted
                      against QUERY_RATE_LIMIT (`queries`, #144)
  /api/v/{snapshot}/{method}
                      one of the dataset's questions, asked of that
                      snapshot and kept for good, counted alike
  /api/ask/challenge  an ALTCHA challenge for the client asking, counted
                      against CHAT_RATE_LIMIT (`challenge`)
  /api/ask            a question, answered by the model as it streams,
                      with a challenge solved for it (`ask`, #140)
  /api/*              anything else there: a JSON 404, where the
                      Worker's fallback answered with the page
  /healthz            the service answers, for a peer outside the edge
                      alone: the check is the container's own
  /assets/*           the built page's assets, named by their content
                      and so cached for good
  anything else       the page, index.html, never cached unasked

What the Worker's asset server sent with the page, this sends with
every response, the API's included, less Turnstile's origin: the page
loads nothing from anywhere else now (#128, section 2.5).

Without DEEPSEEK_API_KEY the chat is off, and both of its routes say
so, the challenge's included: a page is not to solve one for a question
that could not be answered.

While it runs, a watchdog watches its event loop (`watchdog`), and
web.sqlite is tidied as it starts and each hour after: the days of the
spend ledger before yesterday, and the challenges past their expiry.
"""
import asyncio
import os
import sqlite3
from collections.abc import AsyncIterator
from collections.abc import Callable
from collections.abc import Mapping
from contextlib import asynccontextmanager
from datetime import datetime
from datetime import timezone
from pathlib import Path

import structlog
import uvicorn
from fastapi import APIRouter
from fastapi import FastAPI
from fastapi import Request
from fastapi.concurrency import run_in_threadpool
from fastapi.responses import FileResponse
from fastapi.responses import JSONResponse
from fastapi.responses import Response
from fastapi.staticfiles import StaticFiles
from starlette.datastructures import MutableHeaders
from starlette.exceptions import HTTPException
from starlette.types import ASGIApp
from starlette.types import Message
from starlette.types import Receive
from starlette.types import Scope
from starlette.types import Send

from chatsbom.server.ask import Asking
from chatsbom.server.ask import OFF
from chatsbom.server.ask import Pacing
from chatsbom.server.ask import utc_now
from chatsbom.server.challenge import Challenges
from chatsbom.server.clients import client_key
from chatsbom.server.clients import from_edge
from chatsbom.server.queries import Reads
from chatsbom.server.ratelimit import RateLimiter
from chatsbom.server.settings import Settings
from chatsbom.server.spend import Budget
from chatsbom.server.spend import SpendLedger
from chatsbom.server.state import WebState
from chatsbom.server.watchdog import Watchdog

logger = structlog.get_logger('web')

#: The page's policy, as the Worker's asset server sent it less
#: Turnstile's origin. Every source is this origin, and nothing inline
#: or evaluated is allowed: the built page has neither, and a policy that
#: allowed them would allow an injected script too. frame-src named
#: Turnstile alone, and went with it: default-src covers frames.
#:
#: The page is to run ALTCHA's widget within it. Its `altcha` entry
#: draws its styles from a <style> it writes and starts its workers from
#: blob: URLs, which this refuses; `altcha/external`, with `altcha.css`
#: and the workers imported as files of the build, is within it.
POLICY = '; '.join([
    "default-src 'self'",
    "script-src 'self'",
    "style-src 'self'",
    "img-src 'self'",
    "font-src 'self'",
    "connect-src 'self'",
    "object-src 'none'",
    "base-uri 'none'",
    "form-action 'none'",
    "frame-ancestors 'none'",
])

#: On every response.
SECURITY_HEADERS = {
    'Content-Security-Policy': POLICY,
    'X-Content-Type-Options': 'nosniff',
    # Tells a site the page links to where the reader came from, and no
    # more.
    'Referrer-Policy': 'strict-origin-when-cross-origin',
    # A window the page opens, or that opens it, gets no handle on it.
    'Cross-Origin-Opener-Policy': 'same-origin',
}

#: The assets are named by their content, so a changed file is a new
#: name, and a cached one is good for good.
IMMUTABLE = 'public, max-age=31536000, immutable'
#: The page names the assets, so a cached one could name the last
#: build's: it is asked for again each time.
NO_CACHE = 'no-cache'
#: What the API answers is for the one who asked: a challenge is for a
#: client, a refusal for a moment.
NO_STORE = 'no-store'

#: What each of the built page's files is, whatever the machine's
#: /etc/mime.types says, or whether it has one: a slim image has none,
#: and Python's own table has no fonts. With `nosniff`, a script served
#: as anything else is refused.
CONTENT_TYPES = {
    '.css': 'text/css; charset=utf-8',
    '.html': 'text/html; charset=utf-8',
    '.ico': 'image/vnd.microsoft.icon',
    '.js': 'text/javascript; charset=utf-8',
    '.json': 'application/json',
    '.mjs': 'text/javascript; charset=utf-8',
    '.png': 'image/png',
    '.svg': 'image/svg+xml',
    '.txt': 'text/plain; charset=utf-8',
    '.webmanifest': 'application/manifest+json',
    '.woff': 'font/woff',
    '.woff2': 'font/woff2',
}

#: What a question is told when its client has asked too often, as the
#: Worker's chat said it.
TOO_MANY = 'Too many questions. Wait a moment.'

#: How often web.sqlite is tidied.
TIDY_SECONDS = 3600.0


def answer(
    payload: object, status: int = 200,
    headers: Mapping[str, str] | None = None,
) -> JSONResponse:
    """JSON from the API, which nothing may keep."""
    return JSONResponse(
        payload, status,
        headers={**(headers or {}), 'Cache-Control': NO_STORE},
    )


def index_page(spa: Path) -> Response:
    """The page, for any path the SPA routes itself."""
    return FileResponse(
        spa / 'index.html',
        media_type=CONTENT_TYPES['.html'],
        headers={'Cache-Control': NO_CACHE},
    )


def peer(request: Request) -> str | None:
    """The TCP peer's address, as uvicorn saw it (`server`)."""
    return request.client.host if request.client else None


class Assets(StaticFiles):
    """The built page's assets/: each named by its content, and so
    cached for good. Its source maps are built, for reading a stack
    trace, and never served: they are the whole source."""

    async def get_response(self, path: str, scope: Scope) -> Response:
        if path.endswith('.map'):
            raise HTTPException(404)
        return await super().get_response(path, scope)

    def file_response(
        self,
        full_path: str | os.PathLike[str],
        stat_result: os.stat_result,
        scope: Scope,
        status_code: int = 200,
    ) -> Response:
        # A 304 as much as a 200: it renews what the cache holds.
        response = super().file_response(
            full_path, stat_result, scope, status_code,
        )
        response.headers['Cache-Control'] = IMMUTABLE
        if isinstance(response, FileResponse):
            response.headers['Content-Type'] = CONTENT_TYPES.get(
                Path(full_path).suffix.lower(), 'application/octet-stream',
            )
        return response


class SecurityHeaders:
    """Sets SECURITY_HEADERS on every response as it starts.

    Plain ASGI rather than a `BaseHTTPMiddleware`, which reads a
    response into a queue of its own: the chat is to stream its answers
    as they come.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope['type'] != 'http':
            await self.app(scope, receive, send)
            return

        async def sending(message: Message) -> None:
            if message['type'] == 'http.response.start':
                headers = MutableHeaders(scope=message)
                for name, value in SECURITY_HEADERS.items():
                    headers[name] = value
            await send(message)

        await self.app(scope, receive, sending)


async def refused(request: Request, error: Exception) -> Response:
    """An HTTP error, the router's or a route's, as the API answers:
    JSON, `{"error": ...}`, as the Worker's were, and kept by no one."""
    if not isinstance(error, HTTPException):
        raise error
    return answer({'error': error.detail}, error.status_code, error.headers)


async def failed(request: Request, error: Exception) -> Response:
    """Anything a route did not expect, as a 500 that says nothing of
    it: uvicorn logs the error. Answered outside the middleware that
    sets SECURITY_HEADERS, so it sets them itself."""
    return answer({'error': 'Unexpected failure.'}, 500, SECURITY_HEADERS)


def tidy(ledger: SpendLedger, challenges: Challenges) -> None:
    """Delete the spend ledger's days before yesterday, and forget the
    challenges past their expiry. A failure is said and waited out: the
    next hour tries again."""
    now = datetime.now(timezone.utc)
    try:
        reservations = ledger.forget_before_yesterday(now)
        used = challenges.forget_expired(now=now.timestamp())
    except (sqlite3.Error, OSError) as error:
        logger.error('web.sqlite not tidied', error=str(error))
        return
    if reservations or used:
        logger.info(
            'web.sqlite tidied', reservations=reservations, challenges=used,
        )


def create_app(
    settings: Settings,
    *,
    tidy_every: float = TIDY_SECONDS,
    watchdog: Watchdog | None = None,
    challenges: Challenges | None = None,
    clock: Callable[[], datetime] = utc_now,
    pacing: Pacing = Pacing(),
) -> FastAPI:
    """The service, as `settings` configure it. web.sqlite is opened
    here, so that one that cannot be stops `web serve` before it
    listens.

    The rest is for the tests: challenges of another difficulty, a
    clock that says the hour they need, and turns that time out, and
    keep the stream alive, sooner.
    """
    state = WebState(settings.state_dir)
    ledger = SpendLedger(state)
    challenges = challenges or Challenges(settings.altcha_key, state)
    chat_limit = RateLimiter(settings.chat_limit)
    # A snapshot file's id is read here, as it starts.
    reads = Reads(settings.snapshot, RateLimiter(settings.query_limit))
    watching = watchdog or Watchdog()
    asking = None
    if settings.chat is not None and settings.snapshot is not None:
        asking = Asking(
            settings.chat,
            settings.snapshot,
            challenges=challenges,
            limit=chat_limit,
            budget=(
                None if settings.daily_cap_usd is None
                else Budget(ledger, settings.daily_cap_usd)
            ),
            clock=clock,
            pacing=pacing,
        )

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if settings.chat is None:
            logger.info('chat off: DEEPSEEK_API_KEY is not set')
        else:
            logger.info(
                'chat on', model=settings.chat.model,
                base_url=settings.chat.base_url,
                snapshot=str(settings.snapshot),
                max_in_flight=settings.chat.max_in_flight,
            )
        await run_in_threadpool(tidy, ledger, challenges)

        async def tidying() -> None:
            while True:
                await asyncio.sleep(tidy_every)
                await run_in_threadpool(tidy, ledger, challenges)

        tasks = [
            asyncio.create_task(watching.watch()),
            asyncio.create_task(tidying()),
        ]
        try:
            yield
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    # Without FastAPI's own pages: the documentation, and the schema.
    app = FastAPI(
        title='ChatSBOM', docs_url=None, redoc_url=None, openapi_url=None,
        lifespan=lifespan,
    )
    app.add_middleware(SecurityHeaders)
    app.add_exception_handler(HTTPException, refused)
    app.add_exception_handler(Exception, failed)
    # What the chat is answering, for the log and the tests.
    app.state.asking = asking

    # Mounted rather than routed one by one: every path under /api/ is
    # the router's, so one it does not have is a 404 whatever its
    # method, and never the page.
    api = APIRouter()

    def client(request: Request) -> str:
        return client_key(peer(request), request.headers, settings.edge)

    # Not `async`, these two: each opens its snapshot, which runs in a
    # thread, and the connection is that thread's.
    @api.get('/meta')
    def meta(request: Request) -> Response:
        return reads.meta(client(request))

    @api.get('/v/{snapshot}/{method}')
    def read(request: Request, snapshot: str, method: str) -> Response:
        return reads.read(
            client(request), snapshot, method,
            request.query_params.multi_items(),
        )

    # Not `async`: issuing derives a key, which runs in a thread.
    @api.get('/ask/challenge')
    def challenge(request: Request) -> Response:
        if asking is None:
            return answer({'error': OFF, 'code': 'off'}, 503)
        asker = client(request)
        if not chat_limit.admit(asker):
            return answer({'error': TOO_MANY, 'code': 'rate'}, 429)
        return answer(challenges.issue(asker))

    @api.post('/ask')
    async def ask(request: Request) -> Response:
        if asking is None:
            return answer({'error': OFF, 'code': 'off'}, 503)
        return await asking.ask(request, client(request))

    @app.api_route('/healthz', methods=['GET', 'HEAD'])
    async def healthz(request: Request) -> Response:
        # What comes through the tunnel is the public's: for it, there is
        # no such page.
        if from_edge(peer(request), settings.edge):
            raise HTTPException(404)
        return answer({'status': 'ok'})

    app.mount('/api', api)
    app.mount('/assets', Assets(directory=settings.spa / 'assets'))

    @app.api_route('/{path:path}', methods=['GET', 'HEAD'])
    async def page(path: str) -> Response:
        return index_page(settings.spa)

    return app


def server(app: FastAPI, host: str, port: int) -> uvicorn.Server:
    """uvicorn, serving `app` on `host` and `port` as `web serve` does."""
    return uvicorn.Server(
        uvicorn.Config(
            app,
            host=host,
            port=port,
            # The TCP peer is who the request came from, and `clients`
            # decides which headers to believe. uvicorn's default takes
            # X-Forwarded-For from 127.0.0.1 as the client's address,
            # which anyone on the host could write.
            proxy_headers=False,
            server_header=False,
            # Logged where the CLI logs, on stderr and in its format:
            # uvicorn's own configuration writes each request to stdout.
            log_config=None,
            # The same server whatever else is installed: the standard
            # library's event loop, the pure-Python HTTP parser, and no
            # websockets.
            loop='asyncio',
            http='h11',
            ws='none',
            # A lifespan that fails, stops the service.
            lifespan='on',
        ),
    )
