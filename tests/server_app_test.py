"""The web service's routes: the page, a challenge, /healthz (#134).

What the Worker's asset server answered with web/public/_headers, and
the Worker's own routes in code (web/src/worker.ts), in one FastAPI
app: the SPA, its content-hashed assets cached for good and its page
never, the same headers on everything, less Turnstile's origin, and a
JSON 404 for an API path that is not one.
"""
import asyncio
import json
import re
import socket
import threading
import time
import urllib.request
from collections.abc import Iterator
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient

from chatsbom.server import app as web
from chatsbom.server.app import create_app
from chatsbom.server.app import POLICY
from chatsbom.server.app import server
from chatsbom.server.challenge import Challenges
from chatsbom.server.challenge import Verdict
from chatsbom.server.settings import Settings
from chatsbom.server.settings import settings_from
from chatsbom.server.spend import SpendLedger
from chatsbom.server.state import WebState
from tests.server_challenge_test import solved

ROOT = Path(__file__).resolve().parents[1]

INDEX = '<!doctype html><title>ChatSBOM</title>'
SCRIPT = 'index-C0ffee.js'

#: Where cloudflared is, and a visitor it names.
TUNNEL = '172.30.0.2'
VISITOR = '203.0.113.7'
#: A peer outside the edge: the host, or anyone on its network.
OUTSIDE = '198.51.100.20'

IMMUTABLE = 'public, max-age=31536000, immutable'


@pytest.fixture
def spa(tmp_path: Path) -> Path:
    """A built page, with what the build leaves beside it for the
    Cloudflare asset server."""
    root = tmp_path / 'client'
    assets = root / 'assets'
    assets.mkdir(parents=True)
    (root / 'index.html').write_text(INDEX)
    (root / '_headers').write_text('/*\n  X-From-The-File: 1\n')
    (root / '.assetsignore').write_text('*.map\n')
    (assets / SCRIPT).write_text('console.log(1)')
    (assets / f'{SCRIPT}.map').write_text('{"sources": []}')
    (assets / 'index-C0ffee.css').write_text('body{}')
    (assets / 'plex-400-normal.woff2').write_bytes(b'wOF2')
    return root


def configure(spa: Path, tmp_path: Path, **environ: str) -> Settings:
    return settings_from(
        {
            'ALTCHA_HMAC_KEY': 'k' * 32,
            'EDGE_SUBNET': '172.30.0.0/24',
            'WEB_STATE_DIR': str(tmp_path / 'state'),
            **environ,
        },
        spa=spa,
    )


@pytest.fixture
def settings(spa: Path, tmp_path: Path) -> Settings:
    return configure(spa, tmp_path)


@pytest.fixture
def app(settings: Settings) -> FastAPI:
    return create_app(settings)


def visit(app: FastAPI, peer: str = OUTSIDE, **options: Any) -> TestClient:
    """A client whose TCP peer is `peer`."""
    return TestClient(app, client=(peer, 50000), **options)


@pytest.fixture
def client(app: FastAPI) -> Iterator[TestClient]:
    with visit(app) as client:
        yield client


class TestTheHeaders:
    """web/public/_headers, less Turnstile's origin (#128, section 2.5),
    on every response rather than only the asset server's."""

    PATHS = [
        '/', '/query/mail', f'/assets/{SCRIPT}', '/assets/missing.js',
        '/api/ask/challenge', '/api/nothing', '/healthz',
    ]

    @pytest.mark.parametrize('path', PATHS)
    def test_are_on_every_response(self, client, path):
        headers = client.get(path).headers
        assert headers['content-security-policy'] == POLICY
        assert headers['x-content-type-options'] == 'nosniff'
        assert headers['referrer-policy'] == 'strict-origin-when-cross-origin'
        assert headers['cross-origin-opener-policy'] == 'same-origin'

    def test_the_policy_is_the_pages_own_less_turnstile(self):
        """The file the Worker's asset server reads is the source."""
        page = rules((ROOT / 'web' / 'public' / '_headers').read_text())['/*']
        turnstile = 'https://challenges.cloudflare.com'
        theirs = {
            name: [source for source in sources if source != turnstile]
            for name, sources in directives(
                page['content-security-policy'],
            ).items()
        }
        # frame-src named Turnstile alone, and goes with it: default-src
        # covers it.
        assert theirs.pop('frame-src') == []
        assert directives(POLICY) == theirs

    def test_the_other_headers_are_the_pages_own(self, client):
        page = rules((ROOT / 'web' / 'public' / '_headers').read_text())['/*']
        headers = client.get('/').headers
        for name in ('x-content-type-options', 'referrer-policy'):
            assert headers[name] == page[name]

    def test_the_policy_names_no_origin(self):
        """Nothing third-party is left once Turnstile goes."""
        assert not re.search(r'[a-z][a-z0-9+.-]*://', POLICY, re.I)
        for sources in directives(POLICY).values():
            assert set(sources) <= {"'self'", "'none'"}

    def test_the_policy_allows_nothing_inline_or_evaluated(self):
        assert not re.search(
            r"'unsafe-|'strict-dynamic'|\bdata:|\bblob:|\*", POLICY,
        )
        policy = directives(POLICY)
        assert policy['default-src'] == ["'self'"]
        assert policy['object-src'] == ["'none'"]
        assert policy['base-uri'] == ["'none'"]
        assert policy['form-action'] == ["'none'"]

    def test_the_page_may_not_be_framed(self):
        assert directives(POLICY)['frame-ancestors'] == ["'none'"]

    def test_are_on_an_unexpected_failure_too(self, app, monkeypatch):
        """A 500 is answered outside the middleware that adds them."""
        def broken(spa: Path) -> None:
            raise RuntimeError('the disk went away')

        monkeypatch.setattr(web, 'index_page', broken)
        with visit(app, raise_server_exceptions=False) as client:
            response = client.get('/')
        assert response.status_code == 500
        assert response.json() == {'error': 'Unexpected failure.'}
        assert response.headers['cache-control'] == 'no-store'
        assert response.headers['content-security-policy'] == POLICY
        assert 'disk' not in response.text


def rules(text: str) -> dict[str, dict[str, str]]:
    """`_headers` as the asset server reads it (web/test/headers.test.ts):
    a line starting with `/` opens a rule, the `Name: value` lines under
    it are its headers, and `#` starts a comment."""
    by_path: dict[str, dict[str, str]] = {}
    current: dict[str, str] = {}
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line.startswith('#'):
            continue
        if line.startswith('/'):
            current = by_path.setdefault(line, {})
            continue
        name, _, value = line.partition(':')
        current[name.strip().lower()] = value.strip()
    return by_path


def directives(policy: str) -> dict[str, list[str]]:
    found = {}
    for part in policy.split(';'):
        words = part.split()
        if words:
            found[words[0]] = words[1:]
    return found


class TestThePage:
    @pytest.mark.parametrize(
        'path',
        [
            '/', '/index.html',
            # The SPA owns its own routing: `#/query/mail` never reaches
            # the server, but `/query/mail` as a path must not 404.
            '/query/mail',
            # The route that streamed Parquet is gone, not broken.
            '/data/artifacts.parquet',
            # /api/* is the API, and /api alone the page, as the Worker
            # ran first for /api/* only; and /assets/* the assets.
            '/api', '/assets',
            # FastAPI's own pages are off.
            '/docs', '/redoc', '/openapi.json',
        ],
    )
    def test_is_what_every_other_path_answers(self, client, path):
        response = client.get(path)
        assert response.status_code == 200
        assert response.text == INDEX
        assert response.headers['content-type'] == 'text/html; charset=utf-8'

    def test_is_never_cached_without_asking(self, client):
        """A new build names new assets, and a cached page would go on
        asking for the old ones."""
        assert client.get('/query/mail').headers['cache-control'] == 'no-cache'

    @pytest.mark.parametrize('path', ['/_headers', '/.assetsignore'])
    def test_what_the_build_leaves_for_cloudflare_is_not_served(
        self, client, path,
    ):
        assert client.get(path).text == INDEX

    def test_answers_head(self, client):
        response = client.head('/query/mail')
        assert response.status_code == 200
        assert response.content == b''
        assert response.headers['cache-control'] == 'no-cache'

    def test_answers_nothing_else(self, client):
        response = client.post('/query/mail')
        assert response.status_code == 405
        assert response.json() == {'error': 'Method Not Allowed'}
        assert response.headers['cache-control'] == 'no-store'


class TestTheAssets:
    def test_are_cached_for_good(self, client):
        """Named by their content, so a changed file is a new name."""
        response = client.get(f'/assets/{SCRIPT}')
        assert response.status_code == 200
        assert response.text == 'console.log(1)'
        assert response.headers['cache-control'] == IMMUTABLE

    @pytest.mark.parametrize(
        'name,kind',
        [
            (SCRIPT, 'text/javascript; charset=utf-8'),
            ('index-C0ffee.css', 'text/css; charset=utf-8'),
            ('plex-400-normal.woff2', 'font/woff2'),
        ],
    )
    def test_say_what_they_are_wherever_the_service_runs(
        self, client, name, kind,
    ):
        """With `nosniff`, a script served as anything else is refused;
        a slim image has no /etc/mime.types, whose `font/woff2` Python
        would otherwise take."""
        assert client.get(f'/assets/{name}').headers['content-type'] == kind

    def test_answer_a_conditional_request_as_unchanged(self, client):
        first = client.get(f'/assets/{SCRIPT}')
        again = client.get(
            f'/assets/{SCRIPT}',
            headers={'if-none-match': first.headers['etag']},
        )
        assert again.status_code == 304
        assert again.headers['cache-control'] == IMMUTABLE

    def test_answer_head(self, client):
        response = client.head(f'/assets/{SCRIPT}')
        assert response.status_code == 200
        assert response.content == b''

    @pytest.mark.parametrize(
        'path', ['/assets/missing.js', f'/assets/{SCRIPT}.map', '/assets/'],
    )
    def test_one_that_is_not_there_is_a_404_never_the_page(self, client, path):
        """Not the page: HTML cached for a year under a script's name.
        Nor a source map, which the build writes and never publishes
        (web/public/.assetsignore)."""
        response = client.get(path)
        assert response.status_code == 404
        assert response.json() == {'error': 'Not Found'}
        assert response.headers['cache-control'] == 'no-store'


class TestTheAPI:
    @pytest.mark.parametrize(
        'method,path',
        [
            ('GET', '/api/nothing'),
            ('GET', '/api/'),
            # The Worker's query endpoint is not this service's.
            ('POST', '/api/q'),
            ('POST', '/api/chat'),
        ],
    )
    def test_answers_a_path_it_does_not_have_with_a_json_404(
        self, client, method, path,
    ):
        """The Worker's fallback answered one with the page: HTML where
        the caller expected JSON."""
        response = client.request(method, path)
        assert response.status_code == 404
        assert response.json() == {'error': 'Not Found'}
        assert response.headers['cache-control'] == 'no-store'

    def test_answers_a_method_it_does_not_take_with_a_405(self, client):
        response = client.post('/api/ask/challenge')
        assert response.status_code == 405
        assert response.headers['allow'] == 'GET'
        assert response.headers['cache-control'] == 'no-store'


class TestTheChallenge:
    def challenge(self, client: TestClient, **headers: str) -> dict[str, Any]:
        response = client.get('/api/ask/challenge', headers=headers)
        assert response.status_code == 200, response.text
        assert response.headers['cache-control'] == 'no-store'
        issued: dict[str, Any] = response.json()
        return issued

    def test_is_for_the_visitor_the_edge_names(self, app):
        with visit(app, TUNNEL) as client:
            issued = self.challenge(client, **{'cf-connecting-ip': VISITOR})
        assert issued['parameters']['data'] == {'client': VISITOR}
        assert issued['signature']

    def test_is_for_a_peer_outside_the_edge_whatever_it_claims(self, app):
        with visit(app, OUTSIDE) as client:
            issued = self.challenge(client, **{'cf-connecting-ip': VISITOR})
        assert issued['parameters']['data'] == {'client': OUTSIDE}

    def test_is_for_an_ipv6_visitors_64(self, app):
        with visit(app, TUNNEL) as client:
            issued = self.challenge(
                client, **{'cf-connecting-ip': '2001:db8:1:2::7'},
            )
        assert issued['parameters']['data'] == {
            'client': '2001:db8:1:2::/64',
        }

    def test_is_solved_and_verified_for_its_client_alone(self, app, settings):
        """Once, at the real difficulty (about a second here)."""
        with visit(app, TUNNEL) as client:
            issued = self.challenge(client, **{'cf-connecting-ip': VISITOR})
        challenges = Challenges(
            settings.altcha_key, WebState(settings.state_dir),
        )
        payload = solved(issued)
        assert challenges.verify(payload, '203.0.113.8') is (
            Verdict.OTHER_CLIENT
        )
        assert challenges.verify(payload, VISITOR) is Verdict.VERIFIED

    def test_is_counted_against_the_chats_limit(self, spa, tmp_path):
        """A challenge costs a key derivation to make, and is made for a
        question: counted as questions are, per client."""
        app = create_app(configure(spa, tmp_path, CHAT_RATE_LIMIT='2/60'))
        with visit(app, TUNNEL) as client:
            statuses = [
                client.get(
                    '/api/ask/challenge',
                    headers={'cf-connecting-ip': VISITOR},
                ).status_code
                for _ in range(3)
            ]
            other = client.get(
                '/api/ask/challenge',
                headers={'cf-connecting-ip': '203.0.113.8'},
            )
            refused = client.get(
                '/api/ask/challenge', headers={'cf-connecting-ip': VISITOR},
            )
        assert statuses == [200, 200, 429]
        assert other.status_code == 200
        assert refused.json() == {
            'error': 'Too many questions. Wait a moment.',
        }
        assert refused.headers['cache-control'] == 'no-store'

    def test_a_spoofed_header_buys_no_new_budget(self, spa, tmp_path):
        app = create_app(configure(spa, tmp_path, CHAT_RATE_LIMIT='2/60'))
        with visit(app, OUTSIDE) as client:
            statuses = [
                client.get(
                    '/api/ask/challenge',
                    headers={'cf-connecting-ip': f'203.0.113.{n}'},
                ).status_code
                for n in range(3)
            ]
        assert statuses == [200, 200, 429]


class TestHealthz:
    def test_answers_a_peer_outside_the_edge(self, app):
        with visit(app, '127.0.0.1') as client:
            response = client.get('/healthz')
        assert response.status_code == 200
        assert response.json() == {'status': 'ok'}
        assert response.headers['cache-control'] == 'no-store'

    def test_answers_head(self, app):
        with visit(app, '127.0.0.1') as client:
            assert client.head('/healthz').status_code == 200

    @pytest.mark.parametrize('headers', [{}, {'cf-connecting-ip': '127.0.0.1'}])
    def test_is_not_there_for_the_edge(self, app, headers):
        """What comes through the tunnel is the public's; the check is
        the container's own (#128, section 2.7)."""
        with visit(app, TUNNEL) as client:
            response = client.get('/healthz', headers=headers)
        assert response.status_code == 404
        assert response.json() == {'error': 'Not Found'}

    def test_answers_everyone_with_no_edge_configured(self, spa, tmp_path):
        app = create_app(configure(spa, tmp_path, EDGE_SUBNET=''))
        with visit(app, TUNNEL) as client:
            assert client.get('/healthz').status_code == 200


class TestWhileItRuns:
    def test_the_watchdog_watches_the_loop_and_stops_with_it(self, app):
        with visit(app):
            assert watchdogs()
        for thread in watchdogs():
            thread.join(timeout=5)
        assert watchdogs() == []

    def test_web_sqlite_is_tidied_as_it_starts(self, app, settings):
        """Days before yesterday, and challenges past their expiry."""
        now = datetime.now(timezone.utc)
        state = WebState(settings.state_dir)
        ledger = SpendLedger(state)
        ledger.reserve('old', 0.1, 5, now - timedelta(days=3))
        ledger.reserve('today', 0.1, 5, now)
        with state.connect() as db:
            db.execute(
                "INSERT INTO used_challenges VALUES ('ab', ?)",
                (int(now.timestamp()) - 60,),
            )

        with visit(app):
            pass

        with state.connect() as db:
            left = db.execute('SELECT id FROM spend').fetchall()
            used = db.execute('SELECT * FROM used_challenges').fetchall()
        assert left == [('today',)]
        assert used == []

    def test_and_again_each_hour(self, settings):
        now = datetime.now(timezone.utc)
        app = create_app(settings, tidy_every=0.05)
        ledger = SpendLedger(WebState(settings.state_dir))
        with visit(app):
            ledger.reserve('old', 0.1, 5, now - timedelta(days=3))
            waited = time.monotonic()
            while ledger.usage((now - timedelta(days=3)).date().isoformat()).held:
                assert time.monotonic() - waited < 5, 'never tidied'
                time.sleep(0.02)


def watchdogs() -> list[threading.Thread]:
    return [
        thread for thread in threading.enumerate()
        if thread.name == 'event-loop-watchdog' and thread.is_alive()
    ]


class TestTheServer:
    """uvicorn as `web serve` runs it."""

    def test_keys_on_the_tcp_peer_not_a_forwarded_header(self, app):
        """uvicorn, by default, takes X-Forwarded-For from 127.0.0.1 as
        the client's address: a header anyone on the host could write.
        Served as `web serve` serves it, the peer is the peer."""
        with Serving(app) as base:
            request = urllib.request.Request(
                f'{base}/api/ask/challenge',
                headers={
                    'X-Forwarded-For': VISITOR,
                    'CF-Connecting-IP': VISITOR,
                    'Forwarded': f'for={VISITOR}',
                },
            )
            with urllib.request.urlopen(request, timeout=30) as response:
                issued = json.load(response)
                header = response.headers.get('server')
        assert issued['parameters']['data'] == {'client': '127.0.0.1'}
        # And it does not name itself.
        assert header is None


class Serving:
    """`app` on uvicorn, as `web serve` configures it, on a free port of
    127.0.0.1, until the block ends."""

    def __init__(self, app: FastAPI) -> None:
        self.socket = socket.socket()
        self.socket.bind(('127.0.0.1', 0))
        self.server = server(app, '127.0.0.1', self.socket.getsockname()[1])
        self.thread = threading.Thread(
            target=lambda: asyncio.run(self.server.serve([self.socket])),
        )

    def __enter__(self) -> str:
        self.thread.start()
        waited = time.monotonic()
        while not self.server.started:
            assert self.thread.is_alive(), 'the server did not start'
            assert time.monotonic() - waited < 30, 'the server did not start'
            time.sleep(0.01)
        host, port = self.socket.getsockname()
        return f'http://{host}:{port}'

    def __exit__(self, *exc: object) -> None:
        self.server.should_exit = True
        self.thread.join(timeout=30)
        self.socket.close()
