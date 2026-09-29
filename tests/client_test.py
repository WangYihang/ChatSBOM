import json
import sqlite3
import threading
from collections.abc import Iterator
from contextlib import closing
from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer
from pathlib import Path
from typing import Any

import pytest
import requests_cache
from requests.adapters import BaseAdapter
from requests.models import Response

from chatsbom.core.client import get_http_client
from chatsbom.core.client import get_plain_client
from chatsbom.core.logging import setup_logging


@pytest.fixture(autouse=True)
def scratch_directory(tmp_path, monkeypatch):
    """The cached client's database is relative to the working directory
    (`.requests-cache/` unless named), which is the checkout when the
    suite runs: every run left one there, and a `test_cache.sqlite`."""
    monkeypatch.chdir(tmp_path)


def test_get_http_client_returns_session():
    """Test get_http_client returns a requests Session."""
    session = get_http_client()
    assert hasattr(session, 'get')
    assert hasattr(session, 'post')
    assert callable(session.get)


def test_get_http_client_custom_params():
    """Test get_http_client with custom parameters."""
    session = get_http_client(
        cache_name='test_cache',
        expire_after=3600,
        retries=5,
        pool_size=100,
    )
    assert session is not None


def test_get_http_client_has_adapters():
    """Test session has http and https adapters mounted."""
    session = get_http_client()
    assert 'https://' in session.adapters
    assert 'http://' in session.adapters


def test_get_http_client_retry_on_server_errors():
    """Test retry adapter is configured for server errors."""
    session = get_http_client(retries=3)
    adapter = session.get_adapter('https://example.com')
    # The adapter should have retry configuration
    assert adapter.max_retries is not None


# --- the cache, in front of GitHub ----------------------------------------

#: A token, as GitHub writes them.
TOKEN = 'ghp_n0tInTh3C4ch3'

#: What `GitHubService` sends with every call.
AUTHORISED = {
    'Authorization': f'Bearer {TOKEN}',
    'Accept': 'application/vnd.github.v3+json',
    'User-Agent': 'ChatSBOM',
}

#: The validator GitHub's API answers with, and answers 304 to.
ETAG = 'W/"7c0ffee5eed"'

#: What GitHub's API answers with, of what a cache reads: a validator, a
#: lifetime, and the request headers the answer varies by, `Authorization`
#: among them, on the two lines GitHub sends.
GITHUB_HEADERS = [
    ('Content-Type', 'application/json; charset=utf-8'),
    ('Cache-Control', 'private, max-age=60, s-maxage=60'),
    ('ETag', ETAG),
    ('Vary', 'Accept, Authorization, Cookie, X-GitHub-OTP'),
    ('Vary', 'Accept-Encoding, Accept, X-Requested-With'),
    ('X-RateLimit-Limit', '5000'),
    ('X-RateLimit-Remaining', '4999'),
]


#: Where GitHub's API sends a renamed repository's old name: to the
#: repository by its id, with a permanent redirect on the same host, for
#: which requests keeps `Authorization` on the request. And a second
#: name before that one, for two redirects in a row.
MOVED = {
    '/repos/old/r': '/repositories/42',
    '/repos/older/r': '/repos/old/r',
}


class GitHub(ThreadingHTTPServer):
    """GitHub's API, on this machine: every GET is answered as GitHub
    answers, and the path of each request that reached it is kept, with
    the `Authorization` it came with."""

    def __init__(self) -> None:
        super().__init__(('127.0.0.1', 0), GitHubAnswer)
        self.reached: list[str] = []
        self.authorisations: list[tuple[str, str | None]] = []

    def url(self, path: str) -> str:
        return f'http://127.0.0.1:{self.server_port}{path}'


class GitHubAnswer(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        assert isinstance(self.server, GitHub)
        self.server.reached.append(self.path)
        self.server.authorisations.append(
            (self.path, self.headers.get('Authorization')),
        )
        if self.path in MOVED:
            self.moved(self.server.url(MOVED[self.path]))
            return
        if self.headers.get('If-None-Match') == ETAG:
            self.send_response(304)
            for name, value in GITHUB_HEADERS:
                self.send_header(name, value)
            self.end_headers()
            return
        body = b'{"full_name": "o/r"}'
        self.send_response(200)
        for name, value in GITHUB_HEADERS:
            self.send_header(name, value)
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def moved(self, location: str) -> None:
        """A 301, as GitHub answers one: the new address, in `Location`
        and in the body."""
        body = json.dumps({
            'message': 'Moved Permanently',
            'url': location,
            'documentation_url': 'https://docs.github.com/rest',
        }).encode()
        self.send_response(301)
        self.send_header('Location', location)
        self.send_header('Content-Type', 'application/json; charset=utf-8')
        self.send_header('Content-Length', str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, format: str, *args: Any) -> None:
        """Nothing on stderr for each request."""


@pytest.fixture
def github(monkeypatch: pytest.MonkeyPatch) -> Iterator[GitHub]:
    """The server, reached directly whatever proxy the environment
    names."""
    for name in ('no_proxy', 'NO_PROXY'):
        monkeypatch.setenv(name, '127.0.0.1')
    server = GitHub()
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield server
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def github_client(tmp_path: Path) -> requests_cache.CachedSession:
    """The cached client, with a cache of its own and the headers
    `GitHubService` gives it."""
    session = get_http_client(cache_name=str(tmp_path / 'github.sqlite3'))
    session.headers.update(AUTHORISED)
    return session


def test_three_identical_gets_reach_github_once(github, tmp_path):
    """GitHub varies every answer by `Authorization`, which the cache
    leaves out of its keys and redacts from what it keeps. From
    requests-cache 1.3.2 an answer that varies by such a header never
    matches a request that carries it (#88): every call reached GitHub
    and spent quota, while the answer sat in the cache, where
    `GitHubService._is_cached` found it and sent the call on without its
    rate-limit handling."""
    session = github_client(tmp_path)
    url = github.url('/repos/o/r')

    answers = [session.get(url, timeout=10) for _ in range(3)]

    assert github.reached == ['/repos/o/r']
    assert [answer.from_cache for answer in answers] == [False, True, True]


def test_the_token_is_never_written_to_the_cache(github, tmp_path):
    """Neither in the request kept with the answer, nor anywhere in what
    the backend is given to write: its own serializer's output, whatever
    the backend is."""
    session = github_client(tmp_path)
    url = github.url('/repos/o/r')
    for _ in range(3):
        session.get(url, timeout=10)

    responses = session.cache.responses
    [stored] = responses.values()
    assert TOKEN not in str(stored.request.headers)
    assert TOKEN not in str(stored.headers)
    written = responses.serialize(stored)
    if isinstance(written, str):
        written = written.encode()
    assert TOKEN.encode() not in bytes(written)


def holding(secret: str, store: Path) -> list[str]:
    """Where `secret` is in a SQLite store, byte for byte: each of its
    files, and each table with a cell that holds it.

    The files are the database and what SQLite keeps beside it, a
    journal or a write-ahead log. Every byte of them, free pages too:
    whoever copies the file has those. Read once the client is closed,
    with nothing left for it to write.
    """
    wanted = secret.encode()
    found = [
        path.name for path in sorted(store.parent.glob(f'{store.name}*'))
        if wanted in path.read_bytes()
    ]
    with closing(sqlite3.connect(store)) as db:
        tables = [
            name for (name,) in
            db.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
        ]
        for table in tables:
            for row in db.execute(f'SELECT * FROM "{table}"'):
                cells = [
                    cell if isinstance(cell, bytes) else str(cell).encode()
                    for cell in row
                ]
                if any(wanted in cell for cell in cells):
                    found.append(f'table {table}')
                    break
    return found


def test_a_redirect_leaves_no_token_in_the_cache(github, tmp_path):
    """GitHub answers a renamed repository's old name with a redirect on
    the same host, for which requests keeps `Authorization`.
    requests-cache redacts the request the answer came from, and kept
    those of the redirects before it as they were sent: the token, in
    clear, in the file, for whoever can read it or a backup of it."""
    session = github_client(tmp_path)
    old = github.url('/repos/old/r')

    answers = [session.get(old, timeout=10) for _ in range(2)]
    session.close()

    # The redirect is followed, and kept: the second GET is answered from
    # the cache, redirect and all, without reaching GitHub.
    assert github.reached == ['/repos/old/r', '/repositories/42']
    assert [answer.from_cache for answer in answers] == [False, True]
    for answer in answers:
        assert answer.json() == {'full_name': 'o/r'}
        assert answer.url == github.url('/repositories/42')
        assert [hop.status_code for hop in answer.history] == [301]
    assert holding(TOKEN, tmp_path / 'github.sqlite3') == []


def test_two_redirects_leave_no_token_in_the_cache(github, tmp_path):
    """Past the first redirect, requests also keeps on each the request
    it sent next, and requests-cache kept that as it was sent too."""
    session = github_client(tmp_path)
    older = github.url('/repos/older/r')

    answers = [session.get(older, timeout=10) for _ in range(2)]
    session.close()

    assert [hop.status_code for hop in answers[0].history] == [301, 301]
    assert answers[1].from_cache
    assert [answer.json() for answer in answers] == [{'full_name': 'o/r'}] * 2
    assert holding(TOKEN, tmp_path / 'github.sqlite3') == []


def test_a_revalidated_redirect_leaves_no_token_in_the_cache(github, tmp_path):
    """Revalidated, an answer is saved again from the copy in the cache,
    for which GitHub's 304 stands in: through the same redaction."""
    session = github_client(tmp_path)
    old = github.url('/repos/old/r')
    session.get(old, timeout=10)

    revalidated = session.get(old, timeout=10, refresh=True)
    session.close()

    assert github.reached == ['/repos/old/r', '/repositories/42'] * 2
    assert revalidated.from_cache
    assert revalidated.json() == {'full_name': 'o/r'}
    assert holding(TOKEN, tmp_path / 'github.sqlite3') == []


def test_a_redirect_still_sends_the_token(github, tmp_path):
    """The redirects are redacted as the answer is saved, once they have
    been followed. Redacted by a hook as each came, the request after
    one would have gone without the token, being a copy of the one
    before: GitHub answers that as it answers anyone, from a rate limit
    of 60 an hour."""
    session = github_client(tmp_path)

    session.get(github.url('/repos/older/r'), timeout=10)

    assert github.authorisations == [
        ('/repos/older/r', f'Bearer {TOKEN}'),
        ('/repos/old/r', f'Bearer {TOKEN}'),
        ('/repositories/42', f'Bearer {TOKEN}'),
    ]


# --- the plain client, for conditional requests ---------------------------

class CountingAdapter(BaseAdapter):
    """Answers 200 to everything, and counts what reached it."""

    def __init__(self):
        super().__init__()
        self.sent = 0

    def send(self, request, **kwargs):
        self.sent += 1
        response = Response()
        response.status_code = 200
        response.request = request
        response.url = request.url
        response._content = b'{}'
        return response

    def close(self):
        pass


def test_get_plain_client_does_not_cache():
    """Conditional requests are their own cache. A second one in front
    answers from disk, and GitHub never gets to say 304."""
    session = get_plain_client()
    adapter = CountingAdapter()
    session.mount('https://', adapter)

    session.get('https://api.github.com/repos/o/r')
    session.get('https://api.github.com/repos/o/r')
    assert adapter.sent == 2


def test_get_plain_client_retries_server_errors():
    retry = get_plain_client().get_adapter('https://api.github.com').max_retries
    assert retry.is_retry('GET', 503)


def test_get_plain_client_hands_a_rate_limit_straight_back():
    """urllib3 honours Retry-After by sleeping and asking again, three
    times, while the token is refused. The caller has to see the 429 at
    once: a slice would rather stop than keep asking."""
    retry = get_plain_client().get_adapter('https://api.github.com').max_retries
    assert not retry.is_retry('GET', 429, has_retry_after=True)


# --- what the request log shows ------------------------------------------

#: A report's temporary download link, signed as S3 and Azure sign them.
#: Whoever holds it can fetch the report until it expires.
SIGNED = (
    'https://sbom-exports.example/uuid.json'
    '?X-Amz-Credential=AKIAEXAMPLE&X-Amz-Signature=5ec7e75ec7e7'
    '&sig=s3cr3t&jwt=eyJhbGciOiJIUzI1NiJ9'
)


def logged(capsys: pytest.CaptureFixture[str]) -> str:
    """Everything printed, its lines joined back together.

    Rich folds a URL longer than the line, which would hide a secret
    from `in` without hiding it from whoever reads the log.
    """
    captured = capsys.readouterr()
    return ''.join((captured.out + captured.err).splitlines())


def request(url: str, headers: dict[str, str] | None = None) -> None:
    setup_logging('INFO')
    session = get_plain_client()
    session.mount('https://', CountingAdapter())
    session.get(url, headers=headers)


def test_a_signed_url_is_logged_without_its_signature(capsys):
    request(SIGNED)

    log = logged(capsys)
    assert "url='https://sbom-exports.example/uuid.json?*****'" in log
    for secret in ('AKIAEXAMPLE', '5ec7e75ec7e7', 's3cr3t', 'eyJhbGciOi'):
        assert secret not in log


def test_the_token_is_never_logged(capsys):
    request(
        'https://api.github.com/repos/o/r',
        headers={'Authorization': 'Bearer ghp_n0tl0gg3d'},
    )

    log = logged(capsys)
    assert "url='https://api.github.com/repos/o/r'" in log
    assert 'ghp_n0tl0gg3d' not in log


@pytest.mark.parametrize(
    'url, shown',
    [
        # The API's own search and paging, which say what a line is for.
        (
            'https://api.github.com/search/repositories'
            '?q=language%3Ago&sort=stars&order=desc&per_page=100&page=3',
            'https://api.github.com/search/repositories'
            '?q=language%3Ago&sort=stars&order=desc&per_page=100&page=3',
        ),
        (
            'https://api.github.com/repos/o/r/releases?per_page=100&page=2',
            'https://api.github.com/repos/o/r/releases?per_page=100&page=2',
        ),
        # Anything else in a query may be what lets a reader fetch it,
        # under any name at all, beside ours or with no value.
        (
            'https://raw.example/o/r/f?token=GHSAT0AAAA',
            'https://raw.example/o/r/f?*****',
        ),
        (
            'https://api.github.com/repos/o/r?page=2&access_token=abc',
            'https://api.github.com/repos/o/r?*****',
        ),
        ('https://files.example/f?GHSAT0AAAA', 'https://files.example/f?*****'),
        # And a password in the address itself.
        (
            'https://octocat:hunter2@files.example/f',
            'https://files.example/f',
        ),
    ],
)
def test_what_the_request_log_shows_of_a_url(url, shown, capsys):
    request(url)

    assert f'url={shown!r}' in logged(capsys)
