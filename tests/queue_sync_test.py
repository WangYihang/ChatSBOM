"""`queue sync` end to end, over the sessions it really uses.

The service tests drive `SyncService` with a fake observer, so they
cannot see which session the command hands to `conditional_get`. That is
where the bug was: sync went through the requests-cache session, which
answered from disk for a week and, once stale, turned GitHub's 304 back
into the stored 200. No re-check was ever counted as unchanged, and every
one was billed against the quota.

So here the transport underneath *every* session is faked. Whichever one
the command picks, the test sees exactly what reached GitHub.
"""
import io
import json
from pathlib import Path

import pytest
from requests.adapters import HTTPAdapter
from requests.models import Response
from requests.structures import CaseInsensitiveDict
from typer.testing import CliRunner
from urllib3.response import HTTPResponse

from chatsbom.__main__ import app
from chatsbom.core.container import Container
from chatsbom.core.ledger import Ledger

USER = 'https://api.github.com/user'
REPO = 'https://api.github.com/repos/o/r'
ETAG = 'W/"v1"'
LEDGER = Path('data/ledger.sqlite3')

runner = CliRunner()


def _response(request, status, headers, payload=None) -> Response:
    body = json.dumps(payload).encode() if payload is not None else b''
    response = Response()
    response.request = request
    response.url = request.url
    response.status_code = status
    response.headers = CaseInsensitiveDict(headers)
    response.encoding = 'utf-8'
    # requests-cache stores the urllib3 response, so it needs a real one.
    response.raw = HTTPResponse(
        body=io.BytesIO(body), headers=headers, status=status,
        preload_content=False, request_url=request.url,
    )
    return response


class FakeGitHub:
    """GitHub as `queue sync` sees it: the token check and one repository.

    The repository comes back with GitHub's own caching headers, and a
    request carrying its ETag is answered 304 — the free re-check the
    whole design rests on.
    """

    def __init__(self) -> None:
        self.refuse = False
        self.calls: list[dict[str, str]] = []

    def answer(self, request) -> Response:
        if request.url == USER:
            return _response(request, 200, {}, {'login': 'octocat'})

        assert request.url == REPO, request.url
        self.calls.append(dict(request.headers))

        if self.refuse:
            return _response(
                request, 403,
                {'X-RateLimit-Remaining': '0', 'X-RateLimit-Reset': '1789999999'},
                {'message': 'API rate limit exceeded'},
            )
        if request.headers.get('If-None-Match') == ETAG:
            return _response(
                request, 304, {'ETag': ETAG, 'X-RateLimit-Remaining': '4999'},
            )
        return _response(
            request, 200,
            {
                'ETag': ETAG,
                'Cache-Control': 'private, max-age=60, s-maxage=60',
                'Vary': 'Accept, Authorization, Cookie, X-GitHub-OTP',
                'Content-Type': 'application/json; charset=utf-8',
                'X-RateLimit-Remaining': '4998',
            },
            {'pushed_at': '2026-09-14T09:00:00Z'},
        )


@pytest.fixture
def github(tmp_path, monkeypatch):
    """A fresh working directory, container and GitHub for each test.

    The ledger and the requests-cache database both live under the
    working directory, so every test starts with an empty cache that
    cannot answer for GitHub.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(Container, '_instance', None)

    fake = FakeGitHub()
    monkeypatch.setattr(
        HTTPAdapter, 'send',
        lambda adapter, request, **kwargs: fake.answer(request),
    )

    with Ledger(LEDGER) as ledger:
        ledger.track(1, 'o', 'r', 'go')
    return fake


def sync():
    # --recheck-hours 0 makes the repository due again straight away.
    return runner.invoke(
        app,
        ['queue', 'sync', '--token', 'test-token', '--recheck-hours', '0'],
    )


def test_a_recheck_reaches_github_and_is_answered_304(github):
    first = sync()
    assert first.exit_code == 0, first.output

    second = sync()
    assert second.exit_code == 0, second.output

    assert len(github.calls) == 2, (
        'the re-check was answered from the local cache, not by GitHub'
    )
    assert github.calls[1].get('If-None-Match') == ETAG
    # GitHub only waives the rate limit for a 304 to an authorised request.
    assert github.calls[1].get('Authorization') == 'Bearer test-token'
    assert 'unchanged 1' in second.output


def test_a_refused_token_is_reported_not_blamed_on_the_repository(github):
    github.refuse = True
    result = sync()
    assert result.exit_code == 0, result.output

    with Ledger(LEDGER) as ledger:
        assert ledger.get(1).failure_count == 0
    # Rich wraps long lines; compare words, not layout.
    output = ' '.join(result.output.split())
    assert 'released the rest' in output
    assert 'again at 2026-09-21 14:13:19 UTC' in output, 'from X-RateLimit-Reset'
