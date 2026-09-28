"""The dependency-graph stage over GitHub's asynchronous SBOM report.

The synchronous endpoint closes on 2026-11-13; after it, a graph is
asked for, looked at until GitHub has generated it, and downloaded from
a temporary URL. The stage reads `fetch`'s outcome, and the report adds
one: GitHub accepted the request and is still generating it. That is not
a failure, and it is not "no graph": `fetch` waits for it within a
bound, and if it is still not ready the stage records it as `pending`
and looks again in 15 minutes. The run is not a failure for it.

Only the transport, `git ls-remote` and the clock are faked, as in
`depgraph_command_test.py`, so the real service, sessions, redirect
handling, ledger and store are what answer.
"""
import json
from collections import defaultdict
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import pytest
import requests
from requests.adapters import HTTPAdapter
from requests.models import Response

from chatsbom.core.container import Container
from chatsbom.services import dependency_graph_service
from chatsbom.services.dependency_graph_service import DependencyGraphService
from tests.depgraph_command_test import _age
from tests.depgraph_command_test import _document
from tests.depgraph_command_test import _fetches
from tests.depgraph_command_test import _response
from tests.depgraph_command_test import _said
from tests.depgraph_command_test import _state
from tests.depgraph_command_test import _track
from tests.depgraph_command_test import depgraph
from tests.depgraph_command_test import GRAPH
from tests.depgraph_command_test import heads  # noqa: F401 - a fixture
from tests.depgraph_command_test import REFUSED
from tests.depgraph_command_test import SPENT
from tests.depgraph_command_test import USER

EXPORT = 'https://api.github.com/repos/o/{name}/dependency-graph/sbom'
#: "a temporary download URL": signed, and not the API.
DOWNLOADS = 'sbom-exports.example'

#: Longer than any bound: a report GitHub never finishes.
NEVER = 10 ** 6

REFUSALS = [
    (429, SPENT),
    (403, SPENT),
    (403, {'X-RateLimit-Remaining': '4000', 'Retry-After': '60'}),
]
REFUSAL_IDS = ['429', '403-no-quota-left', '403-secondary-limit']


class FakeGitHub:
    """GitHub's SBOM endpoints, as `github depgraph` and `run` see them.

    Each repository's report is ready at the first look, unless
    `not_yet` says for how many looks it is still being generated.
    `answers` replaces one call's answer for one repository —
    `('generate' | 'report' | 'download' | 'export', name)` to a status,
    headers and body, or an exception raised mid-request.
    """

    def __init__(self) -> None:
        self.not_yet: dict[str, int] = {}
        self.answers: dict[tuple[str, str], object] = {}
        #: `(call, repository)` for each request, in order.
        self.asked: list[tuple[str, str]] = []
        #: Each call's `Authorization` headers, as sent.
        self.tokens: dict[str, set[str | None]] = defaultdict(set)

    def answer(self, adapter: HTTPAdapter, request) -> Response:
        if request.url == USER:
            return _response(request, 200, {}, {'login': 'octocat'})

        call, name = self._route(request.url)
        self.asked.append((call, name))
        self.tokens[call].add(request.headers.get('Authorization'))

        answer = self.answers.get((call, name)) or self._default(call, name)
        if isinstance(answer, BaseException):
            raise answer
        assert isinstance(answer, tuple)
        status, headers, payload = answer
        return _response(request, status, headers, payload)

    @staticmethod
    def _route(url: str) -> tuple[str, str]:
        parts = urlsplit(url)
        if parts.netloc == DOWNLOADS:
            return 'download', parts.path.strip('/').removesuffix('.json')
        name = url.split('/')[5]
        export = EXPORT.format(name=name)
        if url == export:
            return 'export', name
        if url == f'{export}/generate-report':
            return 'generate', name
        assert url == f'{export}/fetch-report/uuid-{name}', url
        return 'report', name

    def _default(self, call: str, name: str) -> tuple:
        if call == 'export':
            return 200, {}, GRAPH
        if call == 'generate':
            report = f'{EXPORT.format(name=name)}/fetch-report/uuid-{name}'
            return 201, {}, {'sbom_url': report}
        if call == 'report':
            if self.not_yet.get(name, 0) > 0:
                self.not_yet[name] -= 1
                return 202, {}, None
            location = f'https://{DOWNLOADS}/{name}.json?signature=s3cr3t'
            return 302, {'Location': location}, None
        # The download: the SPDX document itself, without the wrapper
        # the synchronous endpoint put round it.
        return 200, {}, GRAPH['sbom']


class Clock:
    """Time that passes only when the service sleeps."""

    def __init__(self) -> None:
        self.now = 0.0
        self.slept: list[float] = []

    def monotonic(self) -> float:
        return self.now

    def sleep(self, seconds: float) -> None:
        self.slept.append(seconds)
        self.now += seconds


@pytest.fixture
def clock(monkeypatch) -> Clock:
    fake = Clock()
    monkeypatch.setattr(dependency_graph_service, 'time', fake)
    return fake


@pytest.fixture
def github(tmp_path, monkeypatch, clock, heads) -> FakeGitHub:  # noqa: F811
    """A fresh working directory, container and GitHub for each test,
    with the asynchronous flow chosen, as it is from 2026-11-13."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(Container, '_instance', None)
    monkeypatch.setenv('CHATSBOM_DEPGRAPH_API', 'async')
    monkeypatch.delenv('CHATSBOM_DEPGRAPH_TOKENS', raising=False)

    fake = FakeGitHub()
    monkeypatch.setattr(
        HTTPAdapter, 'send',
        lambda adapter, request, **kwargs: fake.answer(adapter, request),
    )
    return fake


def json_of(path: Path) -> Any:
    return json.loads(path.read_text(encoding='utf-8'))


# --- the stage over reports -----------------------------------------------------

def test_a_report_is_stored_as_the_synchronous_endpoint_stored_it(github):
    """Byte for byte the document `db index`, `db edges` and `db raw`
    have always read."""
    _track('a')

    result = depgraph()

    assert result.exit_code == 0, result.output
    assert github.asked == [
        ('generate', 'a'), ('report', 'a'), ('download', 'a'),
    ]
    assert json_of(_document('a')) == GRAPH
    assert _state('a').outcome == 'ok'


def test_the_token_never_leaves_the_api(github):
    """The download is a signed URL on another host. GitHub's token is
    for GitHub's API."""
    _track('a')

    depgraph()

    assert github.tokens['generate'] == {'Bearer test-token'}
    assert github.tokens['report'] == {'Bearer test-token'}
    assert github.tokens['download'] == {None}


def test_the_signed_download_url_is_never_logged(github):
    """Its query is the signature: whoever reads it can fetch the report
    until the link expires. The request log says which file it was."""
    _track('a')

    result = depgraph()

    assert result.exit_code == 0, result.output
    # Lines joined back: Rich folds a URL longer than the line.
    log = ''.join(result.output.splitlines())
    assert f"url='https://{DOWNLOADS}/a.json?*****'" in log
    assert 's3cr3t' not in log
    assert 'test-token' not in log


def test_a_report_ready_after_a_few_looks_is_collected(github, clock):
    _track('a')
    github.not_yet['a'] = 3

    result = depgraph()

    assert result.exit_code == 0, result.output
    assert json_of(_document('a')) == GRAPH
    assert github.asked.count(('report', 'a')) == 4
    assert sum(clock.slept) <= DependencyGraphService.REPORT_WAIT


def test_a_report_still_being_generated_is_pending_not_a_failure(github):
    """Nothing stored, and neither "no graph" nor a failure — so the run
    exits cleanly, and the stage looks again in 15 minutes."""
    _track('a', 'b', 'c')
    github.not_yet['b'] = NEVER

    result = depgraph()

    assert result.exit_code == 0, result.output
    assert _fetches('b') == []
    said = _said(result)
    assert 'no graph 0' in said and 'failed 0' in said
    assert 'pending 1' in said
    assert ('generate', 'c') in github.asked, 'a wait stops nothing'
    state = _state('b')
    assert state.outcome == 'pending' and state.failure_count == 0


def test_a_report_still_being_generated_is_waited_for_within_a_bound(
    github, clock,
):
    _track('a')
    github.not_yet['a'] = NEVER

    depgraph()

    assert 0 < sum(clock.slept) <= DependencyGraphService.REPORT_WAIT
    assert github.asked.count(('report', 'a')) == len(clock.slept)


def test_a_pending_report_is_collected_by_a_later_pass(github):
    _track('a')
    github.not_yet['a'] = NEVER
    depgraph()
    github.not_yet['a'] = 0
    _age('a', minutes=1)

    result = depgraph()

    assert result.exit_code == 0, result.output
    assert json_of(_document('a')) == GRAPH
    assert _state('a').outcome == 'ok'


def test_no_graph_when_the_report_is_asked_for(github):
    _track('a', 'b')
    github.answers[('generate', 'a')] = (404, {}, {'message': 'Not Found'})

    result = depgraph()

    assert result.exit_code == 0, result.output
    assert _fetches('a') == [] and len(_fetches('b')) == 1
    assert 'no graph 1' in _said(result)
    assert _state('a').outcome == 'absent'


@pytest.mark.parametrize('call', ['generate', 'report'])
@pytest.mark.parametrize('status,headers', REFUSALS, ids=REFUSAL_IDS)
def test_a_refusal_on_either_call_stops_the_asking(
    github, call, status, headers,
):
    _track('a', 'b', 'c')
    github.answers[(call, 'b')] = (status, headers, REFUSED)

    result = depgraph()

    assert result.exit_code != 0, 'a refused run is not a clean one'
    assert [asked for asked in github.asked if asked[1] == 'c'] == []
    said = _said(result)
    assert 'no graph 0' in said, 'b was refused, not found without a graph'
    assert 'rate limited' in said.lower()
    assert _state('b').outcome == '', 'nothing recorded for it'


def test_a_refusal_while_looking_says_when_to_come_back(github):
    _track('a')
    github.answers[('report', 'a')] = (429, SPENT, REFUSED)

    result = depgraph()

    assert result.exit_code != 0
    assert '2026-09-21 14:13:19 UTC' in _said(result), 'from X-RateLimit-Reset'


@pytest.mark.parametrize(
    'call,answer',
    [
        ('generate', (502, {}, {'message': 'Server Error'})),
        ('generate', requests.ConnectionError('reset')),
        # The report GitHub accepted a moment before is gone: that says
        # nothing about whether the repository has a graph.
        ('report', (404, {}, {'message': 'Not Found'})),
        ('report', (500, {}, {'message': 'Server Error'})),
        # The signed URL expired, or points at nothing.
        ('download', (403, {}, None)),
        ('download', (404, {}, None)),
    ],
    ids=[
        'generate-5xx', 'generate-transport', 'report-gone', 'report-5xx',
        'download-expired', 'download-gone',
    ],
)
def test_a_failure_on_any_call_fails_the_run_but_not_the_batch(
    github, call, answer,
):
    _track('a', 'b')
    github.answers[(call, 'a')] = answer

    result = depgraph()

    assert result.exit_code != 0
    assert _fetches('a') == [] and len(_fetches('b')) == 1
    said = _said(result)
    assert 'no graph 0' in said and 'failed 1' in said
    assert _state('a').outcome == 'failed'


def test_auto_collects_a_graph_the_synchronous_endpoint_times_out_on(
    github, monkeypatch,
):
    """Whichever side of the closure this runs on: before it, the report
    is what a failed export falls back to, and after it, the only way."""
    monkeypatch.setenv('CHATSBOM_DEPGRAPH_API', 'auto')
    _track('a')
    github.answers[('export', 'a')] = (
        500, {}, {'message': 'Request timed out'},
    )

    result = depgraph()

    assert result.exit_code == 0, result.output
    assert json_of(_document('a')) == GRAPH


def test_a_report_is_paced_by_every_request_it_cost(github, monkeypatch):
    """A report is two requests or more against the same bucket, so the
    token's next fetch waits for each of them."""
    from chatsbom.services import depgraph_stage
    spent: list[int] = []
    real = depgraph_stage.Pacer.spent

    def record(self, requests):
        spent.append(requests)
        real(self, requests)
    monkeypatch.setattr(depgraph_stage.Pacer, 'spent', record)
    _track('a')

    depgraph()

    assert spent == [2], 'generate, then one look'
