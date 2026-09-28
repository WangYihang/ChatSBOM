"""`github depgraph` and `run`, over GitHub's asynchronous SBOM report.

The synchronous endpoint closes on 2026-11-13; after it, a graph is
asked for, looked at until GitHub has generated it, and downloaded from
a temporary URL. Both callers read `fetch`'s outcome, and the report adds
one: GitHub accepted the request and is still generating it. That is not
a failure, and it is not "no graph":

* `github depgraph` waits for it within a bound, and if it is still not
  ready, counts it as pending and records nothing, so the next run asks
  again. The run is not a failure for it;
* `run` leaves the stage due, and goes on to the next repository.

Only the transport and the clock are faked, as in
`depgraph_command_test.py`, so the real service, sessions and redirect
handling are what answer.
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
from tests.depgraph_command_test import _document
from tests.depgraph_command_test import _indexed
from tests.depgraph_command_test import _ledger
from tests.depgraph_command_test import _repository
from tests.depgraph_command_test import _response
from tests.depgraph_command_test import _said
from tests.depgraph_command_test import _stage
from tests.depgraph_command_test import BEFORE_RESET
from tests.depgraph_command_test import depgraph
from tests.depgraph_command_test import GRAPH
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
def github(tmp_path, monkeypatch, clock) -> FakeGitHub:
    """A fresh working directory, container and GitHub for each test,
    with the asynchronous flow chosen, as it is from 2026-11-13."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(Container, '_instance', None)
    monkeypatch.setenv('CHATSBOM_DEPGRAPH_API', 'async')

    fake = FakeGitHub()
    monkeypatch.setattr(
        HTTPAdapter, 'send',
        lambda adapter, request, **kwargs: fake.answer(adapter, request),
    )
    return fake


def json_of(path: Path) -> Any:
    return json.loads(path.read_text(encoding='utf-8'))


# --- github depgraph ------------------------------------------------------------

def test_a_report_is_stored_as_the_synchronous_endpoint_stored_it(github):
    """Byte for byte the document `db index`, `db edges` and `db raw`
    have always read."""
    _ledger('a')

    result = depgraph()

    assert result.exit_code == 0, result.output
    assert github.asked == [
        ('generate', 'a'), ('report', 'a'), ('download', 'a'),
    ]
    assert json_of(_document('a')) == GRAPH
    assert sorted(_indexed()) == [1]


def test_the_token_never_leaves_the_api(github):
    """The download is a signed URL on another host. GitHub's token is
    for GitHub's API."""
    _ledger('a')

    depgraph()

    assert github.tokens['generate'] == {'Bearer test-token'}
    assert github.tokens['report'] == {'Bearer test-token'}
    assert github.tokens['download'] == {None}


def test_a_report_ready_after_a_few_looks_is_collected(github, clock):
    _ledger('a')
    github.not_yet['a'] = 3

    result = depgraph()

    assert result.exit_code == 0, result.output
    assert json_of(_document('a')) == GRAPH
    assert github.asked.count(('report', 'a')) == 4
    assert sum(clock.slept) <= DependencyGraphService.REPORT_WAIT


def test_a_report_still_being_generated_is_pending_not_a_failure(github):
    """Left for the next run: nothing stored, nothing indexed, and
    neither "no graph" nor a failure — so the run exits cleanly and says
    to run again."""
    _ledger('a', 'b', 'c')
    github.not_yet['b'] = NEVER

    result = depgraph()

    assert result.exit_code == 0, result.output
    assert sorted(_indexed()) == [1, 3]
    assert not _document('b').exists()
    said = _said(result)
    assert 'no graph 0' in said and 'failed 0' in said
    assert 'pending 1' in said
    assert 'run again' in said
    assert ('generate', 'c') in github.asked, 'a wait stops nothing'


def test_a_report_still_being_generated_is_waited_for_within_a_bound(
    github, clock,
):
    _ledger('a')
    github.not_yet['a'] = NEVER

    depgraph()

    assert 0 < sum(clock.slept) <= DependencyGraphService.REPORT_WAIT
    assert github.asked.count(('report', 'a')) == len(clock.slept)


def test_a_pending_report_is_collected_by_the_next_run(github):
    _ledger('a')
    github.not_yet['a'] = NEVER
    depgraph()
    github.not_yet['a'] = 0

    result = depgraph()

    assert result.exit_code == 0, result.output
    assert json_of(_document('a')) == GRAPH
    assert sorted(_indexed()) == [1]


def test_no_graph_when_the_report_is_asked_for(github):
    _ledger('a', 'b')
    github.answers[('generate', 'a')] = (404, {}, {'message': 'Not Found'})

    result = depgraph()

    assert result.exit_code == 0, result.output
    assert sorted(_indexed()) == [2]
    assert 'no graph 1' in _said(result)


@pytest.mark.parametrize('call', ['generate', 'report'])
@pytest.mark.parametrize('status,headers', REFUSALS, ids=REFUSAL_IDS)
def test_a_refusal_on_either_call_stops_the_run(github, call, status, headers):
    _ledger('a', 'b', 'c')
    github.answers[(call, 'b')] = (status, headers, REFUSED)

    result = depgraph()

    assert result.exit_code != 0, 'a refused run is not a clean one'
    assert [asked for asked in github.asked if asked[1] == 'c'] == []
    assert sorted(_indexed()) == [1]
    said = _said(result)
    assert 'no graph 0' in said, 'b was refused, not found without a graph'
    assert 'rate limited' in said.lower()


def test_a_refusal_while_looking_says_when_to_come_back(github):
    _ledger('a')
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
    _ledger('a', 'b')
    github.answers[(call, 'a')] = answer

    result = depgraph()

    assert result.exit_code != 0
    assert sorted(_indexed()) == [2]
    said = _said(result)
    assert 'no graph 0' in said and 'failed 1' in said


def test_auto_collects_a_graph_the_synchronous_endpoint_times_out_on(
    github, monkeypatch,
):
    """Whichever side of the closure this runs on: before it, the report
    is what a failed export falls back to, and after it, the only way."""
    monkeypatch.setenv('CHATSBOM_DEPGRAPH_API', 'auto')
    _ledger('a')
    github.answers[('export', 'a')] = (
        500, {}, {'message': 'Request timed out'},
    )

    result = depgraph()

    assert result.exit_code == 0, result.output
    assert json_of(_document('a')) == GRAPH


# --- the same answers, as `chatsbom run` meets them ------------------------------

def test_run_stores_a_report_whole(github):
    stage = _stage()

    produced = stage(_repository('a'), {})

    assert produced == {'depgraph_path': str(_document('a'))}
    assert json_of(_document('a')) == GRAPH
    assert (stage.fetched, stage.pending, stage.failed) == (1, 0, 0)


def test_run_leaves_a_pending_report_due_and_asks_the_next(github, clock):
    """Not the stage's work, so not recorded, and the stage stays due for
    a later pass. Not a refusal either: the next repository is asked."""
    github.not_yet['a'] = NEVER
    stage = _stage()

    assert stage(_repository('a'), {}) is None
    assert 0 < sum(clock.slept) <= DependencyGraphService.REPORT_WAIT
    assert stage(_repository('b'), {}) == {
        'depgraph_path': str(_document('b')),
    }

    assert not _document('a').exists()
    assert (stage.pending, stage.fetched) == (1, 1)
    assert (stage.absent, stage.failed, stage.unasked) == (0, 0, 0)
    summary = ' '.join((stage.summary(BEFORE_RESET) or '').split())
    assert 'pending 1' in summary
    assert 'no graph 0' in summary and 'failed 0' in summary


def test_a_pending_report_alone_is_worth_a_summary(github):
    github.not_yet['a'] = NEVER
    stage = _stage()

    stage(_repository('a'), {})

    assert 'pending 1' in ' '.join((stage.summary(BEFORE_RESET) or '').split())


@pytest.mark.parametrize('call', ['generate', 'report'])
def test_a_refusal_on_either_call_stops_the_asking_in_run(github, call):
    github.answers[(call, 'a')] = (429, SPENT, REFUSED)
    stage = _stage()

    produced = [stage(_repository(name), {}) for name in ('a', 'b')]

    assert produced == [None, None]
    assert [asked for asked in github.asked if asked[1] == 'b'] == []
    assert (stage.unasked, stage.pending, stage.failed) == (1, 0, 0)
    summary = ' '.join((stage.summary(BEFORE_RESET) or '').split())
    assert 'rate limited' in summary and 'o/a' in summary
    assert '2026-09-21 14:13:19 UTC' in summary
