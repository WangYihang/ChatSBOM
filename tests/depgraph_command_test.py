"""`github depgraph` and `run --stage depgraph` end to end, over a faked
GitHub.

The stage used to walk the `07-sbom` lists, so a repository whose Syft
SBOM had failed never got its graph; it overwrote one file per
repository on every fetch; and it had no memory of a 404, so every pass
asked again about every repository without a graph. GitHub's endpoint
closes after 2026-11-13, which makes each of those a loss that cannot be
made good later (#51, #55).

It also once counted a refused token as "no graph": `fetch` answered None
for a 429 exactly as for a 404, and 890 refusals were folded into "3,133
with no graph published".

Only the transport and `git ls-remote` are faked, so the real service,
sessions, ledger and store are what answer.
"""
from __future__ import annotations

import io
import json
from datetime import date
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

import pytest
import requests
from requests.adapters import HTTPAdapter
from requests.models import Response
from requests.structures import CaseInsensitiveDict
from typer.testing import CliRunner
from urllib3.response import HTTPResponse

from chatsbom.__main__ import app
from chatsbom.core.container import Container
from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import Stage
from chatsbom.services.dependency_graph_service import closed_reason
from chatsbom.services.git_service import GitService

USER = 'https://api.github.com/user'
GRAPH_URL = 'https://api.github.com/repos/o/{name}/dependency-graph/sbom'

LEDGER = Path('data/ledger.sqlite3')
ROOT = Path('data/09-github-depgraph')

#: Repository name -> id.
REPOSITORIES = {'a': 1, 'b': 2, 'c': 3, 'd': 4}

#: The smallest document GitHub sends for a repository with a graph.
GRAPH = {'sbom': {'spdxVersion': 'SPDX-2.3', 'packages': []}}

#: What `git ls-remote --symref` says the default branch's HEAD is.
HEAD = ('main', 'a' * 40)

#: A spent token, as GitHub reports one. 1789999999 is
#: 2026-09-21 14:13:19 UTC.
SPENT = {'X-RateLimit-Remaining': '0', 'X-RateLimit-Reset': '1789999999'}

REFUSED = {'message': 'API rate limit exceeded'}

#: Fast enough that pacing never slows a test: 0.36 ms between requests.
RATE = '10000000'
#: Slow enough that one token cannot finish four repositories before a
#: second has started: 0.2 s between requests.
SHARED_RATE = '18000'

#: A day the synchronous endpoint still answers.
BEFORE_CLOSING = date(2026, 9, 28)

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
    """GitHub as the stage sees it: the token checks, then one
    dependency graph per repository.

    Every repository has a graph unless `answers` says otherwise, with a
    status, headers and body — or an exception, raised mid-request.
    `refuse` names tokens GitHub answers 401 at `/user`.
    """

    def __init__(self) -> None:
        self.answers: dict[str, object] = {}
        self.asked: list[str] = []
        #: The `Authorization` header of each graph request, in order.
        self.tokens: list[str | None] = []
        self.refuse: set[str] = set()
        #: For each graph request, whether the session that sent it
        #: would sleep through a 429 carrying `Retry-After` and ask again.
        self.slept_through: list[bool] = []

    def answer(self, adapter: HTTPAdapter, request) -> Response:
        authorization = request.headers.get('Authorization')
        if request.url == USER:
            token = str(authorization or '').removeprefix('Bearer ').strip()
            if token in self.refuse:
                return _response(request, 401, {}, {'message': 'Bad'})
            login = 'octocat' if token == 'test-token' else 'hubot'
            return _response(request, 200, {}, {'login': login})

        name = request.url.split('/')[5]
        assert request.url == GRAPH_URL.format(name=name), request.url
        self.asked.append(name)
        self.tokens.append(authorization)
        self.slept_through.append(
            adapter.max_retries.is_retry('GET', 429, has_retry_after=True),
        )

        answer = self.answers.get(name, (200, {}, GRAPH))
        if isinstance(answer, BaseException):
            raise answer
        if callable(answer):
            answer = answer(authorization)
        assert isinstance(answer, tuple)
        status, headers, payload = answer
        return _response(request, status, headers, payload)


class Heads:
    """`git ls-remote`, as the stage asks it: nothing leaves the test."""

    def __init__(self) -> None:
        self.answers: dict[str, tuple[str, str] | None] = {}
        self.asked: list[str] = []

    def __call__(self, owner: str, repo: str):
        self.asked.append(repo)
        return self.answers.get(repo, HEAD)


@pytest.fixture
def heads(monkeypatch) -> Heads:
    fake = Heads()
    monkeypatch.setattr(GitService, 'default_branch_head', fake)
    return fake


@pytest.fixture
def github(tmp_path, monkeypatch, heads) -> FakeGitHub:
    """A fresh working directory, container and GitHub for each test.

    `data/` and the requests-cache database both resolve against the
    working directory, so nothing here reaches the real ones.
    """
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(Container, '_instance', None)
    # The synchronous endpoint, by name: this GitHub serves nothing else,
    # and `auto` would ask for a report when an export fails, and for
    # nothing but reports from 2026-11-13. The report's answers are in
    # depgraph_report_command_test.
    monkeypatch.setenv('CHATSBOM_DEPGRAPH_API', 'sync')
    monkeypatch.delenv('CHATSBOM_DEPGRAPH_TOKENS', raising=False)
    # `sync` turns the stage off from the closing day; these run on a
    # day before it, whatever the date is.
    monkeypatch.setattr(
        'chatsbom.commands.github.depgraph.closed_reason',
        lambda setting, today: closed_reason(setting, BEFORE_CLOSING),
    )

    fake = FakeGitHub()
    monkeypatch.setattr(
        HTTPAdapter, 'send',
        lambda adapter, request, **kwargs: fake.answer(adapter, request),
    )
    return fake


def _track(*names: str, language: str = 'java', **stars: int) -> None:
    """The queue: the repositories the stage may ask about."""
    with Ledger(LEDGER) as ledger:
        for name in names:
            ledger.track(REPOSITORIES[name], 'o', name, language)
            if name in stars:
                ledger.seed(
                    REPOSITORIES[name], 'o', name, snapshot='all-test',
                    stars=stars[name],
                )


def _fetches(name: str) -> list[Path]:
    """Every document kept for a repository, oldest first."""
    return sorted(ROOT.glob(f'{REPOSITORIES[name]}/*/sbom.spdx.json'))


def _document(name: str) -> Path:
    """The one document kept for a repository."""
    [document] = _fetches(name)
    return document


def _state(name: str):
    with Ledger(LEDGER) as ledger:
        return ledger.stage_state(REPOSITORIES[name], Stage.DEPGRAPH)


def _age(name: str, **delta) -> None:
    """Move a repository's next attempt into the past."""
    with Ledger(LEDGER) as ledger:
        state = ledger.stage_state(REPOSITORIES[name], Stage.DEPGRAPH)
        assert state is not None
        state.next_attempt_at = datetime.now(timezone.utc) - timedelta(**delta)
        ledger.record_stage(state)


def _indexed() -> dict[int, list[dict]]:
    """`index.jsonl`: repository id -> each fetch logged for it."""
    path = ROOT / 'index.jsonl'
    if not path.exists():
        return {}
    out: dict[int, list[dict]] = {}
    for line in path.read_text(encoding='utf-8').splitlines():
        record = json.loads(line)
        out.setdefault(record['id'], []).append(record)
    return out


def depgraph(*args: str):
    return runner.invoke(
        app,
        ['github', 'depgraph', '--token', 'test-token', '--rate', RATE, *args],
    )


def _said(result) -> str:
    """The output as words. Rich wraps long lines; compare words, not
    layout."""
    return ' '.join(result.output.split())


# --- independent of every other stage ---------------------------------------

def test_every_tracked_repository_is_asked_without_an_sbom(github):
    """No `07-sbom` list, no content, no record: the graph needs only
    `owner/repo`."""
    _track('a', 'b')

    result = depgraph()

    assert result.exit_code == 0, result.output
    assert github.asked == ['a', 'b']
    assert json.loads(_document('a').read_text()) == GRAPH
    assert not Path('data/07-sbom').exists()


def test_a_repository_only_a_snapshot_listed_is_asked(github):
    """Seeded with no language: the language-keyed stages leave it
    alone, and the dependency graph does not."""
    with Ledger(LEDGER) as ledger:
        ledger.seed(1, 'o', 'a', snapshot='all-2026-03-09', stars=5)

    result = depgraph()

    assert result.exit_code == 0, result.output
    assert github.asked == ['a']


def test_run_stage_depgraph_is_the_same_stage(github):
    _track('a')

    result = runner.invoke(
        app,
        [
            'run', '--token', 'test-token', '--stage', 'depgraph',
            '--rate', RATE,
        ],
    )

    assert result.exit_code == 0, result.output
    assert github.asked == ['a']
    assert _state('a').outcome == 'ok'


def test_run_refuses_a_stage_that_does_not_run_alone(github):
    """`lock` runs a package manager, so it stays in its container."""
    _track('a')

    result = runner.invoke(
        app, ['run', '--token', 'test-token', '--stage', 'lock'],
    )

    assert result.exit_code == 2
    assert github.asked == []


def test_a_limit_bounds_the_pass(github):
    _track('a', 'b', 'c')

    result = depgraph('--limit', '2')

    assert result.exit_code == 0, result.output
    assert github.asked == ['a', 'b']
    assert _state('c') is None, 'never claimed'


def test_the_most_starred_is_asked_first(github):
    _track('a', 'b', 'c', a=1, b=300, c=20)

    depgraph()

    assert github.asked == ['b', 'c', 'a']


# --- kept for good, and stamped ----------------------------------------------

def test_a_fetch_is_kept_under_the_repository_id_with_its_stamp(github):
    _track('a')

    depgraph()

    document = _document('a')
    assert document.parent.parent == ROOT / '1'
    assert document.parent.name.endswith('-' + HEAD[1])
    meta = json.loads((document.parent / 'meta.json').read_text())
    assert meta['ref'] == 'main'
    assert meta['commit_sha'] == HEAD[1]
    assert meta['http_status'] == 200
    assert meta['owner'] == 'o' and meta['repo'] == 'a'
    [logged] = _indexed()[1]
    assert logged['depgraph_path'] == str(document)
    assert logged['commit_sha'] == HEAD[1]


def test_the_stamp_is_the_graphs_own_head(github, heads):
    """Read by `git ls-remote` immediately before the fetch, not copied
    from the Syft scan."""
    _track('a')

    depgraph()

    assert heads.asked == ['a']


def test_a_head_git_cannot_read_is_recorded_as_unknown(github, heads):
    _track('a')
    heads.answers['a'] = None
    with Ledger(LEDGER) as ledger:
        ledger.seed(1, 'o', 'a', snapshot='all-test', default_branch='trunk')

    depgraph()

    document = _document('a')
    assert document.parent.name.endswith('-unknown')
    meta = json.loads((document.parent / 'meta.json').read_text())
    assert meta['commit_sha'] == ''
    assert meta['ref'] == 'trunk', "the snapshot's default branch"


def test_a_second_fetch_is_kept_beside_the_first(github):
    """Never overwritten: a graph that changed leaves the one before."""
    _track('a')
    depgraph()
    _age('a', days=1)
    github.answers['a'] = (
        200, {}, {'sbom': {'spdxVersion': 'SPDX-2.3', 'packages': [{}]}},
    )

    result = depgraph()

    assert result.exit_code == 0, result.output
    first, second = _fetches('a')
    assert json.loads(first.read_text()) == GRAPH
    assert json.loads(second.read_text())['sbom']['packages'] == [{}]
    assert len(_indexed()[1]) == 2


def test_an_identical_document_is_not_kept_twice(github):
    _track('a')
    depgraph()
    _age('a', days=1)

    result = depgraph()

    assert result.exit_code == 0, result.output
    assert len(_fetches('a')) == 1
    assert 'unchanged 1' in _said(result)
    assert _state('a').outcome == 'ok'


def test_the_legacy_document_is_left_as_it_was(github):
    """Moved by PR B's migration, never by this stage."""
    legacy = ROOT / 'java' / 'o' / 'a' / 'sbom.spdx.json'
    legacy.parent.mkdir(parents=True)
    legacy.write_text('{"sbom": {"legacy": true}}')
    _track('a')

    depgraph()

    assert legacy.read_text() == '{"sbom": {"legacy": true}}'
    assert len(_fetches('a')) == 1


# --- when it is due ------------------------------------------------------------

def test_a_graph_is_not_asked_for_again_within_30_days(github):
    _track('a')
    depgraph()

    result = depgraph()

    assert result.exit_code == 0, result.output
    assert github.asked == ['a']
    assert 'No dependency graph due' in _said(result)
    state = _state('a')
    assert state.next_attempt_at - state.done_at == timedelta(days=30)


def test_a_graph_fetched_before_the_ledger_kept_state_waits_its_30_days(
    github,
):
    """The legacy watermark stands in for a `stage_state` row: a graph
    fetched a week ago is not due; one fetched two months ago is, after
    every repository never asked."""
    _track('a', 'b', 'c')
    now = datetime.now(timezone.utc)
    with Ledger(LEDGER) as ledger:
        for name, age in (('a', 60), ('b', 7)):
            state = ledger.get(REPOSITORIES[name])
            state.stage_watermarks[Stage.DEPGRAPH] = now - timedelta(days=age)
            ledger.upsert(state)

    depgraph()

    assert github.asked == ['c', 'a']


def test_a_deleted_repository_is_not_asked(github):
    _track('a', 'b')
    now = datetime.now(timezone.utc)
    with Ledger(LEDGER) as ledger:
        ledger.record_absent(1, now, now - timedelta(days=1))

    depgraph()

    assert github.asked == ['b']


# --- the negative cache ------------------------------------------------------------

def test_no_graph_is_cached_for_30_days(github):
    """404: the graph was never built, or is switched off, which this
    endpoint answers the same way. Not a failure, and not asked again
    for a month."""
    _track('a', 'b')
    github.answers['a'] = (404, {}, {'message': 'Not Found'})

    result = depgraph()

    assert result.exit_code == 0, result.output
    assert 'no graph 1' in _said(result)
    assert _fetches('a') == []
    state = _state('a')
    assert state.outcome == 'absent' and state.http_status == 404
    assert state.next_attempt_at - datetime.now(timezone.utc) > timedelta(
        days=29,
    )

    depgraph()
    assert github.asked == ['a', 'b'], 'not asked again'


def test_a_negative_cache_that_expired_is_asked_again_and_grows(github):
    _track('a')
    github.answers['a'] = (404, {}, {'message': 'Not Found'})
    depgraph()
    _age('a', minutes=1)

    depgraph()

    assert github.asked == ['a', 'a']
    state = _state('a')
    assert state.failure_count == 2
    assert state.next_attempt_at - datetime.now(timezone.utc) > timedelta(
        days=59,
    )


def test_a_graph_switched_on_is_found_when_the_cache_expires(github):
    _track('a')
    github.answers['a'] = (404, {}, {'message': 'Not Found'})
    depgraph()
    _age('a', minutes=1)
    del github.answers['a']

    depgraph()

    assert _state('a').outcome == 'ok'
    assert len(_fetches('a')) == 1


# --- a refused token is not a missing graph ---------------------------------

@pytest.mark.parametrize(
    'status,headers',
    [
        (429, SPENT),
        (403, SPENT),
        (403, {'X-RateLimit-Remaining': '4000', 'Retry-After': '60'}),
    ],
    ids=['429', '403-no-quota-left', '403-secondary-limit'],
)
def test_a_refused_token_stops_its_asking(github, status, headers):
    """A refusal says nothing about the repository asked for, and every
    later request with the same token would be refused the same way."""
    _track('a', 'b', 'c')
    github.answers['b'] = (status, headers, REFUSED)

    result = depgraph()

    assert result.exit_code != 0, 'a refused run is not a clean one'
    assert github.asked == ['a', 'b'], 'nothing is asked once refused'
    said = _said(result)
    assert 'no graph 0' in said, 'b was refused, not found without a graph'
    assert 'rate limited' in said.lower()
    state = _state('b')
    assert state.outcome == '' and not state.claimed_by, 'released, as due'
    assert _state('c').claimed_by == '', 'nothing left leased'


def test_a_refused_repository_is_asked_first_next_time(github):
    _track('a', 'b')
    github.answers['a'] = (429, SPENT, REFUSED)
    depgraph()
    del github.answers['a']

    depgraph()

    assert github.asked == ['a', 'a', 'b']


def test_a_refusal_says_when_to_come_back(github):
    _track('a')
    github.answers['a'] = (429, SPENT, REFUSED)

    result = depgraph()

    assert result.exit_code != 0
    assert '2026-09-21 14:13:19 UTC' in _said(result), 'from X-RateLimit-Reset'


def test_a_refusal_comes_straight_back(github):
    """urllib3 honours `Retry-After` by sleeping and asking again, three
    times, while the token stays refused — and the cached session is
    mounted that way. Sent through it, a refusal is slept through and
    then surfaces as a transport error, which the stage cannot tell from
    a failing repository, so it would carry on asking."""
    _track('a')

    depgraph()

    assert github.slept_through == [False]


# --- failures back off ----------------------------------------------------------

def test_a_failure_fails_the_run_but_not_the_batch(github):
    """Nor is a 5xx or a dropped connection "no graph". spring-boot
    answers 500 "Request timed out" for this endpoint, so the batch
    carries on — but the run says so, and exits non-zero."""
    _track('a', 'b', 'c')
    github.answers['a'] = (502, {}, {'message': 'Server Error'})
    github.answers['b'] = requests.ConnectionError('reset')

    result = depgraph()

    assert result.exit_code != 0
    assert github.asked == ['a', 'b', 'c']
    said = _said(result)
    assert 'no graph 0' in said
    assert 'failed 2' in said
    for name in ('a', 'b'):
        state = _state(name)
        assert state.outcome == 'failed' and state.failure_count == 1
        wait = state.next_attempt_at - datetime.now(timezone.utc)
        assert timedelta(minutes=14) < wait <= timedelta(minutes=15)


def test_a_failure_is_not_asked_again_during_its_backoff(github):
    _track('a')
    github.answers['a'] = (502, {}, {'message': 'Server Error'})
    depgraph()

    depgraph()

    assert github.asked == ['a']


# --- several tokens ------------------------------------------------------------------

def test_every_token_shares_the_work(github, monkeypatch):
    """GitHub meters the bucket per token: a second one is a second
    worker. Both ask, and between them everything is asked once."""
    monkeypatch.setenv('CHATSBOM_DEPGRAPH_TOKENS', 'second-token')
    _track('a', 'b', 'c', 'd')

    result = depgraph('--rate', SHARED_RATE)

    assert result.exit_code == 0, result.output
    assert sorted(github.asked) == ['a', 'b', 'c', 'd']
    assert set(github.tokens) == {
        'Bearer test-token', 'Bearer second-token',
    }
    assert 'tokens 2' in _said(result)


def test_a_token_is_never_printed(github, monkeypatch):
    monkeypatch.setenv('CHATSBOM_DEPGRAPH_TOKENS', 'second-token')
    _track('a')
    github.answers['a'] = (429, SPENT, REFUSED)

    result = depgraph()

    assert 'second-token' not in result.output
    assert 'test-token' not in result.output


def test_a_token_listed_twice_is_one_worker(github, monkeypatch):
    monkeypatch.setenv('CHATSBOM_DEPGRAPH_TOKENS', 'test-token, test-token')
    _track('a')

    result = depgraph()

    assert 'tokens 1' in _said(result)


def test_a_rejected_extra_token_is_skipped(github, monkeypatch):
    monkeypatch.setenv('CHATSBOM_DEPGRAPH_TOKENS', 'expired-token')
    github.refuse.add('expired-token')
    _track('a', 'b')

    result = depgraph()

    assert result.exit_code == 0, result.output
    assert set(github.tokens) == {'Bearer test-token'}
    assert 'tokens 1' in _said(result)


def test_one_refused_token_leaves_the_work_to_the_other(github, monkeypatch):
    monkeypatch.setenv('CHATSBOM_DEPGRAPH_TOKENS', 'second-token')
    _track('a', 'b', 'c', 'd')

    def refuse_the_second(authorization):
        if authorization == 'Bearer second-token':
            return 429, SPENT, REFUSED
        return 200, {}, GRAPH
    for name in REPOSITORIES:
        github.answers[name] = refuse_the_second

    result = depgraph('--rate', SHARED_RATE)

    assert result.exit_code != 0, 'a refusal is still reported'
    with Ledger(LEDGER) as ledger:
        outcomes = ledger.stage_outcomes(Stage.DEPGRAPH)
    # The repository the second token was refused at is released, and
    # the first takes it in the same pass.
    assert outcomes == {'ok': 4}, outcomes
    assert github.tokens.count('Bearer second-token') == 1


# --- closing --------------------------------------------------------------------------

def test_off_asks_nothing(github, monkeypatch):
    monkeypatch.setenv('CHATSBOM_DEPGRAPH_API', 'off')
    _track('a')

    result = depgraph()

    assert result.exit_code == 0, result.output
    assert github.asked == []
    assert 'disabled' in _said(result)
    assert _state('a') is None


def test_sync_turns_the_stage_off_once_the_endpoint_has_closed(
    github, monkeypatch,
):
    """Said once, and nothing recorded: every stored document stands, and
    no other stage waits on this one."""
    monkeypatch.setattr(
        'chatsbom.commands.github.depgraph.closed_reason',
        lambda setting, today: closed_reason(setting, date(2026, 11, 13)),
    )
    _track('a')

    result = depgraph()

    assert result.exit_code == 0, result.output
    assert github.asked == []
    said = _said(result)
    assert 'disabled' in said and '2026-11-13' in said
    assert _state('a') is None
