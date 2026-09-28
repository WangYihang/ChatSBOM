"""`github depgraph` end to end, over a faked GitHub.

The per-language index, `data/09-github-depgraph/<lang>.jsonl`, is how
`db index`, `queue backfill` and `db raw` find a stored dependency graph.
The command opened it with `'w'` and wrote back only what the current
run reached, so `--limit`, a Ctrl-C or a crash left it short while every
other repository's document sat on disk, unread. Once, that cut Java from
1,215 indexed repositories to 87.

It also counted a refused token as "no graph": `fetch` answered None for
a 429 exactly as for a 404, and 890 refusals were folded into "3,133 with
no graph published".

Only the transport is faked, so the real service, sessions and rate-limit
parsing are what answer.
"""
import io
import json
import os
import time
from datetime import datetime
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
from chatsbom.commands.run import DependencyGraphStage
from chatsbom.core.container import Container
from chatsbom.models.repository import Repository
from chatsbom.services.dependency_graph_service import DependencyGraphService

USER = 'https://api.github.com/user'
GRAPH_URL = 'https://api.github.com/repos/o/{name}/dependency-graph/sbom'

LEDGER = Path('data/07-sbom/java.jsonl')
INDEX = Path('data/09-github-depgraph/java.jsonl')

#: Repository name -> id, in the order the SBOM ledger lists them.
REPOSITORIES = {'a': 1, 'b': 2, 'c': 3, 'd': 4}

#: The smallest document GitHub sends for a repository with a graph.
GRAPH = {'sbom': {'spdxVersion': 'SPDX-2.3', 'packages': []}}

#: A spent token, as GitHub reports one. 1789999999 is
#: 2026-09-21 14:13:19 UTC.
SPENT = {'X-RateLimit-Remaining': '0', 'X-RateLimit-Reset': '1789999999'}

REFUSED = {'message': 'API rate limit exceeded'}

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
    """GitHub as `github depgraph` sees it: the token check, then one
    dependency graph per repository.

    Every repository has a graph unless `answers` says otherwise, with a
    status, headers and body — or an exception, raised mid-request.
    """

    def __init__(self) -> None:
        self.answers: dict[str, object] = {}
        self.asked: list[str] = []
        #: For each graph request, whether the session that sent it
        #: would sleep through a 429 carrying `Retry-After` and ask again.
        self.slept_through: list[bool] = []

    def answer(self, adapter: HTTPAdapter, request) -> Response:
        if request.url == USER:
            return _response(request, 200, {}, {'login': 'octocat'})

        name = request.url.split('/')[5]
        assert request.url == GRAPH_URL.format(name=name), request.url
        self.asked.append(name)
        self.slept_through.append(
            adapter.max_retries.is_retry('GET', 429, has_retry_after=True),
        )

        answer = self.answers.get(name, (200, {}, GRAPH))
        if isinstance(answer, BaseException):
            raise answer
        assert isinstance(answer, tuple)
        status, headers, payload = answer
        return _response(request, status, headers, payload)


@pytest.fixture
def github(tmp_path, monkeypatch) -> FakeGitHub:
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

    fake = FakeGitHub()
    monkeypatch.setattr(
        HTTPAdapter, 'send',
        lambda adapter, request, **kwargs: fake.answer(adapter, request),
    )
    return fake


def _ledger(*names: str, **stars: int) -> None:
    """The SBOM ledger: the repositories the command walks, in order."""
    LEDGER.parent.mkdir(parents=True, exist_ok=True)
    LEDGER.write_text(
        ''.join(
            json.dumps({
                'id': REPOSITORIES[name],
                'owner': 'o',
                'name': name,
                'language': 'Java',
                'stargazers_count': stars.get(name, 10),
            }) + '\n'
            for name in names
        ),
        encoding='utf-8',
    )


def _document(name: str) -> Path:
    return Path(f'data/09-github-depgraph/java/o/{name}/sbom.spdx.json')


def _collected(*names: str) -> None:
    """What an earlier, complete run left: a stored document for each
    repository, and an index entry pointing at it."""
    INDEX.parent.mkdir(parents=True, exist_ok=True)
    with INDEX.open('a', encoding='utf-8') as index:
        for name in names:
            document = _document(name)
            document.parent.mkdir(parents=True, exist_ok=True)
            document.write_text(json.dumps(GRAPH), encoding='utf-8')
            index.write(
                json.dumps({
                    'id': REPOSITORIES[name],
                    'owner': 'o',
                    'repo': name,
                    'stars': 10,
                    'depgraph_path': str(document),
                }) + '\n',
            )


def _indexed() -> dict[int, dict]:
    """The index as its readers see it: repository id -> record."""
    records = [
        json.loads(line)
        for line in INDEX.read_text(encoding='utf-8').splitlines()
        if line.strip()
    ]
    return {record['id']: record for record in records}


def depgraph(*args: str):
    return runner.invoke(
        app,
        [
            'github', 'depgraph', '--token', 'test-token',
            '--language', 'java', *args,
        ],
    )


def _said(result) -> str:
    """The output as words. Rich wraps long lines; compare words, not
    layout."""
    return ' '.join(result.output.split())


# --- the index only grows ---------------------------------------------------

def test_a_limited_run_keeps_the_rest_of_the_index(github):
    """`--limit` bounds the work, not the file.

    Rewritten from the repositories one `--limit` run reached, the index
    once took Java from 1,215 indexed repositories to 87 — and `db index`
    finds depgraph documents through nothing else.
    """
    _collected('a', 'b', 'c')
    _ledger('a', 'b', 'c', a=99)

    result = depgraph('--limit', '1')

    assert result.exit_code == 0, result.output
    indexed = _indexed()
    assert sorted(indexed) == [1, 2, 3], 'b and c were not reached, not lost'
    assert indexed[1]['stars'] == 99, 'the repository it reached is refreshed'
    assert github.asked == [], 'a stored document is not fetched again'


def test_a_limited_run_adds_what_it_collects(github):
    _collected('b', 'c', 'd')
    _ledger('a', 'b', 'c', 'd')

    result = depgraph('--limit', '1')

    assert result.exit_code == 0, result.output
    assert github.asked == ['a']
    assert sorted(_indexed()) == [1, 2, 3, 4]
    assert _indexed()[1]['depgraph_path'] == str(_document('a'))


def test_an_interrupted_run_leaves_the_index_it_found(github):
    """A crash midway must not cost the repositories the run had not
    reached yet: their documents are still on disk."""
    _collected('a', 'b', 'c')
    _ledger('a', 'b', 'c')
    github.answers['b'] = RuntimeError('interrupted')

    result = depgraph('--force')

    assert result.exit_code != 0
    assert github.asked == ['a', 'b']
    assert sorted(_indexed()) == [1, 2, 3]
    # Written beside the index and renamed over it, so nothing half
    # written is left behind — least of all a `*.jsonl` that `db raw`
    # and `queue backfill` would glob up as another language's ledger.
    assert sorted(
        path.name for path in INDEX.parent.iterdir() if path.is_file()
    ) == ['java.jsonl']


@pytest.mark.parametrize(
    'answer',
    [
        (404, {}, {'message': 'Not Found'}),
        (502, {}, {'message': 'Server Error'}),
        requests.ConnectionError('reset'),
    ],
    ids=['no-graph-now', 'server-error', 'transport-error'],
)
def test_an_answer_without_a_document_never_costs_a_stored_one(
    github, answer,
):
    """`--force` asks again for a graph already on disk. Whatever comes
    back instead of a document, the one on disk and its entry stay: the
    index lists what is stored, and nothing was deleted."""
    _collected('a', 'b')
    _ledger('a', 'b')
    github.answers['a'] = answer

    depgraph('--force')

    assert sorted(_indexed()) == [1, 2]
    assert json.loads(_document('a').read_text(encoding='utf-8')) == GRAPH


def test_a_document_cut_short_by_a_full_disk_is_not_left_behind(
    github, full_disk,
):
    """Documents were written in place. One cut short stayed on disk, the
    next run counted it as cached and indexed it, and `db index` then
    failed that repository on every run with "unreadable dependency
    graph" (#13)."""
    _ledger('a')
    full_disk.fill(_document('a').parent)

    result = depgraph()

    assert result.exit_code != 0
    assert list(_document('a').parent.iterdir()) == [], 'nor a temporary file'

    full_disk.free()
    result = depgraph()

    assert result.exit_code == 0, result.output
    assert github.asked == ['a', 'a'], 'asked again, not taken from disk'
    assert json.loads(_document('a').read_text(encoding='utf-8')) == GRAPH
    assert sorted(_indexed()) == [1]


def test_a_document_left_cut_short_is_fetched_again(github):
    """What an in-place write left when it was killed, before writes were
    atomic. It exists, so it was counted as cached and indexed."""
    _ledger('a')
    document = _document('a')
    document.parent.mkdir(parents=True)
    whole = json.dumps(GRAPH)
    document.write_text(whole[:len(whole) // 2], encoding='utf-8')

    result = depgraph()

    assert result.exit_code == 0, result.output
    assert github.asked == ['a']
    assert json.loads(document.read_text(encoding='utf-8')) == GRAPH
    assert sorted(_indexed()) == [1]


def test_an_entry_whose_document_is_gone_is_dropped(github):
    """The index lists documents on disk. One that is gone is not
    evidence of anything, and every reader would skip it anyway."""
    _collected('a', 'b')
    _document('b').unlink()
    _ledger('c')

    result = depgraph()

    assert result.exit_code == 0, result.output
    assert sorted(_indexed()) == [1, 3]


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
def test_a_refused_token_stops_the_run(github, status, headers):
    """A refusal says nothing about the repository asked for, and every
    later request with the same token would be refused the same way."""
    _ledger('a', 'b', 'c')
    github.answers['b'] = (status, headers, REFUSED)

    result = depgraph()

    assert result.exit_code != 0, 'a refused run is not a clean one'
    assert github.asked == ['a', 'b'], 'nothing is asked once refused'
    assert sorted(_indexed()) == [1]
    said = _said(result)
    assert 'no graph 0' in said, 'b was refused, not found without a graph'
    assert 'rate limited' in said.lower()


def test_a_refusal_says_when_to_come_back(github):
    _ledger('a')
    github.answers['a'] = (429, SPENT, REFUSED)

    result = depgraph()

    assert result.exit_code != 0
    assert '2026-09-21 14:13:19 UTC' in _said(result), 'from X-RateLimit-Reset'


def test_a_refusal_keeps_the_graph_already_stored(github):
    """`--force` asks again for a graph already on disk. Refused, that
    graph and its entry must both survive."""
    _collected('a', 'b')
    _ledger('a', 'b')
    github.answers['a'] = (429, SPENT, REFUSED)

    result = depgraph('--force')

    assert result.exit_code != 0
    assert github.asked == ['a']
    assert sorted(_indexed()) == [1, 2]
    assert json.loads(_document('a').read_text(encoding='utf-8')) == GRAPH


def test_a_refusal_comes_straight_back(github):
    """urllib3 honours `Retry-After` by sleeping and asking again, three
    times, while the token stays refused — and the cached session is
    mounted that way. Sent through it, a refusal is slept through and
    then surfaces as a transport error, which the run cannot tell from a
    failing repository, so it would carry on asking."""
    _ledger('a')

    depgraph()

    assert github.slept_through == [False]


# --- what is and is not a failure -------------------------------------------

def test_no_graph_is_not_a_failure(github):
    """404: the graph was never built, or is switched off, which this
    endpoint answers the same way."""
    _ledger('a', 'b')
    github.answers['a'] = (404, {}, {'message': 'Not Found'})

    result = depgraph()

    assert result.exit_code == 0, result.output
    assert sorted(_indexed()) == [2]
    assert 'no graph 1' in _said(result)


def test_a_failed_repository_fails_the_run_but_not_the_batch(github):
    """Nor is a 5xx or a dropped connection "no graph". spring-boot
    answers 500 "Request timed out" for this endpoint, so the batch
    carries on — but the run says so, and exits non-zero."""
    _ledger('a', 'b', 'c')
    github.answers['a'] = (502, {}, {'message': 'Server Error'})
    github.answers['b'] = requests.ConnectionError('reset')

    result = depgraph()

    assert result.exit_code != 0
    assert github.asked == ['a', 'b', 'c']
    assert sorted(_indexed()) == [3]
    said = _said(result)
    assert 'no graph 0' in said
    assert 'failed 2' in said


# --- the same answers, as `chatsbom run` meets them -------------------------
#
# `run` walks every stage of each repository it claims, the dependency
# graph among them, through `DependencyGraphStage`. It reads `fetch`'s
# outcome as `github depgraph` does, but a refusal stops the asking
# rather than the pass: the other stages are not metered by this bucket.

#: GitHub's reset in SPENT has passed by the time these run; a summary
#: printed then still names it.
BEFORE_RESET = datetime(2026, 9, 21, 13, 0, tzinfo=timezone.utc)

#: A week, as the stage is given it: `cache_ttl`.
WEEK = 7 * 24 * 3600


def _stage(max_age: float = WEEK) -> DependencyGraphStage:
    container = Container.get_instance()
    service = DependencyGraphService(container.get_github_service('token'))
    return DependencyGraphStage(service, container.config.paths, max_age)


def _repository(name: str) -> Repository:
    """As `RunService` builds one, from the ledger's own columns."""
    return Repository.model_validate({
        'id': REPOSITORIES[name], 'owner': 'o', 'name': name,
        'language': 'Java',
    })


def test_run_stores_a_graph_whole_and_hands_on_its_path(github):
    stage = _stage()

    produced = stage(_repository('a'), {})

    assert produced == {'depgraph_path': str(_document('a'))}
    assert json.loads(_document('a').read_text()) == GRAPH
    assert (stage.fetched, stage.absent, stage.failed) == (1, 0, 0)


def test_run_counts_no_graph_and_a_failure_apart(github):
    """Neither is the stage's work, so neither is recorded and both stay
    due; but a 5xx is not "no graph", and the summary says which."""
    github.answers['a'] = (404, {}, {'message': 'Not Found'})
    github.answers['b'] = (502, {}, {'message': 'Server Error'})
    stage = _stage()

    assert stage(_repository('a'), {}) is None
    assert stage(_repository('b'), {}) is None

    assert (stage.absent, stage.failed) == (1, 1)
    assert not _document('a').exists() and not _document('b').exists()
    summary = ' '.join((stage.summary(BEFORE_RESET) or '').split())
    assert 'no graph 1' in summary and 'failed 1' in summary


@pytest.mark.parametrize(
    'status, headers',
    [(429, SPENT), (403, SPENT), (403, {'Retry-After': '60'})],
    ids=['429', 'spent-403', 'retry-after-403'],
)
def test_a_refusal_stops_the_asking_in_run_not_the_pass(
    github, status, headers,
):
    """The token was refused, not the repository, and every later request
    would be refused the same way. Nothing is recorded for any of them,
    and the stage stays due."""
    github.answers['a'] = (status, headers, REFUSED)
    stage = _stage()

    produced = [stage(_repository(name), {}) for name in ('a', 'b', 'c')]

    assert produced == [None, None, None]
    assert github.asked == ['a'], 'asked again after a refusal'
    assert (stage.absent, stage.failed, stage.unasked) == (0, 0, 2)
    summary = ' '.join((stage.summary(BEFORE_RESET) or '').split())
    assert 'rate limited' in summary and 'o/a' in summary
    assert 'no graph 0' in summary


def test_a_refusal_in_run_says_when_to_come_back(github):
    github.answers['a'] = (429, SPENT, REFUSED)
    stage = _stage()

    stage(_repository('a'), {})

    summary = ' '.join((stage.summary(BEFORE_RESET) or '').split())
    assert '2026-09-21 14:13:19 UTC' in summary, 'from X-RateLimit-Reset'


def test_run_reuses_a_graph_fetched_this_week(github):
    """A pass walks the whole chain of each repository it claims, due or
    not. The cached session kept that from spending the dependency-graph
    bucket, which is about 100 an hour, on a graph fetched days before;
    `fetch` no longer goes through it, and the stored document does
    that instead."""
    _collected('a')
    stage = _stage()

    produced = stage(_repository('a'), {})

    assert produced == {'depgraph_path': str(_document('a'))}
    assert github.asked == []
    assert stage.reused == 1


@pytest.mark.parametrize('how', ['old', 'cut short'])
def test_run_fetches_a_graph_that_is_old_or_cut_short(github, how):
    _collected('a')
    document = _document('a')
    if how == 'old':
        week_ago = time.time() - WEEK - 60
        os.utime(document, (week_ago, week_ago))
    else:
        document.write_text(json.dumps(GRAPH)[:-2])
    stage = _stage()

    assert stage(_repository('a'), {}) == {'depgraph_path': str(document)}
    assert github.asked == ['a']
    assert json.loads(document.read_text()) == GRAPH
