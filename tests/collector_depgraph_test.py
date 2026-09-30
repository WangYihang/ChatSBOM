"""The dependency graph, fetched on a clock, on the collector's client
(#162: part 6d of #155; #128 section 2.1).

GitHub's report flow (#50), against the stand-in (tests/fake_github_test
.py): a report asked for, looked at until GitHub has made it, and the
graph downloaded from the signed link its 302 points to, without the
token. The reports pending are kept in collector.sqlite and outlive a
restart. A graph is fetched again once its repository was pushed after
it was last learned, or at the backstop; a repository GitHub has no
graph of is asked again after the negative cache's delay; and a graph
the same as the last one kept, but for what GitHub makes anew for each
report, is not written again. Every request of the API draws from the
dependency graph's own bucket, whose refusals back it off.

The stand-in's clock is the budget's and the steps': a month passes in
no time.
"""
import asyncio
import json
import uuid
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Sequence
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any
from typing import TypeVar
from urllib.parse import urlsplit

import httpx2
import pytest
import structlog.testing

from chatsbom.collector.budget import BudgetManager
from chatsbom.collector.client import Answer
from chatsbom.collector.client import API
from chatsbom.collector.client import GitHubClient
from chatsbom.collector.depgraph import AT_ONCE
from chatsbom.collector.depgraph import BUCKET
from chatsbom.collector.depgraph import CHECKED
from chatsbom.collector.depgraph import Depgraph
from chatsbom.collector.depgraph import depgraph_settings
from chatsbom.collector.depgraph import DepgraphSettings
from chatsbom.collector.depgraph import FIRST_LOOK
from chatsbom.collector.depgraph import KEY
from chatsbom.collector.depgraph import look_after
from chatsbom.collector.depgraph import LOOKS
from chatsbom.collector.depgraph import MAX_AGE
from chatsbom.collector.depgraph import MIN_INTERVAL
from chatsbom.collector.depgraph import NO_GRAPH
from chatsbom.collector.depgraph import STAGE
from chatsbom.collector.depgraph import Step
from chatsbom.collector.errors import Unauthorized
from chatsbom.collector.settings import SettingsError
from chatsbom.collector.state import backoff
from chatsbom.collector.state import CollectorState
from chatsbom.collector.state import FAILED
from chatsbom.collector.state import NOTHING
from chatsbom.collector.state import Observed
from chatsbom.collector.state import Outcome
from chatsbom.collector.state import STATE_FILE
from chatsbom.collector.tokens import Token
from chatsbom.core import depgraph_store
from chatsbom.core.logging import setup_logging
from tests.fake_github_test import EXPORTS_HOST
from tests.fake_github_test import FakeClock
from tests.fake_github_test import FakeGitHub
from tests.fake_github_test import Reply
from tests.fake_github_test import Repo
from tests.fake_github_test import Report
from tests.fake_github_test import Seen
from tests.fake_github_test import START

ONE = 'ghp_depgraph_token_one_00000000000000000'
TWO = 'ghp_depgraph_token_two_00000000000000000'
T1 = Token('token 1', ONE)
T2 = Token('token 2', TWO)

DAY = 86_400.0

#: The stand-in's HEAD, unless a repository says another.
HEAD = 'a' * 40

GENERATE_ONE = '/repos/octo/one/dependency-graph/sbom/generate-report'

Result = TypeVar('Result')
Use = Callable[[Depgraph, CollectorState], Awaitable[Result]]


def at(seconds: float) -> datetime:
    return datetime.fromtimestamp(seconds, timezone.utc)


def graph_of(name: str, *packages: str) -> dict[str, Any]:
    """A dependency graph as a finished report downloads it: an SPDX
    document, without the `sbom` the synchronous endpoint wrapped it
    in."""
    return {
        'SPDXID': 'SPDXRef-DOCUMENT',
        'spdxVersion': 'SPDX-2.3',
        'creationInfo': {
            'created': '2026-09-14T03:56:20Z',
            'creators': ['Tool: GitHub.com-Dependency-Graph'],
        },
        'name': name,
        'packages': [
            {
                'name': f'npm:{package}',
                'SPDXID': f'SPDXRef-npm-{package}',
                'versionInfo': '1.0.0',
                'externalRefs': [{
                    'referenceCategory': 'PACKAGE-MANAGER',
                    'referenceType': 'purl',
                    'referenceLocator': f'pkg:npm/{package}@1.0.0',
                }],
            }
            for package in packages or ('left-pad',)
        ],
    }


@pytest.fixture
def fake() -> FakeGitHub:
    fake = FakeGitHub(FakeClock())
    # Each report made anew, as GitHub makes them.
    fake.stamp_reports = True
    fake.token(ONE, 'alice')
    fake.token(TWO, 'bob')
    fake.add(Repo(1, 'octo', 'one', graph=graph_of('octo/one')))
    # GitHub has no graph of it.
    fake.add(Repo(2, 'octo', 'two'))
    fake.add(
        Repo(3, 'octo', 'three', head='c' * 40, graph=graph_of('octo/three')),
    )
    return fake


def observed(repo: Repo, **changes: Any) -> Observed:
    """A repository as collector.sqlite would have observed it."""
    return replace(
        Observed(
            repository_id=repo.id, node_id=repo.node_id,
            full_name=repo.full_name, stars=repo.stars,
            archived=repo.archived, pushed_at=None,
            default_branch=repo.default_branch, head=repo.head,
            release_tag=None, release_at=None, observed_at=at(START),
        ),
        **changes,
    )


def everything(fake: FakeGitHub) -> list[Observed]:
    return [observed(repo) for repo in fake.repos.values()]


def budget_for(fake: FakeGitHub, *tokens: Token) -> BudgetManager:
    return BudgetManager(
        tokens or (T1,), clock=fake.clock, sleep=fake.clock.sleep,
    )


def run(
    fake: FakeGitHub, root: Path, use: Use[Result], *tokens: Token,
    settings: DepgraphSettings | None = None, at_once: int = AT_ONCE,
    budget: BudgetManager | None = None,
) -> Result:
    """`use` of the dependency graph against the stand-in, with
    collector.sqlite and the store under `root`, and everything closed
    after: one run of the collector."""
    async def using() -> Result:
        with CollectorState.open(root / STATE_FILE) as state:
            async with GitHubClient(
                budget or budget_for(fake, *tokens), validators=state,
                transport=fake.transport(),
            ) as github:
                async with Depgraph(
                    github, state, root / 'store', settings=settings,
                    downloads=fake.transport(), at_once=at_once,
                ) as depgraph:
                    return await use(depgraph, state)

    return asyncio.run(using())


async def settle(
    fake: FakeGitHub, depgraph: Depgraph, state: CollectorState,
    repositories: Sequence[Observed],
) -> list[Step]:
    """Steps, the clock moved on to when each said the next was due,
    until no report is pending."""
    steps = [await depgraph.step(repositories)]
    while state.reports():
        assert len(steps) < 40, 'it never settles'
        next_at = steps[-1].next_at
        assert next_at is not None
        fake.clock.now = max(fake.clock.now, next_at.timestamp())
        steps.append(await depgraph.step(repositories))
    return steps


def settled(
    fake: FakeGitHub, repositories: Sequence[Observed],
) -> Use[list[Step]]:
    return lambda depgraph, state: settle(fake, depgraph, state, repositories)


def asked(fake: FakeGitHub) -> list[str]:
    """The repositories reports were asked for, in order, by name."""
    return [
        seen.path.split('/')[3] for seen in fake.requests
        if seen.path.endswith('/generate-report')
    ]


def looks(fake: FakeGitHub) -> list[Seen]:
    return [seen for seen in fake.requests if '/fetch-report/' in seen.path]


def downloads(fake: FakeGitHub) -> list[Seen]:
    return [seen for seen in fake.requests if seen.host == EXPORTS_HOST]


def download_path(number: int) -> str:
    """The path the stand-in's `number`th report downloads from."""
    return f'/sbom/{uuid.UUID(int=number)}.spdx.json'


def outcomes(state: CollectorState) -> list[Outcome]:
    return list(state.outcomes(STAGE))


def total(steps: Sequence[Step], count: str) -> int:
    return sum(getattr(step, count) for step in steps)


def keep(root: Path, repo: Repo, when: float, *packages: str) -> None:
    """A graph kept in the store, as fetched at `when`."""
    depgraph_store.store(
        root, repository_id=repo.id, owner=repo.owner, repo=repo.name,
        payload={'sbom': graph_of(repo.full_name, *packages)},
        fetched_at=at(when), ref=repo.default_branch, head_sha=repo.head,
        http_status=200,
    )


#: A repository whose graph was asked for at START, pushed 19 days on,
#: and asked for again at AGAIN: past the minimum, and the push long
#: settled.
PUSHED_AGAIN = at(START + 19 * DAY)
AGAIN = START + 20 * DAY

#: Its next push, and a step past the minimum after AGAIN.
PUSHED_LATER = at(START + 40 * DAY)
LATER = START + 41 * DAY


class TestTheReportFlow:
    def test_a_graph_is_asked_for_looked_at_and_downloaded(
        self, fake, tmp_path,
    ):
        repo = fake.repos[1]

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            first = await depgraph.step([observed(repo)])
            report = state.report(1)
            assert first.next_at is not None
            fake.clock.now = first.next_at.timestamp()
            second = await depgraph.step([observed(repo)])
            return first, report, second, state.reports()

        first, report, second, pending = run(fake, tmp_path, use)

        assert (first.asked, first.stored, first.pending) == (1, 0, 1)
        assert report.url.startswith(
            f'{API}/repos/octo/one/dependency-graph/sbom/fetch-report/',
        )
        assert report.head == HEAD
        assert report.due_at == at(START) + FIRST_LOOK == first.next_at
        assert (second.looked, second.stored, second.pending) == (1, 1, 0)
        assert pending == []
        # Asked for, looked at, and downloaded from the link its 302
        # pointed to, off the API, with no token.
        assert [(seen.host, seen.status) for seen in fake.requests] == [
            ('api.github.com', 201),
            ('api.github.com', 302),
            (EXPORTS_HOST, 200),
        ]
        assert fake.requests[-1].token is None
        assert fake.requests[0].token == fake.requests[1].token == ONE

    def test_is_kept_in_the_stores_layout_as_the_old_endpoint_answered(
        self, fake, tmp_path,
    ):
        """Under the repository's id, stamped with when its report was
        asked for and the HEAD it was asked for at, and wrapped in `sbom`,
        as every reader of a graph expects it (core/depgraph_store.py)."""
        run(fake, tmp_path, settled(fake, [observed(fake.repos[1])]))

        store = tmp_path / 'store'
        fetch = depgraph_store.newest(store, 1)
        assert fetch is not None
        # Asked for at START, and downloaded two seconds later.
        fetched = at(START)
        assert fetch.document == (
            store / '1' / f'{fetched:%Y%m%dT%H%M%SZ}-{HEAD}' / 'sbom.spdx.json'
        )
        # As the report came, stamps and all.
        [report] = fake.reports.values()
        assert report.document['name'] == 'octo/one'
        assert json.loads(fetch.document.read_text()) == {
            'sbom': report.document,
        }
        assert json.loads((fetch.directory / 'meta.json').read_text()) == {
            'repository_id': 1,
            'owner': 'octo',
            'repo': 'one',
            'ref': 'main',
            'commit_sha': HEAD,
            'fetched_at': fetched.isoformat(),
            'http_status': 200,
            'sha256': fetch.sha256,
        }
        # And logged in the store's own index of fetches.
        [line] = (store / depgraph_store.INDEX).read_text().splitlines()
        assert json.loads(line)['depgraph_path'] == str(fetch.document)

    def test_the_synchronous_endpoint_is_never_asked(self, fake, tmp_path):
        """It closes after 2026-11-13 (#50)."""
        steps = run(fake, tmp_path, settled(fake, everything(fake)))
        assert total(steps, 'stored') == 2
        assert [
            seen for seen in fake.requests
            if seen.path.endswith('/dependency-graph/sbom')
        ] == []

    def test_a_report_not_ready_is_looked_at_again_later_and_later(
        self, fake, tmp_path,
    ):
        fake.report_seconds = 10
        steps = run(fake, tmp_path, settled(fake, [observed(fake.repos[1])]))

        assert [step.next_at for step in steps[:-1]] == [
            at(START + 2), at(START + 6), at(START + 14),
        ]
        assert [
            (step.looked, step.not_ready, step.stored) for step in steps
        ] == [(0, 0, 0), (1, 1, 0), (1, 1, 0), (1, 0, 1)]
        # Asked for once, however often it is looked at.
        assert asked(fake) == ['one']
        assert FIRST_LOOK == look_after(0) == timedelta(seconds=2)
        assert look_after(1) == timedelta(seconds=4)
        assert look_after(2) == timedelta(seconds=8)
        assert look_after(40) == timedelta(minutes=15)

    def test_a_retry_after_on_a_report_not_ready_is_waited(
        self, fake, tmp_path,
    ):
        fake.report_seconds = 60
        repo = observed(fake.repos[1])

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            first = await depgraph.step([repo])
            report = state.report(1)
            assert report is not None and first.next_at is not None
            fake.script(
                Reply(202, headers={'Retry-After': '30'}, billed=True),
                path=urlsplit(report.url).path,
            )
            fake.clock.now = first.next_at.timestamp()
            return await depgraph.step([repo]), state.report(1)

        second, report = run(fake, tmp_path, use)
        assert second.not_ready == 1
        assert report.attempts == 1
        assert report.due_at == at(START + 2 + 30) == second.next_at

    def test_a_retry_after_on_the_request_puts_the_first_look_off(
        self, fake, tmp_path,
    ):
        fake.reports['kept'] = Report(1, graph_of('octo/one'), START)
        fake.script(
            Reply(
                201, {
                    'sbom_url': (
                        f'{API}/repos/octo/one/dependency-graph/sbom/'
                        'fetch-report/kept'
                    ),
                }, headers={'Retry-After': '5'}, billed=True,
            ),
            path=GENERATE_ONE,
        )
        steps = run(fake, tmp_path, settled(fake, [observed(fake.repos[1])]))
        assert steps[0].next_at == at(START + 5)
        assert total(steps, 'stored') == 1

    def test_a_report_never_ready_is_given_up_then_asked_for_anew(
        self, fake, tmp_path,
    ):
        fake.report_seconds = 10 * DAY
        repo = observed(fake.repos[1])

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            steps = await settle(fake, depgraph, state, [repo])
            given_up = fake.clock.now
            [outcome] = outcomes(state)
            fake.clock.now = outcome.due_at.timestamp() - 1
            early = await depgraph.step([repo])
            fake.clock.now = outcome.due_at.timestamp()
            due = await depgraph.step([repo])
            return steps, given_up, outcome, early, due

        steps, given_up, outcome, early, due = run(fake, tmp_path, use)

        assert len(looks(fake)) == LOOKS == 10
        assert total(steps, 'not_ready') == LOOKS
        assert (steps[-1].failed, steps[-1].pending) == (1, 0)
        assert (outcome.kind, outcome.attempts) == (FAILED, 1)
        assert 'not ready' in outcome.detail
        assert outcome.due_at == at(given_up) + backoff(1)
        assert steps[-1].next_at == outcome.due_at
        # Not asked for again before then; asked for anew once due.
        assert early.asked == 0
        assert due.asked == 1
        assert asked(fake) == ['one', 'one']

    def test_a_report_gone_is_asked_for_anew_after_a_backoff(
        self, fake, tmp_path,
    ):
        repo = observed(fake.repos[1])

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            first = await depgraph.step([repo])
            assert first.next_at is not None
            fake.reports.clear()
            fake.clock.now = first.next_at.timestamp()
            second = await depgraph.step([repo])
            return second, state.reports(), outcomes(state)

        second, pending, kept = run(fake, tmp_path, use)
        assert (second.looked, second.failed, second.stored) == (1, 1, 0)
        assert pending == []
        [outcome] = kept
        assert outcome.kind == FAILED
        assert '404' in outcome.detail
        assert outcome.due_at == at(START + 2) + backoff(1) == second.next_at

    def test_a_look_that_fails_is_tried_again(self, fake, tmp_path):
        repo = observed(fake.repos[1])

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            await depgraph.step([repo])
            report = state.report(1)
            assert report is not None
            fake.script(
                Reply(502, {'message': 'Server Error'}, billed=True),
                path=urlsplit(report.url).path,
            )
            return await settle(fake, depgraph, state, [repo])

        steps = run(fake, tmp_path, use)
        assert total(steps, 'stored') == 1
        assert [seen.status for seen in looks(fake)] == [502, 302]
        assert asked(fake) == ['one']

    @pytest.mark.parametrize(
        'failure', [
            Reply(
                403, b'<Error><Code>AccessDenied</Code><Message>Request has '
                b'expired</Message></Error>',
            ),
            Reply(500, b'<Error><Code>InternalError</Code></Error>'),
            Reply(0, raises=httpx2.ConnectError('connection refused')),
            Reply(302, headers={'Location': 'https://elsewhere.example/r'}),
        ],
        ids=['expired', 'server-error', 'connection', 'another-redirect'],
    )
    def test_a_download_that_fails_is_looked_at_again_for_a_new_link(
        self, fake, tmp_path, failure,
    ):
        fake.script(failure, path=download_path(1))
        steps = run(fake, tmp_path, settled(fake, [observed(fake.repos[1])]))

        assert total(steps, 'stored') == 1
        assert len(looks(fake)) == 2
        assert [seen.status for seen in downloads(fake)] == [
            failure.status, 200,
        ]
        # A link signed anew for the second.
        assert len({
            seen.query['X-Amz-Signature'] for seen in downloads(fake)
        }) == 2
        assert asked(fake) == ['one']
        # A redirect from the link is not followed either.
        assert [
            seen for seen in fake.requests if seen.host == 'elsewhere.example'
        ] == []

    def test_a_link_that_cannot_be_read_is_looked_at_again(
        self, fake, tmp_path,
    ):
        """No URL a request can be made of, which httpx2 refuses to
        read: that look failed, and the next gives another link."""
        repo = observed(fake.repos[1])

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            await depgraph.step([repo])
            report = state.report(1)
            assert report is not None
            fake.script(
                Reply(
                    302, headers={
                        'Location': f'https://{EXPORTS_HOST}/r\x00.json',
                    }, billed=True,
                ),
                path=urlsplit(report.url).path,
            )
            return await settle(fake, depgraph, state, [repo])

        steps = run(fake, tmp_path, use)
        assert total(steps, 'stored') == 1
        assert [seen.status for seen in looks(fake)] == [302, 302]
        assert len(downloads(fake)) == 1

    def test_a_link_no_request_can_be_made_of_is_looked_at_again(
        self, fake, tmp_path, monkeypatch,
    ):
        """However such a link came to be answered, the step goes on,
        and the report is looked at again."""
        repo = observed(fake.repos[1])

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            first = await depgraph.step([repo])
            report = state.report(1)
            assert report is not None and first.next_at is not None
            asking = depgraph.github.get

            async def get(where: str, **options: Any) -> Answer:
                if where != report.url:
                    return await asking(where, **options)
                return Answer(
                    status=302,
                    headers={'location': f'https://{EXPORTS_HOST}/r\x00.json'},
                    content=b'', url=report.url, token=T1.label,
                    bucket=BUCKET,
                )

            monkeypatch.setattr(depgraph.github, 'get', get)
            fake.clock.now = first.next_at.timestamp()
            return await depgraph.step([repo]), state.report(1)

        second, report = run(fake, tmp_path, use)
        assert (second.looked, second.failed, second.pending) == (1, 0, 1)
        assert report.attempts == 1
        assert downloads(fake) == []

    @pytest.mark.parametrize(
        'graph', [
            ['not', 'a', 'document'],
            {'unexpected': True},
            {'sbom': 'not a document'},
            b'<html>unicorn</html>',
        ],
        ids=['a-list', 'no-spdx', 'sbom-not-an-object', 'not-json'],
    )
    def test_a_download_that_is_no_spdx_document_fails(
        self, fake, tmp_path, graph,
    ):
        """Nothing to keep, and no evidence that there is no graph."""
        fake.repos[1].graph = graph

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            steps = await settle(
                fake, depgraph, state, [observed(fake.repos[1])],
            )
            return steps, outcomes(state)

        steps, kept = run(fake, tmp_path, use)
        assert (steps[-1].failed, total(steps, 'stored')) == (1, 0)
        assert [outcome.kind for outcome in kept] == [FAILED]
        assert not (tmp_path / 'store' / '1').exists()

    def test_a_report_that_comes_wrapped_is_kept_as_it_came(
        self, fake, tmp_path,
    ):
        fake.repos[1].graph = {'sbom': graph_of('octo/one')}
        run(fake, tmp_path, settled(fake, [observed(fake.repos[1])]))
        fetch = depgraph_store.newest(tmp_path / 'store', 1)
        assert fetch is not None
        [report] = fake.reports.values()
        assert 'sbom' in report.document
        assert json.loads(fetch.document.read_text()) == report.document

    @pytest.mark.parametrize(
        'location', [
            None,
            'http://sbom-exports.example/r.json?X-Amz-Signature=5ec7e75',
            'https://[sbom-exports.example/r.json?X-Amz-Signature=5ec7e75',
        ],
        ids=['nowhere', 'plain-http', 'unreadable'],
    )
    def test_a_ready_report_whose_link_is_not_https_is_given_up(
        self, fake, tmp_path, location,
    ):
        repo = observed(fake.repos[1])

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            first = await depgraph.step([repo])
            report = state.report(1)
            assert report is not None and first.next_at is not None
            fake.script(
                Reply(
                    302, headers={} if location is None
                    else {'Location': location}, billed=True,
                ),
                path=urlsplit(report.url).path,
            )
            fake.clock.now = first.next_at.timestamp()
            return await depgraph.step([repo]), outcomes(state)

        second, kept = run(fake, tmp_path, use)
        assert (second.failed, second.pending) == (1, 0)
        assert [outcome.kind for outcome in kept] == [FAILED]
        assert downloads(fake) == []

    def test_a_report_that_moved_with_its_repository_is_given_up(
        self, fake, tmp_path,
    ):
        """A 301 says the repository was renamed since, not that its
        report is ready: it is asked for anew, by its new name, once the
        backoff is over."""
        repo = observed(fake.repos[1])

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            first = await depgraph.step([repo])
            report = state.report(1)
            assert report is not None and first.next_at is not None
            fake.script(
                Reply(
                    301, headers={'Location': f'{API}/repositories/1/x'},
                    billed=True,
                ),
                path=urlsplit(report.url).path,
            )
            fake.clock.now = first.next_at.timestamp()
            return await depgraph.step([repo]), outcomes(state)

        second, kept = run(fake, tmp_path, use)
        assert (second.failed, second.pending) == (1, 0)
        assert [outcome.kind for outcome in kept] == [FAILED]
        assert downloads(fake) == []

    @pytest.mark.parametrize(
        'body', [
            {},
            {'sbom_url': None},
            {'sbom_url': 42},
            {
                'sbom_url': 'https://elsewhere.example/repos/octo/one/'
                'dependency-graph/sbom/fetch-report/x',
            },
            {
                'sbom_url': 'http://api.github.com/repos/octo/one/'
                'dependency-graph/sbom/fetch-report/x',
            },
            {
                'sbom_url': f'{API}/repos/octo/one/dependency-graph/sbom/'
                'fetch-report/x?X-Amz-Signature=5ec7e75',
            },
            {'sbom_url': 'https://[api.github.com/fetch-report/x'},
            ['not', 'an', 'object'],
            b'<html>unicorn</html>',
        ],
        ids=[
            'missing', 'null', 'not-a-string', 'another-host', 'plain-http',
            'signed', 'unreadable', 'a-list', 'not-json',
        ],
    )
    def test_an_answer_that_says_nowhere_on_the_api_to_look_fails(
        self, fake, tmp_path, body,
    ):
        """The look carries the token, so it goes to the API or nowhere;
        and what is kept of a report is never a signed link."""
        fake.script(Reply(201, body, billed=True), path=GENERATE_ONE)

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            step = await depgraph.step([observed(fake.repos[1])])
            return step, state.reports(), outcomes(state)

        step, pending, kept = run(fake, tmp_path, use)
        assert (step.asked, step.failed, step.pending) == (1, 1, 0)
        assert pending == []
        assert [outcome.kind for outcome in kept] == [FAILED]
        assert len(fake.requests) == 1

    def test_a_report_kept_off_the_api_is_never_looked_at(
        self, fake, tmp_path,
    ):
        """However it came to be kept, it is given up, and nothing is
        sent: the token goes to the API alone."""
        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            state.pend_report(
                1, 'https://elsewhere.example/repos/octo/one/'
                'dependency-graph/sbom/fetch-report/x',
                head=None, now=at(START), due_at=at(START),
            )
            step = await depgraph.step([observed(fake.repos[1])])
            return step, state.reports(), outcomes(state)

        step, pending, kept = run(fake, tmp_path, use)
        assert (step.looked, step.failed, step.asked) == (0, 1, 0)
        assert pending == []
        assert [outcome.kind for outcome in kept] == [FAILED]
        assert fake.requests == []

    def test_with_nothing_to_keep_a_graph_of_there_is_nothing_to_do(
        self, fake, tmp_path,
    ):
        step = run(fake, tmp_path, lambda depgraph, state: depgraph.step([]))
        assert step == Step()
        assert step.next_at is None
        assert fake.requests == []


class TestPendingReports:
    def test_are_looked_at_after_a_restart_not_asked_for_anew(
        self, fake, tmp_path,
    ):
        repo = fake.repos[1]
        first = run(
            fake, tmp_path,
            lambda depgraph, state: depgraph.step([observed(repo)]),
        )
        assert first.pending == 1 and first.next_at is not None
        fake.clock.now = first.next_at.timestamp()

        # Pushed since: the graph is stamped with the HEAD it was asked
        # for at.
        second = run(
            fake, tmp_path,
            lambda depgraph, state: depgraph.step(
                [observed(repo, head='f' * 40)],
            ),
        )

        assert (second.asked, second.looked, second.stored) == (0, 1, 1)
        assert asked(fake) == ['one']
        fetch = depgraph_store.newest(tmp_path / 'store', 1)
        assert fetch is not None and fetch.commit_sha == HEAD

    def test_at_most_at_once_are_pending(self, fake, tmp_path):
        for number in range(4, 8):
            fake.add(
                Repo(
                    number, 'octo', f'r{number}',
                    graph=graph_of(f'octo/r{number}'),
                ),
            )
        repositories = everything(fake)

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            pending = []
            step = await depgraph.step(repositories)
            pending.append(len(state.reports()))
            while state.reports():
                assert step.next_at is not None
                fake.clock.now = step.next_at.timestamp()
                step = await depgraph.step(repositories)
                pending.append(len(state.reports()))
            return pending

        pending = run(fake, tmp_path, use, at_once=2)
        assert max(pending) == 2
        assert asked(fake) == ['one', 'two', 'three', 'r4', 'r5', 'r6', 'r7']

    def test_at_least_one_at_once(self, fake, tmp_path):
        with pytest.raises(ValueError, match='at least one'):
            run(
                fake, tmp_path, lambda depgraph, state: depgraph.step([]),
                at_once=0,
            )

    def test_a_repository_whose_report_is_pending_is_not_asked_again(
        self, fake, tmp_path,
    ):
        fake.report_seconds = 60
        repo = observed(fake.repos[1])

        async def use(depgraph: Depgraph, state: CollectorState) -> Step:
            first = await depgraph.step([repo])
            assert first.next_at is not None
            fake.clock.now = first.next_at.timestamp()
            return await depgraph.step([repo])

        second = run(fake, tmp_path, use)
        assert (second.asked, second.looked, second.not_ready) == (0, 1, 1)
        assert asked(fake) == ['one']


class TestWhenAGraphIsDue:
    """Fetched again once its repository was pushed after its graph was
    last learned, but not within the minimum of that; or, pushed or not,
    once that is older than the backstop (the owner's decisions,
    2026-09-30). Last learned: when the newest graph kept was fetched, or
    when a fetch last found it unchanged, whichever is later. Pushed:
    `pushedAt`, as the sweep observed it (#160)."""

    def test_a_push_after_the_graph_was_fetched_makes_it_due(
        self, fake, tmp_path,
    ):
        repo = observed(fake.repos[1], pushed_at=at(START - DAY))

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            await settle(fake, depgraph, state, [repo])
            fake.clock.now = AGAIN
            quiet = await depgraph.step([repo])
            pushed = replace(repo, pushed_at=PUSHED_AGAIN)
            return quiet, await depgraph.step([pushed])

        quiet, due = run(fake, tmp_path, use)
        assert quiet.asked == 0
        assert due.asked == 1

    def test_not_again_within_14_days_of_when_it_was_learned(
        self, fake, tmp_path,
    ):
        """A push within the minimum after the graph was learned waits for
        it to end, and is due as it does."""
        repo = observed(fake.repos[1], pushed_at=at(START - DAY))
        pushed = replace(repo, pushed_at=at(START + DAY))

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            await settle(fake, depgraph, state, [repo])
            fake.clock.now = START + 2 * DAY
            early = await depgraph.step([pushed])
            fake.clock.now = (at(START) + MIN_INTERVAL).timestamp()
            return early, await depgraph.step([pushed])

        early, due = run(fake, tmp_path, use)
        assert MIN_INTERVAL == timedelta(days=14)
        assert early.asked == 0
        assert early.next_at == at(START) + MIN_INTERVAL
        assert due.asked == 1

    def test_the_minimum_is_a_setting(self, fake, tmp_path):
        repo = observed(fake.repos[1], pushed_at=at(START - DAY))
        settings = DepgraphSettings(min_interval=timedelta(days=2))

        async def use(depgraph: Depgraph, state: CollectorState) -> Step:
            await settle(fake, depgraph, state, [repo])
            fake.clock.now = START + 2 * DAY
            return await depgraph.step(
                [replace(repo, pushed_at=at(START + DAY))],
            )

        assert run(fake, tmp_path, use, settings=settings).asked == 1

    def test_waiting_out_the_minimum_it_keeps_its_place(
        self, fake, tmp_path,
    ):
        """It waits from the push it was first found pushed with, not the
        latest: pushed again while the minimum runs, it keeps its place."""
        store = tmp_path / 'store'
        repos = {
            number: fake.add(
                Repo(
                    10 + number, 'octo', f'r{number}',
                    graph=graph_of(f'octo/r{number}', 'new'),
                ),
            )
            for number in (1, 2)
        }
        keep(store, repos[1], START, 'old')
        keep(store, repos[2], START, 'old')
        found = [
            observed(repos[1], pushed_at=at(START + DAY)),
            observed(repos[2], pushed_at=at(START + 2 * DAY)),
        ]
        # r1 pushed again while the minimum runs.
        then = [replace(found[0], pushed_at=at(START + 5 * DAY)), found[1]]

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            fake.clock.now = START + 3 * DAY
            waiting = await depgraph.step(found)
            fake.clock.now = (at(START) + MIN_INTERVAL).timestamp()
            return waiting, await depgraph.step(then)

        waiting, due = run(fake, tmp_path, use, at_once=1)
        assert waiting.asked == 0
        assert waiting.next_at == at(START) + MIN_INTERVAL
        assert due.asked == 1
        assert asked(fake) == ['r1']

    @pytest.mark.parametrize(
        'pushed_at', [at(START - DAY), None], ids=['before', 'unknown'],
    )
    def test_unpushed_since_it_waits_for_the_backstop(
        self, fake, tmp_path, pushed_at,
    ):
        """And a repository whose push the sweep has not observed has the
        backstop alone."""
        repo = observed(fake.repos[1], pushed_at=pushed_at)
        due_at = at(START) + MAX_AGE

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            steps = await settle(fake, depgraph, state, [repo])
            fake.clock.now = due_at.timestamp() - 1
            early = await depgraph.step([repo])
            fake.clock.now = due_at.timestamp()
            return steps, early, await depgraph.step([repo])

        steps, early, due = run(fake, tmp_path, use)
        assert MAX_AGE == timedelta(days=180)
        assert steps[-1].next_at == due_at == early.next_at
        assert early.asked == 0
        assert due.asked == 1

    def test_the_backstop_is_a_setting(self, fake, tmp_path):
        repo = observed(fake.repos[1])
        settings = DepgraphSettings(
            max_age=timedelta(days=7), min_interval=timedelta(days=7),
        )

        async def use(depgraph: Depgraph, state: CollectorState) -> Step:
            await settle(fake, depgraph, state, [repo])
            fake.clock.now = START + 7 * DAY
            return await depgraph.step([repo])

        assert run(fake, tmp_path, use, settings=settings).asked == 1

    def test_a_fetch_that_found_it_unchanged_learned_it_too(
        self, fake, tmp_path,
    ):
        """Asked for again after a push, GitHub's graph is the one kept:
        due again at the next push, or the backstop from that fetch."""
        repo = observed(fake.repos[1], pushed_at=at(START - DAY))
        checked = at(AGAIN)

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            await settle(fake, depgraph, state, [repo])
            fake.clock.now = AGAIN
            pushed = replace(repo, pushed_at=PUSHED_AGAIN)
            again = await settle(fake, depgraph, state, [pushed])
            fake.clock.now = AGAIN + 20 * DAY
            quiet = await depgraph.step([pushed])
            fake.clock.now = (checked + MAX_AGE).timestamp()
            return again, quiet, await depgraph.step([pushed])

        again, quiet, backstop = run(fake, tmp_path, use)
        assert total(again, 'unchanged') == 1
        assert quiet.asked == 0
        assert quiet.next_at == checked + MAX_AGE
        assert backstop.asked == 1

    def test_a_push_while_its_report_was_made_makes_it_due_again(
        self, fake, tmp_path,
    ):
        """A graph is learned as of when its report was asked for, as the
        HEAD it is stamped with is: a push after that may not be in it,
        and makes it due again once the minimum has passed."""
        fake.report_seconds = 60
        repo = observed(fake.repos[1], pushed_at=at(START - DAY))

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            steps = await settle(fake, depgraph, state, [repo])
            pushed = replace(repo, pushed_at=at(START + 30))
            again = await depgraph.step([pushed])
            fake.clock.now = (at(START) + MIN_INTERVAL).timestamp()
            return steps, again, await depgraph.step([pushed])

        steps, again, due = run(fake, tmp_path, use)
        assert total(steps, 'stored') == 1
        assert looks(fake)[-1].path.startswith('/repos/octo/one/')
        fetch = depgraph_store.newest(tmp_path / 'store', 1)
        assert fetch is not None and fetch.fetched_at == at(START)
        assert again.asked == 0
        assert again.next_at == at(START) + MIN_INTERVAL
        assert due.asked == 1

    def test_never_asked_then_pushed_longest_waiting_then_the_oldest(
        self, fake, tmp_path,
    ):
        """Never asked about first: nothing is known of its graph. Then
        those pushed since their graph, by when they were pushed, the
        longest waiting first: a change GitHub has and the store has not.
        Then those past the backstop, the oldest first."""
        store = tmp_path / 'store'
        repos = {
            number: fake.add(
                Repo(
                    10 + number, 'octo', f'r{number}',
                    graph=graph_of(f'octo/r{number}', 'new'),
                ),
            )
            for number in range(1, 7)
        }
        keep(store, repos[2], START - 30 * DAY, 'old')
        keep(store, repos[3], START - 30 * DAY, 'old')
        keep(store, repos[4], START - 200 * DAY, 'old')
        keep(store, repos[5], START - 190 * DAY, 'old')
        keep(store, repos[6], START - 10 * DAY, 'old')
        pushed = {
            # Never asked about.
            1: None,
            # Pushed since their graphs, 5 and 20 days ago.
            2: at(START - 5 * DAY),
            3: at(START - 20 * DAY),
            # Past the backstop, pushed before its graph or not observed.
            4: at(START - 250 * DAY),
            5: None,
            # Pushed before a graph that is fresh: not due.
            6: at(START - 11 * DAY),
        }
        repositories = [
            observed(repos[number], pushed_at=pushed[number])
            for number in repos
        ]

        steps = run(fake, tmp_path, settled(fake, repositories), at_once=1)

        assert asked(fake) == ['r1', 'r3', 'r2', 'r4', 'r5']
        assert total(steps, 'stored') == 5
        # The next due is the fresh one, at the backstop.
        assert steps[-1].next_at == at(START - 10 * DAY) + MAX_AGE

    def test_a_push_while_it_waits_keeps_its_place(self, fake, tmp_path):
        """A repository waits from the push it was first found pushed
        with: pushed again while it waits, it is not sent to the back,
        where one pushed as often as the queue moves would wait for
        good."""
        store = tmp_path / 'store'
        repos = {
            number: fake.add(
                Repo(
                    10 + number, 'octo', f'r{number}',
                    graph=graph_of(f'octo/r{number}', 'new'),
                ),
            )
            for number in range(1, 4)
        }
        keep(store, repos[1], START - 30 * DAY, 'old')
        keep(store, repos[2], START - 30 * DAY, 'old')
        quiet = [
            observed(repos[1], pushed_at=at(START - 40 * DAY)),
            observed(repos[2], pushed_at=at(START - 40 * DAY)),
            # Never asked about: first, and the one report at a time.
            observed(repos[3]),
        ]
        # Found pushed while r3's report is made, with no room for more.
        found = [
            replace(quiet[0], pushed_at=at(START - 20 * DAY)),
            replace(quiet[1], pushed_at=at(START - 5 * DAY)),
            quiet[2],
        ]
        # r1 pushed again, while it waits.
        then = [replace(found[0], pushed_at=at(START + 1)), *found[1:]]

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            await depgraph.step(quiet)
            fake.clock.now = START + 1
            waiting = await depgraph.step(found)
            return waiting, await settle(fake, depgraph, state, then)

        waiting, steps = run(fake, tmp_path, use, at_once=1)
        assert (waiting.asked, waiting.pending) == (0, 1)
        assert asked(fake) == ['r3', 'r1', 'r2']
        assert total(steps, 'stored') == 3

    def test_which_graph_is_kept_the_store_says_not_collector_sqlite(
        self, fake, tmp_path,
    ):
        """collector.sqlite is never what says a graph is kept: deleted,
        it costs nothing here."""
        repo = observed(fake.repos[1], pushed_at=at(START - DAY))
        run(fake, tmp_path, settled(fake, [repo]))
        for suffix in ('', '-wal', '-shm'):
            Path(f'{tmp_path / STATE_FILE}{suffix}').unlink(missing_ok=True)

        fake.clock.advance(20 * DAY)
        again = run(
            fake, tmp_path, lambda depgraph, state: depgraph.step([repo]),
        )
        assert again.asked == 0
        assert asked(fake) == ['one']


class TestTheNegativeCache:
    def test_no_graph_is_asked_about_again_after_30_days(
        self, fake, tmp_path,
    ):
        repo = observed(fake.repos[2])

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            first = await depgraph.step([repo])
            [outcome] = outcomes(state)
            fake.clock.now = START + 30 * DAY - 1
            early = await depgraph.step([repo])
            fake.clock.now = START + 30 * DAY
            due = await depgraph.step([repo])
            return first, outcome, early, due

        first, outcome, early, due = run(fake, tmp_path, use)
        assert (first.asked, first.no_graph, first.failed) == (1, 1, 0)
        assert first.pending == 0
        assert NO_GRAPH == timedelta(days=30)
        assert outcome.kind == NOTHING
        assert 'no graph' in outcome.detail
        assert outcome.due_at == at(START) + NO_GRAPH == first.next_at
        assert early.asked == 0
        assert due.no_graph == 1
        assert asked(fake) == ['two', 'two']

    def test_its_delay_is_a_setting(self, fake, tmp_path):
        repo = observed(fake.repos[2])
        settings = DepgraphSettings(no_graph=timedelta(days=10))

        async def use(depgraph: Depgraph, state: CollectorState) -> Step:
            await depgraph.step([repo])
            fake.clock.now = START + 10 * DAY
            return await depgraph.step([repo])

        assert run(fake, tmp_path, use, settings=settings).asked == 1

    def test_a_graph_found_after_all_is_kept_and_the_cache_forgotten(
        self, fake, tmp_path,
    ):
        repo = observed(fake.repos[2])

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            await depgraph.step([repo])
            fake.repos[2].graph = graph_of('octo/two')
            fake.clock.now = START + 30 * DAY
            steps = await settle(fake, depgraph, state, [repo])
            return steps, outcomes(state)

        steps, kept = run(fake, tmp_path, use)
        assert total(steps, 'stored') == 1
        assert kept == []

    def test_outlives_a_restart(self, fake, tmp_path):
        repo = observed(fake.repos[2])
        run(fake, tmp_path, lambda depgraph, state: depgraph.step([repo]))
        fake.clock.advance(29 * DAY)
        again = run(
            fake, tmp_path, lambda depgraph, state: depgraph.step([repo]),
        )
        assert again.asked == 0
        assert asked(fake) == ['two']


async def fetched_then_pushed(
    fake: FakeGitHub, depgraph: Depgraph, state: CollectorState,
) -> list[Step]:
    """Repository 1's graph asked for at START, then at AGAIN, after a
    push: the steps of the second fetch."""
    repo = observed(fake.repos[1], pushed_at=at(START - DAY))
    await settle(fake, depgraph, state, [repo])
    fake.clock.now = AGAIN
    return await settle(
        fake, depgraph, state, [replace(repo, pushed_at=PUSHED_AGAIN)],
    )


class TestAGraphAsItWas:
    def test_is_not_written_again_and_when_it_was_checked_is_kept(
        self, fake, tmp_path,
    ):
        """GitHub's graph is the one kept, byte for byte but for what
        GitHub makes anew for each report, when it made it and its
        namespace: nothing is written, and collector.sqlite keeps when it
        was found so."""
        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            again = await fetched_then_pushed(fake, depgraph, state)
            return again, state.outcome(1, STAGE, CHECKED), outcomes(state)

        again, check, kept = run(fake, tmp_path, use)
        assert (total(again, 'unchanged'), total(again, 'stored')) == (1, 0)
        assert len(depgraph_store.fetches(tmp_path / 'store', 1)) == 1
        assert check is not None and check.kind == NOTHING
        assert 'unchanged' in check.detail
        assert check.last_at == at(AGAIN)
        # And nothing backs the repository off: it is due at the next push.
        assert [outcome.key for outcome in kept] == [CHECKED]

    def test_a_graph_that_changed_is_kept_beside_the_last(
        self, fake, tmp_path,
    ):
        repo = observed(fake.repos[1], pushed_at=at(START - DAY))

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            await settle(fake, depgraph, state, [repo])
            fake.repos[1].graph = graph_of('octo/one', 'left-pad', 'is-odd')
            fake.clock.now = AGAIN
            pushed = replace(repo, pushed_at=PUSHED_AGAIN)
            steps = await settle(fake, depgraph, state, [pushed])
            return steps, outcomes(state)

        steps, kept = run(fake, tmp_path, use)
        assert total(steps, 'stored') == 1
        assert kept == []
        fetches = depgraph_store.fetches(tmp_path / 'store', 1)
        assert [fetch.fetched_at for fetch in fetches] == [
            at(START), at(AGAIN),
        ]
        # As the report came, stamps and all.
        report = list(fake.reports.values())[-1]
        assert json.loads(fetches[-1].document.read_text()) == {
            'sbom': report.document,
        }

    @pytest.mark.parametrize(
        'change', [
            lambda graph: graph['creationInfo'].update(
                creators=['Tool: GitHub.com-Dependency-Graph-2'],
            ),
            lambda graph: graph.update(
                {key: graph.pop(key) for key in list(graph)[::-1]},
            ),
        ],
        ids=['its-creators', 'the-order-of-its-fields'],
    )
    def test_so_is_one_that_changed_in_anything_else(
        self, fake, tmp_path, change,
    ):
        """Byte for byte, but for the two stamps: what else creationInfo
        says, and the order the fields come in, are the graph's."""
        repo = observed(fake.repos[1], pushed_at=at(START - DAY))

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            await settle(fake, depgraph, state, [repo])
            change(fake.repos[1].graph)
            fake.clock.now = AGAIN
            pushed = replace(repo, pushed_at=PUSHED_AGAIN)
            return await settle(fake, depgraph, state, [pushed])

        steps = run(fake, tmp_path, use)
        assert (total(steps, 'stored'), total(steps, 'unchanged')) == (1, 0)
        assert len(depgraph_store.fetches(tmp_path / 'store', 1)) == 2

    def test_a_graph_kept_that_cannot_be_read_is_none_to_compare_with(
        self, fake, tmp_path,
    ):
        repo = observed(fake.repos[1], pushed_at=at(START - DAY))

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            await settle(fake, depgraph, state, [repo])
            kept = depgraph_store.newest(tmp_path / 'store', 1)
            assert kept is not None
            kept.document.write_text('{"sbom": {"packages": [cut short}')
            fake.clock.now = AGAIN
            pushed = replace(repo, pushed_at=PUSHED_AGAIN)
            return await settle(fake, depgraph, state, [pushed])

        steps = run(fake, tmp_path, use)
        assert (total(steps, 'stored'), total(steps, 'unchanged')) == (1, 0)
        assert len(depgraph_store.fetches(tmp_path / 'store', 1)) == 2

    def test_what_a_check_learned_outlives_a_failure(self, fake, tmp_path):
        """A failure after it backs the repository off, and leaves when
        its graph was last learned as it was."""
        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            await fetched_then_pushed(fake, depgraph, state)
            check = state.outcome(1, STAGE, CHECKED)
            fake.clock.now = LATER
            fake.script(
                Reply(502, {'message': 'Server Error'}, billed=True),
                path=GENERATE_ONE,
            )
            pushed = observed(fake.repos[1], pushed_at=PUSHED_LATER)
            failed = await depgraph.step([pushed])
            return check, failed, outcomes(state)

        check, failed, kept = run(fake, tmp_path, use)
        assert failed.failed == 1
        assert sorted((outcome.key, outcome.kind) for outcome in kept) == [
            (KEY, FAILED), (CHECKED, NOTHING),
        ]
        assert [
            outcome.last_at for outcome in kept if outcome.key == CHECKED
        ] == [check.last_at]

    def test_and_no_graph_after_it_then_a_failure(self, fake, tmp_path):
        """A failure after no graph is a first, and what was said of the
        repository before either is forgotten; not what a check learned."""
        pushed = observed(fake.repos[1], pushed_at=PUSHED_LATER)

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            await fetched_then_pushed(fake, depgraph, state)
            check = state.outcome(1, STAGE, CHECKED)
            fake.repos[1].graph = None
            fake.clock.now = LATER
            gone = await depgraph.step([pushed])
            fake.clock.now = LATER + 30 * DAY
            fake.script(
                Reply(502, {'message': 'Server Error'}, billed=True),
                path=GENERATE_ONE,
            )
            failed = await depgraph.step([pushed])
            return check, gone, failed, outcomes(state)

        check, gone, failed, kept = run(fake, tmp_path, use)
        assert (gone.no_graph, failed.failed) == (1, 1)
        assert sorted(
            (outcome.key, outcome.kind, outcome.attempts, outcome.last_at)
            for outcome in kept
        ) == [
            (KEY, FAILED, 1, at(LATER + 30 * DAY)),
            (CHECKED, NOTHING, check.attempts, check.last_at),
        ]

    def test_a_check_after_a_failure_ends_its_backoff(self, fake, tmp_path):
        """Found unchanged after a failure, the repository is backed off
        no more: after a restart too, it waits for its next push."""
        pushed = observed(fake.repos[1], pushed_at=PUSHED_LATER)

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            await fetched_then_pushed(fake, depgraph, state)
            fake.clock.now = LATER
            fake.script(
                Reply(502, {'message': 'Server Error'}, billed=True),
                path=GENERATE_ONE,
            )
            await depgraph.step([pushed])
            [failure] = [
                outcome for outcome in outcomes(state) if outcome.key == KEY
            ]
            fake.clock.now = failure.due_at.timestamp()
            steps = await settle(fake, depgraph, state, [pushed])
            return steps, outcomes(state)

        steps, kept = run(fake, tmp_path, use)
        assert total(steps, 'unchanged') == 1
        checked = at(LATER) + timedelta(minutes=15)
        assert [(outcome.key, outcome.last_at) for outcome in kept] == [
            (CHECKED, checked),
        ]
        fake.clock.advance(DAY)
        again = run(
            fake, tmp_path, lambda depgraph, state: depgraph.step([pushed]),
        )
        assert again.asked == 0
        assert again.next_at == checked + MAX_AGE


class TestFailures:
    def test_back_off_from_15_minutes_doubling(self, fake, tmp_path):
        fake.script(
            Reply(502, {'message': 'Server Error'}, billed=True),
            path=GENERATE_ONE, times=2,
        )
        repo = observed(fake.repos[1])

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            first = await depgraph.step([repo])
            [one] = outcomes(state)
            fake.clock.now = one.due_at.timestamp()
            await depgraph.step([repo])
            [two] = outcomes(state)
            fake.clock.now = two.due_at.timestamp()
            steps = await settle(fake, depgraph, state, [repo])
            return first, one, two, steps, outcomes(state)

        first, one, two, steps, after = run(fake, tmp_path, use)
        assert first.failed == 1
        assert (one.kind, one.attempts) == (FAILED, 1)
        assert one.due_at == at(START) + timedelta(minutes=15)
        assert first.next_at == one.due_at
        assert (two.kind, two.attempts) == (FAILED, 2)
        assert two.due_at == one.due_at + timedelta(minutes=30)
        assert '502' in two.detail
        assert total(steps, 'stored') == 1
        assert after == []

    def test_a_failure_after_no_graph_is_a_first(self, fake, tmp_path):
        """Attempts count failures in a row: the months with no graph
        before it are none."""
        repo = observed(fake.repos[2])

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            await depgraph.step([repo])
            fake.clock.now = START + 30 * DAY
            fake.script(
                Reply(502, {'message': 'Server Error'}, billed=True),
                path='/repos/octo/two/dependency-graph/sbom/generate-report',
            )
            await depgraph.step([repo])
            return outcomes(state)

        [outcome] = run(fake, tmp_path, use)
        assert (outcome.kind, outcome.attempts) == (FAILED, 1)
        assert outcome.due_at == at(START + 30 * DAY) + timedelta(minutes=15)

    def test_a_renamed_repository_fails_until_its_new_name_is_observed(
        self, fake, tmp_path,
    ):
        fake.rename(1, 'octo', 'uno')
        by_old_name = observed(fake.repos[1], full_name='octo/one')

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            first = await depgraph.step([by_old_name])
            [outcome] = outcomes(state)
            fake.clock.now = outcome.due_at.timestamp()
            steps = await settle(
                fake, depgraph, state, [observed(fake.repos[1])],
            )
            return first, outcome, steps

        first, outcome, steps = run(fake, tmp_path, use)
        assert first.failed == 1
        assert outcome.kind == FAILED and '301' in outcome.detail
        assert total(steps, 'stored') == 1
        assert asked(fake) == ['one', 'uno']
        fetch = depgraph_store.newest(tmp_path / 'store', 1)
        assert fetch is not None
        meta = json.loads((fetch.directory / 'meta.json').read_text())
        assert (meta['owner'], meta['repo']) == ('octo', 'uno')

    def test_every_token_refused_is_raised_and_nothing_recorded(
        self, fake, tmp_path,
    ):
        revoked = Token('token 1', 'ghp_revoked_00000000000000000000000000')
        with pytest.raises(Unauthorized):
            run(
                fake, tmp_path,
                lambda depgraph, state: depgraph.step(
                    [observed(fake.repos[1])],
                ),
                revoked,
            )
        with CollectorState.open(tmp_path / STATE_FILE) as state:
            assert outcomes(state) == []
            assert state.reports() == []


class TestTheBucket:
    def test_every_request_of_the_api_draws_from_the_graphs_own(
        self, fake, tmp_path,
    ):
        budget = budget_for(fake)
        run(fake, tmp_path, settled(fake, everything(fake)), budget=budget)

        api = [seen for seen in fake.requests if seen.host == 'api.github.com']
        assert len(api) == 5
        assert {seen.bucket for seen in api} == {BUCKET} == {'dependency_sbom'}
        assert budget.standing(T1, BUCKET).remaining == 200 - len(api)
        # Nothing was taken from the REST API's.
        assert budget.standing(T1, 'core').remaining is None

    def test_spent_elsewhere_it_backs_off_until_its_reset(
        self, fake, tmp_path,
    ):
        """Refused, 403 with nothing left: nothing more is sent before
        the reset, nothing is recorded of the repository, and the REST
        API's bucket is not backed off."""
        meter = fake.meter(ONE, BUCKET)
        meter.remaining = 0
        reset = meter.reset
        repositories = [observed(fake.repos[1]), observed(fake.repos[3])]
        budget = budget_for(fake)

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            first = await depgraph.step(repositories)
            recorded = state.reports(), outcomes(state)
            assert first.next_at is not None
            fake.clock.now = first.next_at.timestamp()
            return first, recorded, await settle(
                fake, depgraph, state, repositories,
            )

        first, recorded, steps = run(fake, tmp_path, use, budget=budget)
        assert first.refused is not None
        assert first.refused.bucket == BUCKET
        assert first.next_at == at(reset + 1)
        assert (first.asked, first.failed, first.pending) == (0, 0, 0)
        assert recorded == ([], [])
        assert [seen.status for seen in fake.requests].count(403) == 1
        assert fake.requests[0].status == 403
        assert budget.standing(T1, 'core').blocked_until is None
        assert total(steps, 'stored') == 2

    def test_spent_as_its_answers_said_it_is_not_asked_at_all(
        self, fake, tmp_path,
    ):
        """The budget reads what each answer says is left, and sends
        nothing it knows would be refused."""
        fake.token(ONE, 'alice', dependency_sbom=(2, 3_600))
        four = fake.add(Repo(4, 'octo', 'four', graph=graph_of('octo/four')))
        repositories = [
            observed(fake.repos[1]), observed(fake.repos[3]), observed(four),
        ]

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            return await depgraph.step(repositories), state.reports()

        first, pending = run(fake, tmp_path, use)
        assert first.asked == 2 and len(pending) == 2
        assert first.refused is not None
        assert first.next_at == at(START + 3_600)
        assert [seen.status for seen in fake.requests] == [201, 201]

    @pytest.mark.parametrize('status', [403, 429])
    def test_a_secondary_limit_backs_off_as_retry_after_says(
        self, fake, tmp_path, status,
    ):
        fake.secondary(ONE, BUCKET, seconds=120, status=status)
        repo = observed(fake.repos[1])

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            first = await depgraph.step([repo])
            assert first.next_at is not None
            fake.clock.now = first.next_at.timestamp()
            return first, await depgraph.step([repo])

        first, second = run(fake, tmp_path, use)
        assert first.refused is not None
        assert first.next_at == at(START + 120)
        assert second.asked == 1
        assert [seen.status for seen in fake.requests] == [status, 201]

    def test_a_short_secondary_limit_is_waited_out(self, fake, tmp_path):
        fake.secondary(ONE, BUCKET, seconds=30)
        step = run(
            fake, tmp_path,
            lambda depgraph, state: depgraph.step([observed(fake.repos[1])]),
        )
        assert step.refused is None and step.asked == 1
        assert fake.clock() >= START + 30

    def test_a_token_whose_bucket_is_spent_leaves_the_other_to_ask(
        self, fake, tmp_path,
    ):
        fake.meter(ONE, BUCKET).remaining = 0
        steps = run(
            fake, tmp_path, settled(fake, [observed(fake.repos[1])]), T1, T2,
        )
        assert steps[0].refused is None
        assert total(steps, 'stored') == 1
        assert [
            (seen.token, seen.status) for seen in fake.requests
            if seen.host == 'api.github.com'
        ] == [(ONE, 403), (TWO, 201), (TWO, 302)]

    def test_a_refusal_while_looking_leaves_the_report_as_it_was(
        self, fake, tmp_path,
    ):
        repo = observed(fake.repos[1])

        async def use(depgraph: Depgraph, state: CollectorState) -> Any:
            first = await depgraph.step([repo])
            meter = fake.meter(ONE, BUCKET)
            meter.remaining = 0
            assert first.next_at is not None
            fake.clock.now = first.next_at.timestamp()
            second = await depgraph.step([repo])
            report = state.report(1)
            assert second.next_at is not None
            fake.clock.now = second.next_at.timestamp()
            return second, report, meter.reset, await depgraph.step([repo])

        second, report, reset, third = run(fake, tmp_path, use)
        assert second.refused is not None
        assert (second.looked, second.failed, second.pending) == (0, 0, 1)
        assert report.attempts == 0
        assert report.due_at == at(START) + FIRST_LOOK
        assert second.next_at == at(reset + 1)
        assert (third.looked, third.stored) == (1, 1)


class TestSecrets:
    def test_no_signed_link_nor_token_in_a_log_collector_sqlite_or_store(
        self, fake, tmp_path, capsys, monkeypatch,
    ):
        """In either log format, at DEBUG, and whatever goes wrong with a
        download."""
        for log_format in ('console', 'json'):
            monkeypatch.setenv('CHATSBOM_LOG_FORMAT', log_format)
            setup_logging('DEBUG')
            first = len(fake.reports) + 1
            fake.script(
                Reply(403, b'<Error>Request has expired</Error>'),
                path=download_path(first),
            )
            fake.script(
                Reply(
                    0, raises=httpx2.ConnectError(
                        f'refused: https://{EXPORTS_HOST}/sbom/x.spdx.json'
                        '?X-Amz-Signature=dead5ec7e75',
                    ),
                ),
                path=download_path(first + 1),
            )
            steps = run(
                fake, tmp_path / log_format, settled(fake, everything(fake)),
            )
            assert total(steps, 'stored') == 2
            fake.clock.advance(DAY)
        monkeypatch.delenv('CHATSBOM_LOG_FORMAT')
        setup_logging('INFO')

        captured = capsys.readouterr()
        logged = captured.out + captured.err
        assert 'Dependency graph stored' in logged
        kept = b''.join(
            path.read_bytes() for path in tmp_path.rglob('*') if path.is_file()
        )
        signatures = [
            seen.query['X-Amz-Signature'] for seen in downloads(fake)
            if 'X-Amz-Signature' in seen.query
        ] + ['dead5ec7e75']
        assert len(signatures) == 9
        for secret in (*signatures, ONE, TWO):
            assert secret not in logged
            assert secret.encode() not in kept
        assert all(seen.token is None for seen in downloads(fake))

    def test_no_event_holds_a_signed_link_before_the_log_redacts_it(
        self, fake, tmp_path,
    ):
        """An error of the download is said without the link where it is
        said: the log's own redaction is a second line, not the only
        one."""
        fake.script(
            Reply(
                0, raises=httpx2.ConnectError(
                    f'refused: https://{EXPORTS_HOST}/sbom/x.spdx.json'
                    '?X-Amz-Signature=dead5ec7e75',
                ),
            ),
            path=download_path(1),
        )
        with structlog.testing.capture_logs() as events:
            steps = run(
                fake, tmp_path, settled(fake, [observed(fake.repos[1])]),
            )
        assert total(steps, 'stored') == 1
        said = repr(events)
        assert 'Dependency graph stored' in said
        assert 'ConnectError' in said
        for seen in downloads(fake):
            assert seen.query['X-Amz-Signature'] not in said
        assert 'dead5ec7e75' not in said


class TestItsSettings:
    """Said as the collector's intervals are, the sweep's and the
    universe's (#160): a whole number and a unit."""

    def test_by_default_180_days_30_and_14(self):
        assert depgraph_settings({}) == DepgraphSettings() == DepgraphSettings(
            max_age=timedelta(days=180), no_graph=timedelta(days=30),
            min_interval=timedelta(days=14),
        )

    def test_as_the_environment_says(self):
        assert depgraph_settings({
            'CHATSBOM_DEPGRAPH_MAX_AGE': '26w',
            'CHATSBOM_DEPGRAPH_NO_GRAPH': ' 90D ',
            'CHATSBOM_DEPGRAPH_MIN_INTERVAL': '3w',
        }) == DepgraphSettings(
            max_age=timedelta(weeks=26), no_graph=timedelta(days=90),
            min_interval=timedelta(weeks=3),
        )

    def test_empty_is_the_default(self):
        assert depgraph_settings({
            'CHATSBOM_DEPGRAPH_MAX_AGE': '',
            'CHATSBOM_DEPGRAPH_NO_GRAPH': ' ',
            'CHATSBOM_DEPGRAPH_MIN_INTERVAL': '',
        }) == DepgraphSettings()

    def test_ten_years_at_most(self):
        assert depgraph_settings({
            'CHATSBOM_DEPGRAPH_MAX_AGE': '3650d',
            'CHATSBOM_DEPGRAPH_NO_GRAPH': '87600h',
            'CHATSBOM_DEPGRAPH_MIN_INTERVAL': '3650d',
        }) == DepgraphSettings(
            max_age=timedelta(days=3_650), no_graph=timedelta(days=3_650),
            min_interval=timedelta(days=3_650),
        )

    @pytest.mark.parametrize(
        'environ', [
            {'CHATSBOM_DEPGRAPH_MIN_INTERVAL': '181d'},
            {'CHATSBOM_DEPGRAPH_MAX_AGE': '13d'},
            {
                'CHATSBOM_DEPGRAPH_MIN_INTERVAL': '3w',
                'CHATSBOM_DEPGRAPH_MAX_AGE': '20d',
            },
        ],
    )
    def test_a_minimum_longer_than_the_backstop_is_refused_naming_both(
        self, environ,
    ):
        """The least time between two fetches cannot be more than the
        most."""
        with pytest.raises(SettingsError) as refused:
            depgraph_settings(environ)
        assert refused.value.setting == 'CHATSBOM_DEPGRAPH_MIN_INTERVAL'
        message = str(refused.value)
        assert 'CHATSBOM_DEPGRAPH_MIN_INTERVAL' in message
        assert 'CHATSBOM_DEPGRAPH_MAX_AGE' in message

    def test_a_minimum_as_long_as_the_backstop_is_not(self):
        assert depgraph_settings({
            'CHATSBOM_DEPGRAPH_MIN_INTERVAL': '14d',
            'CHATSBOM_DEPGRAPH_MAX_AGE': '2w',
        }) == DepgraphSettings(
            max_age=timedelta(days=14), min_interval=timedelta(days=14),
        )

    @pytest.mark.parametrize(
        'name', [
            'CHATSBOM_DEPGRAPH_MAX_AGE', 'CHATSBOM_DEPGRAPH_NO_GRAPH',
            'CHATSBOM_DEPGRAPH_MIN_INTERVAL',
        ],
    )
    @pytest.mark.parametrize(
        'value', [
            '0d', '30', '-7d', '1.5d', '30 days', 'thirty',
            # Past ten years: a date soon cannot hold what comes after.
            '3651d', '522w', '99999999999999w',
        ],
    )
    def test_what_is_no_interval_or_past_ten_years_is_refused_by_name(
        self, name, value,
    ):
        with pytest.raises(SettingsError) as refused:
            depgraph_settings({name: value})
        assert refused.value.setting == name
        message = str(refused.value)
        assert name in message and repr(value) in message

    def test_the_names_it_was_said_by_are_read_no_more(self):
        """There is no one to stay compatible with (#155)."""
        assert depgraph_settings({
            'CHATSBOM_DEPGRAPH_REFRESH': '7d',
            'CHATSBOM_DEPGRAPH_REFRESH_DAYS': '7',
            'CHATSBOM_DEPGRAPH_NO_GRAPH_DAYS': 'not read',
        }) == DepgraphSettings()

    def test_read_from_the_processs_environment(self, monkeypatch):
        monkeypatch.setenv('CHATSBOM_DEPGRAPH_MAX_AGE', '14d')
        monkeypatch.delenv('CHATSBOM_DEPGRAPH_NO_GRAPH', raising=False)
        assert depgraph_settings() == DepgraphSettings(
            max_age=timedelta(days=14),
        )
