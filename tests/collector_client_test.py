"""The collector's GitHub client, on httpx2, against the stand-in (#156).

REST, with conditional requests from the validators collector.sqlite
keeps, where a 304 costs nothing; GraphQL; and search. Each request
takes its token from the budget manager and gives it back with the
answer's headers. What goes wrong is one of four errors, not found,
gone or moved, rate limited, and failed, and no token is ever in one,
or in a log line.

The stand-in is tests/fake_github_test.py, and its clock is the
budget's: a wait of an hour takes no time.
"""
import asyncio
import json
from collections.abc import Awaitable
from collections.abc import Callable
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any
from typing import TypeVar

import httpx2
import pytest

from chatsbom.collector.budget import BudgetManager
from chatsbom.collector.client import Answer
from chatsbom.collector.client import API
from chatsbom.collector.client import API_VERSION
from chatsbom.collector.client import GitHubClient
from chatsbom.collector.client import GraphQLAnswer
from chatsbom.collector.client import request_key
from chatsbom.collector.errors import Failed
from chatsbom.collector.errors import GitHubError
from chatsbom.collector.errors import Gone
from chatsbom.collector.errors import NotFound
from chatsbom.collector.errors import RateLimited
from chatsbom.collector.errors import Unauthorized
from chatsbom.collector.state import CollectorState
from chatsbom.collector.state import STATE_FILE
from chatsbom.collector.state import Validators
from chatsbom.collector.tokens import Token
from chatsbom.core.logging import setup_logging
from tests.fake_github_test import FakeClock
from tests.fake_github_test import FakeGitHub
from tests.fake_github_test import Release
from tests.fake_github_test import Reply
from tests.fake_github_test import Repo
from tests.fake_github_test import START

ONE = 'ghp_collector_token_one_0000000000000000'
TWO = 'ghp_collector_token_two_0000000000000000'
T1 = Token('token 1', ONE)
T2 = Token('token 2', TWO)

JSON = 'application/vnd.github+json'

NODES = '''
query($ids: [ID!]!) {
  nodes(ids: $ids) { ... on Repository { id databaseId pushedAt } }
}
'''

Result = TypeVar('Result')


@pytest.fixture
def fake() -> FakeGitHub:
    fake = FakeGitHub(FakeClock())
    fake.token(ONE, 'alice')
    fake.token(TWO, 'bob')
    fake.add(
        Repo(
            1, 'octo', 'one', stars=5_000, releases=[
                Release('v2.0', '2026-09-10T00:00:00Z'),
                Release('v1.0', '2026-01-10T00:00:00Z'),
                Release('v0.1', '2025-01-10T00:00:00Z'),
            ],
        ),
    )
    fake.add(Repo(2, 'octo', 'two', stars=1_500))
    return fake


def budget_for(
    fake: FakeGitHub, *tokens: Token, reserve: dict[str, int] | None = None,
) -> BudgetManager:
    return BudgetManager(
        tokens or (T1,), reserve=reserve or {}, clock=fake.clock,
        sleep=fake.clock.sleep,
    )


def github(
    fake: FakeGitHub, budget: BudgetManager,
    state: CollectorState | None = None,
) -> GitHubClient:
    return GitHubClient(budget, validators=state, transport=fake.transport())


def run(
    fake: FakeGitHub, use: Callable[[GitHubClient], Awaitable[Result]],
    *tokens: Token, state: CollectorState | None = None,
    budget: BudgetManager | None = None,
) -> Result:
    """`use` of a client of `tokens` against the stand-in, the client
    closed after."""
    async def using() -> Result:
        async with github(
            fake, budget or budget_for(fake, *tokens), state,
        ) as client:
            return await use(client)

    return asyncio.run(using())


def failure(
    fake: FakeGitHub, use: Callable[[GitHubClient], Awaitable[Any]],
    *tokens: Token, **options: Any,
) -> GitHubError:
    with pytest.raises(GitHubError) as failed:
        run(fake, use, *tokens, **options)
    return failed.value


@pytest.fixture
def state(tmp_path: Path) -> Any:
    with CollectorState.open(tmp_path / STATE_FILE) as state:
        yield state


class TestREST:
    def test_answers_with_the_document(self, fake):
        answer = run(fake, lambda github: github.get('/repos/octo/one'))
        assert isinstance(answer, Answer)
        assert answer.status == 200
        assert answer.not_modified is False
        assert answer.json()['full_name'] == 'octo/one'
        assert answer.token == 'token 1'
        assert answer.bucket == 'core'
        assert answer.url == f'{API}/repos/octo/one'

    def test_its_bucket_stands_where_the_answer_said(self, fake):
        budget = budget_for(fake)
        run(fake, lambda github: github.get('/repos/octo/one'), budget=budget)
        standing = budget.standing(T1, 'core')
        assert standing.remaining == 4_999
        assert standing.limit == 5_000
        assert standing.reset == datetime.fromtimestamp(
            START + 3_600, timezone.utc,
        )

    def test_asks_as_github_asks_to_be_asked(self, fake):
        run(fake, lambda github: github.get('/repos/octo/one'))
        sent = fake.requests[0].headers
        assert sent['accept'] == JSON
        assert sent['x-github-api-version'] == API_VERSION == '2022-11-28'
        assert sent['user-agent'].startswith('chatsbom/')
        assert fake.requests[0].token == ONE

    def test_reads_the_pages_links(self, fake):
        async def paging(github: GitHubClient) -> list[str]:
            first = await github.get(
                '/repos/octo/one/releases', params={'per_page': 2},
            )
            second = await github.get(first.links['next'])
            assert 'next' not in second.links
            return [r['tag_name'] for r in first.json() + second.json()]

        assert run(fake, paging) == ['v2.0', 'v1.0', 'v0.1']

    def test_never_asks_rate_limit(self, fake):
        """What it said was not the bucket requests are drawn from."""
        async def asking(github: GitHubClient) -> None:
            await github.get('/repos/octo/one')
            await github.search('repositories', 'stars:>=1000')
            await github.graphql(NODES, {'ids': [fake.repos[1].node_id]})

        run(fake, asking)
        assert fake.seen('/rate_limit') == []

    @pytest.mark.parametrize(
        'url', [
            'https://example.com/repos/octo/one',
            'http://api.github.com/repos/octo/one',
            'https://api.github.com.example.com/repos/octo/one',
            '//example.com/repos/octo/one',
            'repos/octo/one',
        ],
    )
    def test_sends_a_token_to_the_api_alone(self, fake, url):
        with pytest.raises(ValueError):
            run(fake, lambda github: github.get(url))
        assert fake.requests == []


class TestConditionalRequests:
    def test_a_304_is_free(self, fake, state):
        budget = budget_for(fake)

        async def twice(github: GitHubClient) -> tuple[Answer, Answer]:
            return (
                await github.get('/repos/octo/one'),
                await github.get('/repos/octo/one'),
            )

        first, second = run(fake, twice, state=state, budget=budget)

        assert first.status == 200
        assert second.status == 304
        assert second.not_modified is True
        assert second.content == b''
        # Asked with what the first answer said, and billed nothing.
        etag = first.headers['ETag']
        assert fake.requests[1].headers['if-none-match'] == etag
        assert [seen.billed for seen in fake.requests] == [True, False]
        assert budget.standing(T1, 'core').remaining == 4_999
        assert state.validators(
            request_key('GET', '/repos/octo/one', {}, JSON),
        ) == Validators(etag, None)

    def test_a_changed_document_comes_whole_with_new_validators(
        self, fake, state,
    ):
        async def changed(github: GitHubClient) -> tuple[Answer, Answer]:
            first = await github.get('/repos/octo/one')
            fake.repos[1].stars += 1
            return first, await github.get('/repos/octo/one')

        first, second = run(fake, changed, state=state)
        assert second.status == 200
        assert second.json()['stargazers_count'] == 5_001
        assert state.validators(
            request_key('GET', '/repos/octo/one', {}, JSON),
        ) == Validators(second.headers['ETag'], None)
        assert second.headers['ETag'] != first.headers['ETag']

    def test_last_modified_is_sent_back_where_there_is_no_etag(
        self, fake, state,
    ):
        modified = 'Wed, 30 Sep 2026 00:00:00 GMT'
        fake.document(
            '/repos/octo/one/readme', {'content': 'hi'}, etag=False,
            last_modified=modified,
        )

        async def twice(github: GitHubClient) -> Answer:
            await github.get('/repos/octo/one/readme')
            return await github.get('/repos/octo/one/readme')

        assert run(fake, twice, state=state).not_modified is True
        assert fake.requests[1].headers['if-modified-since'] == modified

    def test_unconditional_is_asked_whole(self, fake, state):
        """For a caller that no longer has what it was sent: collector
        .sqlite keeps validators, never documents."""
        async def twice(github: GitHubClient) -> Answer:
            await github.get('/repos/octo/one')
            return await github.get('/repos/octo/one', conditional=False)

        assert run(fake, twice, state=state).status == 200
        assert 'if-none-match' not in fake.requests[1].headers
        assert fake.billed() == 2

    def test_validators_are_kept_by_the_request_they_answered(self):
        key = request_key(
            'GET', '/repos/octo/one/releases',
            {'per_page': '100', 'page': '2'}, JSON,
        )
        assert key == request_key(
            'GET', '/repos/octo/one/releases',
            {'page': '2', 'per_page': '100'}, JSON,
        )
        assert key != request_key(
            'GET', '/repos/octo/one/releases',
            {'page': '3', 'per_page': '100'}, JSON,
        )
        assert key != request_key(
            'GET', '/repos/octo/one/releases',
            {'per_page': '100', 'page': '2'}, 'application/vnd.github.raw',
        )

    def test_a_document_that_is_gone_takes_its_validators_with_it(
        self, fake, state,
    ):
        fake.document('/repos/octo/one/readme', {'content': 'hi'})

        async def gone(github: GitHubClient) -> None:
            await github.get('/repos/octo/one/readme')
            del fake.documents['/repos/octo/one/readme']
            await github.get('/repos/octo/one/readme')

        with pytest.raises(NotFound):
            run(fake, gone, state=state)
        assert state.validators(
            request_key('GET', '/repos/octo/one/readme', {}, JSON),
        ) is None

    def test_without_validators_to_keep_nothing_is_conditional(self, fake):
        async def twice(github: GitHubClient) -> Answer:
            await github.get('/repos/octo/one')
            return await github.get('/repos/octo/one')

        assert run(fake, twice).status == 200
        assert 'if-none-match' not in fake.requests[1].headers


class TestADeletedState:
    def test_costs_requests_not_results(self, fake, tmp_path):
        """Deleting collector.sqlite loses what saved requests: the same
        document comes again, whole, for a request of the budget."""
        path = tmp_path / STATE_FILE

        def fetch() -> Answer:
            with CollectorState.open(path) as state:
                return run(
                    fake, lambda github: github.get('/repos/octo/one'),
                    state=state,
                )

        first = fetch()
        assert fetch().not_modified is True
        assert fake.billed() == 1

        for suffix in ('', '-wal', '-shm'):
            Path(f'{path}{suffix}').unlink(missing_ok=True)
        again = fetch()

        assert again.status == 200
        assert again.json() == first.json()
        assert fake.billed() == 2


class TestGraphQL:
    def test_answers_its_data_from_the_graphql_bucket(self, fake):
        budget = budget_for(fake)
        answer = run(
            fake,
            lambda github: github.graphql(
                NODES, {'ids': [fake.repos[1].node_id]},
            ),
            budget=budget,
        )
        assert isinstance(answer, GraphQLAnswer)
        assert answer.data['nodes'][0]['databaseId'] == 1
        assert answer.errors == ()
        assert answer.bucket == 'graphql'
        assert answer.token == 'token 1'
        assert budget.standing(T1, 'graphql').remaining == 4_999
        assert budget.standing(T1, 'core').remaining is None
        sent = fake.requests[0]
        assert (sent.method, sent.path) == ('POST', '/graphql')
        assert sent.body == {
            'query': NODES, 'variables': {'ids': [fake.repos[1].node_id]},
        }

    def test_hands_back_the_answers_headers(self, fake):
        """What the bucket stood at once the query was charged, for the
        sweep's cost (#160) beside what `rateLimit` says."""
        fake.graphql_cost = 2
        answer = run(
            fake, lambda github: github.graphql(NODES, {'ids': []}, cost=2),
        )
        assert answer.headers['X-RateLimit-Remaining'] == '4998'
        assert answer.headers['x-ratelimit-used'] == '2'
        assert answer.headers['X-RateLimit-Resource'] == 'graphql'

    def test_hands_back_what_it_could_not_resolve_beside_the_rest(self, fake):
        answer = run(
            fake,
            lambda github: github.graphql(
                NODES, {'ids': [fake.repos[1].node_id, 'R_gone']},
            ),
        )
        assert answer.data['nodes'][1] is None
        assert answer.errors[0]['type'] == 'NOT_FOUND'

    def test_an_answer_with_no_data_fails(self, fake):
        fake.resolver = lambda query, variables: (
            None, [{'message': 'Parse error on "}" (RCURLY) at [1, 2]'}],
        )
        failed = failure(fake, lambda github: github.graphql('{}'))
        assert isinstance(failed, Failed)
        assert 'Parse error' in str(failed)

    def test_a_spent_bucket_backs_off_until_the_reset(self, fake):
        """Refused as GitHub refuses GraphQL, with a 200 whose error is
        RATE_LIMITED: the query goes to the other token."""
        fake.meter(ONE, 'graphql').remaining = 0
        budget = budget_for(fake, T1, T2)
        answer = run(
            fake,
            lambda github: github.graphql(NODES, {'ids': []}),
            budget=budget,
        )
        assert answer.token == 'token 2'
        assert budget.standing(T1, 'graphql').blocked_until == (
            datetime.fromtimestamp(START + 3_601, timezone.utc)
        )

    def test_holds_a_query_at_its_cost(self, fake):
        fake.graphql_cost = 3
        budget = budget_for(fake)

        async def twice(github: GitHubClient) -> None:
            await github.graphql(NODES, {'ids': []}, cost=3)
            fake.gate = asyncio.Event()
            asking = asyncio.ensure_future(
                github.graphql(NODES, {'ids': []}, cost=3),
            )
            for _ in range(100):
                if fake.flying[ONE]:
                    break
                await asyncio.sleep(0)
            assert budget.standing(T1, 'graphql').held == 3
            fake.gate.set()
            await asking

        run(fake, twice, budget=budget)
        assert budget.standing(T1, 'graphql').remaining == 4_994


class TestSearch:
    def test_searches_from_the_search_bucket(self, fake):
        budget = budget_for(fake)
        answer = run(
            fake,
            lambda github: github.search(
                'repositories', 'stars:>=1000', sort='stars', order='desc',
            ),
            budget=budget,
        )
        assert [item['id'] for item in answer.json()['items']] == [1, 2]
        assert answer.bucket == 'search'
        assert budget.standing(T1, 'search').remaining == 29
        assert fake.requests[0].query == {
            'q': 'stars:>=1000', 'sort': 'stars', 'order': 'desc',
            'per_page': '100', 'page': '1',
        }

    def test_is_never_conditional(self, fake, state):
        async def twice(github: GitHubClient) -> None:
            await github.search('repositories', 'stars:>=1000')
            await github.search('repositories', 'stars:>=1000')

        run(fake, twice, state=state)
        assert 'if-none-match' not in fake.requests[1].headers
        assert fake.billed() == 2

    def test_a_page_asked_by_its_link_is_never_conditional(self, fake, state):
        """A page of results is asked by its `Link` too. Were it asked
        conditionally, the same page a week later would come back 304,
        and collector.sqlite keeps no page to read in its place."""
        async def paging(github: GitHubClient) -> Answer:
            first = await github.search(
                'repositories', 'stars:>=1000', per_page=1,
            )
            await github.get(first.links['next'])
            return await github.get(first.links['next'])

        answer = run(fake, paging, state=state)
        assert answer.status == 200
        assert [item['id'] for item in answer.json()['items']] == [2]
        assert all(
            'if-none-match' not in seen.headers for seen in fake.requests
        )
        assert fake.billed() == 3

    def test_leaves_its_reserve(self, fake):
        budget = budget_for(fake, reserve={'search': 5})

        async def searching(github: GitHubClient) -> int:
            done = 0
            while True:
                try:
                    await github.search('repositories', 'x', wait=0)
                except RateLimited:
                    return done
                done += 1

        assert run(fake, searching, budget=budget) == 25
        assert budget.standing(T1, 'search').remaining == 5


class TestWhatGoesWrong:
    def test_a_repository_it_does_not_have_is_not_found(self, fake):
        failed = failure(fake, lambda github: github.get('/repos/octo/none'))
        assert isinstance(failed, NotFound)
        assert failed.status == 404
        assert failed.url == f'{API}/repos/octo/none'

    def test_a_renamed_repository_is_gone_and_says_where(self, fake):
        fake.rename(1, 'octo', 'uno')
        failed = failure(fake, lambda github: github.get('/repos/octo/one'))
        assert isinstance(failed, Gone)
        assert failed.status == 301
        assert failed.moved_to == f'{API}/repositories/1'

    def test_a_blocked_repository_is_gone(self, fake):
        fake.block('octo/two')
        failed = failure(fake, lambda github: github.get('/repos/octo/two'))
        assert isinstance(failed, Gone)
        assert failed.status == 451
        assert failed.moved_to is None

    def test_a_server_error_fails(self, fake):
        fake.script(Reply(502, {'message': 'Server Error'}, billed=True))
        failed = failure(fake, lambda github: github.get('/repos/octo/one'))
        assert isinstance(failed, Failed)
        assert failed.status == 502
        assert 'Server Error' in str(failed)

    def test_a_body_it_cannot_read_fails(self, fake):
        fake.script(Reply(200, b'<html>unicorn</html>', billed=True))
        answer = run(fake, lambda github: github.get('/repos/octo/one'))
        with pytest.raises(Failed) as failed:
            answer.json()
        assert 'not JSON' in str(failed.value)

    def test_a_connection_that_fails_fails(self, fake):
        fake.script(Reply(0, raises=httpx2.ConnectError('connection refused')))
        failed = failure(fake, lambda github: github.get('/repos/octo/one'))
        assert isinstance(failed, Failed)
        assert failed.status == 0
        assert 'ConnectError' in str(failed)

    def test_every_error_is_one_of_four(self):
        for error in (NotFound, Gone, RateLimited, Failed):
            assert issubclass(error, GitHubError)
        assert issubclass(Unauthorized, Failed)


class TestRefusals:
    @pytest.mark.parametrize('status', [403, 429])
    def test_a_refusal_is_asked_again_with_another_token(self, fake, status):
        fake.secondary(ONE, seconds=30, status=status)
        budget = budget_for(fake, T1, T2)
        answer = run(
            fake, lambda github: github.get('/repos/octo/one'), budget=budget,
        )
        assert answer.token == 'token 2'
        assert [(seen.token, seen.status) for seen in fake.requests] == [
            (ONE, status), (TWO, 200),
        ]
        assert budget.standing(T1, 'core').blocked_until == (
            datetime.fromtimestamp(START + 30, timezone.utc)
        )

    def test_a_refusal_with_one_token_is_waited_out(self, fake):
        fake.secondary(ONE, seconds=30, status=429)
        answer = run(fake, lambda github: github.get('/repos/octo/one'))
        assert answer.status == 200
        assert fake.clock() >= START + 30

    def test_a_spent_bucket_is_waited_out_to_its_reset(self, fake):
        """Spent by someone else: the budget heard nothing of it."""
        fake.meter(ONE, 'core').remaining = 0
        answer = run(fake, lambda github: github.get('/repos/octo/one'))
        assert answer.status == 200
        assert fake.clock() >= START + 3_600
        assert [seen.status for seen in fake.requests] == [403, 200]

    def test_a_spent_bucket_refused_with_429_is_waited_out_the_same(
        self, fake,
    ):
        """GitHub: a primary limit is a 403 or a 429, with nothing
        left."""
        reset = int(START) + 600
        fake.script(
            Reply(
                429, {'message': 'API rate limit exceeded for user ID 1.'},
                headers={
                    'X-RateLimit-Remaining': '0',
                    'X-RateLimit-Reset': str(reset),
                },
            ),
        )
        budget = budget_for(fake)
        answer = run(
            fake, lambda github: github.get('/repos/octo/one'), budget=budget,
        )
        assert answer.status == 200
        assert fake.clock() >= reset + 1
        assert [seen.status for seen in fake.requests] == [429, 200]

    def test_one_that_cannot_be_waited_out_is_rate_limited(self, fake):
        fake.secondary(ONE, seconds=30)
        failed = failure(
            fake, lambda github: github.get('/repos/octo/one', wait=0),
        )
        assert isinstance(failed, RateLimited)
        assert failed.bucket == 'core'
        assert failed.until == datetime.fromtimestamp(
            START + 30, timezone.utc,
        )

    def test_a_token_github_does_not_know_is_retired(self, fake):
        bad = Token('token 1', 'ghp_revoked_00000000000000000000000000')
        budget = budget_for(fake, bad, T2)

        async def twice(github: GitHubClient) -> list[str]:
            first = await github.get('/repos/octo/one')
            second = await github.get('/repos/octo/two')
            return [first.token, second.token]

        assert run(fake, twice, budget=budget) == ['token 2', 'token 2']
        assert [seen.status for seen in fake.requests] == [401, 200, 200]

    def test_with_no_token_github_knows_it_is_unauthorized(self, fake):
        bad = Token('token 1', 'ghp_revoked_00000000000000000000000000')
        failed = failure(
            fake, lambda github: github.get('/repos/octo/one'), bad,
        )
        assert isinstance(failed, Unauthorized)


class TestInFlight:
    def test_at_most_four_requests_per_token(self, fake):
        async def many(github: GitHubClient) -> list[str]:
            # Each token heard from first: until then, one at a time.
            await github.get('/repos/octo/one')
            await github.get('/repos/octo/one')
            fake.gate = asyncio.Event()
            asking = [
                asyncio.ensure_future(github.get('/repos/octo/two'))
                for _ in range(12)
            ]
            for _ in range(200):
                if sum(fake.flying.values()) == 8:
                    break
                await asyncio.sleep(0)
            for _ in range(20):
                await asyncio.sleep(0)
            assert fake.flying[ONE] == fake.flying[TWO] == 4
            fake.gate.set()
            return [answer.token for answer in await asyncio.gather(*asking)]

        tokens = run(fake, many, T1, T2)
        assert len(tokens) == 12
        assert fake.peak[ONE] == fake.peak[TWO] == 4

    def test_the_token_with_the_most_left_is_asked(self, fake):
        fake.meter(ONE, 'core').remaining = 100

        async def five(github: GitHubClient) -> list[str]:
            return [
                (await github.get('/repos/octo/one')).token for _ in range(5)
            ]

        assert run(fake, five, T1, T2) == [
            'token 1', 'token 2', 'token 2', 'token 2', 'token 2',
        ]


class TestTokensStaySecret:
    def test_no_token_is_in_a_log_line_or_an_error(
        self, fake, tmp_path, capsys, monkeypatch,
    ):
        """Whatever happens, in either log format, and in what
        collector.sqlite keeps."""
        errors: list[BaseException] = []

        async def everything(github: GitHubClient) -> None:
            await github.get('/repos/octo/one')
            # The second token, heard from: a 304 where validators are
            # kept.
            await github.get('/repos/octo/one')
            await github.graphql(NODES, {'ids': ['R_gone']})
            await github.search('repositories', 'stars:>=1000')
            # Refused, and asked again with the other token.
            fake.secondary(TWO, seconds=30)
            await github.get('/repos/octo/two')
            fake.rename(1, 'octo', 'uno')
            fake.block('octo/two')
            fake.script(Reply(502, {'message': f'Bad Gateway {ONE}'}))
            fake.script(
                Reply(
                    0, raises=httpx2.LocalProtocolError(
                        f"Illegal header value b'Bearer {TWO}\\r'",
                    ),
                ),
            )
            fake.script(
                Reply(
                    0, raises=httpx2.ConnectError(
                        f'refused, Authorization: token {ONE}',
                    ),
                ),
            )
            for path in (
                '/repos/octo/two', '/repos/octo/one', '/repos/octo/two',
                '/repos/octo/two', '/repos/octo/one', '/repos/octo/none',
                '/repos/octo/uno',
            ):
                try:
                    await github.get(path, wait=0)
                except GitHubError as error:
                    errors.append(error)
            # Refused on both tokens, where it cannot wait.
            fake.secondary(ONE, 'search', seconds=30, status=429)
            fake.secondary(TWO, 'search', seconds=30, status=429)
            try:
                await github.search('repositories', 'stars:>=1000', wait=0)
            except GitHubError as error:
                errors.append(error)

        for log_format in ('console', 'json'):
            monkeypatch.setenv('CHATSBOM_LOG_FORMAT', log_format)
            setup_logging('DEBUG')
            fake.requests.clear()
            with CollectorState.open(tmp_path / log_format / STATE_FILE) as kept:
                run(fake, everything, T1, T2, state=kept)
            fake.renamed.clear()
            fake.blocked.clear()
            fake.repos[1].owner, fake.repos[1].name = 'octo', 'one'
            fake.clock.advance(60)
        monkeypatch.delenv('CHATSBOM_LOG_FORMAT')
        setup_logging('INFO')

        captured = capsys.readouterr()
        logged = captured.out + captured.err
        assert 'GitHub answered' in logged
        assert len({type(error) for error in errors}) >= 4
        stored = b''.join(
            path.read_bytes() for path in tmp_path.rglob('*') if path.is_file()
        )
        for secret in (ONE, TWO):
            assert secret not in logged
            assert secret.encode() not in stored
            for error in errors:
                shown = ' '.join([
                    str(error), repr(error), repr(error.args),
                    json.dumps(vars(error), default=str),
                ])
                assert secret not in shown
