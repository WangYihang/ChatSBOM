"""The sweep (#160; #128 section 2.1): every hour, each repository of the
universe asked after by its node id, 100 a GraphQL `nodes(ids:)` call,
against the stand-in (tests/fake_github_test.py).

- Each answer is recorded with `CollectorState.observe`, which gives
  back the observation before it: a push, a HEAD or a latest release
  other than before marks the repository changed, which 6c reads.
- A rename costs nothing, since the node id stays; a node that comes
  back null is gone until the next universe.
- A refusal backs off without the sweep losing its place, and a sweep
  cut short goes on where it was, after a restart too.
- What each sweep cost is logged, as `rateLimit { cost }` says it and as
  the headers do, and a disagreement is said.

The stand-in's clock is the budget's: an hour's wait takes no time.
"""
import asyncio
from collections.abc import Iterator
from collections.abc import Mapping
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest
import structlog

from chatsbom.collector.budget import BudgetManager
from chatsbom.collector.client import GitHubClient
from chatsbom.collector.errors import RateLimited
from chatsbom.collector.retry import ATTEMPTS
from chatsbom.collector.retry import PAUSE
from chatsbom.collector.state import CollectorState
from chatsbom.collector.state import Member
from chatsbom.collector.state import Observed
from chatsbom.collector.state import STATE_FILE
from chatsbom.collector.state import Sweep
from chatsbom.collector.state import UniverseSnapshot
from chatsbom.collector.sweep import NODES_PER_CALL
from chatsbom.collector.sweep import QUERY
from chatsbom.collector.sweep import Sweeper
from chatsbom.collector.tokens import Token
from chatsbom.collector.universe import Universe
from tests.fake_github_test import FakeClock
from tests.fake_github_test import FakeGitHub
from tests.fake_github_test import Release
from tests.fake_github_test import Reply
from tests.fake_github_test import Repo
from tests.fake_github_test import SECONDARY
from tests.fake_github_test import START

ONE = 'ghp_sweep_token_one_000000000000000000000'
T1 = Token('token 1', ONE)

NOW = datetime.fromtimestamp(START, timezone.utc)
HOUR = timedelta(hours=1)
GRAPHQL = '/graphql'


@pytest.fixture
def fake() -> FakeGitHub:
    fake = FakeGitHub(FakeClock())
    fake.token(ONE, 'alice')
    return fake


@pytest.fixture
def path(tmp_path: Path) -> Path:
    return tmp_path / 'data' / STATE_FILE


@pytest.fixture
def state(path: Path) -> Iterator[CollectorState]:
    with CollectorState.open(path) as state:
        yield state


def universe(
    fake: FakeGitHub, state: CollectorState, count: int, *, first: int = 1,
) -> list[Repo]:
    """`count` repositories, the universe, as a search found them."""
    made = [
        fake.add(Repo(number, 'octo', f'r{number}', stars=1_000 + number))
        for number in range(first, first + count)
    ]
    state.keep_universe(
        UniverseSnapshot('all-2026-09-20', 'stamp', len(made), NOW),
        [Member(repo.id, repo.node_id) for repo in made],
    )
    return made


def budget_for(fake: FakeGitHub) -> BudgetManager:
    return BudgetManager(
        (T1,), reserve={}, clock=fake.clock, sleep=fake.clock.sleep,
    )


def sweep(
    fake: FakeGitHub, state: CollectorState, *, wait: float | None = None,
    budget: BudgetManager | None = None,
) -> Sweep | None:
    async def sweeping() -> Sweep | None:
        async with GitHubClient(
            budget or budget_for(fake), transport=fake.transport(),
        ) as github:
            sweeper = Sweeper(github, state, sleep=fake.clock.sleep)
            return await sweeper.run(wait=wait)

    return asyncio.run(sweeping())


def asked(fake: FakeGitHub) -> list[list[str]]:
    """The node ids of each GraphQL call, in order."""
    return [seen.body['variables']['ids'] for seen in fake.seen(GRAPHQL)]


def nodes(fake: FakeGitHub, *numbers: int) -> list[str]:
    return [fake.repos[number].node_id for number in numbers]


def resolving(
    fake: FakeGitHub, **instead: tuple[Any, dict[str, Any] | None],
) -> None:
    """GraphQL answers as the stand-in gives them, but for the node ids
    in `instead`, each answered with its node and its error."""
    def resolver(
        query: str, variables: Mapping[str, Any],
    ) -> tuple[Any, list[Any]]:
        by_node = {repo.node_id: repo for repo in fake.repos.values()}
        found, errors = [], []
        for index, node_id in enumerate(variables['ids']):
            if node_id in instead:
                node, error = instead[node_id]
                found.append(node)
                if error is not None:
                    errors.append({**error, 'path': ['nodes', index]})
            else:
                found.append(by_node[node_id].node())
        return {'nodes': found}, errors

    fake.resolver = resolver


class TestTheSweep:
    def test_asks_100_node_ids_a_call_each_once_in_order(self, fake, state):
        made = universe(fake, state, 250)

        swept = sweep(fake, state)

        ids = [repo.node_id for repo in made]
        assert NODES_PER_CALL == 100
        assert asked(fake) == [ids[:100], ids[100:200], ids[200:]]
        assert swept is not None
        assert (swept.calls, swept.nodes, swept.position) == (3, 250, 250)
        assert swept.finished_at is not None
        assert {(seen.method, seen.path) for seen in fake.requests} == {
            ('POST', GRAPHQL),
        }
        assert all(seen.body['query'] == QUERY for seen in fake.requests)

    def test_asks_for_what_the_change_detector_reads(self):
        for field in (
            'databaseId', 'nameWithOwner', 'stargazerCount', 'isArchived',
            'pushedAt', 'defaultBranchRef', 'oid', 'latestRelease',
            'tagName', 'publishedAt', 'rateLimit', 'cost', 'nodes(ids: $ids)',
        ):
            assert field in QUERY

    def test_records_what_it_reads_of_each_repository(self, fake, state):
        universe(fake, state, 2)
        one = fake.repos[1]
        one.archived = True
        one.pushed_at = '2026-09-20T10:00:00Z'
        one.default_branch = 'trunk'
        one.head = 'b' * 40
        one.releases = [
            Release('v2.0', '2026-09-10T00:00:00Z'), Release('v1.0'),
        ]

        sweep(fake, state)

        assert state.observed(1) == Observed(
            repository_id=1, node_id=one.node_id, full_name='octo/r1',
            stars=1_001, archived=True,
            pushed_at=datetime(2026, 9, 20, 10, tzinfo=timezone.utc),
            default_branch='trunk', head='b' * 40, release_tag='v2.0',
            release_at=datetime(2026, 9, 10, tzinfo=timezone.utc),
            observed_at=NOW,
        )
        two = state.observed(2)
        assert two is not None
        assert (two.release_tag, two.release_at) == (None, None)
        assert two.head == 'a' * 40

    def test_an_empty_repository_has_no_push_head_or_release(
        self, fake, state,
    ):
        universe(fake, state, 1)
        empty = {
            **fake.repos[1].node(), 'pushedAt': None,
            'defaultBranchRef': None, 'latestRelease': None,
        }
        resolving(fake, **{fake.repos[1].node_id: (empty, None)})

        swept = sweep(fake, state)

        assert swept is not None and swept.nodes == 1
        observed = state.observed(1)
        assert observed is not None
        assert (
            observed.pushed_at, observed.default_branch, observed.head,
            observed.release_tag,
        ) == (None, None, None, None)


class TestWhatItFinds:
    def test_a_push_a_head_or_a_latest_release_is_a_change(self, fake, state):
        """And stars, or being archived, are recorded, and change nothing
        6c collects."""
        universe(fake, state, 6)
        first = sweep(fake, state)
        assert first is not None and first.changed == 0
        for observed in state.observations():
            state.mark_collected(
                observed.repository_id, as_of=observed.observed_at,
            )
        fake.repos[1].pushed_at = '2026-09-21T00:00:00Z'
        fake.repos[2].head = 'c' * 40
        fake.repos[3].releases = [Release('v3.0', '2026-09-21T00:00:00Z')]
        fake.repos[4].stars += 10
        fake.repos[5].archived = True
        fake.clock.advance(HOUR.total_seconds())

        second = sweep(fake, state)

        assert second is not None and second.changed == 3
        assert [observed.repository_id for observed in state.changed()] == [
            1, 2, 3,
        ]
        four = state.observed(4)
        five = state.observed(5)
        assert four is not None and four.stars == 1_014
        assert five is not None and five.archived is True
        assert four.observed_at == NOW + HOUR

    def test_what_was_never_collected_is_what_it_observed(self, fake, state):
        universe(fake, state, 3)
        sweep(fake, state)
        assert [
            observed.repository_id for observed in state.never_collected()
        ] == [3, 2, 1]

    def test_a_rename_costs_nothing(self, fake, state):
        universe(fake, state, 2)
        sweep(fake, state)
        fake.rename(1, 'octo', 'uno')
        fake.clock.advance(HOUR.total_seconds())
        fake.requests.clear()

        with structlog.testing.capture_logs() as logged:
            swept = sweep(fake, state)

        assert swept is not None
        assert (swept.renamed, swept.changed, swept.calls) == (1, 0, 1)
        observed = state.observed(1)
        assert observed is not None and observed.full_name == 'octo/uno'
        assert [(seen.method, seen.path) for seen in fake.requests] == [
            ('POST', GRAPHQL),
        ]
        said = [event for event in logged if event.get('renamed_to')]
        assert [(e['renamed_from'], e['renamed_to']) for e in said] == [
            ('octo/r1', 'octo/uno'),
        ]

    def test_a_null_node_is_gone_until_the_next_universe(self, fake, state):
        made = universe(fake, state, 3)
        deleted = fake.repos.pop(2)

        first = sweep(fake, state)

        assert first is not None
        assert (first.gone, first.nodes) == (1, 2)
        assert [member.repository_id for member in state.members()] == [1, 3]
        assert [
            observed.repository_id for observed in state.never_collected()
        ] == [3, 1]
        fake.clock.advance(HOUR.total_seconds())
        second = sweep(fake, state)
        assert second is not None and second.gone == 0
        assert asked(fake)[-1] == nodes(fake, 1, 3)

        # Public again, and in the next universe's search.
        fake.add(deleted)
        state.keep_universe(
            UniverseSnapshot('all-2026-09-27', 'stamp', 3, NOW),
            [Member(repo.id, repo.node_id) for repo in made],
        )
        fake.clock.advance(HOUR.total_seconds())
        third = sweep(fake, state)
        assert third is not None and third.nodes == 3
        assert asked(fake)[-1] == nodes(fake, 1, 2, 3)
        assert state.observed(2) is not None

    @pytest.mark.parametrize('kind', ['NOT_FOUND', 'FORBIDDEN'])
    def test_a_node_github_cannot_show_is_gone(self, fake, state, kind):
        """Deleted or made private: NOT_FOUND. Blocked, or behind an
        organisation's SAML: FORBIDDEN, which a live token is to show."""
        universe(fake, state, 2)
        resolving(
            fake, **{
                fake.repos[2].node_id: (
                    None, {'type': kind, 'message': 'It cannot be shown.'},
                ),
            },
        )

        swept = sweep(fake, state)

        assert swept is not None and (swept.gone, swept.unresolved) == (1, 0)
        assert [member.repository_id for member in state.members()] == [1]

    def test_a_node_github_failed_to_resolve_is_asked_after_again(
        self, fake, state,
    ):
        """Null for another reason GitHub gave, as a timeout: not gone."""
        universe(fake, state, 2)
        resolving(
            fake, **{
                fake.repos[2].node_id: (
                    None, {
                        'type': 'SERVICE_UNAVAILABLE',
                        'message': 'Something went wrong.',
                    },
                ),
            },
        )

        swept = sweep(fake, state)

        assert swept is not None
        assert (swept.gone, swept.unresolved, swept.nodes) == (0, 1, 1)
        assert [member.repository_id for member in state.members()] == [1, 2]
        fake.resolver = None
        fake.clock.advance(HOUR.total_seconds())
        again = sweep(fake, state)
        assert again is not None and again.nodes == 2

    def test_a_node_that_is_not_the_repository_asked_after_is_unresolved(
        self, fake, state,
    ):
        universe(fake, state, 2)
        other = {**fake.repos[2].node(), 'databaseId': 99}
        resolving(fake, **{fake.repos[2].node_id: (other, None)})

        swept = sweep(fake, state)

        assert swept is not None and (swept.unresolved, swept.nodes) == (1, 1)
        assert state.observed(99) is None and state.observed(2) is None


class TestItsPlace:
    @pytest.mark.parametrize('refusal', ['primary', 'secondary'])
    def test_a_refusal_backs_off_and_the_sweep_goes_on_where_it_was(
        self, fake, state, refusal,
    ):
        universe(fake, state, 250)
        reset = int(START) + 3_600
        if refusal == 'primary':
            fake.script(
                Reply(
                    200, {
                        'errors': [{
                            'type': 'RATE_LIMITED',
                            'message': 'API rate limit exceeded',
                        }],
                    }, headers={
                        'X-RateLimit-Remaining': '0',
                        'X-RateLimit-Reset': str(reset),
                    },
                ),
                path=GRAPHQL, after=1,
            )
        else:
            fake.script(
                Reply(
                    403, {'message': SECONDARY},
                    headers={'Retry-After': '120'},
                ),
                path=GRAPHQL, after=1,
            )

        swept = sweep(fake, state)

        ids = nodes(fake, *range(1, 251))
        assert asked(fake) == [
            ids[:100], ids[100:200],
            ids[100:200], ids[200:],
        ]
        assert swept is not None
        assert (swept.calls, swept.nodes, swept.failed) == (3, 250, 0)
        waited = reset + 1 if refusal == 'primary' else START + 120
        assert fake.clock() >= waited

    def test_a_sweep_cut_short_goes_on_where_it_was_after_a_restart(
        self, fake, path,
    ):
        with CollectorState.open(path) as state:
            universe(fake, state, 250)
            fake.script(
                Reply(
                    403, {'message': SECONDARY},
                    headers={'Retry-After': '120'},
                ),
                path=GRAPHQL, after=1,
            )
            with pytest.raises(RateLimited):
                sweep(fake, state, wait=0)
            cut = state.latest_sweep()
            assert cut is not None and cut.finished_at is None
            assert (cut.position, cut.calls, cut.nodes) == (100, 1, 100)

        fake.clock.advance(120)
        with CollectorState.open(path) as state:
            swept = sweep(fake, state)
            assert swept is not None
            assert swept.sweep_id == cut.sweep_id
            assert (swept.calls, swept.nodes, swept.position) == (3, 250, 250)
            assert swept.finished_at is not None
            assert len(list(state.observations())) == 250
        ids = nodes(fake, *range(1, 251))
        assert asked(fake) == [
            ids[:100], ids[100:200],
            ids[100:200], ids[200:],
        ]

    def test_a_sweep_cancelled_keeps_what_it_had_swept(self, fake, state):
        """As the collector stopping mid-sweep cancels it."""
        universe(fake, state, 250)

        async def held() -> None:
            for _ in range(1_000):
                if fake.flying[ONE]:
                    return
                await asyncio.sleep(0)
            raise AssertionError('no call came')

        async def cancelling() -> None:
            gate = fake.gate = asyncio.Event()
            async with GitHubClient(
                budget_for(fake), transport=fake.transport(),
            ) as github:
                sweeper = Sweeper(github, state, sleep=fake.clock.sleep)
                sweeping = asyncio.ensure_future(sweeper.run())
                await held()
                # The first call through, and the gate shut behind it.
                gate.set()
                gate.clear()
                for _ in range(1_000):
                    swept = state.latest_sweep()
                    if swept is not None and swept.calls == 1:
                        break
                    await asyncio.sleep(0)
                await held()
                sweeping.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await sweeping

        asyncio.run(cancelling())
        fake.gate = None

        cut = state.latest_sweep()
        assert cut is not None and cut.finished_at is None
        assert cut.position == 100
        swept = sweep(fake, state)
        assert swept is not None and swept.nodes == 250

    def test_a_call_that_fails_is_asked_again_after_a_pause(
        self, fake, state,
    ):
        universe(fake, state, 250)
        fake.script(
            Reply(502, {'message': 'Server Error'}), path=GRAPHQL, after=1,
        )

        swept = sweep(fake, state)

        assert swept is not None
        assert (swept.nodes, swept.failed) == (250, 0)
        assert len(asked(fake)) == 4
        assert fake.clock() >= START + PAUSE

    def test_one_that_fails_every_time_is_skipped_until_the_next_sweep(
        self, fake, state,
    ):
        universe(fake, state, 250)
        fake.script(
            Reply(502, {'message': 'Server Error'}), path=GRAPHQL, after=1,
            times=ATTEMPTS,
        )

        swept = sweep(fake, state)

        assert swept is not None
        assert (swept.nodes, swept.failed, swept.calls) == (150, 1, 2)
        assert swept.position == 250 and swept.finished_at is not None
        assert state.observed(150) is None
        assert [member.repository_id for member in state.members()] == list(
            range(1, 251),
        )
        fake.clock.advance(HOUR.total_seconds())
        again = sweep(fake, state)
        assert again is not None and again.nodes == 250
        assert state.observed(150) is not None


class TestItsCost:
    def test_is_logged_as_rate_limit_and_the_headers_say_it(
        self, fake, state,
    ):
        fake.graphql_cost = 2
        universe(fake, state, 250)

        with structlog.testing.capture_logs() as logged:
            swept = sweep(fake, state)

        assert swept is not None and swept.cost == 6
        [said] = [
            event for event in logged
            if event['event'] == 'The universe was swept'
        ]
        assert said['sweep'] == swept.sweep_id
        assert (said['calls'], said['cost'], said['nodes']) == (3, 6, 250)
        # The headers measure what a call cost from the call before it.
        assert (said['spent'], said['measured']) == (4, 4)
        assert (said['remaining'], said['limit']) == (4_994, 5_000)
        assert said['resets_at'] == '2026-09-21T15:13:20Z'
        assert not [
            event for event in logged if event['log_level'] == 'warning'
        ]

    def test_says_so_when_rate_limit_and_the_headers_disagree(
        self, fake, state,
    ):
        """What #128 asks to be measured on a live token before the
        collector relies on it."""
        fake.graphql_cost = 3
        universe(fake, state, 250)

        def said_cost_1(query: str, variables: Mapping[str, Any]) -> Any:
            by_node = {repo.node_id: repo for repo in fake.repos.values()}
            return {
                'nodes': [by_node[node].node() for node in variables['ids']],
                'rateLimit': {'cost': 1, 'remaining': 1, 'resetAt': ''},
            }, []

        fake.resolver = said_cost_1

        with structlog.testing.capture_logs() as logged:
            swept = sweep(fake, state)

        assert swept is not None and swept.cost == 3
        [warned] = [
            event for event in logged if event['log_level'] == 'warning'
        ]
        assert 'disagree' in warned['event']
        assert (warned['cost'], warned['spent']) == (2, 6)

    def test_a_call_is_held_at_what_the_last_one_cost(self, fake, state):
        """The budget holds a query at its cost while it is in flight."""
        fake.graphql_cost = 5
        universe(fake, state, 150)
        budget = budget_for(fake)
        held: list[int] = []

        def resolver(query: str, variables: Mapping[str, Any]) -> Any:
            held.append(budget.standing(T1, 'graphql').held)
            by_node = {repo.node_id: repo for repo in fake.repos.values()}
            return {
                'nodes': [by_node[node].node() for node in variables['ids']],
            }, []

        fake.resolver = resolver

        swept = sweep(fake, state, budget=budget)

        assert swept is not None and swept.cost == 10
        assert held == [1, 5]


class TestWhenItIsDue:
    def test_hourly_from_the_last_ones_start(self, fake, state):
        async def asking() -> list[bool]:
            async with GitHubClient(
                budget_for(fake), transport=fake.transport(),
            ) as github:
                sweeper = Sweeper(github, state, sleep=fake.clock.sleep)
                due = [sweeper.due(HOUR)]
                universe(fake, state, 2)
                due.append(sweeper.due(HOUR))
                fake.clock.advance(60)
                await sweeper.run()
                due.append(sweeper.due(HOUR))
                fake.clock.advance(HOUR.total_seconds() - 1)
                due.append(sweeper.due(HOUR))
                fake.clock.advance(1)
                due.append(sweeper.due(HOUR))
                return due

        # No universe to sweep, then none swept, then an hour from the
        # start of the last.
        assert asyncio.run(asking()) == [False, True, False, False, True]

    def test_one_that_did_not_finish_is_due_at_once(self, fake, state):
        universe(fake, state, 2)
        begun = state.begin_sweep(NOW)
        state.keep_sweep(replace(begun, position=1, calls=1, nodes=1))

        async def asking() -> bool:
            async with GitHubClient(
                budget_for(fake), transport=fake.transport(),
            ) as github:
                return Sweeper(github, state).due(HOUR)

        assert asyncio.run(asking()) is True
        swept = sweep(fake, state)
        assert swept is not None and swept.sweep_id == begun.sweep_id
        assert asked(fake) == [nodes(fake, 2)]

    def test_with_no_universe_nothing_is_swept(self, fake, state):
        assert sweep(fake, state) is None
        assert state.latest_sweep() is None
        assert fake.requests == []


def test_sweeps_the_universe_the_search_found(fake, state, tmp_path):
    for number in range(1, 121):
        fake.add(Repo(number, 'octo', f'r{number}', stars=1_000 + number))

    async def refreshing_and_sweeping() -> Sweep | None:
        async with GitHubClient(
            budget_for(fake), transport=fake.transport(),
        ) as github:
            await Universe(
                github, state, tmp_path / 'data' / '01-github-search',
                sleep=fake.clock.sleep,
            ).refresh()
            return await Sweeper(github, state, sleep=fake.clock.sleep).run()

    swept = asyncio.run(refreshing_and_sweeping())

    assert swept is not None and swept.nodes == 120
    assert asked(fake) == [
        nodes(fake, *range(1, 101)), nodes(fake, *range(101, 121)),
    ]
