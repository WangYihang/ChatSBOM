"""The budget manager: every token's buckets, as GitHub's answers say
they stand (#156, #128 section 2.1).

A request takes a lease on a bucket before it is sent. The lease names
the token with the most left in that bucket, holds what the request may
cost while it is in flight, and is closed by the answer, whose
`X-RateLimit-*` headers say where the bucket stands: never `GET
/rate_limit`, which misreported (TODO.md). Each bucket keeps a reserve
for manual work. A refusal, 403 or 429, backs the token's bucket off:
to its reset for a primary limit, by `Retry-After` for a secondary one.
About four requests are in flight per token, and there is no split
between tokens.

The clock is the stand-in's (tests/fake_github_test.py): a wait for a
reset an hour away takes no time.
"""
import asyncio
from datetime import datetime
from datetime import timezone

import pytest
import structlog

from chatsbom.collector.budget import BudgetManager
from chatsbom.collector.budget import IN_FLIGHT
from chatsbom.collector.budget import Lease
from chatsbom.collector.budget import Standing
from chatsbom.collector.errors import RateLimited
from chatsbom.collector.errors import Unauthorized
from chatsbom.collector.tokens import Token
from tests.fake_github_test import FakeClock
from tests.fake_github_test import START

A = Token('token 1', 'ghp_' + 'a' * 36)
B = Token('token 2', 'ghp_' + 'b' * 36)

RESET = START + 3_600


def at(seconds: float) -> datetime:
    return datetime.fromtimestamp(seconds, timezone.utc)


def said(
    remaining: int, *, limit: int = 5_000, reset: float = RESET,
    resource: str = 'core', retry_after: int | None = None,
) -> dict[str, str]:
    """The headers an answer carries."""
    headers = {
        'X-RateLimit-Limit': str(limit),
        'X-RateLimit-Remaining': str(remaining),
        'X-RateLimit-Used': str(limit - remaining),
        'X-RateLimit-Reset': str(int(reset)),
        'X-RateLimit-Resource': resource,
    }
    if retry_after is not None:
        headers['Retry-After'] = str(retry_after)
    return headers


@pytest.fixture
def clock() -> FakeClock:
    return FakeClock()


def manager(
    clock: FakeClock, *tokens: Token, reserve: dict[str, int] | None = None,
) -> BudgetManager:
    return BudgetManager(
        tokens or (A,), reserve=reserve or {}, clock=clock, sleep=clock.sleep,
    )


def taken(budget: BudgetManager, bucket: str = 'core', cost: int = 1) -> Lease:
    lease = budget.try_lease(bucket, cost=cost)
    assert lease is not None
    return lease


def warm(
    budget: BudgetManager, token: Token, remaining: int,
    bucket: str = 'core', **more: int,
) -> None:
    """`token`'s `bucket` heard from: `remaining` left."""
    lease = taken(budget, bucket)
    assert lease.token == token
    lease.answered(said(remaining, resource=bucket, **more))


class TestTheHeaders:
    def test_a_bucket_stands_where_the_last_answer_said(self, clock):
        budget = manager(clock)
        lease = taken(budget)
        assert lease.token == A
        assert budget.standing(A, 'core').held == 1
        lease.answered(said(4_990))
        assert budget.standing(A, 'core') == Standing(
            token='token 1', bucket='core', limit=5_000, remaining=4_990,
            reset=at(RESET), held=0, blocked_until=None,
        )

    def test_a_bucket_not_heard_from_stands_nowhere(self, clock):
        assert manager(clock).standing(A, 'graphql') == Standing(
            token='token 1', bucket='graphql', limit=None, remaining=None,
            reset=None, held=0, blocked_until=None,
        )

    def test_an_answer_counts_in_the_bucket_it_names(self, clock):
        """GitHub's `X-RateLimit-Resource`, whatever the request was
        taken as."""
        budget = manager(clock)
        taken(budget).answered(said(95, limit=100, resource='dependency_sbom'))
        assert budget.standing(A, 'dependency_sbom').remaining == 95
        assert budget.standing(A, 'core').remaining is None
        assert budget.standing(A, 'core').held == 0

    def test_a_late_answer_does_not_raise_what_is_left(self, clock):
        """Answers come back in any order; in one window what is left
        only falls."""
        budget = manager(clock)
        warm(budget, A, 4_999)
        early, late = taken(budget), taken(budget)
        late.answered(said(4_980))
        early.answered(said(4_990))
        assert budget.standing(A, 'core').remaining == 4_980

    def test_a_new_window_replaces_the_last(self, clock):
        budget = manager(clock)
        warm(budget, A, 10)
        clock.advance(3_600)
        taken(budget).answered(said(4_999, reset=RESET + 3_600))
        assert budget.standing(A, 'core').remaining == 4_999
        assert budget.standing(A, 'core').reset == at(RESET + 3_600)

    def test_an_answer_from_the_window_before_is_ignored(self, clock):
        budget = manager(clock)
        warm(budget, A, 4_999, reset=RESET + 3_600)
        taken(budget).answered(said(10))
        assert budget.standing(A, 'core').remaining == 4_999

    def test_an_answer_that_says_nothing_costs_one_and_a_304_none(self, clock):
        budget = manager(clock)
        warm(budget, A, 100)
        taken(budget).answered({})
        assert budget.standing(A, 'core').remaining == 99
        taken(budget).answered({}, free=True)
        assert budget.standing(A, 'core').remaining == 99

    def test_a_spent_bucket_is_whole_again_at_its_reset(self, clock):
        budget = manager(clock)
        warm(budget, A, 0)
        assert budget.try_lease('core') is None
        clock.advance(3_600)
        assert taken(budget).token == A


class TestABucketNoAnswerNames:
    """A request is taken from the bucket its caller names, which may be a
    guess: the dependency graph's, before a live token has said its name
    (#162). GitHub's answers name the bucket it drew from. A bucket no
    answer has named follows, for the token, the one its requests'
    answers name: what is taken from it is taken from that one, counted,
    reserved and backed off with it, until an answer names it."""

    def test_follows_the_one_its_answers_name(self, clock):
        budget = manager(clock)
        first = taken(budget, 'dependency_sbom')
        assert first.bucket == 'dependency_sbom'
        first.answered(said(4_000))
        # Taken from `core`, and so no longer asked one at a time.
        again = taken(budget, 'dependency_sbom')
        more = taken(budget, 'dependency_sbom')
        assert again.bucket == more.bucket == 'core'
        assert budget.standing(A, 'core').held == 2
        assert budget.standing(A, 'dependency_sbom').held == 0

    def test_keeps_the_reserve_of_the_one_it_follows(self, clock):
        budget = manager(clock, reserve={'core': 500})
        taken(budget, 'dependency_sbom').answered(said(502))
        taken(budget, 'dependency_sbom')
        taken(budget, 'dependency_sbom')
        assert budget.try_lease('dependency_sbom') is None

    def test_backs_off_with_the_one_it_follows(self, clock):
        budget = manager(clock)
        taken(budget, 'dependency_sbom').answered(said(4_000))
        taken(budget).refused(said(4_000, retry_after=30))
        assert budget.try_lease('dependency_sbom') is None
        clock.advance(30)
        assert budget.try_lease('dependency_sbom') is not None

    def test_a_lease_on_it_waits_for_the_one_it_follows(self, clock):
        """Spent, the one it follows has room again at its reset, and a
        lease taken as the one that follows waits until then; there is
        nothing else to wait for."""
        budget = manager(clock)
        taken(budget, 'dependency_sbom').answered(
            said(0, reset=START + 600),
        )
        lease = asyncio.run(budget.lease('dependency_sbom', wait=1_000))
        assert lease.bucket == 'core'
        assert clock() >= START + 600

    def test_for_the_token_whose_answers_said_so(self, clock):
        budget = manager(clock, A, B)
        first = taken(budget, 'dependency_sbom')
        assert first.token == A
        first.answered(said(4_000))
        # B's is its own, and asked once before anything else is.
        second = taken(budget, 'dependency_sbom')
        assert (second.token, second.bucket) == (B, 'dependency_sbom')
        third = taken(budget, 'dependency_sbom')
        assert (third.token, third.bucket) == (A, 'core')

    def test_is_said_once(self, clock):
        budget = manager(clock, A, B)
        with structlog.testing.capture_logs() as logs:
            for token in (A, B):
                lease = taken(budget, 'dependency_sbom')
                assert lease.token == token
                lease.answered(said(4_000))
        following = [log for log in logs if 'follows' in log['event']]
        assert len(following) == 1
        assert (following[0]['taken'], following[0]['drawn']) == (
            'dependency_sbom', 'core',
        )

    def test_until_an_answer_names_it(self, clock):
        budget = manager(clock)
        taken(budget, 'dependency_sbom').answered(said(4_000))
        assert taken(budget, 'dependency_sbom').bucket == 'core'
        # Named after all, by another request's answer: its own bucket.
        taken(budget, 'graphql').answered(
            said(150, limit=200, resource='dependency_sbom'),
        )
        lease = taken(budget, 'dependency_sbom')
        assert lease.bucket == 'dependency_sbom'
        assert budget.standing(A, 'dependency_sbom').remaining == 150

    def test_a_bucket_an_answer_has_named_follows_none(self, clock):
        """`core`, named by its first answer, is never taken as another
        bucket: a caller that names the wrong one for a request moves
        nothing else."""
        budget = manager(clock)
        warm(budget, A, 4_999)
        taken(budget).answered(said(95, limit=100, resource='dependency_sbom'))
        assert taken(budget).bucket == 'core'
        assert budget.standing(A, 'core').remaining == 4_999


class TestTheReserve:
    def test_is_left_untouched(self, clock):
        budget = manager(clock, reserve={'core': 500})
        warm(budget, A, 503)
        assert all(budget.try_lease('core') for _ in range(3))
        assert budget.try_lease('core') is None

    def test_a_lease_that_cannot_wait_for_the_reset_is_rate_limited(
        self, clock,
    ):
        budget = manager(clock, reserve={'core': 500})
        warm(budget, A, 500)
        with pytest.raises(RateLimited) as refused:
            asyncio.run(budget.lease('core', wait=0))
        assert refused.value.bucket == 'core'
        assert refused.value.until == at(RESET)
        assert A.secret not in str(refused.value)

    def test_a_lease_that_can_wait_is_given_at_the_reset(self, clock):
        budget = manager(clock, reserve={'core': 500})
        warm(budget, A, 500)
        lease = asyncio.run(budget.lease('core'))
        assert lease.token == A
        assert clock() >= RESET

    def test_is_per_bucket(self, clock):
        budget = manager(clock, reserve={'core': 500, 'search': 5})
        warm(budget, A, 4_000)
        warm(budget, A, 6, 'search', limit=30, reset=START + 60)
        assert budget.try_lease('search') is not None
        assert budget.try_lease('search') is None
        assert budget.try_lease('core') is not None

    def test_one_that_leaves_nothing_is_said_once(self, clock):
        """Or the collector would wait for the bucket for ever, and say
        nothing."""
        budget = manager(clock, A, B, reserve={'search': 30})
        with structlog.testing.capture_logs() as logged:
            warm(budget, A, 29, 'search', limit=30, reset=START + 60)
            warm(budget, B, 30, 'search', limit=30, reset=START + 60)
        said = [
            event for event in logged
            if event['event'].startswith('The reserve is the whole bucket')
        ]
        assert [(e['bucket'], e['limit'], e['reserve']) for e in said] == [
            ('search', 30, 30),
        ]
        assert budget.try_lease('search') is None

    def test_a_bucket_without_one_is_spent_to_the_last(self, clock):
        budget = manager(clock, reserve={'core': 500})
        warm(budget, A, 2, 'graphql')
        assert budget.try_lease('graphql') is not None
        assert budget.try_lease('graphql') is not None
        assert budget.try_lease('graphql') is None


class TestBackingOff:
    def test_a_primary_limit_backs_off_until_the_reset(self, clock):
        budget = manager(clock)
        backoff = taken(budget).refused(said(0, reset=START + 600))
        assert backoff.primary is True
        # A second past it: GitHub's clock and this one may differ.
        assert backoff.until == at(START + 601)
        assert budget.standing(A, 'core').blocked_until == at(START + 601)
        assert budget.try_lease('core') is None
        clock.advance(600)
        assert budget.try_lease('core') is None
        clock.advance(1)
        assert budget.try_lease('core') is not None

    def test_a_secondary_limit_backs_off_by_retry_after(self, clock):
        budget = manager(clock)
        backoff = taken(budget).refused(said(4_000, retry_after=30))
        assert backoff.primary is False
        assert backoff.until == at(START + 30)
        clock.advance(29)
        assert budget.try_lease('core') is None
        clock.advance(1)
        assert budget.try_lease('core') is not None

    def test_retry_after_comes_before_the_reset(self, clock):
        """GitHub's order: `Retry-After` if it is there, then the reset
        of a spent bucket."""
        budget = manager(clock)
        backoff = taken(budget).refused(
            said(0, reset=START + 600, retry_after=30),
        )
        assert backoff.until == at(START + 30)

    def test_a_secondary_limit_saying_no_time_waits_a_minute_then_longer(
        self, clock,
    ):
        """GitHub: wait at least a minute, and longer each time it goes
        on refusing."""
        budget = manager(clock)
        assert taken(budget).refused(said(4_000)).until == at(START + 60)
        clock.advance(60)
        assert taken(budget).refused(said(4_000)).until == at(START + 180)
        clock.advance(120)
        taken(budget).answered(said(3_999))
        assert taken(budget).refused(said(3_998)).until == at(START + 240)

    def test_a_refusal_naming_another_bucket_backs_off_the_one_taken_too(
        self, clock,
    ):
        """A request taken from one bucket, which GitHub refused from
        another, the dependency graph's before its name is verified say:
        both back off, until the same time. Left open, the one it was
        taken from would have it taken again at once, and refused again,
        without end (#162)."""
        budget = manager(clock)
        backoff = taken(budget, 'dependency_sbom').refused(
            said(0, reset=START + 600),
        )
        assert backoff.until == at(START + 601)
        assert budget.standing(A, 'core').blocked_until == at(START + 601)
        assert budget.standing(A, 'dependency_sbom').blocked_until == (
            at(START + 601)
        )
        assert budget.try_lease('dependency_sbom') is None
        clock.advance(601)
        assert budget.try_lease('dependency_sbom') is not None

    def test_a_backoff_holds_one_token_in_one_bucket(self, clock):
        budget = manager(clock, A, B)
        warm(budget, A, 4_999)
        warm(budget, B, 4_000)
        lease = taken(budget)
        assert lease.token == A
        lease.refused(said(4_999, retry_after=60))
        assert taken(budget).token == B
        assert taken(budget, 'graphql').token == A

    def test_a_wait_for_it_ends_when_the_backoff_does(self, clock):
        budget = manager(clock)
        taken(budget).refused(said(4_000, retry_after=30))
        lease = asyncio.run(budget.lease('core'))
        assert lease.token == A
        assert clock() >= START + 30

    def test_a_wait_it_cannot_keep_is_rate_limited(self, clock):
        budget = manager(clock)
        taken(budget).refused(said(4_000, retry_after=30))
        with pytest.raises(RateLimited) as refused:
            asyncio.run(budget.lease('core', wait=10))
        assert refused.value.bucket == 'core'
        assert refused.value.until == at(START + 30)
        assert A.secret not in str(refused.value)
        assert A.secret not in repr(refused.value.args)


class TestChoosingAToken:
    def test_the_one_with_the_most_left(self, clock):
        budget = manager(clock, A, B)
        warm(budget, A, 4_000)
        warm(budget, B, 4_500)
        assert taken(budget).token == B

    def test_what_is_held_counts_against_what_is_left(self, clock):
        budget = manager(clock, A, B)
        warm(budget, A, 4_000)
        warm(budget, B, 4_001)
        assert taken(budget).token == B
        # 4,000 each now, and B has one in flight.
        assert taken(budget).token == A

    def test_one_not_heard_from_is_asked_once_before_the_rest(self, clock):
        budget = manager(clock, A, B)
        warm(budget, A, 4_000)
        probe = taken(budget)
        assert probe.token == B
        # Until it answers, B's bucket could be spent: one at a time.
        assert taken(budget).token == A
        probe.answered(said(100))
        assert taken(budget).token == A

    def test_there_is_no_split_between_tokens(self, clock):
        budget = manager(clock, A, B)
        warm(budget, A, 5_000)
        warm(budget, B, 100)
        used = []
        for left in range(4_999, 4_989, -1):
            lease = taken(budget)
            used.append(lease.token)
            lease.answered(said(left))
        assert used == [A] * 10

    def test_a_retired_token_is_never_chosen(self, clock):
        budget = manager(clock, A, B)
        budget.retire(A, 'GitHub answered 401, Bad credentials')
        used = set()
        for left in range(4_999, 4_995, -1):
            lease = taken(budget)
            used.add(lease.token)
            lease.answered(said(left))
        assert used == {B}

    def test_with_every_token_retired_a_lease_is_unauthorized(self, clock):
        budget = manager(clock, A)
        budget.retire(A, 'GitHub answered 401, Bad credentials')
        with pytest.raises(Unauthorized) as refused:
            asyncio.run(budget.lease('core'))
        assert 'token 1' in str(refused.value)
        assert A.secret not in str(refused.value)


class TestInFlight:
    def test_is_at_most_four_per_token(self, clock):
        assert IN_FLIGHT == 4
        budget = manager(clock)
        warm(budget, A, 4_000)
        leases = [taken(budget) for _ in range(4)]
        assert budget.in_flight(A) == 4
        assert budget.try_lease('core') is None
        leases[0].answered(said(3_999))
        assert budget.try_lease('core') is not None

    def test_counts_every_bucket_of_a_token(self, clock):
        budget = manager(clock)
        warm(budget, A, 4_000)
        warm(budget, A, 4_000, 'graphql')
        for _ in range(4):
            taken(budget)
        assert budget.try_lease('graphql') is None

    def test_is_per_token(self, clock):
        budget = manager(clock, A, B)
        warm(budget, A, 4_000)
        warm(budget, B, 4_000)
        tokens = [taken(budget).token for _ in range(8)]
        assert tokens.count(A) == tokens.count(B) == 4
        assert budget.try_lease('core') is None

    def test_a_lease_waits_for_one_to_come_back(self, clock):
        budget = manager(clock)
        warm(budget, A, 4_000)

        async def waiting() -> Lease:
            leases = [taken(budget) for _ in range(4)]
            waiter = asyncio.ensure_future(budget.lease('core'))
            for _ in range(5):
                await asyncio.sleep(0)
            assert not waiter.done()
            leases[0].answered(said(3_999))
            return await asyncio.wait_for(waiter, 10)

        assert asyncio.run(waiting()).token == A
        # A request's answer freed it, not the clock.
        assert clock() == START


class TestALease:
    def test_lost_counts_as_spent(self, clock):
        budget = manager(clock)
        warm(budget, A, 100)
        taken(budget).lost()
        assert budget.standing(A, 'core').remaining == 99
        assert budget.standing(A, 'core').held == 0
        assert budget.in_flight(A) == 0

    def test_released_costs_nothing(self, clock):
        budget = manager(clock)
        warm(budget, A, 100)
        taken(budget).release()
        assert budget.standing(A, 'core').remaining == 100
        assert budget.in_flight(A) == 0

    def test_left_open_by_its_block_counts_as_spent(self, clock):
        budget = manager(clock)
        warm(budget, A, 100)

        async def failing() -> None:
            async with await budget.lease('core'):
                raise RuntimeError('the request never came back')

        with pytest.raises(RuntimeError):
            asyncio.run(failing())
        assert budget.standing(A, 'core').remaining == 99
        assert budget.in_flight(A) == 0

    def test_closed_in_its_block_is_closed_once(self, clock):
        budget = manager(clock)
        warm(budget, A, 100)

        async def answering() -> None:
            async with await budget.lease('core') as lease:
                lease.answered(said(99))

        asyncio.run(answering())
        assert budget.standing(A, 'core').remaining == 99

    def test_closed_twice_is_a_mistake(self, clock):
        budget = manager(clock)
        lease = taken(budget)
        lease.answered(said(99))
        with pytest.raises(RuntimeError):
            lease.answered(said(98))

    def test_holds_what_it_may_cost(self, clock):
        """A GraphQL query costs its points."""
        budget = manager(clock)
        warm(budget, A, 10, 'graphql')
        taken(budget, 'graphql', cost=4)
        assert budget.standing(A, 'graphql').held == 4
        taken(budget, 'graphql', cost=4)
        assert budget.try_lease('graphql', cost=4) is None
        assert budget.try_lease('graphql', cost=2) is not None
