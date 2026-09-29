"""The web service's rate limits, counted over a window that slides (#134).

Ported from the Worker's `RateLimiter` (web/src/ratelimit.ts, #115) and
held to its tests (web/test/ratelimit.test.ts): at most `limit`
requests from a client in any `period` seconds. The limiters had
counted in windows aligned to the wall clock, so a client's budget came
back whole at every multiple of the period: its budget just before one
and again just after, twice it in moments.

Two things differ, as #128 section 2.5 decided. The counts are kept in
memory, so a restart forgets them; the Worker stored them only because
workerd evicts an idle object after ten seconds. And how many clients
are kept is bounded, least recently seen first out.
"""
import math
import threading

import pytest

from chatsbom.server.ratelimit import RateLimit
from chatsbom.server.ratelimit import RateLimiter

LIMIT = 20
PERIOD = 60
#: A multiple of the period, where a fixed window would start: the
#: Worker's test's 2026-09-14T10:01:00Z, in seconds.
BOUNDARY = 1_789_466_460.0


class Clock:
    """A clock the test sets, in seconds from BOUNDARY."""

    def __init__(self) -> None:
        self.now = BOUNDARY

    def at(self, seconds: float) -> None:
        self.now = BOUNDARY + seconds

    def __call__(self) -> float:
        return self.now


@pytest.fixture
def clock() -> Clock:
    return Clock()


def limiter_on(clock: Clock, **options: int) -> RateLimiter:
    return RateLimiter(RateLimit(LIMIT, PERIOD), clock=clock, **options)


def burst(limiter: RateLimiter, count: int, client: str = '203.0.113.7') -> int:
    """How many of `count` requests from `client` are let through."""
    return sum(limiter.admit(client) for _ in range(count))


def test_the_boundary_is_one_a_fixed_window_would_start_at():
    assert BOUNDARY % PERIOD == 0


def test_lets_a_clients_budget_through_and_not_one_request_more(clock):
    limiter = limiter_on(clock)
    clock.at(10)
    assert burst(limiter, LIMIT + 5) == LIMIT


def test_gives_a_burst_across_a_window_boundary_one_budget_not_two(clock):
    limiter = limiter_on(clock)
    clock.at(-0.001)
    assert burst(limiter, LIMIT) == LIMIT
    clock.at(0.001)
    # A fixed window let all of these through: its count starts over at
    # the boundary, and the last burst was a moment ago.
    assert burst(limiter, LIMIT) == 0


def test_gives_the_budget_back_as_the_window_slides_past_it(clock):
    limiter = limiter_on(clock)
    clock.at(-0.001)
    burst(limiter, LIMIT)
    # Half a period on, half of that burst is taken to have slid out.
    clock.at(PERIOD / 2)
    assert burst(limiter, LIMIT) == LIMIT / 2
    # A whole period of quiet after it, all of it.
    clock.at(2 * PERIOD + 1)
    assert burst(limiter, LIMIT) == LIMIT


def test_counts_nothing_it_refuses(clock):
    """A client told to wait is not kept waiting longer for asking again."""
    limiter = limiter_on(clock)
    clock.at(-0.001)
    burst(limiter, LIMIT + 50)
    clock.at(PERIOD / 2)
    assert burst(limiter, LIMIT) == LIMIT / 2


def test_keeps_each_clients_count_apart(clock):
    limiter = limiter_on(clock)
    clock.at(1)
    assert burst(limiter, LIMIT, '203.0.113.7') == LIMIT
    assert burst(limiter, LIMIT, '198.51.100.9') == LIMIT
    assert burst(limiter, 1, '203.0.113.7') == 0


def test_forgets_a_client_once_nothing_it_counted_weighs_any_more(clock):
    limiter = limiter_on(clock)
    clock.at(1)
    burst(limiter, LIMIT, '203.0.113.7')
    clock.at(PERIOD + 1)
    burst(limiter, 1, '198.51.100.9')
    assert len(limiter) == 2
    # Two periods on, the first client's window is two behind, and gone.
    clock.at(2 * PERIOD + 1)
    assert burst(limiter, 1, '192.0.2.1') == 1
    assert len(limiter) == 2
    assert '203.0.113.7' not in limiter


def test_a_restart_forgets_the_windows(clock):
    """In memory, as #128 decided: nothing but a restart resets them."""
    clock.at(1)
    burst(limiter_on(clock), LIMIT)
    assert burst(limiter_on(clock), LIMIT) == LIMIT


class TestTheBound:
    """How many clients are kept is bounded, least recently seen out
    first. An evicted client starts over, so eviction only ever gives a
    budget back, never takes one: a flood of new keys costs memory up
    to the bound and no one else their requests."""

    def test_keeps_no_more_clients_than_the_bound(self, clock):
        limiter = limiter_on(clock, max_clients=3)
        clock.at(1)
        for client in ('192.0.2.1', '192.0.2.2', '192.0.2.3', '192.0.2.4'):
            limiter.admit(client)
        assert len(limiter) == 3
        assert '192.0.2.1' not in limiter

    def test_the_least_recently_seen_goes_first(self, clock):
        limiter = limiter_on(clock, max_clients=3)
        clock.at(1)
        for client in ('192.0.2.1', '192.0.2.2', '192.0.2.3'):
            limiter.admit(client)
        # Seen again, even refused, it is the most recent.
        burst(limiter, LIMIT + 1, '192.0.2.1')
        limiter.admit('192.0.2.4')
        assert '192.0.2.1' in limiter
        assert '192.0.2.2' not in limiter

    def test_an_evicted_client_starts_over(self, clock):
        limiter = limiter_on(clock, max_clients=1)
        clock.at(1)
        burst(limiter, LIMIT, '203.0.113.7')
        limiter.admit('198.51.100.9')
        assert burst(limiter, LIMIT, '203.0.113.7') == LIMIT

    def test_refuses_a_bound_of_none(self, clock):
        with pytest.raises(ValueError):
            limiter_on(clock, max_clients=0)


def test_requests_arriving_together_are_counted_one_after_another(clock):
    """As the Durable Object took one call at a time: a sync route runs
    in a thread pool, and two counts must never interleave."""
    limiter = RateLimiter(RateLimit(500, PERIOD), clock=clock)
    clock.at(1)
    admitted: list[bool] = []
    start = threading.Barrier(8)

    def ask() -> None:
        start.wait()
        admitted.extend(limiter.admit('203.0.113.7') for _ in range(100))

    threads = [threading.Thread(target=ask) for _ in range(8)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()
    assert sum(admitted) == 500


def test_a_long_client_key_is_cut_to_the_workers_bound(clock):
    limiter = limiter_on(clock)
    clock.at(1)
    assert burst(limiter, LIMIT, 'x' * 200) == LIMIT
    # The same first 128 characters are the same client.
    assert burst(limiter, 1, 'x' * 128 + 'y') == 0


class TestTheSetting:
    """A limit: a whole number of requests, in a positive number of
    seconds. The Worker refused every request under one that was not;
    here it is refused at start (settings_test)."""

    @pytest.mark.parametrize(
        'limit,period',
        [
            (math.nan, PERIOD),
            (LIMIT, math.nan),
            (LIMIT, math.inf),
            (0, PERIOD),
            (-1, PERIOD),
            (1.5, PERIOD),
            (True, PERIOD),
            (LIMIT, 0),
            (LIMIT, -1),
        ],
    )
    def test_refuses_one_that_is_not_a_limit(self, limit, period):
        with pytest.raises(ValueError):
            RateLimit(limit, period)

    def test_takes_a_period_that_is_not_whole(self):
        assert RateLimit(100, 2.5).period == 2.5
