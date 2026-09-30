"""Per-client rate limits, counted over a window that slides.

Ported from the Worker's `RateLimiter` (#115; #151 deleted the
Worker), with its settings and their defaults (`CHAT_RATE_LIMIT` and
`QUERY_RATE_LIMIT`): at most `limit` requests from a client in any
`period` seconds.

`wrangler dev` simulated Cloudflare's own limiter with a count per
window aligned to the wall clock, starting over at every multiple of
the period, so a client's budget just before one and its budget again
just after got through: twice the limit in moments (#31). So the window
slides. A request is let through if the client's requests in the last
`period` seconds, one more with it, are within the limit, where the
last `period` seconds are this window so far and as much of the one
before as they still cover: the previous window's count, weighted by
that share, since how its requests fell within it is not kept. A burst
on either side of a boundary gets one budget between them, and the
budget comes back as the window slides past it.

A refused request is not counted: a client told to wait is not kept
waiting longer for having asked again.

Two things differ from the Worker, as #128 section 2.5 decided. The
counts are held in memory alone, so a restart forgets them: the Worker
stored them only because workerd evicts an object idle for ten seconds.
And how many clients are held is bounded, the least recently seen going
first. An evicted client starts over, so eviction only ever gives a
budget back, never takes one: a flood of new keys, from the /64s a
single IPv6 client may hold, costs memory up to the bound and no one
else their requests.
"""
import math
import threading
import time
from collections import OrderedDict
from collections.abc import Callable
from dataclasses import dataclass
from typing import NamedTuple

#: The most clients a limiter holds. Only a client seen within the last
#: two periods weighs on a count, and far fewer than this are, unless a
#: flood of keys is on its way: then the oldest go.
MAX_CLIENTS = 10_000

#: The longest client key counted as it is, as the Worker's MAX_CLIENT.
#: `clients.client_key` makes none longer; a caller could.
MAX_KEY = 128


@dataclass(frozen=True)
class RateLimit:
    """At most `limit` requests from a client in `period` seconds: the
    two numbers each `<NAME>_RATE_LIMIT` holds.

    Checked as it is made: the Worker refused every request under a
    setting that was not one, since it could not refuse to start, and
    a setting here is read before the service starts (`settings`).
    """

    limit: int
    period: float

    def __post_init__(self) -> None:
        limit, period = self.limit, self.period
        # bool is an int to Python, and `true` is no number of requests.
        if isinstance(limit, bool) or not isinstance(limit, int) or limit < 1:
            raise ValueError(
                f'limit is not a whole number of requests, 1 or more: '
                f'{limit!r}',
            )
        if (
            isinstance(period, bool)
            or not isinstance(period, (int, float))
            or not math.isfinite(period)
            or period <= 0
        ):
            raise ValueError(
                f'period is not a number of seconds above 0: {period!r}',
            )


class _Counts(NamedTuple):
    """A client's requests let through: in window `window`, and in the
    one before it. A window is its start over the period, as a count of
    periods."""

    window: int
    current: int
    previous: int


class RateLimiter:
    """One limit's counts, by client.

    Calls from several threads are counted one after another, as the
    Durable Object took one call at a time: a route that is not `async`
    runs in a thread pool.
    """

    def __init__(
        self,
        setting: RateLimit,
        *,
        max_clients: int = MAX_CLIENTS,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if max_clients < 1:
            raise ValueError(f'max_clients is below 1: {max_clients}')
        self.setting = setting
        self._max_clients = max_clients
        self._clock = clock
        # Least recently seen first.
        self._clients: OrderedDict[str, _Counts] = OrderedDict()
        self._lock = threading.Lock()

    def admit(self, client: str) -> bool:
        """Count a request from `client`, and say whether it may go on."""
        key = client[:MAX_KEY]
        limit, period = self.setting.limit, self.setting.period
        with self._lock:
            # How far into the current window the clock is, as a share of
            # it; from one quotient, so that where a window starts and
            # how far into it we are cannot disagree by a rounding.
            windows = self._clock() / period
            window = math.floor(windows)
            into = windows - window
            self._forget_before(window - 1)

            previous, current = self._counts(key, window)
            # The last `period` seconds: this window so far, and the share
            # of the one before that they still cover.
            recent = previous * (1 - into) + current
            admitted = recent + 1 <= limit
            if admitted:
                self._clients[key] = _Counts(window, current + 1, previous)
            if key in self._clients:
                self._clients.move_to_end(key)
            while len(self._clients) > self._max_clients:
                self._clients.popitem(last=False)
            return admitted

    def __len__(self) -> int:
        """How many clients are held."""
        return len(self._clients)

    def __contains__(self, client: object) -> bool:
        return client in self._clients

    def _counts(self, key: str, window: int) -> tuple[int, int]:
        """`key`'s count in the window before `window`, and in it."""
        held = self._clients.get(key)
        if held is None:
            return 0, 0
        if held.window == window:
            return held.previous, held.current
        if held.window == window - 1:
            return held.current, 0
        return 0, 0

    def _forget_before(self, oldest: int) -> None:
        """Forget the clients, least recently seen first, whose counts
        are all in windows before `oldest`: none of them weighs on a
        count any more. The first that does ends the sweep, and the
        bound keeps whatever it leaves behind it."""
        while self._clients:
            key, held = next(iter(self._clients.items()))
            if held.window >= oldest:
                return
            del self._clients[key]
