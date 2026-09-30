"""The budget manager: every token's buckets, as GitHub's answers say
they stand (#156, #128 section 2.1).

GitHub meters each token's account in buckets: `core` for the REST API,
`graphql`, `search`, and whatever else `X-RateLimit-Resource` names,
the dependency graph's among them. Every answer says where its bucket
stands, in `X-RateLimit-Limit`, `-Remaining` and `-Reset`, and that is
all this reads: `GET /rate_limit` said 5,000 of 5,000 left while a real
answer said 3,156 (TODO.md), so a budget that asked it would never stop.

A request takes a `Lease` before it is sent. The lease names the token
with the most left in the request's bucket, holds what the request may
cost while it is in flight, and is closed by the answer: `answered`,
with the answer's headers; `refused`, a rate limit, which backs the
token's bucket off; or `lost`, no answer, which counts as spent. There
is no split between tokens: whichever has the most left is asked, and
tokens take turns only as what they have left says.

- **The reserve.** Each bucket of each token keeps what `reserve` says,
  for manual work, a command run by hand with the same tokens. A lease
  that would reach into it waits for the reset.
- **Backing off.** A bucket refused, 403 or 429, is not asked again
  until `Retry-After` has passed, if GitHub said one; otherwise, for a
  primary limit, nothing left, until its reset; otherwise a minute, and
  twice as long each time GitHub goes on refusing, up to an hour. That
  is GitHub's documented order.
- **In flight.** At most four requests per token at once, in any
  buckets (`IN_FLIGHT`): enough to keep a repository's walk paced by the
  token (#128), and well under GitHub's secondary limit on concurrency.
  A bucket not heard from is asked once, and the answer says the rest.
- **A bucket named otherwise.** A caller names the bucket a request is
  taken from, and may guess wrong: the dependency graph's is named
  before a live token has said its name (#162). A refusal that names
  another bucket backs off both. A bucket no answer has named follows,
  for the token, the one its requests' answers name: what is taken from
  it is taken from that one, until an answer names it. One an answer has
  named, `core` first among them, never follows another.
- **Waiting.** A lease that cannot be had now waits, for an answer that
  frees room or for a time that does, at most as long as the caller
  allows; past that it is `RateLimited`.
- **The order.** Where leases wait for the same room, the one of the
  highest priority is given it first, the lowest number, whenever it
  asked: a lease of a lower priority, waiting or asking now, gives way
  to one that could be given the room now. Equals take their turns as
  before. A task says its priority once (`at_priority`), and every
  lease it and the tasks it starts take is at it: `chatsbom collect`
  gives detection's the first call on a token's requests in flight,
  then the stages', and the dependency graph's the last (#171).

One process holds every token (#128): nothing here is shared with
another, and nothing is locked but by the event loop.
"""
import asyncio
import itertools
import math
import time
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import Sequence
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from email.utils import parsedate_to_datetime
from types import TracebackType
from typing import Self

import structlog

from chatsbom.collector.errors import RateLimited
from chatsbom.collector.errors import Unauthorized
from chatsbom.collector.tokens import Token

logger = structlog.get_logger('collector.budget')

#: Requests in flight at once per token, in all its buckets together.
IN_FLIGHT = 4

#: A secondary limit that says no time is waited out this long, and
#: twice as long each time GitHub goes on refusing, up to `LONGEST`.
SECONDARY = 60.0
LONGEST = 3_600.0

#: Past a reset before a bucket refused as spent is asked again: GitHub's
#: clock and this one need not agree to the second.
MARGIN = 1.0

#: How long a bucket short of room, whose answers said no reset, is left
#: before it is asked again.
NO_RESET = 60.0

#: The priority a task's leases are granted at, the lowest first where
#: requests wait for the same room: 0 unless the task says (`at_priority`).
_PRIORITY: ContextVar[int] = ContextVar('lease_priority', default=0)


def lease_priority() -> int:
    """The priority the running task's leases are granted at."""
    return _PRIORITY.get()


@contextmanager
def at_priority(level: int) -> Iterator[None]:
    """Leases taken within it are granted at `level`: those of the task
    it is entered in, and of every task started within it, which takes
    it as it starts. The lowest first."""
    token = _PRIORITY.set(level)
    try:
        yield
    finally:
        _PRIORITY.reset(token)


def _at(seconds: float) -> datetime:
    return datetime.fromtimestamp(seconds, timezone.utc)


def _number(value: str | None) -> int | None:
    try:
        return None if value is None else int(value.strip())
    except ValueError:
        return None


@dataclass(frozen=True)
class Limits:
    """GitHub's rate-limit headers, as one answer carried them. A header
    missing, or one that is not what it should be, is None: garbage is
    never read as a spent bucket."""

    limit: int | None
    remaining: int | None
    #: When the window ends, in UTC epoch seconds.
    reset: float | None
    resource: str | None
    #: Seconds to wait before asking again.
    retry_after: float | None

    @classmethod
    def of(cls, headers: Mapping[str, str], now: float) -> Self:
        named = {name.lower(): value for name, value in headers.items()}
        reset = _number(named.get('x-ratelimit-reset'))
        return cls(
            limit=_number(named.get('x-ratelimit-limit')),
            remaining=_number(named.get('x-ratelimit-remaining')),
            reset=None if reset is None else float(reset),
            resource=(named.get('x-ratelimit-resource') or '').strip() or None,
            retry_after=_retry_after(named.get('retry-after'), now),
        )


def _retry_after(value: str | None, now: float) -> float | None:
    """`Retry-After`: seconds, as GitHub sends it, or an HTTP date."""
    if value is None:
        return None
    seconds = _number(value)
    if seconds is not None:
        return float(max(seconds, 0))
    try:
        return max(parsedate_to_datetime(value).timestamp() - now, 0.0)
    except (TypeError, ValueError):
        return None


@dataclass(frozen=True)
class Standing:
    """Where one token's bucket stands, as its answers said."""

    #: The token's name, never the token.
    token: str
    bucket: str
    #: None until an answer said.
    limit: int | None
    remaining: int | None
    reset: datetime | None
    #: What the requests in flight may cost.
    held: int
    #: Backing off until then; None when it is not.
    blocked_until: datetime | None


@dataclass(frozen=True)
class Backoff:
    """How a refusal backed its bucket off."""

    until: datetime
    #: Nothing left: the primary limit. Otherwise a secondary one.
    primary: bool


@dataclass(frozen=True)
class _Asking:
    """A lease waited for: at what priority, and for what room."""

    priority: int
    bucket: str
    cost: int


class _Bucket:
    """One token's bucket, as this process knows it."""

    __slots__ = (
        'limit', 'remaining', 'reset', 'held', 'blocked_until', 'strikes',
    )

    def __init__(self) -> None:
        self.limit: int | None = None
        self.remaining: int | None = None
        self.reset: float | None = None
        self.held = 0
        self.blocked_until = 0.0
        #: Refusals in a row.
        self.strikes = 0


class Lease:
    """A request's claim on a token's bucket, from before it is sent
    until its answer is read. Closed once: by `answered`, `refused`,
    `lost` or `release`, or, left open by the block it was used in, as
    `lost`."""

    __slots__ = ('_budget', 'token', 'bucket', 'cost', '_open')

    def __init__(
        self, budget: 'BudgetManager', token: Token, bucket: str, cost: int,
    ) -> None:
        self._budget = budget
        self.token = token
        #: The bucket it was taken from.
        self.bucket = bucket
        self.cost = cost
        self._open = True

    def _close(self) -> None:
        if not self._open:
            raise RuntimeError('a lease is closed once')
        self._open = False

    def answered(
        self, headers: Mapping[str, str], *, free: bool = False,
    ) -> None:
        """GitHub answered, with `headers`. `free`: the answer cost
        nothing, a 304, which matters only where the headers do not say
        what is left."""
        self._close()
        self._budget._answered(self, headers, free)

    def refused(self, headers: Mapping[str, str]) -> Backoff:
        """GitHub refused the request for a rate limit: its bucket backs
        off."""
        self._close()
        return self._budget._refused(self, headers)

    def lost(self) -> None:
        """No answer, which may have been billed all the same."""
        self._close()
        self._budget._lost(self)

    def release(self) -> None:
        """Not answered by the bucket: nothing was spent from it."""
        self._close()
        self._budget._release(self)

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        if self._open:
            self.lost()


class BudgetManager:
    """Every token's buckets, and the leases on them."""

    def __init__(
        self,
        tokens: Sequence[Token],
        *,
        reserve: Mapping[str, int] | None = None,
        in_flight: int = IN_FLIGHT,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        if not tokens:
            raise ValueError('a budget needs a token')
        if in_flight < 1:
            raise ValueError(f'at least one request in flight: {in_flight}')
        self.tokens = tuple(tokens)
        self.reserve = dict(reserve or {})
        #: Now, in UTC epoch seconds, as `X-RateLimit-Reset` counts.
        self.clock = clock
        self._sleep = sleep
        self._cap = in_flight
        self._buckets: dict[tuple[Token, str], _Bucket] = {}
        self._flying = dict.fromkeys(self.tokens, 0)
        self._retired: dict[Token, str] = {}
        self._waiters: list[asyncio.Future[None]] = []
        #: Buckets taken for another, said once each.
        self._told: set[tuple[str, str]] = set()
        #: Buckets the reserve leaves nothing of, said once each.
        self._whole: set[str] = set()
        #: Each token's buckets an answer has named, and those no answer
        #: has, which follow the one their requests' answers named.
        self._named: set[tuple[Token, str]] = set()
        self._follows: dict[tuple[Token, str], str] = {}
        #: Buckets that follow another, said once each.
        self._following: set[tuple[str, str]] = set()
        #: The leases waited for now, by the order they were first asked
        #: in: which a lease of a lower priority gives way to.
        self._asking: dict[int, _Asking] = {}
        self._asked = itertools.count()

    # -- what it knows ----------------------------------------------------

    def in_flight(self, token: Token) -> int:
        return self._flying[token]

    def standing(self, token: Token, bucket: str) -> Standing:
        state = self._buckets.get((token, bucket)) or _Bucket()
        now = self.clock()
        return Standing(
            token=token.label,
            bucket=bucket,
            limit=state.limit,
            remaining=state.remaining,
            reset=None if state.reset is None else _at(state.reset),
            held=state.held,
            blocked_until=(
                _at(state.blocked_until) if state.blocked_until > now
                else None
            ),
        )

    def standings(self) -> list[Standing]:
        """Every bucket of every token heard of, token by token."""
        order = {token: index for index, token in enumerate(self.tokens)}
        return [
            self.standing(token, bucket)
            for token, bucket in sorted(
                self._buckets, key=lambda key: (order[key[0]], key[1]),
            )
        ]

    def retire(self, token: Token, reason: str) -> None:
        """Leaves `token` out from now on: GitHub does not take it."""
        if token in self._retired:
            return
        self._retired[token] = reason
        logger.warning(
            'A GitHub token is left out until the collector restarts',
            token=token.label, reason=reason,
            tokens_left=len(self.tokens) - len(self._retired),
        )
        self._notify()

    # -- leases -----------------------------------------------------------

    def try_lease(self, bucket: str, *, cost: int = 1) -> Lease | None:
        """A lease on `bucket` now, at the running task's priority
        (`at_priority`); or None if no token has room, but for what a
        lease of a higher priority waits for."""
        return self._try(bucket, cost, lease_priority(), None)[0]

    def _try(
        self, bucket: str, cost: int, priority: int, asking: int | None,
    ) -> tuple[Lease | None, bool]:
        """A lease on `bucket` now, or None; and whether a token had room
        for it that a lease of a higher priority, waiting, is given
        first. `asking` is this lease's, if it waits: never behind
        itself."""
        if cost < 1:
            raise ValueError(f'a request costs at least 1: {cost}')
        now = self.clock()
        best: tuple[float, int, int] | None = None
        chosen: Token | None = None
        held_back = False
        for index, token in enumerate(self.tokens):
            room = self._room(token, bucket, cost, now)
            if room is None:
                continue
            if self._first_call(token, priority, asking, now):
                held_back = True
                continue
            rank = (room, -self._flying[token], -index)
            if best is None or rank > best:
                best, chosen = rank, token
        if chosen is None:
            return None, held_back
        taken = self._taken(chosen, bucket)
        self._bucket(chosen, taken).held += cost
        self._flying[chosen] += 1
        return Lease(self, chosen, taken, cost), False

    def _first_call(
        self, token: Token, priority: int, asking: int | None, now: float,
    ) -> bool:
        """Whether a lease of a higher priority than `priority`, waiting
        now, could be given on `token` now: it is, before any other."""
        return any(
            other.priority < priority
            and self._room(token, other.bucket, other.cost, now) is not None
            for key, other in self._asking.items() if key != asking
        )

    async def lease(
        self, bucket: str, *, cost: int = 1, wait: float | None = None,
    ) -> Lease:
        """A lease on `bucket`, waited for as long as it takes, or for
        `wait` seconds at most, past which it is `RateLimited`. Where
        leases wait for the same room, it is given to the one of the
        highest priority first, the lowest number (`at_priority`)."""
        priority = lease_priority()
        give_up = None if wait is None else self.clock() + max(wait, 0.0)
        asking = next(self._asked)
        try:
            while True:
                lease, held_back = self._try(bucket, cost, priority, asking)
                if lease is not None:
                    return lease
                if len(self._retired) == len(self.tokens):
                    raise Unauthorized(
                        'GitHub took none of the tokens: '
                        + '; '.join(
                            f'{token.label}, {reason}'
                            for token, reason in self._retired.items()
                        ),
                        status=401,
                    )
                self._asking[asking] = _Asking(priority, bucket, cost)
                now = self.clock()
                wake = self._wake(bucket, cost, now)
                # Held back, it waits for the lease it gave way to: that
                # one is given its room at once, and says so.
                if give_up is not None and (
                    now >= give_up or (
                        not held_back
                        and not any(self._flying.values())
                        and (wake is None or wake > give_up)
                    )
                ):
                    raise RateLimited(
                        f'No GitHub token has room in the {bucket} bucket'
                        + (
                            '' if wake is None else
                            f' before {_at(wake):%Y-%m-%d %H:%M:%S} UTC'
                        ),
                        bucket=bucket,
                        until=None if wake is None else _at(wake),
                    )
                await self._wait(
                    min(
                        (
                            moment for moment in (wake, give_up)
                            if moment is not None
                        ),
                        default=None,
                    ),
                )
        finally:
            # No longer waiting, given its lease or not: a lease that gave
            # way to it may take what is left.
            if self._asking.pop(asking, None) is not None:
                self._notify()

    # -- what a token has room for ----------------------------------------

    def _bucket(self, token: Token, bucket: str) -> _Bucket:
        return self._buckets.setdefault((token, bucket), _Bucket())

    def _taken(self, token: Token, bucket: str) -> str:
        """The bucket a request of `token`'s is taken from when its caller
        names `bucket`: the one `bucket` follows, if it does."""
        return self._follows.get((token, bucket), bucket)

    def _room(
        self, token: Token, bucket: str, cost: int, now: float,
    ) -> float | None:
        """What `token` has left in `bucket` for a request costing `cost`,
        infinite when it has not been heard from, or None when it cannot
        be asked now."""
        if token in self._retired or self._flying[token] >= self._cap:
            return None
        bucket = self._taken(token, bucket)
        state = self._buckets.get((token, bucket))
        if state is None:
            return math.inf
        if state.blocked_until > now:
            return None
        remaining = state.remaining
        if state.reset is not None and now >= state.reset:
            # A window of its own: whole again, as far as anyone knows.
            remaining = state.limit
        if remaining is None:
            # Asked once, and its answer says the rest.
            return math.inf if state.held == 0 else None
        room = remaining - state.held - self.reserve.get(bucket, 0)
        return float(room) if room >= cost else None

    def _wake(self, bucket: str, cost: int, now: float) -> float | None:
        """The soonest time a token may have room in `bucket` again, where
        only time will give it: a backoff ending, or a window."""
        moments = []
        for token in self.tokens:
            taken = self._taken(token, bucket)
            state = self._buckets.get((token, taken))
            if token in self._retired or state is None:
                continue
            if state.blocked_until > now:
                moments.append(state.blocked_until)
                continue
            if state.remaining is None or (
                state.reset is not None and now >= state.reset
            ):
                continue
            room = state.remaining - state.held - self.reserve.get(taken, 0)
            if room < cost:
                moments.append(
                    now + NO_RESET if state.reset is None else state.reset,
                )
        return min(moments, default=None)

    async def _wait(self, until: float | None) -> None:
        """Until a lease is closed or a token retired, or `until`."""
        waiter = asyncio.get_running_loop().create_future()
        self._waiters.append(waiter)
        try:
            if until is None:
                await waiter
                return
            sleeping = asyncio.ensure_future(
                self._sleep(max(until - self.clock(), 0.0)),
            )
            try:
                await asyncio.wait(
                    {waiter, sleeping}, return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                sleeping.cancel()
                await asyncio.gather(sleeping, return_exceptions=True)
        finally:
            if waiter in self._waiters:
                self._waiters.remove(waiter)
            waiter.cancel()

    def _notify(self) -> None:
        waiters, self._waiters = self._waiters, []
        for waiter in waiters:
            if not waiter.done():
                waiter.set_result(None)

    # -- what the answers said --------------------------------------------

    def _resource(self, lease: Lease, limits: Limits) -> str:
        """The bucket an answer drew from: the one it names. The one the
        request was taken from follows it, for the token, if no answer
        has named that one; and one an answer names follows no other."""
        resource = limits.resource or lease.bucket
        if resource != lease.bucket and (lease.bucket, resource) not in self._told:
            self._told.add((lease.bucket, resource))
            logger.warning(
                'A request taken from one GitHub bucket drew from another',
                taken=lease.bucket, drawn=resource,
            )
        if resource != lease.bucket and (
            (lease.token, lease.bucket) not in self._named
        ):
            self._follows[(lease.token, lease.bucket)] = resource
            if (lease.bucket, resource) not in self._following:
                self._following.add((lease.bucket, resource))
                logger.warning(
                    'A GitHub bucket no answer has named follows the one '
                    'its requests drew from',
                    taken=lease.bucket, drawn=resource,
                )
        self._named.add((lease.token, resource))
        self._follows.pop((lease.token, resource), None)
        return resource

    def _update(self, state: _Bucket, bucket: str, limits: Limits) -> None:
        """What an answer said of its bucket. In one window what is left
        only falls, whatever order the answers come in; an answer from a
        window already over says nothing."""
        if limits.remaining is None:
            return
        if limits.limit is not None:
            state.limit = limits.limit
            kept = self.reserve.get(bucket, 0)
            if kept >= limits.limit and bucket not in self._whole:
                self._whole.add(bucket)
                logger.warning(
                    'The reserve is the whole bucket: nothing of it is left '
                    'to ask with',
                    bucket=bucket, limit=limits.limit, reserve=kept,
                )
        if (
            limits.reset is not None and limits.reset == state.reset
            and state.remaining is not None
        ):
            state.remaining = min(state.remaining, limits.remaining)
        elif (
            limits.reset is not None and state.reset is not None
            and limits.reset < state.reset
        ):
            return
        else:
            state.remaining = limits.remaining
            if limits.reset is not None:
                state.reset = limits.reset

    def _answered(
        self, lease: Lease, headers: Mapping[str, str], free: bool,
    ) -> None:
        limits = Limits.of(headers, self.clock())
        resource = self._resource(lease, limits)
        state = self._bucket(lease.token, resource)
        if limits.remaining is not None:
            self._update(state, resource, limits)
        elif not free and state.remaining is not None:
            state.remaining = max(state.remaining - lease.cost, 0)
        state.strikes = 0
        self._release(lease)

    def _refused(self, lease: Lease, headers: Mapping[str, str]) -> Backoff:
        now = self.clock()
        limits = Limits.of(headers, now)
        resource = self._resource(lease, limits)
        state = self._bucket(lease.token, resource)
        self._update(state, resource, limits)
        state.strikes += 1
        doubling = 2.0 ** min(state.strikes - 1, 16)
        primary = limits.remaining == 0
        if limits.retry_after is not None:
            until = now + max(limits.retry_after, MARGIN)
        elif primary and limits.reset is not None:
            until = max(
                limits.reset + MARGIN, now + min(doubling * MARGIN, SECONDARY),
            )
        else:
            until = now + min(SECONDARY * doubling, LONGEST)
        state.blocked_until = max(state.blocked_until, until)
        if resource != lease.bucket:
            # And the bucket it was taken from: left open, it would be
            # taken from again at once, refused again, and so on, with no
            # time passing between (#162).
            taken = self._bucket(lease.token, lease.bucket)
            taken.blocked_until = max(taken.blocked_until, until)
        logger.warning(
            'GitHub refused a request: rate limited',
            token=lease.token.label, bucket=resource,
            limit='primary' if primary else 'secondary',
            until=f'{_at(until):%Y-%m-%d %H:%M:%S} UTC',
            refusals=state.strikes,
        )
        self._release(lease)
        return Backoff(until=_at(until), primary=primary)

    def _lost(self, lease: Lease) -> None:
        state = self._bucket(lease.token, lease.bucket)
        if state.remaining is not None:
            state.remaining = max(state.remaining - lease.cost, 0)
        self._release(lease)

    def _release(self, lease: Lease) -> None:
        self._bucket(lease.token, lease.bucket).held -= lease.cost
        self._flying[lease.token] -= 1
        self._notify()
