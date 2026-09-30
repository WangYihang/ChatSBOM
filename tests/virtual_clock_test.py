"""A clock for a process of many tasks that each sleep (#171): time that
moves only when every task waits for it.

`FakeClock`'s sleep moves the clock at once, by as long as it was asked
to: right for one task, and wrong for several, whose sleeps would add
up where they overlap. Here a sleep waits until the clock reaches its
end, and `drive` moves the clock, whenever nothing else can run, to the
soonest sleep's end, and wakes it: the sleeps end in the order their
ends fall, each at its time, and a day of the collector's schedule runs
in a moment.

Work in a thread is waited for, where it is started by `to_thread`,
which a test puts in `asyncio.to_thread`'s place: the process walks
the store in one. A child process is not waited for beyond a moment:
time may move on meanwhile, as it would for a slow one.

`TestTheClock` holds it to what collector_process_test counts on.
"""
from __future__ import annotations

import asyncio
import heapq
import itertools
import time
from collections.abc import Awaitable
from collections.abc import Callable
from typing import Any
from typing import TypeVar

from tests.fake_github_test import FakeClock
from tests.fake_github_test import START

Result = TypeVar('Result')

#: asyncio's own, which `to_thread` counts the work of.
_TO_THREAD = asyncio.to_thread


class VirtualClock(FakeClock):
    """Time for the stand-in, the budget and the process alike."""

    def __init__(self, now: float = START) -> None:
        super().__init__(now)
        self._sleeping: list[tuple[float, int, asyncio.Future[None]]] = []
        self._order = itertools.count()
        #: Work in threads, not done yet: the clock waits for it.
        self._threads = 0

    async def to_thread(
        self, function: Callable[..., Result], /, *args: Any, **kwargs: Any,
    ) -> Result:
        """`asyncio.to_thread`, whose work the clock waits for."""
        self._threads += 1
        try:
            return await _TO_THREAD(function, *args, **kwargs)
        finally:
            self._threads -= 1

    async def sleep(self, seconds: float) -> None:
        if seconds <= 0:
            await asyncio.sleep(0)
            return
        waking = asyncio.get_running_loop().create_future()
        heapq.heappush(
            self._sleeping, (self.now + seconds, next(self._order), waking),
        )
        await waking

    def _soonest(self) -> float | None:
        while self._sleeping and self._sleeping[0][2].done():
            heapq.heappop(self._sleeping)
        return self._sleeping[0][0] if self._sleeping else None

    async def _settle(self) -> None:
        """Until nothing else can run, and again after a moment of real
        time: what a thread or a child hands back comes in through the
        loop's selector."""
        loop = asyncio.get_running_loop()
        for moment in (0.0005, 0.0):
            quiet = 0
            while quiet < 3:
                await asyncio.sleep(0)
                quiet = 0 if getattr(loop, '_ready', ()) else quiet + 1
            if moment:
                await asyncio.sleep(moment)

    async def drive(self, until: float | None = None) -> None:
        """Move the clock whenever everything waits, to the soonest
        sleep's end; up to `until`, where it stops, the clock there."""
        while True:
            await self._settle()
            if self._threads:
                await asyncio.sleep(0.001)
                continue
            soonest = self._soonest()
            if soonest is None or (until is not None and soonest > until):
                if until is not None:
                    self.now = max(self.now, until)
                    return
                await asyncio.sleep(0.005)
                continue
            self.now = max(self.now, soonest)
            while self._sleeping and self._sleeping[0][0] <= self.now:
                _, _, waking = heapq.heappop(self._sleeping)
                if not waking.done():
                    waking.set_result(None)

    async def run_for(self, seconds: float) -> None:
        """`drive`, for `seconds` of the clock's time from now."""
        await self.drive(self.now + seconds)


def ran(
    clock: VirtualClock, seconds: float,
    *tasks: Callable[[], Awaitable[None]],
) -> None:
    """`tasks`, each a task of its own, while the clock runs for
    `seconds`; then whatever is still waiting, cancelled."""
    async def running() -> None:
        started = [asyncio.ensure_future(task()) for task in tasks]
        await clock.run_for(seconds)
        for task in started:
            task.cancel()
        await asyncio.gather(*started, return_exceptions=True)

    asyncio.run(running())


class TestTheClock:
    def test_sleeps_end_in_the_order_their_ends_fall_each_at_its_time(
        self,
    ):
        clock = VirtualClock()
        woke: list[tuple[str, float]] = []

        def sleeper(
            name: str, seconds: float,
        ) -> Callable[[], Awaitable[None]]:
            async def sleeping() -> None:
                await clock.sleep(seconds)
                woke.append((name, clock.now - START))
            return sleeping

        ran(clock, 3600, sleeper('long', 30), sleeper('short', 10))

        assert woke == [('short', 10), ('long', 30)]
        assert clock.now == START + 3600

    def test_stops_where_it_was_told_and_goes_on_from_there(self):
        clock = VirtualClock()
        woke: list[float] = []

        async def sleeping() -> None:
            await clock.sleep(100)
            woke.append(clock.now - START)

        async def both() -> None:
            waiting = asyncio.ensure_future(sleeping())
            await clock.run_for(40)
            assert (woke, clock.now) == ([], START + 40)
            await clock.run_for(60)
            await waiting

        asyncio.run(both())
        assert woke == [100]

    def test_waits_for_work_in_a_thread_before_it_moves(self):
        """The process walks the store in a thread: a sleep does not end
        while that walk is under way, however soon it is due."""
        clock = VirtualClock()
        said: list[str] = []

        def walk() -> None:
            time.sleep(0.05)
            said.append('walked')

        async def walking() -> None:
            await clock.to_thread(walk)

        async def sleeping() -> None:
            await clock.sleep(1)
            said.append('slept')

        ran(clock, 10, walking, sleeping)

        assert said == ['walked', 'slept']
