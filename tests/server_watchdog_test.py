"""The web service watches its own event loop (#134).

A wedged server does not exit, so nothing restarts it: the Worker's
runtime came back from a crash accepting connections and answering
none, and `restart: unless-stopped` acts only on an exit. So its
container's entrypoint probed the Worker from a shell loop and killed
it after four failed probes. In the Python service a thread watches the
event loop tick instead, and exits the process when it has not for 60
seconds (#128, section 2.5).
"""
import asyncio
import subprocess
import sys
import threading
import time
from collections.abc import Callable
from pathlib import Path

import structlog

from chatsbom.server.watchdog import STALL_SECONDS
from chatsbom.server.watchdog import Watchdog

ROOT = Path(__file__).resolve().parents[1]

#: Short enough for a test, and several ticks long.
TIMEOUT = 0.5
INTERVAL = 0.02


class Exits:
    """Stands in for `os._exit`, keeping what it was asked."""

    def __init__(self) -> None:
        self.statuses: list[int] = []
        self.called = threading.Event()

    def __call__(self, status: int) -> None:
        self.statuses.append(status)
        self.called.set()


def watchdog(exits: Exits, **options: Callable[[], float]) -> Watchdog:
    return Watchdog(TIMEOUT, interval=INTERVAL, exit=exits, **options)


async def watched(
    dog: Watchdog, body: Callable[[], None], *, then: float = 5 * INTERVAL,
) -> None:
    """Run `body` on the loop while `dog` watches it, let the loop run
    for `then` seconds more, then stop it."""
    task = asyncio.create_task(dog.watch())
    await asyncio.sleep(5 * INTERVAL)
    body()
    await asyncio.sleep(then)
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass


def test_waits_a_minute_by_default():
    assert STALL_SECONDS == 60
    assert Watchdog().timeout == 60


def test_leaves_a_loop_that_keeps_ticking_alone():
    exits = Exits()
    asyncio.run(
        watched(watchdog(exits), lambda: None, then=3 * TIMEOUT),
    )
    assert exits.statuses == []


def stalling(exits: Exits) -> Callable[[], None]:
    """What wedges the loop: a call that blocks it, as a synchronous
    query in a coroutine would, for longer than the watchdog waits.
    Blocked until the stand-in exit is called, which a real one would
    not return from, or three times the wait."""
    def stall() -> None:
        exits.called.wait(timeout=3 * TIMEOUT)
    return stall


def test_exits_the_process_when_the_loop_stalls():
    exits = Exits()
    asyncio.run(watched(watchdog(exits), stalling(exits)))
    assert exits.statuses == [1]


def test_says_where_the_loop_is_stuck():
    exits = Exits()
    with structlog.testing.capture_logs() as logged:
        asyncio.run(watched(watchdog(exits), stalling(exits)))

    [event] = [e for e in logged if e['event'] == 'event loop stalled']
    assert event['log_level'] == 'critical'
    assert event['stalled_seconds'] >= TIMEOUT
    # The loop's own thread, stopped in the call that blocks it.
    assert 'in stall\n' in event['stack']
    assert 'exits.called.wait' in event['stack']


def test_stops_watching_once_the_loop_stops_it():
    """When the service shuts down, the watch ends with it: an exit
    after that would take a clean stop for a wedge."""
    exits = Exits()
    before = set(threading.enumerate())
    asyncio.run(watched(watchdog(exits), lambda: None))

    left = [t for t in set(threading.enumerate()) - before if t.is_alive()]
    for thread in left:
        thread.join(timeout=2)
    assert [t.name for t in left if t.is_alive()] == []
    time.sleep(2 * TIMEOUT)
    assert exits.statuses == []


def test_a_frozen_process_is_not_a_stalled_loop():
    """`docker pause`, or a SIGSTOP, stops the watching thread with the
    loop, and the clock goes on. Whichever wakes first, the loop had no
    chance to tick: the time the watch itself was stopped is not held
    against it."""
    exits = Exits()
    offset = [0.0]

    def clock() -> float:
        return time.monotonic() + offset[0]

    def freeze() -> None:
        # The clock jumps while the loop is blocked, for less than the
        # watchdog waits, so that only the jump could look like a stall.
        offset[0] += 100
        time.sleep(TIMEOUT / 2)

    asyncio.run(watched(watchdog(exits, clock=clock), freeze))
    assert exits.statuses == []


def test_ends_a_real_process_that_stalls(tmp_path):
    """With `os._exit`: a stalled loop cannot run a clean shutdown, and
    `sys.exit` from a thread would end only the thread."""
    started = time.monotonic()
    result = subprocess.run(
        [sys.executable, '-c', STALLS],
        cwd=ROOT, capture_output=True, text=True, timeout=60,
    )
    assert result.returncode == 1
    assert 'event loop stalled' in result.stderr
    # Not the 30 s the loop was blocked for.
    assert time.monotonic() - started < 20


#: A service whose loop blocks for 30 s under a watchdog that waits 0.5.
STALLS = """
import asyncio
import time

from chatsbom.core.logging import setup_logging
from chatsbom.server.watchdog import Watchdog

setup_logging('INFO')


async def main():
    asyncio.create_task(Watchdog(0.5, interval=0.05).watch())
    await asyncio.sleep(0.2)
    time.sleep(30)


asyncio.run(main())
"""
