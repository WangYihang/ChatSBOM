"""Exit the process when its event loop stops ticking (#128, section 2.5).

A wedged server does not exit, so nothing restarts it. The Worker's
runtime came back from a crash, measured on a live outage, wedged:
`wrangler dev` said it was ready while `GET /` and `POST /api/q`
accepted the connection and never answered, and `restart:
unless-stopped` acts only on an exit. So its container's entrypoint
probed the Worker from a shell loop, and killed it after four failed
probes, until #151 deleted both.

Here a thread watches the event loop instead. The loop ticks every
second; when it has not for STALL_SECONDS, the thread says where the
loop is stuck and exits the process, with `os._exit`: a stalled loop
cannot run a clean shutdown, and `sys.exit` from a thread would end
only the thread. The restart policy does the rest.
"""
import asyncio
import os
import sys
import threading
import time
import traceback
from collections.abc import Callable

import structlog

logger = structlog.get_logger('watchdog')

#: How long the loop may go without ticking. Nothing the service does
#: holds the loop for more than a moment: it waits, and what blocks
#: runs in a thread.
STALL_SECONDS = 60

#: How often the loop ticks, and the watch looks.
TICK_SECONDS = 1.0


class Watchdog:
    """Watches the loop that runs `watch`, and ends the process with
    `exit(1)` once it has not ticked for `timeout` seconds."""

    def __init__(
        self,
        timeout: float = STALL_SECONDS,
        *,
        interval: float = TICK_SECONDS,
        exit: Callable[[int], object] = os._exit,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self.timeout = timeout
        self._interval = interval
        self._exit = exit
        self._clock = clock
        self._ticked = clock()
        self._loop_thread: int | None = None

    async def watch(self) -> None:
        """Tick until cancelled, with a thread watching the ticks. The
        watch ends with it: an exit after the service has stopped would
        take a clean stop for a wedge."""
        self._loop_thread = threading.get_ident()
        self._ticked = self._clock()
        stop = threading.Event()
        threading.Thread(
            target=self._watch, args=(stop,), name='event-loop-watchdog',
            daemon=True,
        ).start()
        try:
            while True:
                self._ticked = self._clock()
                await asyncio.sleep(self._interval)
        finally:
            stop.set()

    def _watch(self, stop: threading.Event) -> None:
        looked = self._clock()
        while not stop.wait(self._interval):
            now = self._clock()
            # The watch itself slept far past its interval: the whole
            # process was stopped, by `docker pause` or a SIGSTOP, while
            # the clock went on. The loop had no chance to tick either,
            # and whichever thread wakes first, that time is not held
            # against it. A loop that stalls leaves this thread running,
            # which sees it: a blocked loop holds no lock this needs.
            if now - looked > self._interval + self.timeout / 2:
                self._ticked = now
            looked = now
            stalled = now - self._ticked
            if stalled > self.timeout:
                self._stalled(stalled)
                return

    def _stalled(self, seconds: float) -> None:
        """Say where the loop is, and end the process."""
        frame = sys._current_frames().get(self._loop_thread or -1)
        logger.critical(
            'event loop stalled',
            stalled_seconds=round(seconds, 1),
            stack=''.join(traceback.format_stack(frame)) if frame else '',
        )
        sys.stderr.flush()
        self._exit(1)
