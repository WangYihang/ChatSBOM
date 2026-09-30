"""The index pass of `chatsbom collect` (#171; #128 sections 2.3 and 2.4):
what the site serves, made again from what was collected.

In order, each a step as `deploy/collector-loop.sh` ran them:

1. `warehouse build`: the DuckDB warehouse, from the store alone;
2. `snapshot build`: the serving snapshot of it, published only when
   what it serves changed;
3. `export parquet --output data/export`, the weekly export `web`
   serves (#154): when the last is `EXPORT_EVERY` old by its manifest's
   age, or there is none, and once there is a warehouse to export;
4. `data prune --keep 2 --apply`: the scans past the two newest of each
   repository, and the decisions they no longer need, go.

Each step is a child process, the CLI's own command: DuckDB's memory
goes back to the system when it exits, and one that fails or crashes is
said in the log, and the next steps run all the same. A warehouse build
that fails leaves the last warehouse in place, and a snapshot of that
is the snapshot already published. A step that runs past `STEP_TIMEOUT`
is stopped, as a pass given up on, the collector stopping, stops it:
TERM to its process group, and after `KILL_AFTER`, KILL. What a step
stopped leaves is the next pass's to clear, as each step clears its
own: a warehouse's `.building` file and DuckDB's spill.

The process decides when (`process.py`): once something was collected
since the last pass, at most once per CHATSBOM_INDEX_INTERVAL.
"""
from __future__ import annotations

import asyncio
import contextlib
import os
import signal
import sys
import time
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

import structlog

from chatsbom.core.config import PathConfig

logger = structlog.get_logger('collector.index')

#: How old the export may be before the next: a week, by its manifest.
EXPORT_EVERY = timedelta(days=7)

#: Scans kept of each repository, and release decisions: the collector
#: loop's PRUNE_KEEP.
KEEP = 2

#: How long a step may run: a warehouse build of the whole corpus took
#: 13 minutes, and a snapshot 3 (DEPLOY.md).
STEP_TIMEOUT = timedelta(hours=2)

#: How long a step told to stop has to exit before it is killed.
KILL_AFTER = 10.0

#: The CLI, as a step runs it: this interpreter's.
CLI = (sys.executable, '-m', 'chatsbom')


def export_dir(paths: PathConfig) -> Path:
    """Where the export goes, and `web` reads it."""
    return paths.base_data_dir / 'export'


@dataclass(frozen=True)
class StepRun:
    """One step, and how it ended."""

    step: str
    #: Its exit status; None when it was stopped for its time.
    status: int | None
    seconds: float

    @property
    def ok(self) -> bool:
        return self.status == 0


@dataclass
class IndexRun:
    """One pass: each step, as it ended."""

    started_at: datetime
    ran: list[StepRun] = field(default_factory=list)

    @property
    def failed(self) -> list[str]:
        return [step.step for step in self.ran if not step.ok]


def export_due(paths: PathConfig, now: datetime) -> bool:
    """Whether the export is due: its manifest is `EXPORT_EVERY` old, or
    there is none; never with no warehouse to export."""
    if not paths.warehouse_path.is_file():
        return False
    try:
        written = (export_dir(paths) / 'manifest.json').stat().st_mtime
    except OSError:
        return True
    return datetime.fromtimestamp(written, timezone.utc) + EXPORT_EVERY <= now


class IndexPass:
    """The index pass over the store at `paths`, run by `cli`."""

    def __init__(
        self,
        paths: PathConfig,
        *,
        cli: Sequence[str] = CLI,
        environ: Mapping[str, str] | None = None,
        clock: Callable[[], float] = time.time,
        step_timeout: timedelta = STEP_TIMEOUT,
        kill_after: float = KILL_AFTER,
    ) -> None:
        self.paths = paths
        self.cli = tuple(cli)
        self._environ = environ
        self._clock = clock
        self.step_timeout = step_timeout
        self.kill_after = kill_after

    def now(self) -> datetime:
        return datetime.fromtimestamp(self._clock(), timezone.utc)

    def last_at(self) -> datetime | None:
        """When the last pass built the warehouse, as far as the store
        says: none, with no warehouse."""
        try:
            built = self.paths.warehouse_path.stat().st_mtime
        except OSError:
            return None
        return datetime.fromtimestamp(built, timezone.utc)

    def steps(self) -> list[tuple[str, list[str]]]:
        """The steps, each with its arguments; the export's asked at its
        turn, once the warehouse the pass builds is there."""
        return [
            ('warehouse build', ['warehouse', 'build']),
            ('snapshot build', ['snapshot', 'build']),
            (
                'export parquet',
                ['export', 'parquet', '--output', str(export_dir(self.paths))],
            ),
            ('data prune', ['data', 'prune', '--keep', str(KEEP), '--apply']),
        ]

    async def run(self) -> IndexRun:
        """The pass, each step in turn; one line in the log for all."""
        done = IndexRun(started_at=self.now())
        started = time.monotonic()
        for name, args in self.steps():
            if name == 'export parquet' and not export_due(
                self.paths, self.now(),
            ):
                continue
            done.ran.append(await self._step(name, args))
        logger.info(
            'Index pass',
            steps=' '.join(
                f'{step.step.replace(" ", "-")}:'
                + (
                    'ok' if step.ok else
                    'timeout' if step.status is None else
                    f'exit-{step.status}'
                )
                for step in done.ran
            ),
            failed=len(done.failed),
            took=f'{time.monotonic() - started:.0f}s',
        )
        return done

    async def _step(self, name: str, args: list[str]) -> StepRun:
        started = time.monotonic()
        process = await asyncio.create_subprocess_exec(
            *self.cli, *args,
            stdin=asyncio.subprocess.DEVNULL,
            env=dict(os.environ if self._environ is None else self._environ),
            start_new_session=True,
        )
        try:
            status: int | None = await asyncio.wait_for(
                process.wait(), self.step_timeout.total_seconds(),
            )
        except TimeoutError:
            await self._stop(process)
            status = None
        except BaseException:
            # Given up on: the collector stops.
            await self._stop(process)
            raise
        taken = StepRun(name, status, time.monotonic() - started)
        if not taken.ok:
            logger.warning(
                'An index step failed: the next steps run all the same, '
                'and the next pass tries it again',
                step=name,
                status='timeout' if status is None else status,
                took=f'{taken.seconds:.0f}s',
            )
        return taken

    async def _stop(self, process: asyncio.subprocess.Process) -> None:
        """TERM to the step's process group, and KILL after `kill_after`
        seconds; waited for either way."""
        _signal(process, signal.SIGTERM)
        try:
            await asyncio.shield(
                asyncio.wait_for(process.wait(), self.kill_after),
            )
        except TimeoutError:
            pass
        finally:
            # What it started goes too, whether it exited or not.
            _signal(process, signal.SIGKILL)
            with contextlib.suppress(ProcessLookupError):
                await asyncio.shield(process.wait())


def _signal(process: asyncio.subprocess.Process, number: int) -> None:
    with contextlib.suppress(ProcessLookupError, PermissionError):
        os.killpg(process.pid, number)
