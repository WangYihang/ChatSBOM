"""The resolver as a service (#168): passes over what is due, and a sleep
while nothing is.

`chatsbom sbom lock` runs it, and compose's `resolver` runs that. A pass
walks the universe in the store for the directories due (`due.walk`),
sets the sandbox up for them (`sandbox.sweep`, `sandbox.prepare`), and
resolves them, `workers` at once, each in a container of its own whose
one way out is its proxy (`sandbox.generate_lockfile`). What became of
each is kept in resolver.sqlite (`state`): a failure with its backoff,
and a result by forgetting the failure before it. What the proxy
refused is logged with the repository and the directory that asked.

What the project did not do is not kept against it: a resolution told
to stop, and one the sandbox could not run (Docker, the network or the
proxy failed before the project's code ran). The first of those stops
the pass: the next resolution would fail the same way.

Without `--once`, a pass follows a pass that resolved or failed
anything, since `--limit` may have cut it short and more may have
become due meanwhile; and after one that found nothing due, or that the
sandbox stopped, the loop sleeps CHATSBOM_RESOLVE_INTERVAL (an hour,
said as the collector's intervals are: `90m`, `6h`, `1d`). So a
directory the collector makes due waits an hour at most, as does one
whose backoff ends.

A stop (SIGTERM, which `docker compose stop` sends) ends it at once:
each resolution in flight is told (`cancel`), and its container is
removed, then its proxy and its network, as a run cut short is; none is
started after; nothing is kept of what was stopped; and a lockfile is
written whole or not at all (`fs.atomic_write_bytes`). So nothing is
half-written, and the stop takes the few seconds `docker rm -f` does,
well within compose's grace.
"""
from __future__ import annotations

import contextlib
import os
import threading
from collections.abc import Callable
from collections.abc import Collection
from collections.abc import Mapping
from collections.abc import Sequence
from concurrent.futures import as_completed
from concurrent.futures import Future
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

import structlog
from rich.progress import BarColumn
from rich.progress import MofNCompleteColumn
from rich.progress import SpinnerColumn
from rich.progress import TaskProgressColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn
from rich.progress import TimeRemainingColumn

from chatsbom.collector.settings import interval
from chatsbom.collector.state import FAILED
from chatsbom.collector.state import NOTHING
from chatsbom.core.config import PathConfig
from chatsbom.core.logging import progress_bar
from chatsbom.core.sandbox import _excerpt
from chatsbom.core.sandbox import generate_lockfile
from chatsbom.core.sandbox import LockResult
from chatsbom.core.sandbox import prepare
from chatsbom.core.sandbox import SandboxError
from chatsbom.core.sandbox import SandboxLimits
from chatsbom.core.sandbox import sweep
from chatsbom.resolver.due import Due
from chatsbom.resolver.due import Walk
from chatsbom.resolver.due import walk
from chatsbom.resolver.state import ResolverState

logger = structlog.get_logger('resolver')

#: How long the loop sleeps while nothing is due.
DEFAULT_INTERVAL = timedelta(hours=1)


def resolve_interval(environ: Mapping[str, str] | None = None) -> timedelta:
    """CHATSBOM_RESOLVE_INTERVAL, from `environ`, the process's own unless
    given; `SettingsError` for what is no interval."""
    if environ is None:
        environ = os.environ
    return interval(
        'CHATSBOM_RESOLVE_INTERVAL', environ.get('CHATSBOM_RESOLVE_INTERVAL'),
        DEFAULT_INTERVAL,
    )


def _now() -> datetime:
    return datetime.now(timezone.utc)


@dataclass
class Passed:
    """What a pass did."""

    #: What its walk of the universe found.
    walk: Walk
    #: Directories resolved.
    resolved: int = 0
    #: Directories whose failure is kept, with its backoff.
    failed: int = 0
    #: Why the sandbox stopped the pass, if it did.
    halted: str | None = None

    @property
    def worked(self) -> bool:
        """Whether it came to a verdict on any directory."""
        return bool(self.resolved or self.failed)


def _resolve(
    due: Due,
    limits: SandboxLimits,
    stop: threading.Event,
    halted: threading.Event,
) -> LockResult | None:
    """One directory, in a worker; None when it was not started, the
    pass being stopped, or the sandbox having failed."""
    if stop.is_set() or halted.is_set():
        return None
    result = generate_lockfile(
        due.target.ecosystem, due.project, due.output, limits, cancel=stop,
    )
    if result.sandbox_failed:
        # Before the next job this worker takes: it would fail the same.
        halted.set()
    return result


def _where(due: Due) -> dict[str, object]:
    """What a log line says of the directory it is about."""
    return {
        'repository': due.full_name, 'repository_id': due.repository_id,
        'sha': due.sha, 'directory': due.target.directory or '.',
        'ecosystem': due.target.ecosystem,
    }


def _kept(
    state: ResolverState, due: Due, result: LockResult, now: datetime,
) -> str:
    """What became of one resolution, kept: `resolved`, `failed`, or
    `unrun` for what the project did not do."""
    for event in result.refused:
        logger.warning(
            'Egress refused', **_where(due),
            reason=event.get('reason'), request=event.get('request'),
            detail=event.get('detail'),
        )
    if result.ok:
        state.resolved(due.repository_id, due.sha, due.target)
        logger.info(
            'Lockfile resolved', **_where(due),
            files=[path.name for path in result.produced],
        )
        return 'resolved'
    if result.cancelled or result.sandbox_failed:
        return 'unrun'
    kind = NOTHING if result.returncode == 0 else FAILED
    failure = state.failed(
        due.repository_id, due.sha, due.target, kind, now=now,
        detail=_excerpt(result.stderr),
    )
    logger.info(
        'Lockfile not resolved', **_where(due), kind=kind,
        returncode=result.returncode, due_at=failure.due_at.isoformat(),
    )
    return 'failed'


def _resolve_all(
    state: ResolverState,
    due: Sequence[Due],
    limits: SandboxLimits,
    workers: int,
    stop: threading.Event,
    clock: Callable[[], datetime],
) -> Passed:
    """Every directory of `due`, `workers` at once, and what became of
    each, kept as it comes, in this thread, which alone writes
    resolver.sqlite.

    Ctrl-C reaches this thread alone, so the resolutions in flight in the
    others are told (`stop`), and each removes its container before this
    raises: left alone they would run to their deadline, and their
    containers with them.
    """
    passed = Passed(Walk())
    halted = threading.Event()
    failures: list[Path] = []
    pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix='lock')
    try:
        with progress_bar(
            SpinnerColumn(),
            TextColumn('[progress.description]{task.description}'),
            BarColumn(),
            TaskProgressColumn(),
            MofNCompleteColumn(),
            TextColumn('•'),
            TimeElapsedColumn(),
            TextColumn('•'),
            TimeRemainingColumn(),
        ) as progress:
            task = progress.add_task('Locking...', total=len(due))
            futures: dict[Future[LockResult | None], Due] = {
                pool.submit(_resolve, job, limits, stop, halted): job
                for job in due
            }
            for future in as_completed(futures):
                job = futures[future]
                progress.advance(task)
                try:
                    result = future.result()
                except Exception as error:  # noqa: BLE001 - kept, not raised
                    logger.exception(
                        'Lockfile generation failed', **_where(job),
                    )
                    result = LockResult(
                        produced=(), returncode=1,
                        stderr=f'{type(error).__name__}: {error}',
                    )
                if result is None:
                    continue
                if result.sandbox_failed and passed.halted is None:
                    passed.halted = result.stderr
                    logger.error(
                        'The sandbox cannot run a resolution: the pass stops',
                        **_where(job), error=result.stderr,
                    )
                became = _kept(state, job, result, clock())
                if became == 'resolved':
                    passed.resolved += 1
                elif became == 'failed':
                    passed.failed += 1
                    failures.append(job.output)
    except BaseException:
        stop.set()
        raise
    finally:
        pool.shutdown(wait=True, cancel_futures=True)

    # Nothing half-made is left: the directory of a failed resolution
    # goes, if it holds nothing. Only once every worker is done, since
    # two recipes can share a directory, and the other may be writing to
    # it.
    for output in failures:
        with contextlib.suppress(OSError):
            output.rmdir()
    return passed


def run_pass(
    paths: PathConfig,
    state: ResolverState,
    *,
    stop: threading.Event,
    limits: SandboxLimits,
    workers: int = 1,
    ecosystems: Collection[str] | None = None,
    names: Sequence[str] | None = None,
    limit: int | None = None,
    force: bool = False,
    clock: Callable[[], datetime] = _now,
) -> Passed:
    """One pass: what is due, resolved, and what became of each kept.

    `stop`, once set, ends it as the deadline ends a resolution, and is
    what the loop stops on (`serve`). The sandbox is set up only when
    something is due: while nothing is, Docker is not asked anything.
    """
    now = clock()
    state.forget(now=now)
    found = walk(
        paths, state, now=now, ecosystems=ecosystems, names=names,
        limit=limit, force=force,
    )
    if found.universe is None:
        logger.warning(
            'No universe yet: nothing is due until a search snapshot is '
            'complete',
            search_dir=str(paths.search_dir),
        )
    if found.unknown:
        logger.warning(
            'Not in the universe, left out', count=len(found.unknown),
            first=found.unknown[:10],
        )
    if not found.due or stop.is_set():
        return Passed(found)
    try:
        sweep()
        prepare({due.target.recipe for due in found.due})
    except SandboxError as error:
        return Passed(found, halted=str(error))
    passed = _resolve_all(state, found.due, limits, workers, stop, clock)
    passed.walk = found
    return passed


def serve(
    run: Callable[[], Passed],
    *,
    interval: timedelta,
    stop: threading.Event,
    once: bool = False,
) -> None:
    """Runs a pass, and another at once after one that came to a verdict
    on anything; after one that found nothing due, or that the sandbox
    stopped, sleeps `interval` first. Until `stop` is set; or, `once`,
    after one pass."""
    while not stop.is_set():
        passed = run()
        if once or stop.is_set():
            return
        if passed.halted is not None:
            logger.error(
                'The sandbox cannot be set up: trying again after the '
                'interval', error=passed.halted,
                interval=f'{interval.total_seconds():.0f}s',
            )
        elif passed.worked:
            continue
        stop.wait(interval.total_seconds())
