"""`chatsbom collect`: the collector, one long-running process (#171, part
6e of #155; #128 section 2.1).

It holds collector.sqlite, whose lock says so to anything else, and one
set of tools (`runner.tools_for`): the budget of every token, the GitHub
client on it, raw content, git and the Syft pool. Its parts run beside
each other on them, each a task:

- **The universe** (#160), searched again when due
  (CHATSBOM_UNIVERSE_INTERVAL, a week): about 26 minutes of the search
  bucket, the first time before anything else can start.
- **The sweep** (#160), when due (CHATSBOM_SWEEP_INTERVAL, an hour):
  GraphQL `nodes(ids:)` over the universe, which says what changed.
- **The collections** (#161): the stages due of each repository
  (`runner.collect`), CHATSBOM_REPOSITORIES_AT_ONCE of them at once,
  each by one task, and none by two. Which ones, highest priority first:
  what detection found (`due.detected`), the changed, each once its last
  collection is CHATSBOM_RECOLLECT_INTERVAL old (a week, #188), and then
  the never collected; then what a walk of the universe in the store finds
  (`due.walk_universe`), paged, a stage due again once its backoff has
  passed, before the never collected, and a rescan for a tool's new
  version, after them. A stage backing off waits in collector.sqlite,
  where the walk finds it again: nothing of it is held here. The walk
  goes round again an interval of the sweep's after it ends.
- **The dependency graph** (#162): one step at a time, then a sleep
  until the step says the next is due, or until a sweep ends, since a
  push it saw can make a graph due sooner. It steps over the universe as
  last observed, read again after each sweep, not for every step.
- **The index pass** (`index.py`): once something was collected since
  the last, at most once per CHATSBOM_INDEX_INTERVAL (a day), counted
  from when the last built the warehouse.

**The budget.** The parts' buckets are apart: the universe's search, the
sweep's GraphQL, the stages' REST and the graph's own, so a collection
cannot spend what detection needs. A token's four requests in flight are
one for them all, and leases are granted in order of priority where they
wait for the same room (`budget.at_priority`): detection first, then the
collections, a change's before a new repository's before a rescan's, and
the dependency graph last. A bucket GitHub refuses holds back only the
leases of that bucket: the rest go on.

**Stopping.** On SIGTERM or SIGINT it takes no more work. What detection,
the graph and the index pass have in flight is given up at once: a sweep
goes on where it was, a refresh of the universe leaves the last one
standing, a report pending stays pending, and an index step is stopped
with what it started. A collection in flight has `FINISH` seconds to end,
and is then given up on too, its git and its Syft killed with what they
started. Each stage writes whole or not at all, and collector.sqlite by
transactions, so nothing is left half written, and what was given up is
due again at the next start. A second signal gives up everything at once.

**Health** (`health.py`): the heartbeat, which compose's healthcheck
reads, says each part's state every `TICK`: idle until it is due, or
busy with a deadline far past what its work takes.

**Logs:** one line per sweep, per search of the universe and per index
pass, their own; one per repository collected, here; and what goes
wrong. No token is in any of them: each is a token's label where one is
named, and every error is scrubbed before it is said.

A part that fails is said, and tried again a little later. What no
retry mends stops the process, with status 1, for its restart policy
and a person to see: every token refused (`Unauthorized`), and a part
that ends when it never should.
"""
from __future__ import annotations

import asyncio
import functools
import signal
import time
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Coroutine
from collections.abc import Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any
from typing import Protocol

import httpx2
import structlog

from chatsbom.collector import runner
from chatsbom.collector.budget import at_priority
from chatsbom.collector.depgraph import Depgraph
from chatsbom.collector.depgraph import DepgraphSettings
from chatsbom.collector.depgraph import Step
from chatsbom.collector.due import Candidate
from chatsbom.collector.due import detected
from chatsbom.collector.due import Priority
from chatsbom.collector.due import read_page
from chatsbom.collector.due import UniverseWalk
from chatsbom.collector.due import walk_page
from chatsbom.collector.errors import Unauthorized
from chatsbom.collector.gitremote import GITHUB
from chatsbom.collector.health import Heartbeat
from chatsbom.collector.health import heartbeat_path
from chatsbom.collector.health import TICK
from chatsbom.collector.index import export_dir
from chatsbom.collector.index import IndexPass
from chatsbom.collector.index import IndexRun
from chatsbom.collector.index import STEP_TIMEOUT
from chatsbom.collector.runner import Collected
from chatsbom.collector.runner import DONE
from chatsbom.collector.settings import CollectorSettings
from chatsbom.collector.stages import Tools
from chatsbom.collector.state import CollectorState
from chatsbom.collector.state import Observed
from chatsbom.collector.state import Sweep
from chatsbom.collector.sweep import Sweeper
from chatsbom.collector.syftpool import SyftSettings
from chatsbom.collector.universe import Refreshed
from chatsbom.collector.universe import Universe
from chatsbom.core.config import PathConfig

logger = structlog.get_logger('collector')

#: The order leases are granted in, the lowest first: detection, then
#: each collection at its `due.Priority`, then the dependency graph.
DETECTION = 0
GRAPH = max(Priority) + 1

#: Seconds a collection in flight has to end once the process is told
#: to stop, before it is given up on. Compose gives the process 30.
FINISH = 10.0

#: Members of the universe a page of its walk reads: the collections are
#: woken by each, and never wait for one (#193).
PAGE = 200

#: Rescans a walk keeps in hand while the collections have more urgent
#: work; the rest are found again the next time round.
RESCANS_KEPT = 100

#: How long a part that failed waits before it tries again.
AGAIN_AFTER = timedelta(minutes=5)

#: A repository whose collection failed of itself, not of a stage, which
#: collector.sqlite would keep, is left this long: long enough that a
#: fault that persists costs little.
COOLING = timedelta(minutes=15)

#: The least between two steps of the dependency graph.
GRAPH_PAUSE = timedelta(seconds=1)

#: Members of the universe read at a time as last observed, for the
#: graph, the loop going on between pages: a hundred thousand of them
#: take seconds to read.
OBSERVED_PAGE = 5_000

#: How long each part's unit of work may take before it is taken for
#: wedged (`health.py`): far past what each takes, a wait for a rate
#: limit's window included.
UNIVERSE_DEADLINE = timedelta(hours=6)
SWEEP_DEADLINE = timedelta(hours=2)
COLLECTION_DEADLINE = timedelta(hours=3)
GRAPH_DEADLINE = timedelta(hours=2)
WALK_DEADLINE = timedelta(minutes=30)
INDEX_DEADLINE = 4 * STEP_TIMEOUT + timedelta(minutes=10)


class Searching(Protocol):
    """The universe's search, as the process drives it."""

    def load(self) -> object:
        ...

    def next_due(self, every: timedelta) -> datetime:
        ...

    async def refresh(self) -> Refreshed:
        ...


class Sweeping(Protocol):
    def next_due(self, every: timedelta) -> datetime | None:
        ...

    async def run(self, *, wait: float | None = None) -> Sweep | None:
        ...


class Graphing(Protocol):
    async def step(self, repositories: Iterable[Observed]) -> Step:
        ...


class Indexing(Protocol):
    def last_at(self) -> datetime | None:
        ...

    async def run(self) -> IndexRun:
        ...


#: One repository's collection, for the push it was observed with.
Collecting = Callable[[Observed, Priority], Awaitable[Collected]]


@dataclass(frozen=True)
class Parts:
    """What the process runs: detection, the stages, the graph and the
    index pass, and the Syft the walk judges SBOMs by."""

    universe: Searching
    sweeper: Sweeping
    depgraph: Graphing
    index: Indexing
    collect: Collecting
    syft_version: Callable[[], Awaitable[str | None]]


class Collector:
    """The process's parts, on one collector.sqlite and one clock."""

    def __init__(
        self,
        paths: PathConfig,
        state: CollectorState,
        settings: CollectorSettings,
        parts: Parts,
        *,
        clock: Callable[[], float] = time.time,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        finish: float = FINISH,
        tick: timedelta = TICK,
    ) -> None:
        self.paths = paths
        self.state = state
        self.settings = settings
        self.parts = parts
        self._clock = clock
        self._sleep = sleep
        self.finish = finish
        self.tick = tick
        self.heartbeat = Heartbeat(heartbeat_path(paths.base_data_dir), clock)
        #: Set once it is to stop; `hurry` at a second signal.
        self.stopping = asyncio.Event()
        self.hurry = asyncio.Event()
        #: Why it stopped, where it was not asked to: its status is 1.
        self.failure: str | None = None
        #: Woken: the collections by a sweep, a new universe or a
        #: collection ending; the graph by a sweep; the sweep by a
        #: universe with members; the index pass by what it indexes.
        self._collections = asyncio.Event()
        self._graph = asyncio.Event()
        self._members = asyncio.Event()
        self._written = asyncio.Event()
        #: The collections in flight, by repository; and those whose
        #: collection failed of itself, until when they are left.
        self.running: dict[int, asyncio.Task[None]] = {}
        self._cooling: dict[int, datetime] = {}
        #: The universe as last observed, for the graph: None to be read
        #: again, after a sweep or a search; and how many times it was
        #: let go, which tells a read that one cut across.
        self._observed: list[Observed] | None = None
        self._let_go = 0
        #: The walk of the universe, a task of its own: where it is, what
        #: it found and has not given out, the collections begun since
        #: its page was read, and when it goes round again. Each is
        #: touched on the loop's thread alone, never the walk's.
        self._position = 0
        self._walked: list[Candidate] = []
        self._begun: set[int] = set()
        self._walk_at: datetime | None = None
        self._walk_syft: str | None = None
        self._signals = 0

    def now(self) -> datetime:
        return datetime.fromtimestamp(self._clock(), timezone.utc)

    # -- stopping -----------------------------------------------------------

    def stop(self, reason: str = 'asked to') -> None:
        """Take no more work, and end what is in flight: at once, when
        told a second time."""
        self._signals += 1
        if self._signals == 1:
            logger.info('The collector is stopping', reason=reason)
        else:
            logger.info(
                'The collector is stopping at once', reason=reason,
            )
            self.hurry.set()
        self.stopping.set()

    def _fail(self, why: str) -> None:
        if self.failure is None:
            self.failure = why
        self.stop('failed')

    async def _pause(
        self, until: datetime | None, *wake: asyncio.Event,
    ) -> None:
        """Until `until`, or one of `wake` is set, or the process stops."""
        waits: list[asyncio.Future[Any]] = [
            asyncio.ensure_future(self.stopping.wait()),
            *(asyncio.ensure_future(event.wait()) for event in wake),
        ]
        if until is not None:
            seconds = max((until - self.now()).total_seconds(), 0.0)
            waits.append(asyncio.ensure_future(self._sleep(seconds)))
        try:
            await asyncio.wait(waits, return_when=asyncio.FIRST_COMPLETED)
        finally:
            for waiting in waits:
                waiting.cancel()
            await asyncio.gather(*waits, return_exceptions=True)

    # -- the process --------------------------------------------------------

    async def run(self) -> int:
        """Every part, until it is told to stop or one fails for good:
        0, or 1 when one did."""
        # Where the export goes, which `web` mounts, and does not start
        # without (#154): made now, not by the first export, a
        # warehouse and a week away.
        export_dir(self.paths).mkdir(parents=True, exist_ok=True)
        parts: dict[str, Coroutine[Any, Any, None]] = {
            'universe': self._search(),
            'sweep': self._sweep(),
            'collections': self._collect_all(),
            'walk': self._walk_all(),
            'graph': self._step_graphs(),
            'index': self._index(),
        }
        tasks = {
            name: asyncio.create_task(part, name=f'collector {name}')
            for name, part in parts.items()
        }
        for name, task in tasks.items():
            task.add_done_callback(functools.partial(self._ended, name))
        try:
            while not self.stopping.is_set():
                self._beat()
                await self._pause(self.now() + self.tick)
        finally:
            await self._shut_down(tasks)
            self._beat(stopped=True)
        if self.failure is not None:
            logger.error('The collector stopped', failure=self.failure)
            return 1
        logger.info('The collector stopped')
        return 0

    def _ended(self, name: str, task: asyncio.Task[None]) -> None:
        """A part's task ended: before the process was told to stop, it
        never should have."""
        if task.cancelled() or self.stopping.is_set():
            return
        error = task.exception()
        if isinstance(error, Unauthorized):
            logger.error(
                'GitHub took none of the tokens: the collector stops. Set '
                'GITHUB_TOKEN, and CHATSBOM_GITHUB_TOKENS, to tokens GitHub '
                'takes', part=name, error=str(error),
            )
            self._fail('unauthorized')
            return
        logger.error(
            'A part of the collector ended: the collector stops',
            part=name,
            error=None if error is None else f'{type(error).__name__}: '
            f'{error}',
            exc_info=error,
        )
        self._fail(f'{name} ended')

    def _beat(self, *, stopped: bool = False) -> None:
        try:
            self.heartbeat.write(stopped=stopped)
        except OSError as error:
            logger.warning(
                'The heartbeat could not be written',
                path=str(self.heartbeat.path), error=str(error),
            )

    async def _shut_down(self, tasks: dict[str, asyncio.Task[None]]) -> None:
        """Detection, the graph and the index pass given up at once; the
        collections in flight given `finish` seconds, then given up."""
        for name, task in tasks.items():
            if name != 'collections':
                task.cancel()
        running = list(self.running.values())
        if running and not self.hurry.is_set():
            logger.info(
                'Collections in flight are given time to end',
                running=len(running), seconds=self.finish,
            )
            ended = asyncio.ensure_future(asyncio.wait(running))
            hurry = asyncio.ensure_future(self.hurry.wait())
            try:
                await asyncio.wait(
                    [ended, hurry], timeout=self.finish,
                    return_when=asyncio.FIRST_COMPLETED,
                )
            finally:
                for waiting in (ended, hurry):
                    waiting.cancel()
                await asyncio.gather(ended, hurry, return_exceptions=True)
        given_up = sorted(
            key for key, task in self.running.items() if not task.done()
        )
        if given_up:
            logger.info(
                'Collections given up: each is due again at the next start',
                repositories=given_up,
            )
        everything = [*tasks.values(), *self.running.values()]
        for task in everything:
            task.cancel()
        await asyncio.gather(*everything, return_exceptions=True)

    # -- detection ------------------------------------------------------------

    async def _search(self) -> None:
        """The universe: loaded as it stands, then searched again when
        due."""
        every = self.settings.universe_interval
        universe = self.parts.universe
        with at_priority(DETECTION):
            await self._again(lambda: self._load())
            while not self.stopping.is_set():
                due_at = universe.next_due(every)
                if due_at > self.now():
                    self.heartbeat.idle(
                        'universe', due_at, 'the next search',
                    )
                    await self._pause(due_at)
                    continue
                self.heartbeat.busy(
                    'universe', UNIVERSE_DEADLINE, 'searching',
                )
                try:
                    await universe.refresh()
                except Unauthorized:
                    raise
                except Exception:  # noqa: BLE001 - said by the universe
                    # It waits an hour, and the last universe stands.
                    continue
                self._universe_changed()
                self._written.set()

    async def _load(self) -> None:
        self.parts.universe.load()
        self._universe_changed()

    def _universe_changed(self) -> None:
        self._observed_changed()
        if self.state.members(limit=1):
            self._members.set()
        self._collections.set()

    async def _sweep(self) -> None:
        """The sweep, when due; and after it, the collections and the
        graph woken, for what it found."""
        every = self.settings.sweep_interval
        with at_priority(DETECTION):
            while not self.stopping.is_set():
                due_at = self.parts.sweeper.next_due(every)
                if due_at is None:
                    self._members.clear()
                    self.heartbeat.idle('sweep', None, 'a universe')
                    await self._pause(None, self._members)
                    continue
                if due_at > self.now():
                    self.heartbeat.idle('sweep', due_at, 'the next sweep')
                    await self._pause(due_at)
                    continue
                self.heartbeat.busy('sweep', SWEEP_DEADLINE, 'sweeping')
                swept = await self._again(self.parts.sweeper.run)
                if swept is not None:
                    self._observed_changed()
                    self._graph.set()
                    self._collections.set()

    async def _again(
        self, attempt: Callable[[], Awaitable[Any]],
    ) -> Any:
        """`attempt`, and once more after `AGAIN_AFTER` while it fails:
        what no pause mends goes to the process, which stops."""
        while not self.stopping.is_set():
            try:
                return await attempt()
            except Unauthorized:
                raise
            except Exception as error:  # noqa: BLE001 - said, then again
                logger.warning(
                    'A part of the collector failed: it tries again',
                    error=f'{type(error).__name__}: {error}',
                    again_after=str(AGAIN_AFTER),
                )
                await self._pause(self.now() + AGAIN_AFTER)
        return None

    # -- the dependency graph ---------------------------------------------------

    def _observed_changed(self) -> None:
        self._observed = None
        self._let_go += 1

    async def _universe_observed(self) -> list[Observed]:
        """The universe as last observed, read once between sweeps, a
        page at a time: read again whole where a sweep or a search ended
        while it was read, which would leave it half the one before."""
        while self._observed is None:
            let_go = self._let_go
            found: list[Observed] = []
            while page := self.state.observed_members(
                after=found[-1].repository_id if found else 0,
                limit=OBSERVED_PAGE,
            ):
                found.extend(page)
                await asyncio.sleep(0)
            if let_go == self._let_go:
                self._observed = found
        return self._observed

    async def _graph_step(self) -> Step:
        return await self.parts.depgraph.step(await self._universe_observed())

    async def _step_graphs(self) -> None:
        """A step, then a sleep until the next is due or a sweep ends."""
        with at_priority(GRAPH):
            while not self.stopping.is_set():
                self._graph.clear()
                self.heartbeat.busy('graph', GRAPH_DEADLINE, 'a step')
                step = await self._again(self._graph_step)
                if step is None:
                    continue
                self._said(step)
                if step.stored:
                    self._written.set()
                pause = self.now() + GRAPH_PAUSE
                wake = (
                    None if step.next_at is None
                    else max(step.next_at, pause)
                )
                self.heartbeat.idle('graph', wake, 'the next step')
                await self._pause(pause)
                if wake is None or wake > self.now():
                    await self._pause(wake, self._graph)

    def _said(self, step: Step) -> None:
        """A step that did something, in a line; one that did nothing,
        where debugging asks."""
        counts = {
            'asked': step.asked, 'looked': step.looked,
            'stored': step.stored, 'unchanged': step.unchanged,
            'no_graph': step.no_graph, 'failed': step.failed,
        }
        said = logger.info if any(
            counts[name] for name in (
                'stored', 'unchanged', 'no_graph', 'failed',
            )
        ) else logger.debug
        said(
            'Dependency graphs', **counts, pending=step.pending,
            next_at=(
                None if step.next_at is None
                else f'{step.next_at:%Y-%m-%d %H:%M:%S} UTC'
            ),
        )

    # -- the collections ------------------------------------------------------

    async def _collect_all(self) -> None:
        """Collections started while there are free slots and something
        to collect, highest priority first; then a wait for a slot, a
        sweep, or what the walk finds."""
        while not self.stopping.is_set():
            self._collections.clear()
            free = self.settings.at_once - len(self.running)
            if free > 0:
                for candidate in self._choose(free):
                    self._start(candidate)
            if self.stopping.is_set():
                return
            wake: datetime | None = None
            if len(self.running) < self.settings.at_once:
                # With a slot free: a repository left to cool.
                moments = list(self._cooling.values())
                if any(moment <= self.now() for moment in moments):
                    continue
                wake = min(moments, default=None)
            # Otherwise a slot coming free, or a page walked, wakes it.
            self.heartbeat.idle('collections', wake, 'something to collect')
            await self._pause(wake, self._collections)

    def _choose(self, free: int) -> list[Candidate]:
        """The next `free` repositories to collect: changed, detected and
        walked; never collected; rescans. None in flight or left to
        cool. What the walk found so far is taken as it stands: no page
        of it is waited for (#193)."""
        now = self.now()
        self._cooling = {
            key: until for key, until in self._cooling.items() if until > now
        }
        taken = set(self.running) | set(self._cooling)
        found = [
            candidate
            for candidate in detected(
                self.state, limit=free + len(taken),
                collected_before=now - self.settings.recollect_interval,
            )
            if candidate.observed.repository_id not in taken
        ]
        changed = [c for c in found if c.priority is Priority.CHANGED]
        new = [c for c in found if c.priority is Priority.NEW]
        ordered = changed + [
            candidate for candidate in self._walked
            if candidate.priority is Priority.CHANGED
        ] + new + [
            candidate for candidate in self._walked
            if candidate.priority is Priority.RESCAN
        ]
        chosen: list[Candidate] = []
        for candidate in ordered:
            key = candidate.observed.repository_id
            if key in taken:
                continue
            taken.add(key)
            chosen.append(candidate)
            if len(chosen) == free:
                break
        given = {candidate.observed.repository_id for candidate in chosen}
        self._walked = [
            candidate for candidate in self._walked
            if candidate.observed.repository_id not in given
            and candidate.observed.repository_id not in self.running
        ]
        return chosen

    # -- the walk of the universe ---------------------------------------------

    def _walk_due(self) -> bool:
        return self._walk_at is None or self._walk_at <= self.now()

    async def _walk_all(self) -> None:
        """The walk of the universe, a page at a time, beside the
        collections, which take what it has found so far and are woken
        by each page; round again a sweep's interval after a round
        ends."""
        while not self.stopping.is_set():
            if not self._walk_due():
                self.heartbeat.idle('walk', self._walk_at, 'the next round')
                await self._pause(self._walk_at)
                continue
            await self._again(self._walk)
            self._collections.set()

    async def _walk(self) -> None:
        """A page of the walk of the universe: read of collector.sqlite
        here, on the loop's thread, whose connection it is; walked in the
        store in a thread, which reads nothing of collector.sqlite; and
        what it found kept here, on the loop's thread again."""
        if self._position == 0:
            self._walk_syft = await self.parts.syft_version()
        self._begun.clear()
        page = read_page(self.state, after=self._position, limit=PAGE)
        self.heartbeat.busy('walk', WALK_DEADLINE, 'a page of the walk')
        try:
            walked: UniverseWalk = await asyncio.to_thread(
                walk_page, page, paths=self.paths,
                syft_version=self._walk_syft, now=self.now(),
            )
        finally:
            self.heartbeat.done('walk')
        # A repository collected since the page was read had every stage
        # due collected: what the page says of it is stale.
        self._walked += [
            candidate for candidate in walked.candidates
            if candidate.observed.repository_id not in self._begun
            and candidate.observed.repository_id not in self.running
        ]
        self._position = walked.position
        rescans = [
            c for c in self._walked if c.priority is Priority.RESCAN
        ]
        if len(rescans) > RESCANS_KEPT:
            dropped = {id(c) for c in rescans[RESCANS_KEPT:]}
            self._walked = [c for c in self._walked if id(c) not in dropped]
        if self._position == 0:
            # Round again once a sweep's interval has passed.
            self._walk_at = self.now() + self.settings.sweep_interval
        else:
            self._walk_at = None

    def _start(self, candidate: Candidate) -> None:
        key = candidate.observed.repository_id
        with at_priority(int(candidate.priority)):
            task = asyncio.create_task(
                self._collect(candidate), name=f'collect {key}',
            )
        self.running[key] = task
        self._begun.add(key)

        def ended(task: asyncio.Task[None]) -> None:
            self.running.pop(key, None)
            self.heartbeat.done(f'collect {key}')
            self._collections.set()
            if not task.cancelled() and isinstance(
                task.exception(), Unauthorized,
            ):
                self._ended('collections', task)

        task.add_done_callback(ended)

    async def _collect(self, candidate: Candidate) -> None:
        observed = candidate.observed
        key = observed.repository_id
        self.heartbeat.busy(
            f'collect {key}', COLLECTION_DEADLINE, observed.full_name,
        )
        started = time.monotonic()
        try:
            collected = await self.parts.collect(observed, candidate.priority)
        except Unauthorized:
            raise
        except Exception as error:  # noqa: BLE001 - said, and left to cool
            self._cooling[key] = self.now() + COOLING
            logger.warning(
                'A repository could not be collected: it is left a while',
                repo=observed.full_name, repository_id=key,
                error=f'{type(error).__name__}: {error}',
                again_after=str(COOLING),
            )
            return
        if any(ran.result == DONE for ran in collected.ran):
            self._written.set()
        standing = collected.standing
        logger.info(
            'Repository collected',
            repo=observed.full_name, repository_id=key,
            priority=candidate.priority.name.lower(),
            ran=' '.join(
                f'{ran.stage}:{ran.result}' for ran in collected.ran
            ) or 'nothing',
            current=standing is not None and standing.current,
            took=f'{time.monotonic() - started:.1f}s',
        )

    # -- the index pass -------------------------------------------------------

    async def _index(self) -> None:
        """The index pass, once something was written that it indexes,
        at most once per CHATSBOM_INDEX_INTERVAL."""
        every = self.settings.index_interval
        index = self.parts.index
        last = index.last_at()
        while not self.stopping.is_set():
            if not self._written.is_set():
                self.heartbeat.idle('index', None, 'something to index')
                await self._pause(None, self._written)
                continue
            due_at = self.now() if last is None else last + every
            if due_at > self.now():
                self.heartbeat.idle('index', due_at, 'the next pass')
                await self._pause(due_at)
                continue
            self._written.clear()
            last = self.now()
            self.heartbeat.busy('index', INDEX_DEADLINE, 'a pass')
            await self._again(index.run)
            self.heartbeat.idle('index', None, 'something to index')


# -- the process, as the command runs it ---------------------------------------


@dataclass(frozen=True)
class Upstream:
    """What stands in for GitHub where a test has it: its API, its
    dependency graph's downloads, raw content and git."""

    github: httpx2.AsyncBaseTransport | None = None
    downloads: httpx2.AsyncBaseTransport | None = None
    raw: httpx2.AsyncBaseTransport | None = None
    git_base: str = GITHUB


@asynccontextmanager
async def collector_for(
    paths: PathConfig,
    state: CollectorState,
    settings: CollectorSettings,
    syft: SyftSettings,
    depgraph: DepgraphSettings,
    *,
    index: Indexing | None = None,
    upstream: Upstream = Upstream(),
    clock: Callable[[], float] = time.time,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    finish: float | None = None,
) -> AsyncIterator[Collector]:
    """The collector, on one set of tools, closed after. `finish` is
    `FINISH` unless given."""
    async with runner.tools_for(
        paths, state, settings, syft, clock=clock, sleep=sleep,
        github_transport=upstream.github, raw_transport=upstream.raw,
        git_base=upstream.git_base,
    ) as tools:
        async with Depgraph(
            tools.github, state, paths.depgraph_dir, settings=depgraph,
            downloads=upstream.downloads,
        ) as graph:
            yield Collector(
                paths, state, settings,
                _parts(tools, paths, state, graph, index, clock, sleep),
                clock=clock, sleep=sleep,
                finish=FINISH if finish is None else finish,
            )


def _parts(
    tools: Tools,
    paths: PathConfig,
    state: CollectorState,
    graph: Depgraph,
    index: Indexing | None,
    clock: Callable[[], float],
    sleep: Callable[[float], Awaitable[None]],
) -> Parts:
    async def collect(observed: Observed, priority: Priority) -> Collected:
        return await runner.collect(tools, observed, priority=priority)

    return Parts(
        universe=Universe(tools.github, state, paths.search_dir, sleep=sleep),
        sweeper=Sweeper(tools.github, state, sleep=sleep),
        depgraph=graph,
        index=index if index is not None else IndexPass(paths, clock=clock),
        collect=collect,
        syft_version=tools.syft.version,
    )


async def run(
    paths: PathConfig,
    state: CollectorState,
    settings: CollectorSettings,
    syft: SyftSettings,
    depgraph: DepgraphSettings,
    *,
    upstream: Upstream = Upstream(),
    index: Indexing | None = None,
) -> int:
    """`chatsbom collect`: the collector until SIGTERM or SIGINT, or a
    failure no retry mends. Its exit status."""
    loop = asyncio.get_running_loop()
    async with collector_for(
        paths, state, settings, syft, depgraph, upstream=upstream,
        index=index,
    ) as collector:
        logger.info(
            'The collector starts',
            tokens=', '.join(token.label for token in settings.tokens),
            repositories_at_once=settings.at_once,
            sweep_every=str(settings.sweep_interval),
            universe_every=str(settings.universe_interval),
            index_every=str(settings.index_interval),
            syft_slots=syft.slots,
        )
        caught = (signal.SIGTERM, signal.SIGINT)
        for number in caught:
            loop.add_signal_handler(
                number, collector.stop, signal.Signals(number).name,
            )
        try:
            return await collector.run()
        finally:
            for number in caught:
                loop.remove_signal_handler(number)
