"""`chatsbom collect`: the collector, one process (#171, part 6e of
#155; #128 section 2.1).

The schedule is run here on a virtual clock (tests/virtual_clock.py),
which the stand-in GitHub (tests/fake_github_test.py) and the budget
read too: the universe, the sweep and the dependency graph are the
real ones, against the stand-in; the collections are a stand-in of the
test's own, which says what it was asked to collect and when, and holds
a collection as long as a test wants; and so is the index pass. A day
of the schedule runs in a moment.

- **Each interval is kept:** the sweep hourly, the universe weekly, the
  index pass at most daily and only once something was written, the
  graph at the time its step says and after every sweep.
- **The order:** changed before new, and new before rescans; a stage
  due again after its backoff before new.
- **At once:** no more collections than the setting, and one repository
  by one task.
- **The budget:** a refused bucket holds back only the work that needs
  it, and the collections do not starve the sweep.
- **Stopping and health:** a stop ends what is in flight within its
  grace, and the heartbeat says when a part is wedged.

`collector_process_run_test` runs the real stages end to end, and the
command as a process of its own.
"""
from __future__ import annotations

import asyncio
import json
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Iterable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest
import structlog

from chatsbom.collector import process
from chatsbom.collector.budget import BudgetManager
from chatsbom.collector.budget import IN_FLIGHT
from chatsbom.collector.budget import lease_priority
from chatsbom.collector.client import GitHubClient
from chatsbom.collector.depgraph import Depgraph
from chatsbom.collector.depgraph import Step
from chatsbom.collector.due import detected
from chatsbom.collector.due import Priority
from chatsbom.collector.health import check
from chatsbom.collector.index import IndexRun
from chatsbom.collector.process import COLLECTION_DEADLINE
from chatsbom.collector.process import Collector
from chatsbom.collector.process import DETECTION
from chatsbom.collector.process import GRAPH
from chatsbom.collector.process import Parts
from chatsbom.collector.runner import Collected
from chatsbom.collector.runner import DONE
from chatsbom.collector.runner import Ran
from chatsbom.collector.settings import CollectorSettings
from chatsbom.collector.stages import Target
from chatsbom.collector.state import CollectorState
from chatsbom.collector.state import FAILED
from chatsbom.collector.state import Member
from chatsbom.collector.state import Observed
from chatsbom.collector.state import STATE_FILE
from chatsbom.collector.state import Sweep
from chatsbom.collector.state import UniverseSnapshot
from chatsbom.collector.sweep import Sweeper
from chatsbom.collector.tokens import Token
from chatsbom.collector.universe import Refreshed
from chatsbom.collector.universe import Universe
from chatsbom.core.config import PathConfig
from chatsbom.core.ledger import Stage
from tests.collector_due_test import _collected
from tests.collector_due_test import _released
from tests.fake_github_test import FakeGitHub
from tests.fake_github_test import Repo
from tests.fake_github_test import START
from tests.virtual_clock import VirtualClock

TOKEN = 'ghp_collect_process_000000000000000000000'

HOUR = timedelta(hours=1)
DAY = timedelta(days=1)
WEEK = timedelta(days=7)
NOW = datetime.fromtimestamp(START, timezone.utc)


def at(seconds: float) -> datetime:
    return datetime.fromtimestamp(seconds, timezone.utc)


class Timed:
    """When each of a part's units of work began, by the clock."""

    def __init__(self, clock: VirtualClock) -> None:
        self.clock = clock
        self.times: list[datetime] = []

    def began(self) -> None:
        self.times.append(at(self.clock()))


class TimedUniverse(Universe):
    def __init__(self, *args: Any, timed: Timed, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.timed = timed

    async def refresh(self) -> Refreshed:
        self.timed.began()
        assert lease_priority() == DETECTION
        return await super().refresh()


class TimedSweeper(Sweeper):
    def __init__(self, *args: Any, timed: Timed, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.timed = timed

    async def run(self, *, wait: float | None = None) -> Sweep | None:
        self.timed.began()
        assert lease_priority() == DETECTION
        return await super().run(wait=wait)


class TimedGraph:
    """The dependency graph, each step timed."""

    def __init__(self, graph: Depgraph, timed: Timed) -> None:
        self.graph = graph
        self.timed = timed
        #: The universe each step was given.
        self.universes: list[list[Observed]] = []

    async def step(self, repositories: Iterable[Observed]) -> Step:
        self.timed.began()
        assert lease_priority() == GRAPH
        listed = list(repositories)
        self.universes.append(listed)
        return await self.graph.step(listed)


@dataclass
class Collections:
    """The collections, as the test's own: each marked collected as of
    what it was asked for, having written a stage, unless told."""

    clock: VirtualClock
    state: CollectorState
    #: What each was asked to collect: the repository, its priority,
    #: when, and the priority its leases were taken at.
    asked: list[tuple[int, Priority, datetime, int]] = field(
        default_factory=list,
    )
    running: set[int] = field(default_factory=set)
    peak: int = 0
    #: Held until set, where set.
    gate: asyncio.Event | None = None
    #: How long each takes, by the clock.
    takes: timedelta = timedelta(0)
    writes: bool = True
    #: What each does with the API, where a test has it ask.
    ask: Callable[[int], Awaitable[None]] | None = None
    #: Repositories that raise of themselves.
    broken: set[int] = field(default_factory=set)
    #: Collected more than once at a time.
    twice: list[int] = field(default_factory=list)

    async def __call__(
        self, observed: Observed, priority: Priority,
    ) -> Collected:
        key = observed.repository_id
        if key in self.running:
            self.twice.append(key)
        self.asked.append((key, priority, at(self.clock()), lease_priority()))
        self.running.add(key)
        self.peak = max(self.peak, len(self.running))
        try:
            if key in self.broken:
                raise OSError(f'repository {key} is broken')
            if self.ask is not None:
                await self.ask(key)
            if self.gate is not None:
                await self.gate.wait()
            if self.takes:
                await self.clock.sleep(self.takes.total_seconds())
        finally:
            self.running.discard(key)
        self.state.mark_collected(key, as_of=observed.observed_at)
        return Collected(
            Target(key, observed.full_name), observed.pushed_at, '1.52.0',
            ran=[Ran(Stage.RELEASE, 'k', DONE, 'decided')] if self.writes
            else [],
        )

    def order(self) -> list[int]:
        return [key for key, *_ in self.asked]


@dataclass
class Indexes:
    """The index pass, as the test's own."""

    clock: VirtualClock
    last: datetime | None = None
    runs: list[datetime] = field(default_factory=list)

    def last_at(self) -> datetime | None:
        return self.last

    async def run(self) -> IndexRun:
        self.runs.append(at(self.clock()))
        return IndexRun(started_at=at(self.clock()))


class Stand:
    """The stand-in GitHub, collector.sqlite and the store, on one
    virtual clock."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.clock = VirtualClock()
        self.github = FakeGitHub(self.clock)
        self.github.token(TOKEN, 'alice')
        self.paths = PathConfig(base_data_dir=root / 'data')
        self.state = CollectorState.open(self.paths.base_data_dir / STATE_FILE)
        self.collections = Collections(self.clock, self.state)
        self.indexes = Indexes(self.clock)
        self.searches = Timed(self.clock)
        self.sweeps = Timed(self.clock)
        self.steps = Timed(self.clock)
        self.settings = CollectorSettings(
            tokens=(Token('token 1', TOKEN),), reserve={}, at_once=2,
        )
        self.syft = '1.52.0'
        self.budget: BudgetManager | None = None
        self.graph: TimedGraph | None = None
        self.made: Collector | None = None

    def close(self) -> None:
        self.state.close()

    def repos(self, count: int, *, first: int = 1) -> list[Repo]:
        """`count` repositories of 1,000 stars or more, each with a star
        more than the one before."""
        return [
            self.github.add(
                Repo(number, 'octo', f'r{number}', stars=1_000 + number),
            )
            for number in range(first, first + count)
        ]

    def universe(self, *repos: Repo) -> None:
        """`repos`, the universe, searched a moment ago, as a snapshot on
        disk says: not due again for a week."""
        search_dir = self.paths.search_dir
        search_dir.mkdir(parents=True, exist_ok=True)
        day = f'{at(self.clock() - 86_400):%Y-%m-%d}'
        path = search_dir / f'all-{day}.jsonl'
        path.write_text(
            ''.join(json.dumps(repo.rest()) + '\n' for repo in repos),
        )
        (search_dir / f'all-{day}.jsonl.complete').write_text(
            json.dumps({'finished_at': NOW.isoformat()}) + '\n',
        )
        self.state.keep_universe(
            UniverseSnapshot(f'all-{day}', 'stamp', len(repos), NOW),
            [Member(repo.id, repo.node_id) for repo in repos],
        )

    def swept(self, *repos: Repo) -> None:
        """A sweep began now and finished, having observed `repos`: not
        due again for an hour."""
        sweep = self.state.begin_sweep(NOW)
        for repo in repos:
            self.observe(repo)
        self.state.keep_sweep(replace(sweep, finished_at=NOW))

    def observe(self, repo: Repo) -> Observed:
        observed = Observed(
            repository_id=repo.id, node_id=repo.node_id,
            full_name=repo.full_name, stars=repo.stars,
            archived=repo.archived,
            pushed_at=datetime.fromisoformat(
                repo.pushed_at.replace('Z', '+00:00'),
            ),
            default_branch=repo.default_branch, head=repo.head,
            release_tag=None, release_at=None, observed_at=NOW,
        )
        self.state.observe(observed)
        return observed

    @asynccontextmanager
    async def collector(self, **options: Any) -> AsyncIterator[Collector]:
        budget = self.budget or BudgetManager(
            self.settings.tokens, reserve={}, clock=self.clock,
            sleep=self.clock.sleep,
        )
        self.budget = budget
        async with GitHubClient(
            budget, transport=self.github.transport(),
        ) as github:
            async with Depgraph(
                github, self.state, self.paths.depgraph_dir,
                downloads=self.github.transport(),
            ) as depgraph:
                self.graph = TimedGraph(depgraph, self.steps)
                self.github_client = github

                async def syft_version() -> str | None:
                    return self.syft

                parts = Parts(
                    universe=TimedUniverse(
                        github, self.state, self.paths.search_dir,
                        sleep=self.clock.sleep, timed=self.searches,
                    ),
                    sweeper=TimedSweeper(
                        github, self.state, sleep=self.clock.sleep,
                        timed=self.sweeps,
                    ),
                    depgraph=self.graph,
                    index=self.indexes,
                    collect=self.collections,
                    syft_version=syft_version,
                )
                options.setdefault('tick', HOUR)
                options.setdefault('finish', 0.5)
                self.made = Collector(
                    self.paths, self.state, self.settings, parts,
                    clock=self.clock, sleep=self.clock.sleep, **options,
                )
                yield self.made

    async def running(
        self, seconds: float, *, then: Callable[[], Awaitable[None]] | None = None,
        **options: Any,
    ) -> int:
        """The collector for `seconds` of the clock, then stopped: its
        status."""
        async with self.collector(**options) as collector:
            running = asyncio.ensure_future(collector.run())
            await self.clock.run_for(seconds)
            if then is not None:
                await then()
            collector.stop('the test is over')
            driving = asyncio.ensure_future(self.clock.drive())
            try:
                return await asyncio.wait_for(running, 30)
            finally:
                driving.cancel()
                await asyncio.gather(driving, return_exceptions=True)

    def run(self, seconds: float, **options: Any) -> int:
        return asyncio.run(self.running(seconds, **options))


@pytest.fixture
def stand(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> Iterable[Stand]:
    stand = Stand(tmp_path)
    # The walk's work in a thread, which the clock waits for.
    monkeypatch.setattr(asyncio, 'to_thread', stand.clock.to_thread)
    try:
        yield stand
    finally:
        stand.close()


def gaps(times: list[datetime]) -> list[timedelta]:
    return [later - earlier for earlier, later in zip(times, times[1:])]


# -- the first start ------------------------------------------------------------


class TestTheFirstStart:
    def test_searches_then_sweeps_then_collects_the_most_stars_first(
        self, stand,
    ):
        """No universe, and nothing observed: the universe is searched,
        the sweep observes it, and the never collected are collected."""
        stand.repos(3)

        assert stand.run(HOUR.total_seconds() / 2) == 0

        assert stand.searches.times == [NOW]
        assert stand.sweeps.times == [NOW]
        assert stand.collections.order() == [3, 2, 1]
        assert {
            priority for _, priority, *_ in stand.collections.asked
        } == {Priority.NEW}
        # Each at its own priority, as its leases are.
        assert {level for *_, level in stand.collections.asked} == {
            int(Priority.NEW),
        }
        # Something was written: the index pass ran, once.
        assert stand.indexes.runs == [NOW]

    def test_makes_the_export_directory_web_mounts(self, stand):
        """`web` does not start without data/export (#154)."""
        stand.run(1)
        assert (stand.paths.base_data_dir / 'export').is_dir()


# -- each interval --------------------------------------------------------------


class TestEachInterval:
    def test_the_sweep_is_hourly(self, stand):
        repos = stand.repos(2)
        stand.universe(*repos)
        stand.swept(*repos)

        stand.run((5 * HOUR).total_seconds())

        assert stand.sweeps.times == [NOW + n * HOUR for n in range(1, 6)]
        assert stand.searches.times == []

    def test_the_universe_is_searched_weekly(self, stand):
        repos = stand.repos(2)
        stand.universe(*repos)
        stand.swept(*repos)
        stand.settings = replace(stand.settings, sweep_interval=DAY)

        stand.run((15 * DAY).total_seconds())

        assert len(stand.searches.times) == 2
        assert all(gap >= WEEK for gap in gaps(stand.searches.times))
        assert all(gap >= DAY for gap in gaps(stand.sweeps.times))

    def test_the_index_pass_runs_once_something_was_written_at_most_daily(
        self, stand,
    ):
        repos = stand.repos(2)
        stand.universe(*repos)
        stand.swept(*repos)
        stand.indexes.last = NOW - HOUR
        for repo in repos:
            stand.state.mark_collected(repo.id, as_of=NOW)

        async def pushed_every_few_hours() -> None:
            for hours in range(4, 60, 4):
                stand.github.repos[1].pushed_at = (
                    f'{NOW + hours * HOUR:%Y-%m-%dT%H:%M:%SZ}'
                )
                await stand.clock.run_for(4 * HOUR.total_seconds())

        async def running() -> int:
            async with stand.collector(tick=HOUR) as collector:
                task = asyncio.ensure_future(collector.run())
                await pushed_every_few_hours()
                collector.stop()
                driving = asyncio.ensure_future(stand.clock.drive())
                try:
                    return await asyncio.wait_for(task, 30)
                finally:
                    driving.cancel()
                    await asyncio.gather(driving, return_exceptions=True)

        assert asyncio.run(running()) == 0
        runs = stand.indexes.runs
        # A pass a day at most, the first a day after the last.
        assert len(runs) == 2
        assert runs[0] == NOW - HOUR + DAY
        assert all(gap >= DAY for gap in gaps(runs))

    def test_nothing_written_runs_no_index_pass(self, stand):
        repos = stand.repos(2)
        stand.universe(*repos)
        stand.swept(*repos)
        for repo in repos:
            stand.state.mark_collected(repo.id, as_of=NOW)

        stand.run((3 * DAY).total_seconds())

        assert stand.indexes.runs == []
        assert stand.collections.asked == []

    def test_the_graph_is_stepped_when_due_and_after_every_sweep(
        self, stand,
    ):
        repos = stand.repos(2)
        stand.universe(*repos)
        stand.swept(*repos)

        stand.run((3 * HOUR).total_seconds())

        steps = stand.steps.times
        # At the start, then after each sweep: nothing else is due, the
        # graphs asked of GitHub having none (404, a month's wait).
        assert steps[0] == NOW
        for sweep in stand.sweeps.times:
            assert any(sweep <= step <= sweep + HOUR / 60 for step in steps)
        assert stand.graph is not None
        # Stepped over the universe as last observed.
        assert {len(each) for each in stand.graph.universes} == {2}

    def test_the_graph_reads_the_universe_a_page_at_a_time(
        self, stand, monkeypatch,
    ):
        monkeypatch.setattr(process, 'OBSERVED_PAGE', 2)
        repos = stand.repos(5)
        stand.universe(*repos)
        stand.swept(*repos)
        observed = stand.state.observed_members()
        reads: list[int] = []
        read = stand.state.observed_members

        def counted(**page: Any) -> list[Observed]:
            reads.append(page['after'])
            return read(**page)

        monkeypatch.setattr(stand.state, 'observed_members', counted)

        stand.run((HOUR / 2).total_seconds())

        assert stand.graph is not None
        assert stand.graph.universes[0] == observed
        assert reads == [0, 2, 4, 5]

    def test_a_read_that_a_sweep_cut_across_is_read_again(
        self, stand, monkeypatch,
    ):
        monkeypatch.setattr(process, 'OBSERVED_PAGE', 2)
        repos = stand.repos(3)
        stand.universe(*repos)
        stand.swept(*repos)
        read = stand.state.observed_members
        cut = []

        def cut_across(**page: Any) -> list[Observed]:
            found = read(**page)
            if not cut:
                # A sweep ends between the first page and the second,
                # having observed the first repository starred again.
                cut.append(page)
                repos[0].stars += 1
                stand.observe(repos[0])
                assert stand.made is not None
                stand.made._observed_changed()
            return found

        monkeypatch.setattr(stand.state, 'observed_members', cut_across)

        stand.run((HOUR / 2).total_seconds())

        assert stand.graph is not None
        assert stand.graph.universes[0] == read()
        assert stand.graph.universes[0][0].stars == repos[0].stars


# -- the order, and how many at once ---------------------------------------------


class TestTheOrder:
    def test_changed_goes_before_new_and_new_before_rescans(self, stand):
        """With a new Syft: the changed, a stage due again after its
        backoff, the never collected, the most stars first, then the
        rescans."""
        repos = stand.repos(7)
        stand.universe(*repos)
        stand.swept(*repos)
        paths, state = stand.paths, stand.state
        push = stand.observe(repos[0]).pushed_at
        assert push is not None
        # 1 and 2 changed since they were collected: 2 the longer.
        for key in (1, 2):
            state.mark_collected(key, as_of=NOW - HOUR)
        state.mark_changed(1, at=NOW)
        state.mark_changed(2, at=NOW - HOUR / 2)
        # 3, never collected; 4 with more stars.
        # 5 and 6: current, but for the Syft now running.
        for key in (5, 6):
            _collected(paths, key, push)
            state.mark_collected(key, as_of=NOW)
        # 7: its commit stage failed, and its backoff has passed.
        _released(paths, 7, push)
        state.record(7, 'commit', 'tag:v1.0.0', FAILED, now=NOW - HOUR)
        state.mark_collected(7, as_of=NOW)
        stand.syft = '1.53.0'
        stand.settings = replace(stand.settings, at_once=1)

        stand.run((HOUR / 2).total_seconds())

        assert [
            (key, priority.name) for key, priority, *_ in
            stand.collections.asked
        ] == [
            (2, 'CHANGED'), (1, 'CHANGED'), (7, 'CHANGED'),
            (4, 'NEW'), (3, 'NEW'), (5, 'RESCAN'), (6, 'RESCAN'),
        ]

    def test_no_more_at_once_than_the_setting(self, stand):
        repos = stand.repos(6)
        stand.universe(*repos)
        stand.swept(*repos)
        stand.settings = replace(stand.settings, at_once=3)
        stand.collections.takes = timedelta(minutes=10)

        stand.run(HOUR.total_seconds())

        assert stand.collections.peak == 3
        assert sorted(stand.collections.order()) == [1, 2, 3, 4, 5, 6]

    def test_one_repository_is_collected_by_one_task_at_a_time(self, stand):
        """Changed again while it is collected: collected again once it
        is done, and never twice at once."""
        repos = stand.repos(1)
        stand.universe(*repos)
        stand.swept(*repos)
        stand.settings = replace(stand.settings, at_once=4)
        stand.collections.takes = 3 * HOUR

        stand.github.repos[1].pushed_at = f'{NOW + HOUR:%Y-%m-%dT%H:%M:%SZ}'
        stand.run((5 * HOUR).total_seconds())

        assert stand.collections.twice == []
        assert stand.collections.order() == [1, 1]
        first, second = (when for _, _, when, _ in stand.collections.asked)
        assert second - first >= 3 * HOUR

    def test_a_repository_that_fails_of_itself_is_left_a_while(self, stand):
        repos = stand.repos(2)
        stand.universe(*repos)
        stand.swept(*repos)
        stand.collections.broken = {2}

        with structlog.testing.capture_logs() as logs:
            stand.run((HOUR / 2).total_seconds())

        # Left fifteen minutes each time, not asked again at once.
        assert stand.collections.order().count(2) in (2, 3)
        assert 1 in stand.collections.order()
        assert any(
            log['event'].startswith('A repository could not be collected')
            and log['repository_id'] == 2 for log in logs
        )


# -- the budget -----------------------------------------------------------------


class TestTheBudget:
    def test_a_refused_bucket_holds_back_only_the_work_that_needs_it(
        self, stand,
    ):
        """The REST API's bucket spent until its window ends: the
        collections, which ask it, wait; the sweep, on GraphQL, and the
        dependency graph, on its own, go on."""
        repos = stand.repos(2)
        stand.universe(*repos)
        stand.swept(*repos)
        core = stand.github.meter(TOKEN, 'core')
        core.remaining, core.reset = 0, int(START + 4 * 3_600)
        answered: list[datetime] = []

        async def ask(key: int) -> None:
            await stand.github_client.get(
                f'/repositories/{key}/releases', conditional=False,
            )
            answered.append(at(stand.clock()))

        stand.collections.ask = ask

        stand.run((5 * HOUR).total_seconds())

        # The sweeps kept their hours while the collections waited.
        assert stand.sweeps.times[:3] == [NOW + n * HOUR for n in (1, 2, 3)]
        assert len(stand.steps.times) >= 4
        # The collections were answered once the window ended, and not
        # before.
        assert answered and min(answered) >= NOW + 4 * HOUR

    def test_the_collections_do_not_starve_the_sweep(self, stand):
        """Collections filling the token's requests in flight: when one
        comes back, the sweep, due meanwhile, is given its room before
        the next collection's."""
        repos = stand.repos(8)
        stand.universe(*repos)
        stand.swept(*repos)
        stand.settings = replace(stand.settings, at_once=8)
        # Each bucket heard from, as the process's would be after its
        # first answers: one not heard from is asked once at a time.
        stand.budget = BudgetManager(
            stand.settings.tokens, reserve={}, clock=stand.clock,
            sleep=stand.clock.sleep,
        )
        for bucket in ('core', 'graphql', 'dependency_sbom'):
            lease = stand.budget.try_lease(bucket)
            assert lease is not None
            lease.answered(stand.github.meter(TOKEN, bucket).headers(bucket))
        gate = asyncio.Event()
        order: list[str] = []

        async def ask(key: int) -> None:
            await stand.github_client.get(
                f'/repositories/{key}/releases', conditional=False,
            )
            order.append(f'core {key}')

        stand.collections.ask = ask

        async def then() -> None:
            # The sweep is due at the hour, and waits for a slot.
            await stand.clock.run_for(HOUR.total_seconds())
            assert order == []
            assert stand.budget is not None
            token = stand.settings.tokens[0]
            assert stand.budget.in_flight(token) == IN_FLIGHT
            gate.set()
            await stand.clock.run_for(60)

        stand.github.gate = gate
        stand.run(1, then=then)

        graphql = [
            index for index, seen in enumerate(stand.github.requests)
            if seen.path == '/graphql'
        ]
        core = [
            index for index, seen in enumerate(stand.github.requests)
            if seen.path.endswith('/releases')
        ]
        # The four in flight answered first, then the sweep's call, then
        # the other four collections'.
        assert len(core) == 8
        assert graphql and graphql[0] < core[IN_FLIGHT]


# -- stopping, and health --------------------------------------------------------


class TestStopping:
    def test_a_collection_in_flight_has_its_grace_then_is_given_up(
        self, stand,
    ):
        repos = stand.repos(1)
        stand.universe(*repos)
        stand.swept(*repos)
        stand.collections.gate = asyncio.Event()

        with structlog.testing.capture_logs() as logs:
            status = stand.run(60, finish=0.2)

        assert status == 0
        assert stand.collections.asked
        # Given up on: not marked collected, due again at the next start.
        [candidate] = detected(stand.state, limit=10)
        assert candidate.observed.repository_id == 1
        events = [log['event'] for log in logs]
        assert 'Collections in flight are given time to end' in events
        assert (
            'Collections given up: each is due again at the next start'
            in events
        )
        said = check(
            stand.paths.base_data_dir / 'collector.heartbeat', at(stand.clock()),
        )
        assert 'stopped at' in said

    def test_one_that_ends_within_its_grace_is_kept(self, stand):
        repos = stand.repos(1)
        stand.universe(*repos)
        stand.swept(*repos)
        gate = asyncio.Event()
        stand.collections.gate = gate

        async def then() -> None:
            asyncio.get_running_loop().call_later(0.05, gate.set)

        assert stand.run(60, then=then, finish=5) == 0
        assert stand.state.observed(1) is not None
        assert detected(stand.state, limit=10) == []

    def test_every_token_refused_stops_it_with_status_1(self, stand):
        stand.repos(2)
        stand.settings = replace(
            stand.settings, tokens=(Token('token 1', 'ghp_unknown'),),
        )

        with structlog.testing.capture_logs() as logs:
            status = stand.run(HOUR.total_seconds())

        assert status == 1
        assert any(
            log['event'].startswith('GitHub took none of the tokens')
            for log in logs
        )
        assert all('ghp_unknown' not in json.dumps(log, default=str)
                   for log in logs)


class TestHealth:
    def test_a_collection_that_hangs_stalls_the_heartbeat(self, stand):
        """The loop runs on, the sweep keeps its hours, and nothing more
        is collected: the check fails once the collection is past its
        deadline."""
        repos = stand.repos(1)
        stand.universe(*repos)
        stand.swept(*repos)
        stand.collections.gate = asyncio.Event()
        heartbeat = stand.paths.base_data_dir / 'collector.heartbeat'
        seen: list[str] = []

        async def then() -> None:
            seen.append(check(heartbeat, at(stand.clock())))
            await stand.clock.run_for(COLLECTION_DEADLINE.total_seconds())
            seen.append(check(heartbeat, at(stand.clock())))

        stand.run(HOUR.total_seconds(), then=then)

        assert seen[0] == ''
        assert seen[1].startswith('stalled: collect 1: busy since')
        assert len(stand.sweeps.times) >= 3

    def test_waiting_for_the_next_sweep_is_healthy(self, stand):
        repos = stand.repos(1)
        stand.universe(*repos)
        stand.swept(*repos)
        stand.state.mark_collected(1, as_of=NOW)
        heartbeat = stand.paths.base_data_dir / 'collector.heartbeat'
        seen: list[str] = []

        async def then() -> None:
            seen.append(check(heartbeat, at(stand.clock())))

        stand.run((2 * DAY).total_seconds(), then=then)

        assert seen == ['']


# -- the logs --------------------------------------------------------------------


def test_logs_a_line_per_sweep_search_and_repository_and_no_token(stand):
    stand.repos(3)

    with structlog.testing.capture_logs() as logs:
        stand.run(HOUR.total_seconds() / 2)

    events = [log['event'] for log in logs]
    assert events.count('The universe was searched again') == 1
    assert events.count('The universe was swept') == 1
    collected = [log for log in logs if log['event'] == 'Repository collected']
    assert [log['repository_id'] for log in collected] == [3, 2, 1]
    assert collected[0]['ran'] == 'release:done'
    assert collected[0]['priority'] == 'new'
    assert TOKEN not in json.dumps(logs, default=str)
