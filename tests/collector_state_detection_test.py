"""collector.sqlite's step for detection (#160): what the weekly
universe and the hourly sweep keep, and what 6c reads of them.

- **The universe:** the repositories of the newest complete search
  snapshot, by the node ids the sweep asks for them by, and which
  snapshot that is. A repository whose node came back null is gone, and
  left out until the next universe.
- **The sweeps:** each one's start, how far it got and what it found
  and cost, so that one cut short goes on where it was.
- **What 6c reads:** beside each repository's latest observation, when
  an observation last found its push, HEAD or latest release changed,
  and which observation 6c last collected it as of. The changed ones,
  and those never collected, are a query.

The step is one of `MIGRATIONS`: a file of the first schema is brought
forward with what it kept, and an earlier collector refuses the file it
makes, untouched.
"""
import sqlite3
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest

from chatsbom.collector.state import CollectorState
from chatsbom.collector.state import Member
from chatsbom.collector.state import MIGRATIONS
from chatsbom.collector.state import Observed
from chatsbom.collector.state import SCHEMA_VERSION
from chatsbom.collector.state import STATE_FILE
from chatsbom.collector.state import Sweep
from chatsbom.collector.state import TooNew
from chatsbom.collector.state import UniverseSnapshot

NOW = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)
HOUR = timedelta(hours=1)


@pytest.fixture
def path(tmp_path: Path) -> Path:
    return tmp_path / 'data' / STATE_FILE


def observed(repository_id: int, **changes: Any) -> Observed:
    return replace(
        Observed(
            repository_id=repository_id,
            node_id=f'R_{repository_id}',
            full_name=f'octo/r{repository_id}',
            stars=1_000 + repository_id,
            archived=False,
            pushed_at=datetime(2026, 9, 29, tzinfo=timezone.utc),
            default_branch='main',
            head='a' * 40,
            release_tag=None,
            release_at=None,
            observed_at=NOW,
        ),
        **changes,
    )


def snapshot(
    name: str = 'all-2026-09-28', repositories: int = 3,
) -> UniverseSnapshot:
    return UniverseSnapshot(
        snapshot=name, stamp=f'{name}:1', repositories=repositories,
        loaded_at=NOW,
    )


def members(*ids: int) -> list[Member]:
    return [Member(number, f'R_{number}') for number in ids]


def ids(found: list[Observed]) -> list[int]:
    return [observation.repository_id for observation in found]


class TestTheStep:
    def test_a_file_of_the_first_schema_is_brought_forward_with_what_it_kept(
        self, path,
    ):
        with CollectorState.open(path, migrations=MIGRATIONS[:1]) as state:
            assert state.version == 1
            state.observe(observed(1))

        with CollectorState.open(path) as state:
            assert state.version == SCHEMA_VERSION
            assert state.observed(1) == observed(1)
            # Nothing detected yet: no universe, no sweep, and so nothing
            # for 6c, though a repository was observed.
            assert state.universe() is None
            assert state.members() == []
            assert state.latest_sweep() is None
            assert state.changed() == []
            assert state.never_collected() == []

    def test_a_file_it_made_is_refused_by_an_earlier_collector_untouched(
        self, path,
    ):
        with CollectorState.open(path) as state:
            state.keep_universe(snapshot(), members(1))
        before = path.read_bytes()

        with pytest.raises(TooNew):
            CollectorState.open(path, migrations=MIGRATIONS[:1])

        assert path.read_bytes() == before
        with CollectorState.open(path) as state:
            assert state.members() == members(1)

    def test_adds_its_tables_and_columns(self, path):
        with CollectorState.open(path):
            pass
        db = sqlite3.connect(path)
        try:
            tables = {
                row[0] for row in db.execute(
                    "SELECT name FROM sqlite_master WHERE type = 'table'",
                )
            }
            columns = {
                row[1] for row in db.execute('PRAGMA table_info(repository)')
            }
        finally:
            db.close()
        assert {'universe', 'universe_snapshot', 'sweep'} <= tables
        assert {'changed_at', 'collected_at'} <= columns


class TestTheUniverse:
    def test_is_the_snapshot_it_was_loaded_from_and_its_members(self, path):
        with CollectorState.open(path) as state:
            state.keep_universe(snapshot(), members(3, 1, 2))
            assert state.universe() == snapshot()
            # By id, which is the order a sweep goes in.
            assert state.members() == members(1, 2, 3)
            assert state.members(after=1) == members(2, 3)
            assert state.members(after=1, limit=1) == members(2)
            assert state.members(after=3) == []

    def test_a_new_one_replaces_the_last_whole(self, path):
        with CollectorState.open(path) as state:
            state.keep_universe(snapshot(), members(1, 2, 3))
            newer = snapshot('all-2026-10-05', repositories=2)
            state.keep_universe(newer, members(2, 4))
            assert state.universe() == newer
            assert state.members() == members(2, 4)

    def test_a_member_whose_node_is_gone_is_left_out_until_the_next(
        self, path,
    ):
        with CollectorState.open(path) as state:
            state.keep_universe(snapshot(), members(1, 2, 3))
            state.mark_gone(2, now=NOW)
            assert state.members() == members(1, 3)
            state.keep_universe(snapshot('all-2026-10-05'), members(1, 2, 3))
            assert state.members() == members(1, 2, 3)

    def test_its_members_as_last_observed_a_page_at_a_time(self, path):
        # What the dependency graph steps through between two sweeps.
        with CollectorState.open(path) as state:
            state.keep_universe(snapshot(), members(4, 3, 2, 1, 6))
            for repository_id in (1, 2, 3, 5, 6):
                state.observe(observed(repository_id, stars=repository_id))
            state.mark_gone(2, now=NOW)
            # 4 was never observed, 2 is gone, and 5 is no member.
            assert state.observed_members() == [
                state.observed(1), state.observed(3), state.observed(6),
            ]
            assert state.observed_members(after=1, limit=1) == [
                state.observed(3),
            ]
            assert state.observed_members(after=3) == [state.observed(6)]
            assert state.observed_members(after=6) == []

    def test_is_kept_across_a_restart(self, path):
        with CollectorState.open(path) as state:
            state.keep_universe(snapshot(), members(1, 2, 3))
            state.mark_gone(3, now=NOW)
        with CollectorState.open(path) as state:
            assert state.universe() == snapshot()
            assert state.members() == members(1, 2)


class TestTheSweeps:
    def test_one_begun_is_the_latest_until_the_next(self, path):
        with CollectorState.open(path) as state:
            assert state.latest_sweep() is None
            first = state.begin_sweep(NOW)
            assert first == Sweep(
                sweep_id=1, started_at=NOW, finished_at=None, position=0,
                calls=0, cost=0, nodes=0, changed=0, renamed=0, gone=0,
                unresolved=0, failed=0,
            )
            assert state.latest_sweep() == first
            went_on = replace(
                first, position=100, calls=1, cost=1, nodes=99, changed=3,
                renamed=1, gone=1, unresolved=0, failed=0,
            )
            state.keep_sweep(went_on)
            assert state.latest_sweep() == went_on
            done = replace(went_on, finished_at=NOW + timedelta(minutes=10))
            state.keep_sweep(done)
            second = state.begin_sweep(NOW + HOUR)
            assert second.sweep_id == 2
            assert state.latest_sweep() == second

    def test_how_far_one_got_is_kept_across_a_restart(self, path):
        with CollectorState.open(path) as state:
            sweep = replace(state.begin_sweep(NOW), position=200, calls=2)
            state.keep_sweep(sweep)
        with CollectorState.open(path) as state:
            assert state.latest_sweep() == sweep


class TestWhat6cReads:
    def test_members_observed_and_never_collected_most_stars_first(
        self, path,
    ):
        with CollectorState.open(path) as state:
            state.keep_universe(snapshot(), members(1, 2, 3, 5))
            state.observe(observed(1, stars=10))
            state.observe(observed(2, stars=500))
            state.observe(observed(5, stars=None))
            # 3 is a member not yet observed, whose push 6c cannot know;
            # 4 was observed and is not a member.
            state.observe(observed(4, stars=9_999))
            assert ids(state.never_collected()) == [2, 1, 5]
            assert state.never_collected()[0] == observed(2, stars=500)
            assert ids(state.never_collected(limit=1)) == [2]
            assert state.changed() == []

    def test_one_collected_is_changed_once_a_change_is_observed_after(
        self, path,
    ):
        later = NOW + HOUR
        with CollectorState.open(path) as state:
            state.keep_universe(snapshot(), members(1, 2))
            state.observe(observed(1))
            state.observe(observed(2))
            state.mark_collected(1, as_of=NOW)
            state.mark_collected(2, as_of=NOW)
            assert state.changed() == []
            assert state.never_collected() == []

            pushed = observed(1, pushed_at=later, observed_at=later)
            state.observe(pushed)
            state.mark_changed(1, at=later)
            state.observe(observed(2, observed_at=later))
            assert state.changed() == [pushed]

            state.mark_collected(1, as_of=later)
            assert state.changed() == []

    def test_a_change_observed_while_6c_collected_is_not_lost(self, path):
        """6c collects what the sweep at T1 observed; the sweep at T2 sees
        another push before 6c says it collected T1's."""
        t1, t2 = NOW + HOUR, NOW + 2 * HOUR
        with CollectorState.open(path) as state:
            state.keep_universe(snapshot(), members(1))
            state.observe(observed(1))
            state.mark_collected(1, as_of=NOW)
            state.observe(observed(1, pushed_at=t1, observed_at=t1))
            state.mark_changed(1, at=t1)
            state.observe(observed(1, pushed_at=t2, observed_at=t2))
            state.mark_changed(1, at=t2)
            state.mark_collected(1, as_of=t1)
            assert ids(state.changed()) == [1]
            # And said out of order, what was collected does not go back.
            state.mark_collected(1, as_of=t2)
            state.mark_collected(1, as_of=t1)
            assert state.changed() == []

    def test_the_longest_changed_comes_first(self, path):
        with CollectorState.open(path) as state:
            state.keep_universe(snapshot(), members(1, 2, 3))
            for repository_id in (1, 2, 3):
                state.observe(observed(repository_id))
                state.mark_collected(repository_id, as_of=NOW)
            for repository_id, hours in ((3, 1), (1, 2), (2, 3)):
                at = NOW + hours * HOUR
                state.observe(
                    observed(repository_id, pushed_at=at, observed_at=at),
                )
                state.mark_changed(repository_id, at=at)
            assert ids(state.changed()) == [3, 1, 2]
            assert ids(state.changed(limit=2)) == [3, 1]

    def test_gone_members_and_others_are_left_out(self, path):
        with CollectorState.open(path) as state:
            state.keep_universe(snapshot(), members(1, 2))
            for repository_id in (1, 2, 3):
                state.observe(observed(repository_id))
            state.mark_collected(2, as_of=NOW)
            state.observe(observed(2, pushed_at=NOW + HOUR))
            state.mark_changed(2, at=NOW + HOUR)
            state.mark_gone(1, now=NOW)
            state.mark_gone(2, now=NOW)
            assert state.never_collected() == []
            assert state.changed() == []

    def test_collecting_what_was_never_observed_is_refused(self, path):
        with CollectorState.open(path) as state:
            with pytest.raises(KeyError):
                state.mark_collected(7, as_of=NOW)
