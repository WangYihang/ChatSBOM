"""The universe (#160, #100 Q1): the newest complete, unfiltered search
snapshot of the repositories with at least 1,000 stars, refreshed
weekly, against the stand-in (tests/fake_github_test.py).

- **The search** answers at most 1,000 results a query, so it is split
  as `services/search_service.py` split it: into windows of star counts,
  from the most down, and a star count that alone has more is split by
  when its repositories were created.
- **The snapshot** is written where and as `core/catalog.py` reads it,
  `01-github-search/all-<date>.jsonl`, whole or not at all: a refresh
  that fails, or lists far fewer than the last, leaves the last one
  standing.
- **collector.sqlite** keeps each repository's node id, which the sweep
  asks after it by, and follows the newest complete snapshot.

The stand-in's clock is the budget's: a pause of a minute, and the
search bucket's windows, take no time.
"""
import asyncio
import json
import os
from collections.abc import Iterator
from datetime import date
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest
import structlog

from chatsbom.collector.budget import BudgetManager
from chatsbom.collector.client import GitHubClient
from chatsbom.collector.errors import Failed
from chatsbom.collector.retry import ATTEMPTS
from chatsbom.collector.retry import PAUSE
from chatsbom.collector.state import CollectorState
from chatsbom.collector.state import Member
from chatsbom.collector.state import STATE_FILE
from chatsbom.collector.tokens import Token
from chatsbom.collector.universe import AGAIN_AFTER
from chatsbom.collector.universe import CutShort
from chatsbom.collector.universe import load_universe
from chatsbom.collector.universe import MIN_STARS
from chatsbom.collector.universe import Refreshed
from chatsbom.collector.universe import Universe
from chatsbom.collector.universe import universe_due
from chatsbom.core import catalog
from tests.fake_github_test import _found
from tests.fake_github_test import FakeClock
from tests.fake_github_test import FakeGitHub
from tests.fake_github_test import Reply
from tests.fake_github_test import Repo
from tests.fake_github_test import START

ONE = 'ghp_universe_token_one_00000000000000000'
T1 = Token('token 1', ONE)

#: The stand-in's day, as the snapshot is dated: 2026-09-21.
TODAY = datetime.fromtimestamp(START, timezone.utc).date()
NOW = datetime.fromtimestamp(START, timezone.utc)
WEEK = timedelta(days=7)

SEARCH = '/search/repositories'


@pytest.fixture
def fake() -> FakeGitHub:
    fake = FakeGitHub(FakeClock())
    fake.token(ONE, 'alice')
    return fake


@pytest.fixture
def data(tmp_path: Path) -> Path:
    return tmp_path / 'data'


@pytest.fixture
def search_dir(data: Path) -> Path:
    return data / '01-github-search'


@pytest.fixture
def state(data: Path) -> Iterator[CollectorState]:
    with CollectorState.open(data / STATE_FILE) as state:
        yield state


def repos(
    fake: FakeGitHub, count: int, *, first: int = 1, **options: Any,
) -> list[Repo]:
    """`count` repositories, each with a star more than the one before,
    from 1,000, unless `options` say otherwise."""
    made = []
    for number in range(first, first + count):
        settings = {'stars': MIN_STARS - 1 + number, **options}
        made.append(fake.add(Repo(number, 'octo', f'r{number}', **settings)))
    return made


def refresh(
    fake: FakeGitHub, state: CollectorState, search_dir: Path,
    **options: Any,
) -> Refreshed:
    async def refreshing() -> Refreshed:
        budget = BudgetManager(
            (T1,), reserve={}, clock=fake.clock, sleep=fake.clock.sleep,
        )
        async with GitHubClient(budget, transport=fake.transport()) as github:
            universe = Universe(
                github, state, search_dir, sleep=fake.clock.sleep, **options,
            )
            return await universe.refresh()

    return asyncio.run(refreshing())


def queries(fake: FakeGitHub) -> list[str]:
    """What was searched for, in order, each once."""
    asked: list[str] = []
    for seen in fake.seen(SEARCH):
        if seen.query['q'] not in asked:
            asked.append(seen.query['q'])
    return asked


def listed(path: Path) -> dict[int, dict[str, Any]]:
    lines = path.read_text(encoding='utf-8').splitlines()
    records = [json.loads(line) for line in lines]
    assert len(records) == len({record['id'] for record in records}), (
        'each repository once'
    )
    return {record['id']: record for record in records}


def snapshot_file(
    search_dir: Path, day: str, *records: dict[str, Any],
) -> Path:
    """A snapshot as `github search` wrote one, by hand."""
    search_dir.mkdir(parents=True, exist_ok=True)
    path = search_dir / f'all-{day}.jsonl'
    path.write_text(
        ''.join(json.dumps(record) + '\n' for record in records),
        encoding='utf-8',
    )
    return path


def line(number: int, **extra: Any) -> dict[str, Any]:
    return {
        'id': number, 'owner': 'octo', 'repo': f'r{number}', 'stars': 1_000,
        'node_id': f'R_{number}', **extra,
    }


class TestTheSearch:
    def test_lists_every_repository_with_1000_stars_or_more_past_the_cap(
        self, fake, state, search_dir,
    ):
        """In windows of star counts, most first: each window ends where
        its first 1,000 did, and the next asks again from there, since
        more may have as many stars."""
        repos(fake, 2_500)
        fake.add(Repo(9_001, 'octo', 'small', stars=999))

        report = refresh(fake, state, search_dir)

        assert set(listed(report.path)) == set(range(1, 2_501))
        assert report.repositories == 2_500
        assert queries(fake) == [
            'stars:>=1000', 'stars:1000..2509', 'stars:1000..1519',
        ]
        assert report.queries == 3
        assert report.requests == 26 == len(fake.seen(SEARCH))
        assert all(
            seen.query['sort'] == 'stars' and seen.query['order'] == 'desc'
            and seen.query['per_page'] == '100'
            for seen in fake.seen(SEARCH)
        )

    def test_splits_a_star_count_past_the_cap_by_when_they_were_created(
        self, fake, state, search_dir,
    ):
        """1,500 with 1,000 stars: no window of star counts lists them
        all, so they are listed by creation date, in halves, until each
        half is listed whole. A half with more than 1,000 is asked for
        its count alone, its first page, and split."""
        day = date(2012, 1, 1)
        for number in range(1, 1_501):
            created = day + timedelta(days=number)
            fake.add(
                Repo(
                    number, 'octo', f'r{number}', stars=1_000,
                    created_at=f'{created:%Y-%m-%d}T12:00:00Z',
                ),
            )
        repos(fake, 300, first=2_001)

        report = refresh(fake, state, search_dir)

        assert set(listed(report.path)) == (
            set(range(1, 1_501)) | set(range(2_001, 2_301))
        )
        asked = queries(fake)
        assert asked[:3] == [
            'stars:>=1000', 'stars:1000', 'stars:1000 created:<=2017-03-27',
        ]
        slices = asked[1:]
        assert all(q.startswith('stars:1000') for q in slices)
        for q in slices:
            found = sum(1 for repo in fake.repos.values() if _found(repo, q))
            pages = sorted(
                int(seen.query['page']) for seen in fake.seen(SEARCH)
                if seen.query['q'] == q
            )
            if found > 1_000:
                # Split: its first page said how many, and no more.
                assert pages == [1]
            else:
                assert pages == list(range(1, max(1, -(-found // 100)) + 1))
        assert report.queries == len(asked)
        assert report.beyond == 0

    def test_a_count_past_the_cap_on_one_day_is_listed_as_far_as_it_goes(
        self, fake, state, search_dir,
    ):
        """A day cannot be split, and no query reaches the rest: it is
        said, and the snapshot has what could be listed."""
        repos(fake, 1_050, stars=1_000, created_at='2016-05-04T00:00:00Z')

        with structlog.testing.capture_logs() as logged:
            report = refresh(fake, state, search_dir)

        assert report.repositories == 1_000
        assert report.beyond == 50
        assert 'stars:1000 created:2016-05-04' in queries(fake)
        said = [
            event for event in logged if event.get('query') ==
            'stars:1000 created:2016-05-04'
            and event['log_level'] == 'warning'
        ]
        assert said and said[0]['beyond'] == 50

    def test_asks_a_page_that_failed_again_after_a_pause(
        self, fake, state, search_dir,
    ):
        repos(fake, 150)
        fake.script(
            Reply(502, {'message': 'Server Error'}), path=SEARCH, after=1,
        )

        report = refresh(fake, state, search_dir)

        assert report.repositories == 150
        pages = [seen.query['page'] for seen in fake.seen(SEARCH)]
        statuses = [seen.status for seen in fake.seen(SEARCH)]
        assert pages == ['1', '2', '2']
        assert statuses == [200, 502, 200]
        assert fake.clock() >= START + PAUSE

    def test_asks_a_page_github_says_is_incomplete_again(
        self, fake, state, search_dir,
    ):
        """GitHub says `incomplete_results` of a query that timed out: it
        is asked again, and after the last attempt what it gave is
        listed, and counted."""
        made = repos(fake, 3)
        fake.script(
            Reply(
                200, {
                    'total_count': 3, 'incomplete_results': True,
                    'items': [made[2].rest()],
                },
            ),
            path=SEARCH,
        )
        whole = refresh(fake, state, search_dir)
        assert whole.repositories == 3
        assert whole.incomplete == 0
        assert len(fake.seen(SEARCH)) == 2

        fake.requests.clear()
        fake.clock.advance(WEEK.total_seconds())
        fake.script(
            Reply(
                200, {
                    'total_count': 3, 'incomplete_results': True,
                    'items': [made[2].rest(), made[1].rest(), made[0].rest()],
                },
            ),
            path=SEARCH, times=ATTEMPTS,
        )
        kept = refresh(fake, state, search_dir)
        assert kept.repositories == 3
        assert kept.incomplete == 1
        assert len(fake.seen(SEARCH)) == ATTEMPTS

    def test_a_result_that_is_no_repository_is_left_out_and_counted(
        self, fake, state, search_dir,
    ):
        made = repos(fake, 2)
        fake.script(
            Reply(
                200, {
                    'total_count': 3, 'incomplete_results': False,
                    'items': [
                        made[1].rest(), {'id': 77, 'name': 'no-owner'},
                        made[0].rest(),
                    ],
                },
            ),
            path=SEARCH,
        )

        report = refresh(fake, state, search_dir)

        assert set(listed(report.path)) == {1, 2}
        assert report.unusable == 1


class TestTheSnapshot:
    def test_is_written_where_and_as_the_catalog_reads_it(
        self, fake, state, search_dir,
    ):
        repos(fake, 3)
        fake.repos[2].default_branch = 'trunk'

        report = refresh(fake, state, search_dir)

        assert report.path == search_dir / f'all-{TODAY:%Y-%m-%d}.jsonl'
        assert report.snapshot == f'all-{TODAY:%Y-%m-%d}'
        newest = catalog.newest_complete(search_dir, TODAY)
        assert newest is not None and newest.path == report.path
        assert newest.marker.is_file()
        read = catalog.read_snapshot(newest)
        assert read.unusable == 0
        assert read.repositories[2].owner == 'octo'
        assert read.repositories[2].repo == 'r2'
        assert read.repositories[2].stars == 1_001
        assert read.repositories[2].default_branch == 'trunk'
        assert read.pushed_at[2] == datetime(2026, 9, 1, tzinfo=timezone.utc)
        assert listed(report.path)[2]['node_id'] == fake.repos[2].node_id

    def test_keeps_each_repositorys_node_id_in_collector_sqlite(
        self, fake, state, search_dir,
    ):
        made = repos(fake, 3)

        report = refresh(fake, state, search_dir)

        assert state.members() == [
            Member(repo.id, repo.node_id) for repo in made
        ]
        loaded = state.universe()
        assert loaded is not None
        assert loaded.snapshot == report.snapshot
        assert loaded.repositories == 3

    def test_a_failed_refresh_leaves_the_last_universe_standing(
        self, fake, state, search_dir,
    ):
        repos(fake, 250)
        first = refresh(fake, state, search_dir)
        kept = first.path.read_bytes()
        members = state.members()
        fake.clock.advance(WEEK.total_seconds())
        repos(fake, 10, first=3_001)
        fake.script(
            Reply(502, {'message': 'Server Error'}), path=SEARCH,
            after=2, times=ATTEMPTS,
        )

        with pytest.raises(Failed):
            refresh(fake, state, search_dir)

        today = TODAY + timedelta(days=7)
        newest = catalog.newest_complete(search_dir, today)
        assert newest is not None and newest.path == first.path
        assert first.path.read_bytes() == kept
        assert sorted(child.name for child in search_dir.iterdir()) == [
            first.path.name, f'{first.path.name}.complete',
        ]
        assert state.members() == members
        universe = state.universe()
        assert universe is not None and universe.snapshot == first.snapshot

    def test_one_listing_far_fewer_than_the_last_is_taken_for_cut_short(
        self, fake, state, search_dir,
    ):
        """As `queue track` took a snapshot that would unlist more than a
        quarter: GitHub answering fewer is likelier than the corpus
        shrinking by a quarter in a week."""
        repos(fake, 100)
        first = refresh(fake, state, search_dir)
        for number in range(1, 31):
            del fake.repos[number]
        fake.clock.advance(WEEK.total_seconds())

        with pytest.raises(CutShort) as refused:
            refresh(fake, state, search_dir)

        assert '70' in str(refused.value) and '100' in str(refused.value)
        today = TODAY + timedelta(days=7)
        newest = catalog.newest_complete(search_dir, today)
        assert newest is not None and newest.path == first.path
        assert len(state.members()) == 100
        assert not [
            child for child in search_dir.iterdir()
            if child.name.startswith('.')
        ]

        del fake.repos[31], fake.repos[32]
        repos(fake, 7, first=101)
        report = refresh(fake, state, search_dir)
        assert report.repositories == 75
        assert len(state.members()) == 75

    def test_one_listing_nothing_is_refused(self, fake, state, search_dir):
        with pytest.raises(CutShort) as refused:
            refresh(fake, state, search_dir)
        assert 'no repository' in str(refused.value)
        assert catalog.newest_complete(search_dir, TODAY) is None
        assert state.universe() is None

    def test_what_a_refresh_cut_short_left_is_removed(
        self, fake, state, search_dir,
    ):
        """A refresh writes beside the snapshot, and renames what it wrote
        into place once whole; one killed leaves what it was writing."""
        repos(fake, 2)
        search_dir.mkdir(parents=True)
        left = search_dir / '.all-2026-09-14.jsonl.k1ll3d.tmp'
        left.write_text('{"id": 1}\n', encoding='utf-8')
        other = search_dir / '.keep'
        other.write_text('', encoding='utf-8')

        refresh(fake, state, search_dir)

        assert not left.exists()
        assert other.exists()


class TestLoading:
    def test_collector_sqlite_follows_the_newest_complete_snapshot(
        self, state, search_dir,
    ):
        snapshot_file(search_dir, '2026-09-19', line(1), line(2))
        snapshot_file(search_dir, '2026-09-20', line(2), line(3), line(4))
        # Today's, unmarked, may be a search still running.
        snapshot_file(search_dir, f'{TODAY:%Y-%m-%d}', line(5))

        loaded = load_universe(state, search_dir, NOW)

        assert loaded is not None
        assert loaded.snapshot == 'all-2026-09-20'
        assert loaded.repositories == 3
        assert loaded.loaded_at == NOW
        assert state.universe() == loaded
        assert state.members() == [
            Member(2, 'R_2'), Member(3, 'R_3'), Member(4, 'R_4'),
        ]

    def test_a_repository_without_a_node_id_is_not_a_member(
        self, state, search_dir,
    ):
        snapshot_file(
            search_dir, '2026-09-20', line(1), line(2, node_id=None),
            {'id': 3, 'owner': 'octo', 'repo': 'r3'},
            {'no': 'id'},
        )

        with structlog.testing.capture_logs() as logged:
            loaded = load_universe(state, search_dir, NOW)

        assert loaded is not None and loaded.repositories == 1
        assert state.members() == [Member(1, 'R_1')]
        said = [event for event in logged if event.get('without_node_id')]
        assert said and said[0]['without_node_id'] == 2

    def test_the_same_snapshot_is_not_loaded_again_and_one_written_again_is(
        self, state, search_dir,
    ):
        """Loading again would bring back the members found gone."""
        path = snapshot_file(search_dir, '2026-09-20', line(1), line(2))
        first = load_universe(state, search_dir, NOW)
        state.mark_gone(1, now=NOW)

        again = load_universe(state, search_dir, NOW + timedelta(hours=1))
        assert again == first
        assert state.members() == [Member(2, 'R_2')]

        snapshot_file(search_dir, '2026-09-20', line(1), line(2), line(3))
        os.utime(path, ns=(10**18, 10**18))
        rewritten = load_universe(state, search_dir, NOW + timedelta(hours=2))
        assert rewritten is not None and rewritten.repositories == 3
        assert [member.repository_id for member in state.members()] == [
            1, 2, 3,
        ]

    def test_with_no_complete_snapshot_the_universe_stands(
        self, state, search_dir,
    ):
        assert load_universe(state, search_dir, NOW) is None
        snapshot_file(search_dir, '2026-09-20', line(1))
        loaded = load_universe(state, search_dir, NOW)
        (search_dir / 'all-2026-09-20.jsonl').unlink()
        assert load_universe(state, search_dir, NOW) == loaded
        assert state.members() == [Member(1, 'R_1')]


class TestWhenItIsDue:
    def test_with_no_complete_snapshot_it_is_due(self, search_dir):
        assert universe_due(search_dir, NOW, WEEK) is True
        snapshot_file(search_dir, f'{TODAY:%Y-%m-%d}', line(1))
        assert universe_due(search_dir, NOW, WEEK) is True

    def test_again_an_interval_after_the_last_was_finished(
        self, fake, state, search_dir,
    ):
        repos(fake, 2)
        refresh(fake, state, search_dir)
        finished = datetime.fromtimestamp(fake.clock(), timezone.utc)

        assert universe_due(search_dir, finished, WEEK) is False
        just_before = finished + WEEK - timedelta(seconds=1)
        assert universe_due(search_dir, just_before, WEEK) is False
        assert universe_due(search_dir, finished + WEEK, WEEK) is True
        hour = timedelta(hours=1)
        assert universe_due(search_dir, finished + hour, hour) is True

    def test_one_that_does_not_say_when_it_was_finished_is_dated_by_its_file(
        self, search_dir,
    ):
        """One `github search` wrote, which marks nothing."""
        path = snapshot_file(search_dir, '2026-09-20', line(1))
        written = NOW - timedelta(days=1)
        os.utime(path, (written.timestamp(), written.timestamp()))

        assert universe_due(search_dir, NOW, WEEK) is False
        assert universe_due(search_dir, written + WEEK, WEEK) is True

    def test_not_again_for_an_hour_after_a_refresh_failed(
        self, fake, state, search_dir,
    ):
        """Or the collector would search again at once, and again, while
        GitHub's search fails."""
        fake.script(
            Reply(502, {'message': 'Server Error'}), path=SEARCH,
            times=ATTEMPTS,
        )

        async def failing() -> list[bool]:
            budget = BudgetManager(
                (T1,), clock=fake.clock, sleep=fake.clock.sleep,
            )
            transport = fake.transport()
            async with GitHubClient(budget, transport=transport) as github:
                universe = Universe(
                    github, state, search_dir, sleep=fake.clock.sleep,
                )
                due = [universe.due(WEEK)]
                with pytest.raises(Failed):
                    await universe.refresh()
                due.append(universe.due(WEEK))
                fake.clock.advance(AGAIN_AFTER.total_seconds() - 1)
                due.append(universe.due(WEEK))
                fake.clock.advance(1)
                due.append(universe.due(WEEK))
                return due

        assert asyncio.run(failing()) == [True, False, False, True]

    def test_is_asked_of_the_universe_on_its_clock(
        self, fake, state, search_dir,
    ):
        async def asking() -> tuple[bool, bool]:
            budget = BudgetManager((T1,), clock=fake.clock)
            transport = fake.transport()
            async with GitHubClient(budget, transport=transport) as github:
                universe = Universe(github, state, search_dir)
                before = universe.due(WEEK)
                snapshot_file(search_dir, '2026-09-20', line(1))
                path = search_dir / 'all-2026-09-20.jsonl'
                os.utime(path, (START, START))
                assert universe.load() is not None
                return before, universe.due(WEEK)

        assert asyncio.run(asking()) == (True, False)
        assert state.members() == [Member(1, 'R_1')]
