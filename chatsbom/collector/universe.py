"""The universe (#160; #100 Q1; #128 section 2.1): the repositories the
collector collects. The newest complete, unfiltered search snapshot of
those with at least 1,000 stars, searched again weekly.

- **The search.** GitHub answers at most 1,000 results a query, so the
  search is split as `services/search_service.py` split it. First into
  windows of star counts, from the most down: each is asked for its
  first 1,000, most stars first, and the next window begins where they
  ended, since more may have as many stars. Then a star count that alone
  has more than 1,000 is split by when its repositories were created, in
  halves, until each half is listed whole: a half with more is asked for
  its first page, which says how many, and split again. About 700
  requests for 65,000 repositories, 25 minutes of the search bucket.
- **The snapshot** is written where and as `core/catalog.py` reads one,
  `01-github-search/all-<date>.jsonl`, a repository a line in the shape
  `github search` wrote, dated by the day the refresh began. It is
  written beside that name and renamed to it once whole, with the
  `.complete` marker that makes today's count. A refresh that fails, or
  lists far fewer than the last universe (`CutShort`), leaves nothing
  behind, and the last snapshot stands; the next waits an hour.
- **collector.sqlite** keeps each member's node id, which the sweep asks
  after it by (`state.members`), and follows the newest complete
  snapshot, whoever wrote it (`load_universe`): a snapshot written again,
  or collector.sqlite deleted, is loaded again from the file.
"""
import asyncio
import json
import os
import re
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import time
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any
from typing import TextIO

import structlog
from pydantic import ValidationError

from chatsbom.collector.client import Answer
from chatsbom.collector.client import GitHubClient
from chatsbom.collector.errors import Failed
from chatsbom.collector.retry import retried
from chatsbom.collector.state import CollectorState
from chatsbom.collector.state import Member
from chatsbom.collector.state import UniverseSnapshot
from chatsbom.core import catalog
from chatsbom.core.fs import atomic_write_text
from chatsbom.core.fs import temporary_beside
from chatsbom.models.repository import Repository

logger = structlog.get_logger('collector.universe')

#: What a repository has at least to be in the universe.
MIN_STARS = 1_000

#: Results GitHub answers of one query at most, and of a page.
SEARCH_CAP = 1_000
PER_PAGE = 100

#: The next window of star counts begins at the most stars among the
#: last of these results. A count read from a result may differ from
#: the one the search indexed it by, and one result whose count fell
#: would otherwise leave out the counts between; all of these would
#: have to.
EDGE = 10

#: A refresh listing fewer than this share of the last universe's
#: members is taken for a search cut short, as `queue track` took a
#: snapshot that would unlist more than a quarter: GitHub answering
#: fewer is likelier than the corpus shrinking so in a week.
AT_LEAST = 0.75

#: After a refresh that failed, the next waits this long, due or not:
#: GitHub's search failing now is likely to fail a minute later.
AGAIN_AFTER = timedelta(hours=1)

#: Before any repository was created: where a star count's halves
#: begin. The first is asked for with no beginning, so a repository that
#: says it was created earlier is listed as well.
FIRST_DAY = date(2007, 10, 1)

#: What a refresh writes beside the snapshots until it renames it.
_WRITING = re.compile(r'^\.all-.+\.tmp$')


class CutShort(Exception):
    """A refresh listed too few to be the universe: the last stands."""


@dataclass(frozen=True)
class Refreshed:
    """A refresh: the snapshot it made, what it found and what it cost."""

    #: `all-<date>`, as the catalog names it.
    snapshot: str
    path: Path
    #: Repositories listed, each once.
    repositories: int
    #: Search requests answered.
    requests: int
    #: Windows of star counts, and halves of one, asked for.
    queries: int
    #: Pages GitHub said were incomplete every time they were asked.
    incomplete: int
    #: Results of one day's star count past the cap, which no query
    #: reaches.
    beyond: int
    #: Results that were no repository.
    unusable: int
    started_at: datetime
    finished_at: datetime


def made_at(snapshot: catalog.Snapshot) -> datetime:
    """When `snapshot` was finished, as its marker says; one with no
    marker to say it, as `github search` wrote one, when its file was
    last written."""
    try:
        said = json.loads(snapshot.marker.read_text(encoding='utf-8'))
        finished = datetime.fromisoformat(said['finished_at'])
    except (OSError, ValueError, KeyError, TypeError):
        pass
    else:
        if finished.tzinfo is None:
            finished = finished.replace(tzinfo=timezone.utc)
        return finished
    try:
        written = snapshot.path.stat().st_mtime
    except OSError:
        return datetime.combine(snapshot.day, time(), timezone.utc)
    return datetime.fromtimestamp(written, timezone.utc)


def universe_due(search_dir: Path, now: datetime, every: timedelta) -> bool:
    """Whether the universe is to be searched again: there is no complete
    snapshot, or the newest was finished `every` ago or longer."""
    newest = catalog.newest_complete(search_dir, now.date())
    return newest is None or made_at(newest) + every <= now


def _stamp(path: Path) -> str:
    """Which writing of `path` it is: its size and when it was written."""
    status = path.stat()
    return f'{status.st_size}:{status.st_mtime_ns}'


def _members(path: Path) -> tuple[list[Member], int]:
    """The repositories a snapshot lists with a node id, and how many
    others it lists."""
    members: dict[int, str] = {}
    without = set()
    with path.open(encoding='utf-8', errors='replace') as handle:
        for line in handle:
            try:
                record = json.loads(line)
                repository_id = int(record['id'])
            except (ValueError, TypeError, KeyError):
                continue
            node_id = record.get('node_id')
            if isinstance(node_id, str) and node_id:
                members[repository_id] = node_id
                without.discard(repository_id)
            else:
                members.pop(repository_id, None)
                without.add(repository_id)
    return (
        [Member(key, value) for key, value in sorted(members.items())],
        len(without),
    )


def load_universe(
    state: CollectorState, search_dir: Path, now: datetime,
) -> UniverseSnapshot | None:
    """collector.sqlite's universe, loaded from the newest complete
    snapshot in `search_dir` unless it was from that writing of it.
    With no complete snapshot, the universe it has stands."""
    held = state.universe()
    newest = catalog.newest_complete(search_dir, now.date())
    if newest is None:
        return held
    try:
        stamp = _stamp(newest.path)
    except OSError:
        return held
    if held is not None and (held.snapshot, held.stamp) == (
        newest.name, stamp,
    ):
        return held
    members, without = _members(newest.path)
    loaded = UniverseSnapshot(
        snapshot=newest.name, stamp=stamp, repositories=len(members),
        loaded_at=now,
    )
    state.keep_universe(loaded, members)
    if without:
        logger.warning(
            'Repositories of the universe with no node id are not swept',
            snapshot=newest.name, without_node_id=without,
        )
    logger.info(
        'The universe is loaded', snapshot=newest.name,
        repositories=len(members),
    )
    return loaded


@dataclass(frozen=True)
class _Page:
    """A page of search results."""

    #: How many GitHub says the query has, past the cap as well.
    total: int
    incomplete: bool
    items: list[dict[str, Any]]


def _page(answer: Answer) -> _Page:
    body = answer.json()
    items = body.get('items') if isinstance(body, dict) else None
    total = body.get('total_count') if isinstance(body, dict) else None
    if not isinstance(items, list) or not isinstance(total, int):
        raise Failed(
            f'GET {answer.url}: {answer.status}, and not a page of search '
            'results', status=answer.status, url=answer.url,
        )
    return _Page(
        total=total, incomplete=body.get('incomplete_results') is True,
        items=[item for item in items if isinstance(item, dict)],
    )


def _stars(item: dict[str, Any]) -> int:
    stars = item.get('stargazers_count')
    return stars if isinstance(stars, int) else 0


def _created(start: date, end: date, today: date) -> str:
    """The qualifier for repositories created from `start` to `end`: none
    for all of them, and no beginning, or no end, for the first and the
    last half."""
    if start <= FIRST_DAY and end >= today:
        return ''
    if start <= FIRST_DAY:
        return f' created:<={end:%Y-%m-%d}'
    if end >= today:
        return f' created:>={start:%Y-%m-%d}'
    if start == end:
        return f' created:{start:%Y-%m-%d}'
    return f' created:{start:%Y-%m-%d}..{end:%Y-%m-%d}'


class _Search:
    """One search of every repository with at least `low` stars, each
    written to `handle` once, as it is found."""

    def __init__(
        self,
        client: GitHubClient,
        handle: TextIO,
        *,
        low: int,
        today: date,
        sleep: Callable[[float], Awaitable[None]],
    ) -> None:
        self.client = client
        self.handle = handle
        self.low = low
        self.today = today
        self._sleep = sleep
        #: Every repository listed, and its node id where it has one.
        self.listed: set[int] = set()
        self.members: dict[int, str] = {}
        self.requests = 0
        self.queries = 0
        self.incomplete = 0
        self.beyond = 0
        self.unusable = 0

    async def run(self) -> None:
        """Window by window of star counts, from the most down."""
        high: int | None = None
        while True:
            if high is not None and high <= self.low:
                if high == self.low:
                    await self._by_date(high)
                return
            query = (
                f'stars:>={self.low}' if high is None
                else f'stars:{self.low}..{high}'
            )
            first = await self._ask(query, 1)
            items = await self._rest(query, first)
            if first.total <= SEARCH_CAP:
                return
            if not items:
                raise Failed(
                    f'The search for {query!r} has {first.total:,} results, '
                    'GitHub says, and gave none',
                )
            edge = max(_stars(item) for item in items[-EDGE:])
            edge = max(self.low, edge if high is None else min(edge, high))
            if high is not None and edge >= high:
                # The first 1,000 all have `high` stars: that count alone
                # has more than a search lists.
                await self._by_date(high)
                high -= 1
            else:
                high = edge

    async def _by_date(self, stars: int) -> None:
        """Every repository with `stars` stars, by when each was created:
        in halves, until each half is listed whole."""
        halves = [(FIRST_DAY, self.today)]
        while halves:
            start, end = halves.pop()
            query = f'stars:{stars}' + _created(start, end, self.today)
            first = await self._ask(query, 1)
            if first.total > SEARCH_CAP and start < end:
                middle = start + (end - start) // 2
                halves.append((middle + timedelta(days=1), end))
                halves.append((start, middle))
                continue
            await self._rest(query, first)
            if first.total > SEARCH_CAP:
                self.beyond += first.total - SEARCH_CAP
                logger.warning(
                    'More repositories were created on one day with as '
                    'many stars than a search lists: the rest are left out',
                    query=query, total=first.total,
                    beyond=first.total - SEARCH_CAP,
                )

    async def _rest(self, query: str, first: _Page) -> list[dict[str, Any]]:
        """`query`'s results, from its first page to the cap, listed."""
        items = list(first.items)
        self._list(first.items)
        reachable = min(first.total, SEARCH_CAP)
        page, last = 1, first
        while len(last.items) >= PER_PAGE and page * PER_PAGE < reachable:
            page += 1
            last = await self._ask(query, page)
            items.extend(last.items)
            self._list(last.items)
        return items

    async def _ask(self, query: str, page: int) -> _Page:
        """A page of `query`'s results, most stars first, asked again
        after a pause while it fails or GitHub says it is incomplete."""
        if page == 1:
            self.queries += 1

        async def asking() -> _Page:
            answer = await self.client.search(
                'repositories', query, sort='stars', order='desc',
                per_page=PER_PAGE, page=page,
            )
            self.requests += 1
            return _page(answer)

        found = await retried(
            asking, what=f'search {query!r}, page {page}', sleep=self._sleep,
            again=lambda found: found.incomplete,
        )
        if found.incomplete:
            self.incomplete += 1
            logger.warning(
                'GitHub said a page of search results was incomplete each '
                'time it was asked', query=query, page=page,
            )
        return found

    def _list(self, items: Iterable[dict[str, Any]]) -> None:
        """Writes each repository of `items` not listed already."""
        for item in items:
            try:
                repository = Repository.model_validate(item)
            except ValidationError:
                self.unusable += 1
                continue
            if repository.id in self.listed:
                continue
            self.listed.add(repository.id)
            self.handle.write(repository.model_dump_json(exclude_none=True))
            self.handle.write('\n')
            node_id = item.get('node_id')
            if isinstance(node_id, str) and node_id:
                self.members[repository.id] = node_id


class Universe:
    """The universe in `search_dir` and collector.sqlite, searched on
    `client`, whose budget's clock says when it is."""

    def __init__(
        self,
        client: GitHubClient,
        state: CollectorState,
        search_dir: Path,
        *,
        min_stars: int = MIN_STARS,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.client = client
        self.state = state
        self.search_dir = Path(search_dir)
        self.min_stars = min_stars
        self._sleep = sleep
        #: When the last refresh failed, if it did.
        self._failed_at: datetime | None = None

    def now(self) -> datetime:
        return datetime.fromtimestamp(self.client.budget.clock(), timezone.utc)

    def due(self, every: timedelta) -> bool:
        """Whether it is to be searched again (`universe_due`), and not
        `AGAIN_AFTER` since a refresh failed."""
        now = self.now()
        if self._failed_at is not None and now < self._failed_at + AGAIN_AFTER:
            return False
        return universe_due(self.search_dir, now, every)

    def next_due(self, every: timedelta) -> datetime:
        """When it is next to be searched again: now, with no complete
        snapshot; else `every` after the newest was finished. Never
        within `AGAIN_AFTER` of a refresh that failed."""
        now = self.now()
        newest = catalog.newest_complete(self.search_dir, now.date())
        due_at = now if newest is None else made_at(newest) + every
        if self._failed_at is not None:
            due_at = max(due_at, self._failed_at + AGAIN_AFTER)
        return due_at

    def load(self) -> UniverseSnapshot | None:
        """collector.sqlite's universe, from the newest complete snapshot
        (`load_universe`)."""
        return load_universe(self.state, self.search_dir, self.now())

    def _clear(self) -> None:
        """Removes what a refresh killed before its rename left."""
        for child in self.search_dir.iterdir():
            if _WRITING.match(child.name) and child.is_file():
                child.unlink(missing_ok=True)

    async def refresh(self) -> Refreshed:
        """The universe searched again, and made the snapshot
        collector.sqlite follows once whole; or, with the error raised,
        the last one left standing."""
        started = self.now()
        name = f'all-{started:%Y-%m-%d}'
        path = self.search_dir / f'{name}.jsonl'
        self.search_dir.mkdir(parents=True, exist_ok=True)
        self._clear()
        last = load_universe(self.state, self.search_dir, started)
        writing = temporary_beside(path)
        try:
            with writing.open('x', encoding='utf-8') as handle:
                search = _Search(
                    self.client, handle, low=self.min_stars,
                    today=started.date(), sleep=self._sleep,
                )
                await search.run()
                handle.flush()
                # On disk before it is renamed into place: otherwise a
                # crash can leave the name on blocks never written.
                os.fsync(handle.fileno())
            members = len(search.members)
            if not members:
                raise CutShort(
                    'The search listed no repository: taken for a search '
                    'cut short, and the last universe stands.',
                )
            if last is not None and members < last.repositories * AT_LEAST:
                raise CutShort(
                    f'The search listed {members:,} repositories, fewer '
                    f'than {AT_LEAST:.0%} of the {last.repositories:,} the '
                    'universe has: taken for a search cut short, and the '
                    'last universe stands.',
                )
            finished = self.now()
            writing.replace(path)
            atomic_write_text(
                path.with_name(f'{path.name}{catalog.COMPLETE_MARKER}'),
                json.dumps({
                    'finished_at': finished.isoformat(),
                    'repositories': len(search.listed),
                    'requests': search.requests,
                }) + '\n',
            )
        except BaseException as error:
            writing.unlink(missing_ok=True)
            if isinstance(error, Exception):
                self._failed_at = self.now()
                logger.warning(
                    'The universe was not searched again: the last one '
                    'stands', snapshot=name, error=str(error),
                    again_after=str(AGAIN_AFTER),
                )
            raise
        self.state.keep_universe(
            UniverseSnapshot(
                snapshot=name, stamp=_stamp(path), repositories=members,
                loaded_at=finished,
            ),
            [Member(*member) for member in sorted(search.members.items())],
        )
        self._failed_at = None
        refreshed = Refreshed(
            snapshot=name, path=path, repositories=len(search.listed),
            requests=search.requests, queries=search.queries,
            incomplete=search.incomplete, beyond=search.beyond,
            unusable=search.unusable, started_at=started,
            finished_at=finished,
        )
        logger.info(
            'The universe was searched again', snapshot=name,
            repositories=refreshed.repositories, members=members,
            requests=refreshed.requests, queries=refreshed.queries,
            incomplete=refreshed.incomplete, beyond=refreshed.beyond,
            unusable=refreshed.unusable,
            took=f'{(finished - started).total_seconds():.0f}s',
        )
        return refreshed
