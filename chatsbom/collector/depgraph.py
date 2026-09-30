"""The dependency graph, fetched on a clock (#162, part 6d of #155; #128
section 2.1).

GitHub's dependency graph is the second SBOM source: GitHub reads the
manifests itself, and has a graph of the Maven and Composer projects
that ship no lockfile, which Syft finds nothing in. It is fetched as
#50 fetches it, through GitHub's report flow, on the collector's
client:

1. a report is asked for, `GET /repos/{owner}/{repo}/dependency-graph/
   sbom/generate-report`, answered 201 with `sbom_url`, where on the API
   to look for it;
2. it is looked at, `GET <sbom_url>`, answered 202 while GitHub makes
   it and then 302, to a link off the API that its signature lets anyone
   fetch for a few minutes;
3. the graph is downloaded from that link without the token, through a
   transport of its own that no client logs a request of. The link is
   never followed with the token, never logged, and never kept.

The synchronous endpoint, which answered with the graph itself, closes
after 2026-11-13, and is never asked (#50).

Every request of the API draws from the dependency graph's own bucket,
`BUCKET`, which GitHub meters apart from the REST API's, at 100 to 200
an hour per token. The budget backs it off on a refusal, and a step it
refuses asks nothing more, records nothing of the repository it was
refused at, and says when to come back.

When a graph is due (the owner's decision, 2026-09-30):

- **Pushed, or old.** A repository's graph is fetched again once the
  repository was pushed after the graph was last learned, as the
  sweep observed `pushedAt` (#160); or, pushed or not, once that is
  older than `DepgraphSettings.max_age`, 180 days, the backstop. Last
  learned: when the newest graph kept was fetched, which the store
  says, or when a fetch last found it unchanged, whichever is later;
  each as of when its report was asked for, since a push after that
  may not be in it. A repository whose push was never observed has the
  backstop alone.
- **In this order.** Never asked about first: nothing is known of its
  graph. Then those pushed since, by when they were pushed, the longest
  waiting first: a change GitHub has and the store has not. Then those
  past the backstop, and those with no graph asked about again, the
  longest since GitHub said anything of them first.
- **No graph.** A repository GitHub has no graph of, 404, is asked
  about again after `DepgraphSettings.no_graph`, 30 days: a `nothing`
  outcome with that delay, the negative cache.
- **As it was.** A graph the same as the newest kept is not written
  again: byte for byte, but for what GitHub makes anew for each report
  of a graph (`unstamped`), when it made it, `creationInfo.created`, and
  the document's `documentNamespace`. A graph kept that cannot be read
  is none to compare with. A `nothing` outcome of its own (`CHECKED`)
  keeps when it was found so, and a failure after it leaves that as it
  was.
- **Failures** back off as collector.sqlite's outcomes do, from 15
  minutes, doubling, up to a week: a request GitHub failed, a report
  gone or given up before it was downloaded, and a download that is no
  SPDX document.

What is kept:

- **The graph,** in the store's layout, as it was:
  `09-github-depgraph/<id>/<fetched>-<head>/sbom.spdx.json` and its
  `meta.json`, stamped with when its report was asked for, and the HEAD
  collector.sqlite had observed then: the graph as GitHub had it then,
  not when it was downloaded, which may be minutes later. Wrapped in
  `sbom`, as the synchronous endpoint answered: a report downloads the
  SPDX document alone (ClickHouse/ClickBOM#119), and every reader of a
  graph expects the wrapper.
- **The reports pending,** `at_once` at most, in collector.sqlite
  (`depgraph_report`): where on the API to look, the HEAD, and how
  often each has been looked at. A restart looks at them again rather
  than asking anew.

No process runs this yet: `chatsbom collect` will (6e), calling `step`
on a clock of its own.
"""
import asyncio
import heapq
import json
import os
from collections.abc import Iterable
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from types import TracebackType
from typing import Any
from typing import Self
from urllib.parse import quote
from urllib.parse import unquote
from urllib.parse import urlsplit

import httpx2
import structlog

from chatsbom.__version__ import __version__
from chatsbom.collector.budget import Limits
from chatsbom.collector.client import Answer
from chatsbom.collector.client import CONNECT_RETRIES
from chatsbom.collector.client import GitHubClient
from chatsbom.collector.client import QUOTED
from chatsbom.collector.client import REDIRECTS
from chatsbom.collector.client import TIMEOUT
from chatsbom.collector.errors import GitHubError
from chatsbom.collector.errors import Gone
from chatsbom.collector.errors import NotFound
from chatsbom.collector.errors import RateLimited
from chatsbom.collector.errors import Unauthorized
from chatsbom.collector.settings import interval
from chatsbom.collector.settings import SettingsError
from chatsbom.collector.state import CollectorState
from chatsbom.collector.state import FAILED
from chatsbom.collector.state import NOTHING
from chatsbom.collector.state import Observed
from chatsbom.collector.state import Outcome
from chatsbom.collector.state import PendingReport
from chatsbom.core import depgraph_store
from chatsbom.core.redact import redact

logger = structlog.get_logger('collector.depgraph')

#: The stage its outcomes are kept under in collector.sqlite, and the
#: key of a failure's or of no graph's: a graph is fetched for no input
#: of its own.
STAGE = 'depgraph'
KEY = ''

#: The key of the `nothing` outcome that says when a fetch last found a
#: repository's graph unchanged: apart from the other, so that a failure
#: after it leaves what it learned.
CHECKED = 'unchanged'

#: The bucket GitHub meters the dependency graph's SBOM from, as its
#: answers' `X-RateLimit-Resource` names it: #50's live probe.
BUCKET = 'dependency_sbom'

#: How long a graph learned stands, pushed or not, before it is fetched
#: again, by default: the backstop.
MAX_AGE = timedelta(days=180)

#: How long a repository GitHub has no graph of is left, by default,
#: before it is asked about again.
NO_GRAPH = timedelta(days=30)

#: Either setting at most: a graph a decade old is as good as one never
#: fetched again, and an instant not far past that is more than a date
#: can hold.
LONGEST = timedelta(days=3_650)

#: The first look at a report, after it is asked for, and the longest
#: between two, each twice the last. GitHub makes most in seconds, one
#: of 25,233 packages in 19 (ClickHouse/ClickBOM#119), and each look
#: draws from the bucket.
FIRST_LOOK = timedelta(seconds=2)
LOOK_CAP = timedelta(minutes=15)

#: Looks at a report before it is given up, about half an hour after it
#: was asked for; asked for again, GitHub may do better.
LOOKS = 10

#: Reports pending at once, at most: so few that if the bucket is spent
#: before they are downloaded, little waits on it.
AT_ONCE = 10

#: How long a request waits for a token with room, at most: for other
#: requests to end, not for a window to.
WAIT = 60.0

#: When a refused bucket, which did not say when it has room again, is
#: tried again.
RETRY = timedelta(minutes=1)

#: A redirect that says the repository moved, not that a report is
#: ready.
MOVED = frozenset({301, 308})

#: Before any instant: when a repository never asked about was.
_NEVER = datetime.min.replace(tzinfo=timezone.utc)


@dataclass(frozen=True)
class DepgraphSettings:
    """How long a graph learned stands unpushed, and how long no graph
    does."""

    max_age: timedelta = MAX_AGE
    no_graph: timedelta = NO_GRAPH


def _interval(
    setting: str, value: str | None, default: timedelta,
) -> timedelta:
    """`setting`, said as the collector's intervals are
    (`settings.interval`), and no longer than `LONGEST`."""
    try:
        said = interval(setting, value, default)
    except OverflowError:
        said = timedelta.max
    if said > LONGEST:
        raise SettingsError(
            setting,
            f'{setting} is {LONGEST.days}d at most, ten years: {value!r}',
        )
    return said


def depgraph_settings(
    environ: Mapping[str, str] | None = None,
) -> DepgraphSettings:
    """The dependency graph's settings, from `environ`: the process's
    environment unless given. CHATSBOM_DEPGRAPH_MAX_AGE is how long a
    graph learned stands unpushed, 180d, and CHATSBOM_DEPGRAPH_NO_GRAPH
    how long no graph does, 30d, unless they say, as the sweep's interval
    is said: a whole number and a unit, `s`, `m`, `h`, `d` or `w`."""
    if environ is None:
        environ = os.environ
    return DepgraphSettings(
        max_age=_interval(
            'CHATSBOM_DEPGRAPH_MAX_AGE',
            environ.get('CHATSBOM_DEPGRAPH_MAX_AGE'), MAX_AGE,
        ),
        no_graph=_interval(
            'CHATSBOM_DEPGRAPH_NO_GRAPH',
            environ.get('CHATSBOM_DEPGRAPH_NO_GRAPH'), NO_GRAPH,
        ),
    )


def look_after(looks: int) -> timedelta:
    """How long after the `looks`th look at a report the next is due:
    `FIRST_LOOK` after it was asked for, twice as long each time after,
    up to `LOOK_CAP`."""
    return min(FIRST_LOOK * (1 << min(max(looks, 0), 30)), LOOK_CAP)


def document_of(body: Any) -> dict[str, Any] | None:
    """A downloaded report as every reader of a graph expects it,
    `{"sbom": {...}}`, as the synchronous endpoint answered. GitHub
    documents a report as "the SBOM in SPDX JSON format", and one has
    been seen to be the document alone (ClickHouse/ClickBOM#119): either
    is taken. None for anything else, which is no graph to keep and no
    evidence that there is none."""
    if isinstance(body, dict):
        if isinstance(body.get('sbom'), dict):
            return body
        if 'sbom' not in body and isinstance(body.get('spdxVersion'), str):
            return {'sbom': body}
    return None


def unstamped(payload: Any) -> Any:
    """A graph kept, `{"sbom": {...}}`, without what GitHub makes anew
    for each report of it: when it made the report, the SPDX document's
    `creationInfo.created`, and the document's own `documentNamespace`.
    Two reports of a graph that did not change differ in these alone.
    Anything else is as it was, in its order; nothing is changed in
    place."""
    sbom = payload.get('sbom') if isinstance(payload, dict) else None
    if not isinstance(sbom, dict):
        return payload
    sbom = {
        key: value for key, value in sbom.items()
        if key != 'documentNamespace'
    }
    info = sbom.get('creationInfo')
    if isinstance(info, dict):
        sbom['creationInfo'] = {
            key: value for key, value in info.items() if key != 'created'
        }
    return {**payload, 'sbom': sbom}


def _as_it_was(document: dict[str, Any], kept: Path) -> bool:
    """Whether `document` is the graph kept at `kept`, byte for byte but
    for its stamps (`unstamped`); not when that cannot be read."""
    try:
        before = json.loads(kept.read_bytes())
    except (OSError, ValueError):
        return False
    return json.dumps(unstamped(before)) == json.dumps(unstamped(document))


@dataclass
class Step:
    """What one step did, and when the next has something to do."""

    #: Reports asked for, and answered.
    asked: int = 0
    #: Looks at the reports pending, answered.
    looked: int = 0
    #: Graphs written to the store, and graphs as the newest kept but
    #: for their stamps, which were not.
    stored: int = 0
    unchanged: int = 0
    #: Repositories GitHub has no graph of.
    no_graph: int = 0
    #: Looks at a report GitHub had not made yet.
    not_ready: int = 0
    #: What failed and backs off: a request, a report, or a download.
    failed: int = 0
    #: Reports pending after it.
    pending: int = 0
    #: The refusal that ended it, when the bucket had no room.
    refused: RateLimited | None = None
    #: When the next step has something to do; None when nothing is
    #: pending, due, or to come.
    next_at: datetime | None = None


class Depgraph:
    """The dependency graphs of the repositories it is given, fetched
    again after each push, and at the backstop.

    One per collector.sqlite, and one step at a time: what the store
    and collector.sqlite said is read once, and kept up with what this
    writes, which nothing else does.
    """

    def __init__(
        self,
        github: GitHubClient,
        state: CollectorState,
        store: Path,
        *,
        settings: DepgraphSettings | None = None,
        downloads: httpx2.AsyncBaseTransport | None = None,
        at_once: int = AT_ONCE,
    ) -> None:
        if at_once < 1:
            raise ValueError(f'at least one report at once: {at_once}')
        self.github = github
        self.state = state
        #: `09-github-depgraph`, where graphs are kept.
        self.store = Path(store)
        self.settings = settings or DepgraphSettings()
        self.at_once = at_once
        #: What a finished report is downloaded through: a transport that
        #: has never held a token, with no client to log its requests.
        self._downloads = (
            downloads if downloads is not None
            else httpx2.AsyncHTTPTransport(retries=CONNECT_RETRIES)
        )
        #: When each repository's newest graph was fetched, as the store
        #: says; the failure or the no graph collector.sqlite keeps of
        #: each; and when a fetch last found each graph unchanged.
        self._graphs: dict[int, datetime] = {}
        self._outcomes: dict[int, Outcome] = {}
        self._checked: dict[int, datetime] = {}
        self._read = False

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        await self.aclose()

    async def aclose(self) -> None:
        await self._downloads.aclose()

    # -- a step -----------------------------------------------------------

    async def step(self, repositories: Iterable[Observed]) -> Step:
        """Looks at each report due, then asks for the graphs due, as
        many as there is room for: each once at most.

        `repositories` are those to keep graphs of, as collector.sqlite
        observed them: the universe. A refusal of the bucket ends the
        step, and leaves what it was refused at as it was. Every token
        refused, 401, is raised (`Unauthorized`), as is what goes wrong
        beyond GitHub, a store that cannot be written say; a report
        pending stays pending.
        """
        known = {
            observed.repository_id: observed for observed in repositories
        }
        await self._read_once()
        done = Step()
        now = self._now()
        try:
            for report in self.state.reports_due(now):
                await self._look(report, known, done)
            pending = {
                report.repository_id for report in self.state.reports()
            }
            for observed in self._due(
                known.values(), pending, now, self.at_once - len(pending),
            ):
                await self._ask(observed, done)
        except RateLimited as refused:
            done.refused = refused
            logger.info(
                'Dependency graph paused: its bucket has no room',
                bucket=refused.bucket,
                until=(
                    f'{refused.until:%Y-%m-%d %H:%M:%S} UTC'
                    if refused.until else None
                ),
            )
        reports = self.state.reports()
        done.pending = len(reports)
        done.next_at = self._next(known.values(), reports, done.refused)
        return done

    async def _read_once(self) -> None:
        """What the store and collector.sqlite say, the first time."""
        if self._read:
            return
        self._graphs = await asyncio.to_thread(self._scan)
        for outcome in self.state.outcomes(STAGE):
            if outcome.key == CHECKED:
                self._checked[outcome.repository_id] = outcome.last_at
            else:
                self._outcomes[outcome.repository_id] = outcome
        self._read = True

    def _scan(self) -> dict[int, datetime]:
        """When each repository's newest whole graph was fetched, as the
        store says."""
        try:
            children = list(self.store.iterdir())
        except OSError:
            return {}
        found = {}
        for child in children:
            if not child.name.isdecimal():
                continue
            newest = depgraph_store.newest(self.store, int(child.name))
            if newest is not None:
                found[int(child.name)] = newest.fetched_at
        return found

    def _now(self) -> datetime:
        return datetime.fromtimestamp(self.github.budget.clock(), timezone.utc)

    # -- what is due ------------------------------------------------------

    def _learned(self, repository_id: int) -> datetime | None:
        """When the repository's graph was last learned: the newest kept
        fetched, or a fetch that found it unchanged, whichever is later;
        None when it never was."""
        moments = [
            moment for moment in (
                self._graphs.get(repository_id),
                self._checked.get(repository_id),
            )
            if moment is not None
        ]
        return max(moments, default=None)

    def _not_before(self, observed: Observed) -> datetime | None:
        """When the repository's graph is due: when a failure's backoff or
        no graph's delay ends, if either holds it; at once when it never
        was learned; since the push, if it was pushed after; and else at
        the backstop. None when it is due now."""
        outcome = self._outcomes.get(observed.repository_id)
        if outcome is not None:
            return outcome.due_at
        learned = self._learned(observed.repository_id)
        if learned is None:
            return None
        pushed = observed.pushed_at
        if pushed is not None and pushed > learned:
            return pushed
        return learned + self.settings.max_age

    def _order(self, observed: Observed) -> tuple[int, datetime, int]:
        """Which due repository comes first. Never learned: never asked
        about, then those whose asking failed, by when. Then pushed since
        their graph was learned, by when they were pushed, the longest
        waiting first. Then the rest, past the backstop or asked about
        again with no graph, the longest since GitHub said anything of
        them first."""
        learned = self._learned(observed.repository_id)
        outcome = self._outcomes.get(observed.repository_id)
        said = outcome.last_at if outcome is not None else None
        if learned is None and (outcome is None or outcome.kind == FAILED):
            return 0, said or _NEVER, observed.repository_id
        pushed = observed.pushed_at
        if learned is not None and pushed is not None and pushed > learned:
            return 1, pushed, observed.repository_id
        heard = [moment for moment in (learned, said) if moment is not None]
        return 2, max(heard, default=_NEVER), observed.repository_id

    def _due(
        self, repositories: Iterable[Observed], pending: set[int],
        now: datetime, room: int,
    ) -> list[Observed]:
        """The first `room` of the repositories due, without a report
        pending, in `_order`."""
        due = []
        for observed in repositories:
            if observed.repository_id in pending:
                continue
            not_before = self._not_before(observed)
            if not_before is None or not_before <= now:
                due.append(observed)
        return heapq.nsmallest(max(room, 0), due, key=self._order)

    def _next(
        self, repositories: Iterable[Observed],
        reports: list[PendingReport], refused: RateLimited | None,
    ) -> datetime | None:
        """When the next step has something to do: a report due, or,
        with room for another, a graph due; and never before the bucket
        has room again."""
        now = self._now()
        moments = [report.due_at for report in reports]
        if len(reports) < self.at_once:
            pending = {report.repository_id for report in reports}
            for observed in repositories:
                if observed.repository_id in pending:
                    continue
                not_before = self._not_before(observed)
                moments.append(
                    now if not_before is None else max(not_before, now),
                )
        next_at = min(moments, default=None)
        if refused is not None:
            resumes = refused.until or now + RETRY
            next_at = resumes if next_at is None else max(next_at, resumes)
        return next_at

    # -- asking for a report ----------------------------------------------

    async def _ask(self, observed: Observed, done: Step) -> None:
        """A report of the repository's graph asked for, and kept as
        pending; or what GitHub said instead."""
        owner, _, name = observed.full_name.partition('/')
        if not owner or not name:
            self._fail(
                observed.repository_id, observed.full_name,
                f'not an owner and a name: {observed.full_name!r}', done,
            )
            return
        owner, name = quote(owner, safe=''), quote(name, safe='')
        path = f'/repos/{owner}/{name}/dependency-graph/sbom/generate-report'
        try:
            answer = await self.github.get(
                path, bucket=BUCKET, conditional=False, wait=WAIT,
            )
        except (RateLimited, Unauthorized):
            raise
        except NotFound:
            done.asked += 1
            done.no_graph += 1
            outcome = self._record(
                observed.repository_id, NOTHING, 'no graph: 404',
                delay=self.settings.no_graph,
            )
            logger.info(
                'No dependency graph', repo=observed.full_name,
                asked_again=f'{outcome.due_at:%Y-%m-%d %H:%M:%S} UTC',
            )
            return
        except GitHubError as error:
            done.asked += 1
            self._fail(
                observed.repository_id, observed.full_name, str(error), done,
            )
            return
        done.asked += 1
        url = _report_url(answer)
        if url is None:
            self._fail(
                observed.repository_id, observed.full_name,
                f'{answer.status}, and no report on the API to look at',
                done,
            )
            return
        retry_after = Limits.of(
            answer.headers, self.github.budget.clock(),
        ).retry_after
        now = self._now()
        report = self.state.pend_report(
            observed.repository_id, url, head=observed.head, now=now,
            due_at=now + max(FIRST_LOOK, timedelta(seconds=retry_after or 0)),
        )
        logger.debug(
            'Dependency graph report asked for', repo=observed.full_name,
            looked_at=f'{report.due_at:%Y-%m-%d %H:%M:%S} UTC',
        )

    # -- looking at a report ----------------------------------------------

    async def _look(
        self, report: PendingReport, known: Mapping[int, Observed],
        done: Step,
    ) -> None:
        """One look at a report due: downloaded when it is ready, looked
        at again later when it is not, and given up when it is gone."""
        try:
            answer = await self.github.get(
                report.url, bucket=BUCKET, conditional=False, redirect=True,
                wait=WAIT,
            )
        except (RateLimited, Unauthorized):
            raise
        except ValueError:
            # Not on the API, where the token goes: never asked at all.
            self._give_up(report, 'the report is not on the API', done)
            return
        except (NotFound, Gone) as error:
            done.looked += 1
            self._give_up(report, f'the report is gone: {error}', done)
            return
        except GitHubError as error:
            done.looked += 1
            self._again(report, str(error), done)
            return
        done.looked += 1
        if answer.status in MOVED:
            self._give_up(
                report, f'{answer.status}: the repository has moved', done,
            )
        elif answer.status in REDIRECTS:
            await self._download(report, answer, known, done)
        else:
            done.not_ready += 1
            retry_after = Limits.of(
                answer.headers, self.github.budget.clock(),
            ).retry_after
            self._again(report, 'the report was not ready', done, retry_after)

    def _again(
        self, report: PendingReport, why: str, done: Step,
        retry_after: float | None = None,
    ) -> None:
        """The report looked at again later; or, looked at `LOOKS` times,
        given up."""
        looks = report.attempts + 1
        if looks >= LOOKS:
            self._give_up(report, f'{why}, after {looks} looks', done)
            return
        wait = max(look_after(looks), timedelta(seconds=retry_after or 0))
        self.state.polled(report.repository_id, due_at=self._now() + wait)
        logger.debug(
            'Dependency graph report looked at again later',
            repository_id=report.repository_id, why=why, looks=looks,
        )

    def _give_up(self, report: PendingReport, why: str, done: Step) -> None:
        """The report dropped, and its repository failed: a report is
        asked for anew once the backoff is over."""
        owner, name, _ = self._names(report, {})
        with self.state.transaction():
            self.state.drop_report(report.repository_id)
            self._fail(report.repository_id, f'{owner}/{name}', why, done)

    async def _download(
        self, report: PendingReport, answer: Answer,
        known: Mapping[int, Observed], done: Step,
    ) -> None:
        """The graph a ready report's link serves, kept."""
        link = answer.location
        if link is None or not link.startswith('https://'):
            self._give_up(
                report, 'the report is ready, but not at a link over https',
                done,
            )
            return
        try:
            status, content = await self._fetch(link)
        except (httpx2.HTTPError, httpx2.InvalidURL) as error:
            # Said without the link, which an error may quote.
            said = redact(f'{type(error).__name__}: {error}')[:QUOTED]
            self._again(report, f'the download failed: {said}', done)
            return
        if status != 200:
            self._again(report, f'the download answered {status}', done)
            return
        owner, name, ref = self._names(report, known)
        # As of when its report was asked for, as its HEAD is: a push
        # after that may not be in it.
        kept = await asyncio.to_thread(
            self._keep, report, content, owner, name, ref,
            report.requested_at,
        )
        if isinstance(kept, str):
            self._give_up(report, kept, done)
            return
        full_name = f'{owner}/{name}'
        with self.state.transaction():
            self.state.drop_report(report.repository_id)
            if kept.written:
                # A graph of its own, which learns all a check did.
                self.state.clear(report.repository_id, STAGE)
            else:
                self.state.clear(report.repository_id, STAGE, KEY)
                check = self.state.record(
                    report.repository_id, STAGE, CHECKED, NOTHING,
                    now=report.requested_at,
                    detail='unchanged: as the graph fetched at '
                    f'{kept.fetch.fetched_at:%Y-%m-%d %H:%M:%S} UTC',
                    delay=self.settings.max_age,
                )
        self._outcomes.pop(report.repository_id, None)
        if kept.written:
            self._graphs[report.repository_id] = kept.fetch.fetched_at
            self._checked.pop(report.repository_id, None)
            done.stored += 1
        else:
            self._checked[report.repository_id] = check.last_at
            done.unchanged += 1
        logger.info(
            'Dependency graph stored' if kept.written
            else 'Dependency graph unchanged',
            repo=full_name, path=str(kept.fetch.document),
            commit_sha=kept.fetch.commit_sha,
        )

    async def _fetch(self, link: str) -> tuple[int, bytes]:
        """What the link answers, asked with no token, and without a
        client, which would log the request, link and all."""
        request = httpx2.Request(
            'GET', link,
            headers={
                'Accept-Encoding': 'gzip',
                'User-Agent': f'chatsbom/{__version__}',
            },
            extensions={'timeout': TIMEOUT.as_dict()},
        )
        response = await self._downloads.handle_async_request(request)
        try:
            content = await response.aread()
        finally:
            await response.aclose()
        return response.status_code, content

    def _keep(
        self, report: PendingReport, content: bytes, owner: str, name: str,
        ref: str, fetched_at: datetime,
    ) -> depgraph_store.Stored | str:
        """The graph downloaded, kept in the store unless it is the
        newest kept but for its stamps; or why it is none. Off the event
        loop: a graph may be tens of megabytes."""
        try:
            body = json.loads(content)
        except ValueError:
            return 'the report is not JSON'
        document = document_of(body)
        if document is None:
            return f'the report is no SPDX document: {type(body).__name__}'
        newest = depgraph_store.newest(self.store, report.repository_id)
        if newest is not None and _as_it_was(document, newest.document):
            return depgraph_store.Stored(fetch=newest, written=False)
        return depgraph_store.store(
            self.store, repository_id=report.repository_id, owner=owner,
            repo=name, payload=document, fetched_at=fetched_at, ref=ref,
            head_sha=report.head or '', http_status=200,
        )

    def _names(
        self, report: PendingReport, known: Mapping[int, Observed],
    ) -> tuple[str, str, str]:
        """The owner, name and default branch the graph is kept under:
        as the repository was observed, or, observed no more, as the
        report's URL names it."""
        observed = (
            known.get(report.repository_id)
            or self.state.observed(report.repository_id)
        )
        if observed is not None:
            owner, _, name = observed.full_name.partition('/')
            return owner, name, observed.default_branch or ''
        parts = urlsplit(report.url).path.split('/')
        if len(parts) > 3 and parts[1] == 'repos':
            return unquote(parts[2]), unquote(parts[3]), ''
        return '', '', ''

    # -- outcomes ---------------------------------------------------------

    def _record(
        self, repository_id: int, kind: str, detail: str, *,
        delay: timedelta | None = None,
    ) -> Outcome:
        """A failure, or no graph, which counts attempts of its kind in a
        row: a failure after months with no graph is a first one. What a
        check learned is left as it was."""
        before = self._outcomes.get(repository_id)
        with self.state.transaction():
            if before is not None and before.kind != kind:
                self.state.clear(repository_id, STAGE, KEY)
            outcome = self.state.record(
                repository_id, STAGE, KEY, kind, now=self._now(),
                detail=detail, delay=delay,
            )
        self._outcomes[repository_id] = outcome
        return outcome

    def _fail(
        self, repository_id: int, full_name: str, why: str, done: Step,
    ) -> None:
        """A failure of the repository's, which backs it off."""
        done.failed += 1
        outcome = self._record(repository_id, FAILED, why)
        logger.warning(
            'Dependency graph failed',
            repo=full_name, repository_id=repository_id,
            error=outcome.detail, attempts=outcome.attempts,
            retry_at=f'{outcome.due_at:%Y-%m-%d %H:%M:%S} UTC',
        )


def _report_url(answer: Answer) -> str | None:
    """Where on the API the answer to a report's request says to look for
    it: on the API's own origin, as the look carries the token, and with
    no query, as what is kept of a report is never a signed link."""
    try:
        body = answer.json()
    except GitHubError:
        return None
    url = body.get('sbom_url') if isinstance(body, dict) else None
    if not isinstance(url, str):
        return None
    try:
        where, asked = urlsplit(url), urlsplit(answer.url)
    except ValueError:
        return None
    if (
        (where.scheme, where.netloc.lower())
        != (asked.scheme, asked.netloc.lower())
        or where.query or where.fragment
    ):
        return None
    return url
