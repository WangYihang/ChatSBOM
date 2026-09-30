"""Running a repository's due stages (#161).

`collect` takes one repository, as an observation had it (#160), from
where the store has it to where it is current for the push observed,
one stage at a time, as the due set says (`collector/due.py`): it runs
the stage due, then asks again, until no stage is due. So a push that
resolves to a commit already collected stops there, with nothing after
it run: the early cutoff. A stage that produces nothing for its key, or
fails, is kept in collector.sqlite with its backoff (#100 Q5), and the
stages after it wait. One that finishes clears what was kept of it.

Then the repository is marked collected as of the observation
(`CollectorState.mark_collected`), whatever became of its stages: what
did not finish is kept with its backoff, and is due again once that has
passed. Left changed, a repository whose stage failed would come first
every time, with nothing it may run. What is not the repository's doing
is not kept against it, and marks nothing: a token GitHub refuses, or
no room in the budget within the wait allowed, stops the collection and
goes to the caller, and the repository stays as detection found it.

`tools_for` makes what the stages run on: the API client on the budget;
raw content, with no token; git; and the Syft pool. `observe_now` asks
GitHub how one repository stands now, and keeps it, as the hourly sweep
does: `chatsbom collect repo` collects for the push it sees.

6e runs this for every repository with a stage due, highest priority
first (`due.Priority`), several at once on one set of tools. One
repository is collected by one caller at a time.
"""
from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import AsyncIterator
from collections.abc import Awaitable
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime

import httpx2
import structlog

from chatsbom.collector.budget import BudgetManager
from chatsbom.collector.client import GitHubClient
from chatsbom.collector.due import CHAIN
from chatsbom.collector.due import Priority
from chatsbom.collector.due import Standing
from chatsbom.collector.due import standing
from chatsbom.collector.due import Verdict
from chatsbom.collector.errors import GitHubError
from chatsbom.collector.errors import RateLimited
from chatsbom.collector.errors import Unauthorized
from chatsbom.collector.gitremote import GITHUB
from chatsbom.collector.gitremote import GitRemote
from chatsbom.collector.gitremote import IN_FLIGHT
from chatsbom.collector.raw import RawClient
from chatsbom.collector.settings import CollectorSettings
from chatsbom.collector.stages import Done
from chatsbom.collector.stages import Nothing
from chatsbom.collector.stages import RepositoryStages
from chatsbom.collector.stages import StageFailed
from chatsbom.collector.stages import Target
from chatsbom.collector.stages import Tools
from chatsbom.collector.state import CollectorState
from chatsbom.collector.state import FAILED
from chatsbom.collector.state import Member
from chatsbom.collector.state import NOTHING
from chatsbom.collector.state import Observed
from chatsbom.collector.sweep import _observed
from chatsbom.collector.sweep import _pushed
from chatsbom.collector.sweep import QUERY
from chatsbom.collector.syftpool import SyftFailed
from chatsbom.collector.syftpool import SyftPool
from chatsbom.collector.syftpool import SyftSettings
from chatsbom.core.config import PathConfig
from chatsbom.core.layout import push_instant
from chatsbom.core.ledger import Stage

logger = structlog.get_logger('collector.runner')

#: What became of a stage that ran.
DONE = 'done'


@asynccontextmanager
async def tools_for(
    paths: PathConfig,
    state: CollectorState,
    settings: CollectorSettings,
    syft: SyftSettings,
    *,
    clock: Callable[[], float] = time.time,
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    github_transport: httpx2.AsyncBaseTransport | None = None,
    raw_transport: httpx2.AsyncBaseTransport | None = None,
    git_base: str = GITHUB,
    wait: float | None = None,
) -> AsyncIterator[Tools]:
    """What the stages run on, closed after: the API on the tokens'
    budget, raw content with no token, git with the first token, and the
    Syft pool. The transports and `git_base` stand in for GitHub's, as a
    test has them."""
    # httpx2 says each request at INFO: a line for every file of every
    # repository. The client says what matters of each at DEBUG
    # (`collector.github`); with `--debug`, httpx2 says it too.
    if logging.getLogger().getEffectiveLevel() > logging.DEBUG:
        logging.getLogger('httpx2').setLevel(logging.WARNING)
    budget = BudgetManager(
        settings.tokens, reserve=settings.reserve, clock=clock, sleep=sleep,
    )
    # No validators: no stage asks conditionally. A release list is
    # decided whole for each push, from the pages as they are, and a
    # commit's date is asked once; a validator kept for either would be a
    # row of collector.sqlite that nothing reads.
    async with GitHubClient(
        budget, validators=None, transport=github_transport,
    ) as github:
        async with RawClient(transport=raw_transport, sleep=sleep) as raw:
            yield Tools(
                paths=paths,
                state=state,
                github=github,
                raw=raw,
                git=GitRemote(
                    token=settings.tokens[0], base=git_base,
                    in_flight=IN_FLIGHT * len(settings.tokens),
                ),
                syft=SyftPool(syft),
                clock=clock,
                wait=wait,
            )


@dataclass(frozen=True)
class Ran:
    """A stage that ran, and what became of it."""

    stage: Stage
    #: The key it ran for.
    key: str
    #: `done`, or the outcome kept: `nothing` or `failed`.
    result: str
    #: What it did, or why it did not, for a person.
    summary: str
    #: When a stage that did not finish is due again.
    due_at: datetime | None = None


@dataclass
class Collected:
    """What a collection did, and where the repository stands after."""

    target: Target
    push: datetime | None
    #: The Syft the SBOM is judged by, and made with.
    syft_version: str | None
    ran: list[Ran] = field(default_factory=list)
    #: Where it stands now.
    standing: Standing | None = None

    @property
    def failed(self) -> bool:
        return any(ran.result == FAILED for ran in self.ran)

    @property
    def cut_off(self) -> bool:
        """Whether it came to a commit already collected: a push resolved,
        and every stage after the commit found present without running."""
        if self.standing is None or not self.standing.current:
            return False
        ran = {ran.stage for ran in self.ran if ran.result == DONE}
        return bool(ran) and ran <= {Stage.RELEASE, Stage.COMMIT}


def _standing(
    tools: Tools, target: Target, push: datetime | None,
    syft_version: str | None,
) -> Standing:
    return standing(
        target.repository_id, push, paths=tools.paths, outcomes=tools.state,
        syft_version=syft_version, now=tools.now(),
    )


async def collect(
    tools: Tools,
    observed: Observed,
    *,
    priority: Priority | None = None,
) -> Collected:
    """Run the repository's due stages for the push `observed` saw, one
    after another, until none is due; then mark it collected as of that
    observation (#160).

    `priority` is why it is collected now, which the Syft pool orders its
    scan by (`Priority`): unsaid, a change's, unless what is due is a
    rescan for a tool's new version (`Standing.rescan`)."""
    collected = await _collect(
        tools, Target(observed.repository_id, observed.full_name),
        observed.pushed_at, priority,
    )
    tools.state.mark_collected(
        observed.repository_id, as_of=observed.observed_at,
    )
    return collected


async def _collect(
    tools: Tools,
    target: Target,
    push: datetime | None,
    priority: Priority | None,
) -> Collected:
    """The due stages for `push`, run."""
    version = await tools.syft.version()
    stages = RepositoryStages(tools, target)
    collected = Collected(target, push_instant(push), version)
    for _ in range(2 * len(CHAIN)):
        found = _standing(tools, target, push, version)
        step = found.next
        if step is None:
            collected.standing = found
            return collected
        last = collected.ran[-1] if collected.ran else None
        if (
            last is not None and last.result == DONE
            and (last.stage, last.key) == (step.stage, step.key)
        ):
            # It ran, and what it wrote is not current: the next run
            # would write the same.
            collected.ran[-1] = _keep(
                tools, target, step, FAILED,
                f'wrote what is still not current ({step.why}): '
                f'{last.summary}',
            )
            break
        ran = await _run(tools, stages, found, step, version, priority)
        collected.ran.append(ran)
        if ran.result != DONE:
            break
    collected.standing = _standing(tools, target, push, version)
    return collected


async def _run(
    tools: Tools,
    stages: RepositoryStages,
    found: Standing,
    step: Verdict,
    version: str | None,
    priority: Priority | None,
) -> Ran:
    """One stage, and what became of it, kept."""
    target = stages.target
    started = time.monotonic()
    try:
        done = await _dispatch(stages, found, step, version, priority)
    except Nothing as nothing:
        return _keep(tools, target, step, NOTHING, str(nothing))
    except (RateLimited, Unauthorized):
        raise
    except Exception as error:  # noqa: BLE001 - kept, not raised
        logger.warning(
            'Stage failed', repo=target.full_name, stage=str(step.stage),
            key=step.key, error=str(error),
            kind=type(error).__name__,
        )
        return _keep(tools, target, step, FAILED, _said(error))
    # Done: what was kept of the stage goes, for this key and for any
    # before it, which no verdict reads again.
    tools.state.clear(target.repository_id, str(step.stage))
    logger.info(
        'Stage done', repo=target.full_name, stage=str(step.stage),
        key=step.key, did=done.summary,
        elapsed=f'{time.monotonic() - started:.3f}s',
    )
    return Ran(step.stage, step.key, DONE, done.summary)


async def _dispatch(
    stages: RepositoryStages,
    found: Standing,
    step: Verdict,
    version: str | None,
    priority: Priority | None,
) -> Done:
    if step.stage is Stage.RELEASE:
        assert found.push is not None
        return await stages.release(found.push)
    if step.stage is Stage.COMMIT:
        assert found.release is not None
        return await stages.commit(found.release)
    assert found.commit is not None
    sha = found.commit.commit_sha
    if step.stage is Stage.TREE:
        return await stages.tree(sha)
    if step.stage is Stage.CONTENT:
        return await stages.content(sha)
    if priority is None:
        priority = Priority.RESCAN if found.rescan else Priority.CHANGED
    return await stages.sbom(sha, version, priority=int(priority))


def _keep(
    tools: Tools, target: Target, step: Verdict, kind: str, detail: str,
) -> Ran:
    """An outcome that is not done, kept with its backoff."""
    outcome = tools.state.record(
        target.repository_id, str(step.stage), step.key, kind,
        now=tools.now(), detail=detail,
    )
    return Ran(step.stage, step.key, kind, outcome.detail, outcome.due_at)


def _said(error: BaseException) -> str:
    """An error, as an outcome's detail says it: what it says, and what
    it is where that is not the stage's own kind."""
    text = str(error) or type(error).__name__
    if isinstance(error, (GitHubError, StageFailed, SyftFailed)):
        return text
    return f'{type(error).__name__}: {text}'


# -- observing one repository -----------------------------------------------


async def observe_now(tools: Tools, member: Member) -> Observed | None:
    """How `member`'s repository stands on GitHub now, asked by its node
    id and kept as the sweep asks and keeps it (#160): with its query
    and its reading of the answer, and a push, HEAD or latest release
    other than the last observed marked a change. Else the sweep, which
    compares with the last observation, would never see a change this
    saw first. None when GitHub has no such repository any more, or
    answers with another."""
    answer = await tools.github.graphql(
        QUERY, {'ids': [member.node_id]}, wait=tools.wait,
    )
    tools.spent[answer.bucket] += 1
    nodes = answer.data.get('nodes')
    node = nodes[0] if isinstance(nodes, list) and nodes else None
    if node is None:
        return None
    now = tools.now()
    found = _observed(node, member, now)
    if found is None:
        return None
    with tools.state.transaction():
        before = tools.state.observe(found)
        if before is not None and _pushed(before) != _pushed(found):
            tools.state.mark_changed(member.repository_id, at=now)
    return found
