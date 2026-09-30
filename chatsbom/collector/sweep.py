"""The sweep (#160; #128 section 2.1): every hour by default, each
repository of the universe asked after by its node id, 100 a GraphQL
`nodes(ids:)` call, about 650 calls for 65,000 repositories.

Of each it reads `databaseId`, `nameWithOwner`, `stargazerCount`,
`isArchived`, `pushedAt`, the default branch's name and HEAD, and the
latest release's tag and date, and records them with
`CollectorState.observe`, which gives back the observation before. A
push, a HEAD or a latest release other than before is a change
(`mark_changed`), which is known without asking again, and which 6c
reads (`CollectorState.changed`, `never_collected`).

- **A rename costs nothing:** the node id stays, and the new name is
  recorded, where asking by name would cost a redirect every time.
- **A node that comes back null** (deleted, made private, blocked) is
  gone, and not asked after until the next universe (`mark_gone`). One
  that GitHub says it failed to resolve for another reason, a timeout
  say, is asked after again the next sweep.
- **What it cost** is read from each answer's `rateLimit { cost }`, and
  where the bucket stood from its rate-limit headers. A sweep logs both,
  and says so where the headers fell by other than `rateLimit` said:
  #128 asks for the cost model to be measured on a live token before
  the collector relies on it, and this is that measure.
- **One call at a time:** GitHub's secondary limits hold GraphQL to
  about a minute of its CPU a minute, and a sweep sequential is done in
  minutes.
- **Its place** is kept in collector.sqlite after each call. A refusal
  backs the bucket off in the budget, and the call is asked again, in
  place. A sweep cut short, by `RateLimited` past `wait`, by being
  cancelled or by the process dying, goes on where it was, and one that
  never finished is due at once.
- **A call that fails** is asked again after a pause (`retry`); one that
  fails every time is skipped, and its repositories asked after the
  next sweep.
"""
import asyncio
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any

import structlog

from chatsbom.collector.client import GitHubClient
from chatsbom.collector.client import GraphQLAnswer
from chatsbom.collector.errors import Failed
from chatsbom.collector.errors import Unauthorized
from chatsbom.collector.retry import retried
from chatsbom.collector.state import CollectorState
from chatsbom.collector.state import Member
from chatsbom.collector.state import Observed
from chatsbom.collector.state import Sweep

logger = structlog.get_logger('collector.sweep')

#: Node ids a call asks after: the most `nodes(ids:)` takes.
NODES_PER_CALL = 100

#: What the sweep reads of each repository (#128 section 2.1), and what
#: the call cost.
QUERY = '''
query($ids: [ID!]!) {
  rateLimit { cost nodeCount remaining resetAt }
  nodes(ids: $ids) {
    __typename
    ... on Repository {
      id
      databaseId
      nameWithOwner
      stargazerCount
      isArchived
      pushedAt
      defaultBranchRef { name target { oid } }
      latestRelease { tagName publishedAt }
    }
  }
}
'''

#: What GitHub says of a node that is null because it cannot be seen:
#: deleted or made private, and blocked or behind an organisation's
#: SAML. A null node it says nothing of is gone too.
GONE = frozenset({'NOT_FOUND', 'FORBIDDEN'})


@dataclass(frozen=True)
class _Call:
    """A call's answer, as the sweep reads it."""

    #: One per node id asked after, in order: a node, or None.
    nodes: Sequence[Any]
    #: What GitHub said of each node it could not resolve, by index.
    errors: Mapping[int, str]
    #: Points, as `rateLimit` said, or None where it said nothing.
    cost: int | None
    token: str
    headers: Mapping[str, str]


def _call(answer: GraphQLAnswer, asked: int) -> _Call:
    """`answer`, or Failed if it is not an answer to the sweep's call."""
    found = answer.data.get('nodes')
    if not isinstance(found, list) or len(found) != asked:
        raise Failed(
            f'GraphQL answered {asked} node ids with no list of as many '
            'nodes',
        )
    errors = {}
    for error in answer.errors:
        path = error.get('path')
        if (
            isinstance(path, list) and len(path) == 2
            and path[0] == 'nodes' and isinstance(path[1], int)
        ):
            errors[path[1]] = str(error.get('type') or '')
    said = answer.data.get('rateLimit')
    cost = said.get('cost') if isinstance(said, dict) else None
    return _Call(
        nodes=found, errors=errors,
        cost=cost if isinstance(cost, int) else None,
        token=answer.token, headers=answer.headers,
    )


def _instant(value: object) -> datetime | None:
    """GitHub's `2026-09-01T00:00:00Z`, aware; None for anything else."""
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def _object(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, dict) else {}


def _observed(node: Any, member: Member, now: datetime) -> Observed | None:
    """What `node` says of `member`'s repository, or None when it is not
    that repository."""
    node = _object(node)
    name = node.get('nameWithOwner')
    if (
        node.get('__typename', 'Repository') != 'Repository'
        or node.get('databaseId') != member.repository_id
        or not isinstance(name, str)
    ):
        return None
    node_id = node.get('id')
    stars = node.get('stargazerCount')
    archived = node.get('isArchived')
    branch = _object(node.get('defaultBranchRef'))
    head = _object(branch.get('target')).get('oid')
    release = _object(node.get('latestRelease'))
    return Observed(
        repository_id=member.repository_id,
        node_id=node_id if isinstance(node_id, str) else member.node_id,
        full_name=name,
        stars=stars if isinstance(stars, int) else None,
        archived=archived if isinstance(archived, bool) else None,
        pushed_at=_instant(node.get('pushedAt')),
        default_branch=branch.get('name') or None,
        head=head if isinstance(head, str) else None,
        release_tag=release.get('tagName') or None,
        release_at=_instant(release.get('publishedAt')),
        observed_at=now,
    )


def _pushed(observed: Observed) -> tuple[object, ...]:
    """What a change is a change of: what 6c collects follows from it."""
    return (
        observed.pushed_at, observed.head, observed.release_tag,
        observed.release_at,
    )


def _number(value: str | None) -> int | None:
    try:
        return None if value is None else int(value)
    except ValueError:
        return None


class _Metered:
    """What a run's calls cost, as `rateLimit` said and as the headers
    fell: from one call to the next with a token, in one window, the
    headers' remaining falls by the later call's cost."""

    def __init__(self) -> None:
        #: What the headers fell by, and what `rateLimit` said of the
        #: same calls.
        self.spent = 0
        self.measured = 0
        #: The last answer's headers.
        self.headers: Mapping[str, str] = {}
        self._last: dict[str, tuple[int, int]] = {}

    def add(self, call: _Call) -> None:
        remaining = _number(call.headers.get('x-ratelimit-remaining'))
        reset = _number(call.headers.get('x-ratelimit-reset'))
        self.headers = call.headers
        if remaining is None or reset is None:
            return
        before = self._last.get(call.token)
        if before is not None and before[0] == reset and call.cost is not None:
            self.spent += before[1] - remaining
            self.measured += call.cost
        self._last[call.token] = (reset, remaining)


class Sweeper:
    """The sweep of the universe collector.sqlite holds, on `client`,
    whose budget's clock says when it is."""

    def __init__(
        self,
        client: GitHubClient,
        state: CollectorState,
        *,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.client = client
        self.state = state
        self._sleep = sleep
        #: What a call is held at while in flight: what the last cost.
        self._cost = 1

    def now(self) -> datetime:
        return datetime.fromtimestamp(self.client.budget.clock(), timezone.utc)

    def due(self, every: timedelta) -> bool:
        """Whether to sweep: the last sweep did not finish, or began
        `every` ago or longer; never with no universe to sweep."""
        if not self.state.members(limit=1):
            return False
        latest = self.state.latest_sweep()
        return (
            latest is None or latest.finished_at is None
            or latest.started_at + every <= self.now()
        )

    async def run(self, *, wait: float | None = None) -> Sweep | None:
        """The universe swept, from where the last sweep stopped if it did
        not finish, or from its start: the sweep, as it finished. None
        when the universe has no member. `wait` is how long a call may
        wait for the budget, past which it is `RateLimited`, and the
        sweep, its place kept, is cut short."""
        sweep = self.state.latest_sweep()
        if sweep is None or sweep.finished_at is not None:
            if not self.state.members(limit=1):
                return None
            sweep = self.state.begin_sweep(self.now())
        metered = _Metered()
        while members := self.state.members(
            after=sweep.position, limit=NODES_PER_CALL,
        ):
            try:
                call = await retried(
                    lambda: self._ask(members, wait),
                    what=f'sweep {sweep.sweep_id}, after repository '
                    f'{sweep.position}', sleep=self._sleep,
                )
            except Unauthorized:
                raise
            except Failed as error:
                sweep = replace(
                    sweep, position=members[-1].repository_id,
                    failed=sweep.failed + 1,
                )
                self.state.keep_sweep(sweep)
                logger.warning(
                    'A sweep call failed every time: its repositories are '
                    'asked after the next sweep', sweep=sweep.sweep_id,
                    first=members[0].repository_id,
                    last=members[-1].repository_id, error=str(error),
                )
                continue
            metered.add(call)
            with self.state.transaction():
                sweep = self._record(sweep, members, call)
                self.state.keep_sweep(sweep)
        sweep = replace(sweep, finished_at=self.now())
        self.state.keep_sweep(sweep)
        self._said(sweep, metered)
        return sweep

    async def _ask(self, members: list[Member], wait: float | None) -> _Call:
        answer = await self.client.graphql(
            QUERY, {'ids': [member.node_id for member in members]},
            cost=self._cost, wait=wait,
        )
        call = _call(answer, len(members))
        if call.cost is not None:
            self._cost = max(call.cost, 1)
        return call

    def _record(
        self, sweep: Sweep, members: list[Member], call: _Call,
    ) -> Sweep:
        """What `call` found of `members`, kept; and `sweep`, gone on."""
        now = self.now()
        nodes = changed = renamed = gone = unresolved = 0
        for index, (member, node) in enumerate(zip(members, call.nodes)):
            if node is None:
                said = call.errors.get(index)
                if said is None or said in GONE:
                    self.state.mark_gone(member.repository_id, now=now)
                    gone += 1
                    logger.info(
                        'A repository is gone: not asked after until the '
                        'next universe', repository_id=member.repository_id,
                        said=said,
                    )
                else:
                    unresolved += 1
                    logger.warning(
                        'GitHub did not resolve a repository: asked after '
                        'again the next sweep',
                        repository_id=member.repository_id, said=said,
                    )
                continue
            observed = _observed(node, member, now)
            if observed is None:
                unresolved += 1
                logger.warning(
                    'GitHub answered a node id with another than the '
                    'repository asked after: asked after again the next '
                    'sweep', repository_id=member.repository_id,
                )
                continue
            nodes += 1
            before = self.state.observe(observed)
            if before is None:
                continue
            if _pushed(before) != _pushed(observed):
                self.state.mark_changed(member.repository_id, at=now)
                changed += 1
            if before.full_name != observed.full_name:
                renamed += 1
                logger.info(
                    'A repository was renamed; its node id is the same',
                    repository_id=member.repository_id,
                    renamed_from=before.full_name,
                    renamed_to=observed.full_name,
                )
        return replace(
            sweep, position=members[-1].repository_id,
            calls=sweep.calls + 1,
            cost=sweep.cost + (self._cost if call.cost is None else call.cost),
            nodes=sweep.nodes + nodes, changed=sweep.changed + changed,
            renamed=sweep.renamed + renamed, gone=sweep.gone + gone,
            unresolved=sweep.unresolved + unresolved,
        )

    def _said(self, sweep: Sweep, metered: _Metered) -> None:
        """What `sweep` found and cost, logged."""
        headers = metered.headers
        reset = _number(headers.get('x-ratelimit-reset'))
        resets_at = None if reset is None else (
            datetime.fromtimestamp(reset, timezone.utc)
            .strftime('%Y-%m-%dT%H:%M:%SZ')
        )
        logger.info(
            'The universe was swept', sweep=sweep.sweep_id,
            calls=sweep.calls, cost=sweep.cost, nodes=sweep.nodes,
            changed=sweep.changed, renamed=sweep.renamed, gone=sweep.gone,
            unresolved=sweep.unresolved, failed=sweep.failed,
            spent=metered.spent, measured=metered.measured,
            remaining=_number(headers.get('x-ratelimit-remaining')),
            limit=_number(headers.get('x-ratelimit-limit')),
            resets_at=resets_at,
            took=f'{(self.now() - sweep.started_at).total_seconds():.0f}s',
        )
        if metered.spent != metered.measured:
            logger.warning(
                "GraphQL's rateLimit and its rate-limit headers disagree on "
                'what the sweep cost', sweep=sweep.sweep_id,
                cost=metered.measured, spent=metered.spent,
            )
