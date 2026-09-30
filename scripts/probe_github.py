#!/usr/bin/env python3
"""What the collector relies on of GitHub, measured on a live token.

The collector (`chatsbom collect`, #171) was built against a stand-in
for GitHub, and four things it counts on were never measured on a live
token (#156, #160, #162):

1. what a GraphQL `nodes(ids:)` call of 100 ids costs, the sweep's
   (#160): 650 of them sweep 65,000 repositories hourly, 650 points of a
   token's 5,000 if each costs one;
2. the `X-RateLimit-Resource` the dependency graph's two endpoints
   answer from (#162): the budget draws them from `dependency_sbom`;
3. that `X-RateLimit-Reset` stays the same within a window, which the
   budget reads to tell a window from the next (#156);
4. whether tokens of one account share their limits (#156), so that a
   second token of the same account adds nothing.

Run it once before the cutover, from the checkout, with the tokens the
collector will use in `.env` or the environment:

    uv run python scripts/probe_github.py            # a report
    uv run python scripts/probe_github.py --json     # the same, as JSON

It says first how many requests it may spend, at most a few dozen, and
spends no more; it writes nothing. What it prints names each token by
its place, `token 1` for GITHUB_TOKEN and on through
CHATSBOM_GITHUB_TOKENS, never by its value. It exits 0 once it has
measured what it could, and 2 when it could not ask GitHub at all.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import asdict
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Any

import httpx2

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from chatsbom.collector.settings import settings_from  # noqa: E402
from chatsbom.collector.settings import SettingsError  # noqa: E402
from chatsbom.collector.sweep import NODES_PER_CALL  # noqa: E402
from chatsbom.collector.sweep import QUERY  # noqa: E402
from chatsbom.collector.tokens import scrub  # noqa: E402
from chatsbom.collector.tokens import Token  # noqa: E402

API = 'https://api.github.com'

#: Tokens compared with the first, at most.
TOKENS = 10

#: Answers of the REST API asked for to see a window's reset hold.
RESETS = 3

#: Seconds between them.
PAUSE = 2.0

#: The query that says where the GraphQL bucket stands, which costs a
#: point: before the sweep's call, to see what that one costs.
STANDING = 'query { rateLimit { cost remaining resetAt } }'

#: What asks a repository, with no conditional request.
HEADERS = {
    'Accept': 'application/vnd.github+json',
    'X-GitHub-Api-Version': '2022-11-28',
    'User-Agent': 'chatsbom-probe',
}


def plan(tokens: int) -> dict[str, int]:
    """The requests the probe spends at most, by bucket, for `tokens`."""
    others = min(tokens, TOKENS) - 1
    return {
        'search': 1,
        'graphql': 2,
        'dependency graph': 2,
        'core': RESETS + 2 * others,
    }


@dataclass(frozen=True)
class Answered:
    """One answer, as the probe keeps it: never its token."""

    token: str
    what: str
    status: int
    resource: str | None
    limit: int | None
    remaining: int | None
    reset: int | None


@dataclass
class Report:
    """What the probe found."""

    tokens: list[str]
    #: The requests it may spend, and those it spent.
    most: int
    spent: int = 0
    nodes: dict[str, Any] = field(default_factory=dict)
    depgraph: dict[str, Any] = field(default_factory=dict)
    resets: dict[str, Any] = field(default_factory=dict)
    sharing: list[dict[str, Any]] = field(default_factory=list)
    #: What went wrong, with no token in it.
    problems: list[str] = field(default_factory=list)


def _one_window(earlier: Answered, later: Answered) -> bool:
    """Whether two answers in turn keep to what a window is: the same
    reset while what is left falls, or a later one, with what is left
    filled again, once a window has ended."""
    if earlier.reset == later.reset:
        return True
    if earlier.reset is None or later.reset is None:
        return True
    return later.reset > earlier.reset and (
        later.remaining is None or earlier.remaining is None
        or later.remaining >= earlier.remaining
    )


def _number(value: str | None) -> int | None:
    try:
        return None if value is None else int(value)
    except ValueError:
        return None


class OutOfRequests(Exception):
    """The probe spent what it said it would."""


class Refused(Exception):
    """GitHub took no token: 401."""


class Probe:
    """The probe, on `tokens`, of the API at `base`."""

    def __init__(
        self,
        tokens: tuple[Token, ...],
        *,
        base: str = API,
        transport: httpx2.AsyncBaseTransport | None = None,
        pause: float = PAUSE,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    ) -> None:
        self.tokens = tokens[:TOKENS]
        self.base = base.rstrip('/')
        self.transport = transport
        self.pause = pause
        self._sleep = sleep
        self.report = Report(
            tokens=[token.label for token in self.tokens],
            most=sum(plan(len(self.tokens)).values()),
        )
        self.answers: list[Answered] = []

    def _said(self, text: str) -> str:
        return scrub(text, self.tokens)

    async def _ask(
        self, client: httpx2.AsyncClient, token: Token, what: str,
        method: str, url: str, body: Any = None,
    ) -> tuple[Answered, Any]:
        """One request, counted: its answer, and its JSON where it has
        some."""
        if self.report.spent >= self.report.most:
            raise OutOfRequests(what)
        self.report.spent += 1
        response = await client.request(
            method, url, json=body,
            headers={**HEADERS, 'Authorization': f'Bearer {token.secret}'},
        )
        headers = response.headers
        answered = Answered(
            token=token.label, what=what, status=response.status_code,
            resource=headers.get('x-ratelimit-resource'),
            limit=_number(headers.get('x-ratelimit-limit')),
            remaining=_number(headers.get('x-ratelimit-remaining')),
            reset=_number(headers.get('x-ratelimit-reset')),
        )
        self.answers.append(answered)
        if response.status_code == 401:
            raise Refused(token.label)
        try:
            body = response.json()
        except ValueError:
            body = None
        return answered, body

    async def run(self) -> Report:
        async with httpx2.AsyncClient(
            base_url=self.base, transport=self.transport,
            follow_redirects=False, timeout=httpx2.Timeout(30.0),
        ) as client:
            for step in (
                self._nodes, self._depgraph, self._resets, self._sharing,
            ):
                try:
                    await step(client)
                except OutOfRequests as spent:
                    self.report.problems.append(
                        f'stopped before {spent}: the requests it said it '
                        'would spend are spent',
                    )
                    break
                except Refused as refused:
                    self.report.problems.append(
                        f'GitHub refused {refused} (401, bad credentials): '
                        'nothing more was asked',
                    )
                    break
                except httpx2.HTTPError as error:
                    self.report.problems.append(
                        self._said(f'{type(error).__name__}: {error}'),
                    )
        self._windows()
        return self.report

    # -- 1. the sweep's call ----------------------------------------------

    async def _nodes(self, client: httpx2.AsyncClient) -> None:
        first = self.tokens[0]
        found, page = await self._ask(
            client, first, 'search', 'GET',
            '/search/repositories?q=stars:%3E%3D1000&sort=stars&order=desc'
            f'&per_page={NODES_PER_CALL}',
        )
        items = page.get('items') if isinstance(page, dict) else None
        if found.status != 200 or not isinstance(items, list):
            self.report.problems.append(
                f'the search answered {found.status}: no node ids to ask '
                'after',
            )
            return
        self._repositories = [
            item for item in items if isinstance(item, dict)
        ]
        ids = [
            item['node_id'] for item in self._repositories
            if isinstance(item.get('node_id'), str)
        ]
        before, _ = await self._ask(
            client, first, 'graphql rateLimit', 'POST', '/graphql',
            {'query': STANDING},
        )
        after, answer = await self._ask(
            client, first, 'graphql nodes', 'POST', '/graphql',
            {'query': QUERY, 'variables': {'ids': ids}},
        )
        data = answer.get('data') if isinstance(answer, dict) else None
        said = data.get('rateLimit') if isinstance(data, dict) else None
        cost = said.get('cost') if isinstance(said, dict) else None
        nodes = data.get('nodes') if isinstance(data, dict) else None
        self.report.nodes = {
            'ids': len(ids),
            'status': after.status,
            'resource': after.resource,
            'cost': cost if isinstance(cost, int) else None,
            'fell_by': (
                before.remaining - after.remaining
                if before.remaining is not None
                and after.remaining is not None
                and before.reset == after.reset else None
            ),
            'resolved': (
                sum(1 for node in nodes if node) if isinstance(nodes, list)
                else None
            ),
        }

    # -- 2. the dependency graph's bucket -----------------------------------

    async def _depgraph(self, client: httpx2.AsyncClient) -> None:
        repositories = getattr(self, '_repositories', [])
        if not repositories:
            self.report.problems.append(
                'no repository to ask the dependency graph of',
            )
            return
        name = repositories[0].get('full_name')
        first = self.tokens[0]
        asked, report = await self._ask(
            client, first, 'depgraph generate-report', 'GET',
            f'/repos/{name}/dependency-graph/sbom/generate-report',
        )
        found: dict[str, Any] = {
            'repository': name,
            'generate_report': {
                'status': asked.status, 'resource': asked.resource,
                'limit': asked.limit,
            },
        }
        where = report.get('sbom_url') if isinstance(report, dict) else None
        # Looked at with the token only where it is on the API.
        if (
            asked.status == 201 and isinstance(where, str)
            and where.startswith(f'{self.base}/')
        ):
            looked, _ = await self._ask(
                client, first, 'depgraph fetch-report', 'GET', where,
            )
            found['fetch_report'] = {
                'status': looked.status, 'resource': looked.resource,
                'limit': looked.limit,
            }
        self.report.depgraph = found

    # -- 3. a window's reset ----------------------------------------------

    async def _resets(self, client: httpx2.AsyncClient) -> None:
        repositories = getattr(self, '_repositories', [])
        name = repositories[0].get('full_name') if repositories else (
            'octocat/Hello-World'
        )
        self._repository = name
        for number in range(RESETS):
            if number:
                await self._sleep(self.pause)
            await self._ask(
                client, self.tokens[0], 'core', 'GET', f'/repos/{name}',
            )

    def _windows(self) -> None:
        """Each token's bucket: its answers, and the resets they said,
        in the order they came; one window's all the same."""
        by: dict[tuple[str, str], list[Answered]] = {}
        for answered in self.answers:
            if answered.resource and answered.reset is not None:
                by.setdefault(
                    (answered.token, answered.resource), [],
                ).append(answered)
        for (token, bucket), answers in sorted(by.items()):
            resets: list[int] = []
            for answered in answers:
                if answered.reset is not None and (
                    not resets or resets[-1] != answered.reset
                ):
                    resets.append(answered.reset)
            self.report.resets[f'{token} {bucket}'] = {
                'answers': len(answers),
                'resets': resets,
                'holds': all(
                    _one_window(earlier, later)
                    for earlier, later in zip(answers, answers[1:])
                ),
            }

    # -- 4. tokens of one account -------------------------------------------

    async def _sharing(self, client: httpx2.AsyncClient) -> None:
        first = self.tokens[0]
        name = getattr(self, '_repository', 'octocat/Hello-World')
        for other in self.tokens[1:]:
            before = self._last(first.label, 'core')
            theirs, _ = await self._ask(
                client, other, 'core', 'GET', f'/repos/{name}',
            )
            after, _ = await self._ask(
                client, first, 'core', 'GET', f'/repos/{name}',
            )
            fell = (
                before.remaining - after.remaining
                if before is not None and before.remaining is not None
                and after.remaining is not None
                and before.reset == after.reset else None
            )
            self.report.sharing.append({
                'tokens': [first.label, other.label],
                'status': [theirs.status, after.status],
                # One request of each between two of the first's answers:
                # the first's bucket fell by two if the other's is its.
                'fell_by': fell,
                'shared': None if fell is None else fell >= 2,
            })

    def _last(self, token: str, bucket: str) -> Answered | None:
        for answered in reversed(self.answers):
            if answered.token == token and answered.resource == bucket:
                return answered
        return None


def said(report: Report) -> str:
    """The report, for a person."""
    lines = [f'Tokens: {", ".join(report.tokens)}.', '']
    nodes = report.nodes
    if nodes:
        lines.append(
            f'1. The sweep\'s call, nodes(ids:) of {nodes["ids"]} ids: '
            f'answered {nodes["status"]} from {nodes["resource"]!r}; '
            f'rateLimit says it cost {nodes["cost"]} point(s), and the '
            f'bucket fell by {nodes["fell_by"]} across it; '
            f'{nodes["resolved"]} of the nodes resolved.',
        )
        if isinstance(nodes['cost'], int):
            hourly = 650 * nodes['cost']
            lines.append(
                f'   A sweep of 65,000 repositories is then {hourly:,} '
                'points of a token\'s 5,000 an hour.',
            )
    graph = report.depgraph
    if graph:
        asked = graph['generate_report']
        lines.append(
            f'2. The dependency graph of {graph["repository"]}: '
            f'generate-report answered {asked["status"]} from '
            f'{asked["resource"]!r} (limit {asked["limit"]})'
            + (
                f'; fetch-report answered {graph["fetch_report"]["status"]} '
                f'from {graph["fetch_report"]["resource"]!r}'
                if 'fetch_report' in graph else ''
            )
            + '. The collector draws both from \'dependency_sbom\'.',
        )
    if report.resets:
        holding = all(bucket['holds'] for bucket in report.resets.values())
        lines.append(
            '3. X-RateLimit-Reset within a window: '
            + ('it holds' if holding else 'it does NOT hold')
            + ' in every bucket seen:',
        )
        for name, bucket in report.resets.items():
            lines.append(
                f'   {name}: {bucket["answers"]} answer(s), '
                f'resets {bucket["resets"]}',
            )
    if report.sharing:
        lines.append('4. Tokens of one account:')
        for pair in report.sharing:
            first, other = pair['tokens']
            verdict = (
                'unknown: the answers came from two windows'
                if pair['shared'] is None
                else 'they share their limits: one account\'s'
                if pair['shared'] else 'their limits are apart'
            )
            lines.append(
                f'   {first} and {other}: {verdict} ({first}\'s bucket '
                f'fell by {pair["fell_by"]} over one request of each).',
            )
    elif len(report.tokens) == 1:
        lines.append(
            '4. Tokens of one account: one token, nothing to compare.',
        )
    for problem in report.problems:
        lines.append(f'Not measured: {problem}')
    lines.append(f'Requests spent: {report.spent} of {report.most}.')
    return '\n'.join(lines)


async def probe(
    tokens: tuple[Token, ...], **options: Any,
) -> Report:
    return await Probe(tokens, **options).run()


def main(
    argv: list[str] | None = None,
    *,
    environ: Mapping[str, str] | None = None,
    **options: Any,
) -> int:
    parser = argparse.ArgumentParser(
        description='What the collector relies on of GitHub, measured on '
        'the tokens it is given.',
    )
    parser.add_argument(
        '--json', action='store_true', help='the report, as JSON',
    )
    arguments = parser.parse_args(argv)
    if environ is None:
        from chatsbom.core.config import load_env_file

        load_env_file()
    try:
        tokens = settings_from(environ).tokens
    except SettingsError as error:
        print(f'Not configured: {error}', file=sys.stderr)
        return 2
    most = plan(min(len(tokens), TOKENS))
    print(
        f'This probe spends at most {sum(most.values())} requests of '
        'GitHub\'s: '
        + ', '.join(f'{count} {bucket}' for bucket, count in most.items())
        + '. It writes nothing, and prints no token.',
        file=sys.stderr, flush=True,
    )
    started = time.monotonic()
    report = asyncio.run(probe(tokens, **options))
    if arguments.json:
        print(json.dumps(asdict(report), indent=1))
    else:
        print(said(report))
        print(f'Took {time.monotonic() - started:.1f} s.', file=sys.stderr)
    measured = report.nodes or report.depgraph or report.resets
    return 0 if measured else 2


if __name__ == '__main__':
    sys.exit(main())
