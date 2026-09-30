"""A stand-in for GitHub's REST and GraphQL APIs, in-process (#156).

No test calls GitHub. The collector's tests call this instead: an ASGI
app, reached through `httpx2.ASGITransport` (`FakeGitHub.transport`),
and servable on a socket by any ASGI server. It answers as GitHub's
documentation says GitHub does (the REST and GraphQL rate-limit pages,
conditional requests, search and the repository endpoints, read on
2026-09-30):

- Every answer to a known token carries the rate-limit headers of the
  bucket the request drew from, `X-RateLimit-Limit`, `-Remaining`,
  `-Used`, `-Reset` and `-Resource`. Buckets are metered per account,
  as GitHub meters a token's user, so two tokens of one account share
  them.
- A GET whose `If-None-Match` or `If-Modified-Since` matches is answered
  304 and costs nothing. Every other answer to a known token costs one
  request, a 404 as much as a 200; a GraphQL query costs `graphql_cost`
  points, which its `rateLimit` says where the query asks for it.
- A spent bucket is refused: 403 with nothing remaining, and GraphQL's
  with a 200 whose error is `RATE_LIMITED`. A secondary limit
  (`secondary`) is refused with 403 or 429, and `Retry-After` unless
  told otherwise, until it has passed.
- A token it does not know is refused with 401, `Bad credentials`.
- A renamed repository is answered 301 by its old name, a blocked one
  451, and one it does not have 404.
- Search reads `stars:`, `created:` and `language:`, sorts by stars,
  pages 100 at most, and answers the first 1,000 results alone.
- `GET /rate_limit` misreports, as it did (TODO.md): every bucket full.
- The dependency graph's report flow (#50, #162), from a bucket of its
  own, `dependency_sbom`: `generate-report` answers 201 with `sbom_url`,
  where on the API to look for the report, or 404 for a repository it
  has no graph of. `fetch-report` answers 202 until the report is ready
  (`report_seconds`), then 302 to a link on a host of its own, off the
  API, signed for `link_seconds`. The link serves the graph as it was
  when the report was asked for, to a request with no token, and
  refuses one with a token, as a signed link refuses a second
  credential. Told to (`stamp_reports`), it stamps each report as
  GitHub does, with when it was made and a namespace of its own.

The repositories it serves are `Repo`s, by name and by id over REST,
and by node id through GraphQL's `nodes(ids:)`. Anything else is a
document it is given (`document`). It records every request
(`requests`), gives a scripted answer in place of its own (`script`),
at once or after so many requests, can hold requests in flight
(`gate`), and keeps time by a clock the test moves (`FakeClock`), which
the budget it is tested against reads too.
`TestTheStandIn` and `TestTheDependencyGraph` hold it to all of the
above.
"""
import asyncio
import copy
import email.utils
import hashlib
import json
import re
import uuid
from collections import Counter
from collections.abc import Awaitable
from collections.abc import Callable
from collections.abc import Mapping
from collections.abc import MutableMapping
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timezone
from typing import Any
from urllib.parse import parse_qsl
from urllib.parse import urlencode

import httpx2
import pytest

#: Where the stand-in says it is, in the URLs it answers with.
API = 'https://api.github.com'

#: Where a finished dependency-graph report is downloaded from: a host
#: of its own, off the API, as GitHub's are.
EXPORTS_HOST = 'sbom-exports.example'
EXPORTS = f'https://{EXPORTS_HOST}'

#: Where every test's clock starts: 2026-09-21 14:13:20 UTC.
START = 1_790_000_000.0

#: Each bucket's size and window, in seconds, as GitHub documents them
#: for a personal access token: the REST API's 5,000 an hour, GraphQL's
#: 5,000 points an hour, search's 30 a minute and code search's 10; and
#: the dependency graph's SBOM, 200 an hour, as a live probe found it
#: (#50). A bucket not named here is metered as the REST API's.
LIMITS: Mapping[str, tuple[int, int]] = {
    'core': (5_000, 3_600),
    'graphql': (5_000, 3_600),
    'search': (30, 60),
    'code_search': (10, 60),
    'dependency_sbom': (200, 3_600),
}

#: The dependency graph's SBOM endpoints, metered from its own bucket.
_DEPGRAPH = re.compile(r'^/repos/[^/]+/[^/]+/dependency-graph/sbom(?:/|$)')

#: What GitHub answers a secondary limit with, in part.
SECONDARY = (
    'You have exceeded a secondary rate limit. Please wait a few minutes '
    'before you try again.'
)

#: Search answers this many results of a query at most.
SEARCH_CAP = 1_000

Message = MutableMapping[str, Any]
Receive = Callable[[], Awaitable[Message]]
Send = Callable[[Message], Awaitable[None]]


class FakeClock:
    """Time as the stand-in and the budget read it, in UTC epoch
    seconds, moved by the test.

    `sleep` moves it as well, at once: a wait for a reset an hour away
    takes no time, and is an hour long by this clock.
    """

    def __init__(self, now: float = START) -> None:
        self.now = now

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds

    async def sleep(self, seconds: float) -> None:
        self.advance(max(0.0, seconds))
        await asyncio.sleep(0)


def stamp(seconds: float) -> str:
    """An instant as GitHub writes one in a document: `...T...Z`."""
    return datetime.fromtimestamp(int(seconds), timezone.utc).strftime(
        '%Y-%m-%dT%H:%M:%SZ',
    )


def _stamp_report(
    document: Any, full_name: str, number: int, now: float,
) -> None:
    """A report's document, the SPDX document or the `sbom` it is
    wrapped in, stamped as GitHub stamps each report it makes: with when
    it was made, and a namespace of its own."""
    if not isinstance(document, dict):
        return
    sbom = document.get('sbom', document)
    if not isinstance(sbom, dict):
        return
    info = sbom.get('creationInfo')
    sbom['creationInfo'] = {
        **(info if isinstance(info, dict) else {}), 'created': stamp(now),
    }
    sbom['documentNamespace'] = (
        f'https://github.com/{full_name}/dependency_graph/sbom-{number:012x}'
    )


@dataclass
class Release:
    tag: str
    published_at: str = '2026-09-01T00:00:00Z'
    prerelease: bool = False
    draft: bool = False

    def rest(self, number: int) -> dict[str, Any]:
        return {
            'id': number, 'tag_name': self.tag, 'name': self.tag,
            'draft': self.draft, 'prerelease': self.prerelease,
            'created_at': self.published_at,
            'published_at': self.published_at,
        }


@dataclass
class Repo:
    """A repository the stand-in serves."""

    id: int
    owner: str
    name: str
    stars: int = 1_000
    archived: bool = False
    language: str | None = 'Python'
    created_at: str = '2020-01-01T00:00:00Z'
    pushed_at: str = '2026-09-01T00:00:00Z'
    default_branch: str = 'main'
    head: str = 'a' * 40
    #: Newest first, as GitHub lists them.
    releases: list[Release] = field(default_factory=list)
    #: Its dependency graph, as a finished report of it downloads: an
    #: SPDX document, or bytes to send as they are. None: GitHub has no
    #: graph of it, and a report of it is answered 404.
    graph: Any = None
    #: A commit's committer date, by its sha: what `GET .../commits/
    #: {sha}` says of a commit this knows.
    commit_dates: dict[str, str] = field(default_factory=dict)

    @property
    def full_name(self) -> str:
        return f'{self.owner}/{self.name}'

    @property
    def node_id(self) -> str:
        return f'R_kgDO{self.id:08d}'

    def rest(self) -> dict[str, Any]:
        return {
            'id': self.id,
            'node_id': self.node_id,
            'name': self.name,
            'full_name': self.full_name,
            'owner': {'login': self.owner},
            'private': False,
            'fork': False,
            'archived': self.archived,
            'stargazers_count': self.stars,
            'watchers_count': self.stars,
            'language': self.language,
            'default_branch': self.default_branch,
            'created_at': self.created_at,
            'pushed_at': self.pushed_at,
            'url': f'{API}/repos/{self.full_name}',
        }

    def node(self) -> dict[str, Any]:
        """As GraphQL's `nodes(ids:)` gives it, every field the change
        detector reads (#128, section 2.1)."""
        latest = self.releases[0] if self.releases else None
        return {
            '__typename': 'Repository',
            'id': self.node_id,
            'databaseId': self.id,
            'nameWithOwner': self.full_name,
            'stargazerCount': self.stars,
            'isArchived': self.archived,
            'pushedAt': self.pushed_at,
            'defaultBranchRef': {
                'name': self.default_branch,
                'target': {'oid': self.head},
            },
            'latestRelease': latest and {
                'tagName': latest.tag, 'publishedAt': latest.published_at,
            },
        }


@dataclass
class Reply:
    """An answer the stand-in gives in place of its own.

    `body` is sent as JSON, or as it is when it is bytes. `billed`: it
    spends a request from the bucket, as an answer GitHub served would,
    and a refusal does not. `raises`: raised in place of an answer, as
    the transport raises a connection that fails.
    """

    status: int
    body: Any = None
    headers: Mapping[str, str] = field(default_factory=dict)
    billed: bool = False
    raises: BaseException | None = None


@dataclass
class _Scripted:
    reply: Reply
    path: str | None
    token: str | None
    times: int
    #: Requests it matches that are answered before it.
    after: int = 0


@dataclass
class Meter:
    """One bucket of one account: its size, what is left of it, and when
    its window ends."""

    limit: int
    window: int
    remaining: int
    reset: int

    def roll(self, now: float) -> None:
        """A window of its own once the last one has ended."""
        if now >= self.reset:
            self.remaining = self.limit
            self.reset = int(now) + self.window

    def headers(self, bucket: str) -> dict[str, str]:
        return {
            'X-RateLimit-Limit': str(self.limit),
            'X-RateLimit-Remaining': str(self.remaining),
            'X-RateLimit-Used': str(self.limit - self.remaining),
            'X-RateLimit-Reset': str(self.reset),
            'X-RateLimit-Resource': bucket,
        }


@dataclass
class Account:
    """A GitHub user, whose tokens share its buckets."""

    number: int
    login: str
    limits: dict[str, tuple[int, int]]
    meters: dict[str, Meter] = field(default_factory=dict)
    #: Per bucket: refused as a secondary limit until then, with the
    #: status and whether to say `Retry-After`.
    secondary: dict[str, tuple[float, int, bool]] = field(
        default_factory=dict,
    )

    def meter(self, bucket: str, now: float) -> Meter:
        if bucket not in self.meters:
            limit, window = self.limits.get(
                bucket, LIMITS.get(bucket, LIMITS['core']),
            )
            self.meters[bucket] = Meter(
                limit, window, limit, int(now) + window,
            )
        meter = self.meters[bucket]
        meter.roll(now)
        return meter


@dataclass(frozen=True)
class Seen:
    """A request the stand-in was sent, and what it answered."""

    method: str
    path: str
    query: dict[str, str]
    #: The token it carried, or None.
    token: str | None
    #: Lower-cased, and without `authorization`.
    headers: dict[str, str]
    body: Any
    status: int
    bucket: str
    billed: bool
    #: The host it was sent to: the API's, where a finished report is
    #: downloaded from, or wherever else the transport took it.
    host: str = 'api.github.com'


@dataclass
class Report:
    """A dependency-graph report the stand-in was asked for."""

    repo_id: int
    #: The graph as it was when the report was asked for.
    document: Any
    #: When it is ready, by the stand-in's clock.
    ready_at: float


@dataclass
class _Answer:
    status: int
    body: Any = None
    headers: dict[str, str] = field(default_factory=dict)
    #: Answered to a conditional GET by its validators.
    etag: bool = True
    last_modified: str | None = None
    billed: bool = True


def bucket_of(path: str) -> str:
    """The bucket GitHub meters a request to `path` from."""
    if path == '/graphql':
        return 'graphql'
    if path.startswith('/search/code'):
        return 'code_search'
    if path.startswith('/search/'):
        return 'search'
    if _DEPGRAPH.match(path):
        return 'dependency_sbom'
    return 'core'


def weak_etag(body: Any) -> str:
    data = json.dumps(body, sort_keys=True).encode()
    return f'W/"{hashlib.sha256(data).hexdigest()[:32]}"'


def _matches_etag(header: str, etag: str) -> bool:
    """`If-None-Match`, compared weakly, as GitHub compares it."""
    wanted = etag.removeprefix('W/')
    for candidate in header.split(','):
        candidate = candidate.strip()
        if candidate == '*' or candidate.removeprefix('W/') == wanted:
            return True
    return False


def _not_modified_since(header: str, last_modified: str) -> bool:
    try:
        since = email.utils.parsedate_to_datetime(header)
        modified = email.utils.parsedate_to_datetime(last_modified)
    except (TypeError, ValueError):
        return False
    return modified <= since


_RANGE = re.compile(r'^(>=|<=|>|<)?(.+)$')


def _in_range(value: Any, spec: str, parse: Callable[[str], Any]) -> bool:
    """`value` against a search qualifier's range: `N`, `>=N`, `>N`,
    `<=N`, `<N`, `N..M`, `N..*` or `*..M`."""
    if '..' in spec:
        low, _, high = spec.partition('..')
        return (low == '*' or value >= parse(low)) and (
            high == '*' or value <= parse(high)
        )
    match = _RANGE.match(spec)
    if match is None:
        return False
    operator, bound = match.groups()
    bound = parse(bound)
    return {
        None: value == bound, '>=': value >= bound, '<=': value <= bound,
        '>': value > bound, '<': value < bound,
    }[operator]


def _found(repo: Repo, q: str) -> bool:
    for term in q.split():
        qualifier, colon, spec = term.partition(':')
        if not colon:
            if term.lower() not in repo.full_name.lower():
                return False
        elif qualifier == 'stars':
            if not _in_range(repo.stars, spec, int):
                return False
        elif qualifier == 'created':
            if not _in_range(repo.created_at[:10], spec, str):
                return False
        elif qualifier == 'language':
            if (repo.language or '').lower() != spec.lower():
                return False
    return True


class FakeGitHub:
    """The stand-in: an ASGI app on the clock it is given.

    Tokens are made known with `token`, repositories with `add`, and any
    other document with `document`.
    """

    def __init__(self, clock: FakeClock | None = None) -> None:
        self.clock = clock or FakeClock()
        self.repos: dict[int, Repo] = {}
        #: An old full name, lower-cased, and the id it moved to.
        self.renamed: dict[str, int] = {}
        #: A full name, lower-cased, and why it is blocked.
        self.blocked: dict[str, str] = {}
        self.documents: dict[str, _Answer] = {}
        self.accounts: dict[str, Account] = {}
        self._by_login: dict[str, Account] = {}
        self.requests: list[Seen] = []
        self._script: list[_Scripted] = []
        #: What a GraphQL query costs, in points.
        self.graphql_cost = 1
        #: Answers a GraphQL query other than `nodes(ids:)`: its data
        #: and its errors.
        self.resolver: Callable[
            [str, Mapping[str, Any]], tuple[Any, list[Any]],
        ] | None = None
        #: Set, each request waits for it before it is answered.
        self.gate: asyncio.Event | None = None
        #: Requests in flight now, and at most, per token.
        self.flying: Counter[str] = Counter()
        self.peak: Counter[str] = Counter()
        #: The dependency graph's reports, by id, and how many it made.
        self.reports: dict[str, Report] = {}
        self._reported = 0
        #: Seconds a report takes to be ready, after it is asked for.
        self.report_seconds = 0.0
        #: Seconds a finished report's link is signed for.
        self.link_seconds = 300.0
        #: Set, each report is stamped as GitHub stamps one: with when it
        #: was asked for, `creationInfo.created`, and a namespace of its
        #: own, `documentNamespace`.
        self.stamp_reports = False
        #: Each link signed, by its signature: its report, and until
        #: when it serves it.
        self._links: dict[str, tuple[str, float]] = {}

    # -- what it serves ---------------------------------------------------

    def token(
        self, secret: str, login: str = 'octocat',
        **limits: tuple[int, int],
    ) -> Account:
        """Makes `secret` a token of `login`'s. `limits` sets a bucket's
        size and window, as `core=(100, 3600)`."""
        account = self._by_login.get(login)
        if account is None:
            account = Account(len(self._by_login) + 1, login, {})
            self._by_login[login] = account
        account.limits.update(limits)
        self.accounts[secret] = account
        return account

    def meter(self, secret: str, bucket: str) -> Meter:
        return self.accounts[secret].meter(bucket, self.clock())

    def add(self, repo: Repo) -> Repo:
        self.repos[repo.id] = repo
        return repo

    def rename(self, repo_id: int, owner: str, name: str) -> None:
        repo = self.repos[repo_id]
        self.renamed[repo.full_name.lower()] = repo_id
        repo.owner, repo.name = owner, name

    def block(self, full_name: str, reason: str = 'dmca') -> None:
        self.blocked[full_name.lower()] = reason

    def document(
        self, path: str, body: Any, *, etag: bool = True,
        last_modified: str | None = None, status: int = 200,
    ) -> None:
        """What a GET of `path` answers, whatever its query."""
        self.documents[path] = _Answer(
            status, body, etag=etag, last_modified=last_modified,
        )

    def script(
        self, reply: Reply, *, path: str | None = None,
        token: str | None = None, times: int = 1, after: int = 0,
    ) -> None:
        """`reply` in place of the next `times` answers, to a request for
        `path` with `token`, either of them any when None, once `after`
        such requests have been answered."""
        self._script.append(_Scripted(reply, path, token, times, after))

    def secondary(
        self, secret: str, bucket: str = 'core', *,
        seconds: float = 60, status: int = 403, retry_after: bool = True,
    ) -> None:
        """A secondary limit on `secret`'s account, in `bucket`, for
        `seconds`: refused with `status`, and `Retry-After` unless
        `retry_after` is false."""
        self.accounts[secret].secondary[bucket] = (
            self.clock() + seconds, status, retry_after,
        )

    def seen(self, path: str) -> list[Seen]:
        return [seen for seen in self.requests if seen.path == path]

    def billed(self, secret: str | None = None) -> int:
        """Requests billed, to `secret` or to any token."""
        return sum(
            1 for seen in self.requests
            if seen.billed and (secret is None or seen.token == secret)
        )

    def transport(self) -> httpx2.ASGITransport:
        return httpx2.ASGITransport(app=self)

    # -- the app ----------------------------------------------------------

    async def __call__(
        self, scope: Message, receive: Receive, send: Send,
    ) -> None:
        if scope['type'] == 'lifespan':
            await self._lifespan(receive, send)
            return
        chunks = []
        while True:
            message = await receive()
            chunks.append(message.get('body', b''))
            if not message.get('more_body'):
                break
        raw = b''.join(chunks)
        headers = {
            name.decode('latin-1').lower(): value.decode('latin-1')
            for name, value in scope['headers']
        }
        method = scope['method']
        path = scope['path']
        query = dict(parse_qsl(scope['query_string'].decode()))
        body = json.loads(raw) if raw else None
        status, reply_headers, reply = await self._answer(
            method, path, query, headers, body,
        )
        if reply is None:
            data, kind = b'', {}
        elif isinstance(reply, bytes):
            data, kind = reply, {'Content-Type': 'text/html; charset=utf-8'}
        else:
            data = json.dumps(reply).encode()
            kind = {'Content-Type': 'application/json; charset=utf-8'}
        await send({
            'type': 'http.response.start',
            'status': status,
            'headers': [
                (name.lower().encode(), value.encode())
                for name, value in {**kind, **reply_headers}.items()
            ],
        })
        await send({'type': 'http.response.body', 'body': data})

    async def _lifespan(self, receive: Receive, send: Send) -> None:
        while True:
            message = await receive()
            if message['type'] == 'lifespan.startup':
                await send({'type': 'lifespan.startup.complete'})
            elif message['type'] == 'lifespan.shutdown':
                await send({'type': 'lifespan.shutdown.complete'})
                return

    async def _answer(
        self, method: str, path: str, query: dict[str, str],
        headers: dict[str, str], body: Any,
    ) -> tuple[int, dict[str, str], Any]:
        now = self.clock()
        bucket = bucket_of(path)
        scheme, _, secret = headers.get('authorization', '').partition(' ')
        token = secret if scheme.lower() in ('bearer', 'token') else None
        shown = {k: v for k, v in headers.items() if k != 'authorization'}

        def record(status: int, billed: bool) -> None:
            self.requests.append(
                Seen(
                    method, path, query, token, shown, body, status, bucket,
                    billed, host=headers.get('host', 'api.github.com'),
                ),
            )

        if headers.get('host') == EXPORTS_HOST:
            # A finished report's link: no bucket, and nothing billed.
            scripted = self._scripted(path, token)
            exported = (
                self._export(method, path, query, headers)
                if scripted is None
                else (scripted.status, dict(scripted.headers), scripted.body)
            )
            self.requests.append(
                Seen(
                    method, path, query, token, shown, body, exported[0], '',
                    False, host=EXPORTS_HOST,
                ),
            )
            if scripted is not None and scripted.raises is not None:
                raise scripted.raises
            return exported

        account = self.accounts.get(token or '')
        if account is None:
            record(401, False)
            return 401, {}, {
                'message': 'Bad credentials',
                'documentation_url': 'https://docs.github.com/rest',
                'status': '401',
            }
        meter = account.meter(bucket, now)
        cost = self.graphql_cost if bucket == 'graphql' else 1

        scripted = self._scripted(path, token)
        if scripted is not None:
            if scripted.billed:
                meter.remaining = max(0, meter.remaining - cost)
            record(scripted.status, scripted.billed)
            if scripted.raises is not None:
                raise scripted.raises
            return scripted.status, {
                **meter.headers(bucket), **scripted.headers,
            }, scripted.body

        refused = self._refused(account, meter, bucket, cost, now)
        if refused is not None:
            record(refused[0], False)
            return refused

        holder = token or ''
        self.flying[holder] += 1
        self.peak[holder] = max(self.peak[holder], self.flying[holder])
        try:
            if self.gate is not None:
                await self.gate.wait()
        finally:
            self.flying[holder] -= 1

        answer = self._route(method, path, query, body, meter, cost)
        conditional = method == 'GET' and answer.status == 200
        if conditional and answer.etag:
            etag = weak_etag(answer.body)
            answer.headers['ETag'] = etag
            match = headers.get('if-none-match')
            if match is not None and _matches_etag(match, etag):
                answer = _Answer(304, None, answer.headers, billed=False)
        if conditional and answer.last_modified:
            answer.headers['Last-Modified'] = answer.last_modified
            since = headers.get('if-modified-since')
            if (
                answer.status == 200 and since is not None
                and 'if-none-match' not in headers
                and _not_modified_since(since, answer.last_modified)
            ):
                answer = _Answer(304, None, answer.headers, billed=False)
        if answer.billed:
            meter.remaining = max(0, meter.remaining - cost)
        record(answer.status, answer.billed)
        return answer.status, {
            **meter.headers(bucket), **answer.headers,
        }, answer.body

    def _scripted(self, path: str, token: str | None) -> Reply | None:
        for entry in self._script:
            if entry.path not in (None, path):
                continue
            if entry.token not in (None, token):
                continue
            if entry.after > 0:
                entry.after -= 1
                continue
            entry.times -= 1
            if entry.times <= 0:
                self._script.remove(entry)
            return entry.reply
        return None

    def _refused(
        self, account: Account, meter: Meter, bucket: str, cost: int,
        now: float,
    ) -> tuple[int, dict[str, str], Any] | None:
        """A refusal, secondary or primary, or None to answer."""
        blocked = account.secondary.get(bucket)
        if blocked is not None and now < blocked[0]:
            until, status, says_when = blocked
            extra = {'Retry-After': str(max(1, int(until - now + 0.999)))}
            return status, {
                **meter.headers(bucket), **(extra if says_when else {}),
            }, {
                'message': SECONDARY,
                'documentation_url': 'https://docs.github.com/rest',
            }
        if meter.remaining >= cost:
            return None
        if bucket == 'graphql':
            return 200, meter.headers(bucket), {
                'errors': [{
                    'type': 'RATE_LIMITED',
                    'message': (
                        'API rate limit already exceeded for user ID '
                        f'{account.number}.'
                    ),
                }],
            }
        return 403, meter.headers(bucket), {
            'message': (
                f'API rate limit exceeded for user ID {account.number}.'
            ),
            'documentation_url': 'https://docs.github.com/rest',
        }

    # -- the routes -------------------------------------------------------

    def _route(
        self, method: str, path: str, query: dict[str, str], body: Any,
        meter: Meter, cost: int,
    ) -> _Answer:
        if method == 'POST' and path == '/graphql':
            return self._graphql(
                body if isinstance(body, dict) else {}, meter, cost,
            )
        if method != 'GET':
            return self._missing()
        if path == '/rate_limit':
            return _Answer(200, self._rate_limit(), etag=False)
        if path == '/search/repositories':
            return self._search(path, query)
        if path in self.documents:
            kept = self.documents[path]
            return _Answer(
                kept.status, kept.body, etag=kept.etag,
                last_modified=kept.last_modified,
            )
        parts = path.strip('/').split('/')
        if len(parts) >= 2 and parts[0] == 'repositories':
            repo = self.repos.get(
                int(parts[1]),
            ) if parts[1].isdigit() else None
            return self._repository(repo, parts[2:], path, query)
        if len(parts) >= 3 and parts[0] == 'repos':
            full_name = f'{parts[1]}/{parts[2]}'.lower()
            if full_name in self.blocked:
                return _Answer(
                    451, {
                        'message': 'Repository access blocked',
                        'block': {
                            'reason': self.blocked[full_name],
                            'created_at': '2026-01-01T00:00:00Z',
                        },
                    }, etag=False,
                )
            if full_name in self.renamed:
                moved = self.renamed[full_name]
                rest = '/'.join(parts[3:])
                url = f'{API}/repositories/{moved}' + (
                    f'/{rest}' if rest else ''
                )
                return _Answer(
                    301, {
                        'message': 'Moved Permanently', 'url': url,
                        'documentation_url': 'https://docs.github.com/rest',
                    }, {'Location': url}, etag=False,
                )
            repo = next(
                (
                    r for r in self.repos.values()
                    if r.full_name.lower() == full_name
                ),
                None,
            )
            return self._repository(repo, parts[3:], path, query)
        return self._missing()

    def _missing(self) -> _Answer:
        return _Answer(
            404, {
                'message': 'Not Found',
                'documentation_url': 'https://docs.github.com/rest',
                'status': '404',
            }, etag=False,
        )

    def _repository(
        self, repo: Repo | None, rest: list[str], path: str,
        query: dict[str, str],
    ) -> _Answer:
        if repo is None:
            return self._missing()
        if not rest:
            return _Answer(200, repo.rest())
        if rest == ['releases']:
            releases = [
                release.rest(number)
                for number, release in enumerate(repo.releases, start=1)
            ]
            page, links = _page(path, query, len(releases), cap=None)
            return _Answer(200, releases[page], links)
        if rest == ['dependency-graph', 'sbom', 'generate-report']:
            return self._generate_report(repo)
        if len(rest) == 4 and rest[:3] == [
            'dependency-graph', 'sbom', 'fetch-report',
        ]:
            return self._fetch_report(repo, rest[3])
        if len(rest) == 2 and rest[0] == 'commits':
            date = repo.commit_dates.get(rest[1])
            if date is None:
                return _Answer(
                    422, {
                        'message': f'No commit found for SHA: {rest[1]}',
                        'documentation_url': 'https://docs.github.com/rest',
                    }, etag=False,
                )
            return _Answer(
                200, {
                    'sha': rest[1],
                    'commit': {
                        'author': {'date': date}, 'committer': {'date': date},
                    },
                },
            )
        return self._missing()

    def _generate_report(self, repo: Repo) -> _Answer:
        """A report of the graph as it is now, and where to look for it."""
        if repo.graph is None:
            return self._missing()
        self._reported += 1
        report_id = str(uuid.UUID(int=self._reported))
        document = copy.deepcopy(repo.graph)
        if self.stamp_reports:
            _stamp_report(
                document, repo.full_name, self._reported, self.clock(),
            )
        self.reports[report_id] = Report(
            repo.id, document, self.clock() + self.report_seconds,
        )
        return _Answer(
            201, {
                'sbom_url': (
                    f'{API}/repos/{repo.full_name}/dependency-graph/sbom/'
                    f'fetch-report/{report_id}'
                ),
            }, etag=False,
        )

    def _fetch_report(self, repo: Repo, report_id: str) -> _Answer:
        """202 while the report is being made, then 302 to a link signed
        for it, a new one each time."""
        report = self.reports.get(report_id)
        if report is None or report.repo_id != repo.id:
            return self._missing()
        now = self.clock()
        if now < report.ready_at:
            return _Answer(202, None, etag=False)
        signature = hashlib.sha256(
            f'{report_id} {len(self._links)}'.encode(),
        ).hexdigest()
        self._links[signature] = (report_id, now + self.link_seconds)
        link = f'{EXPORTS}/sbom/{report_id}.spdx.json?' + urlencode({
            'X-Amz-Expires': str(int(self.link_seconds)),
            'X-Amz-Signature': signature,
        })
        return _Answer(302, None, {'Location': link}, etag=False)

    def _export(
        self, method: str, path: str, query: dict[str, str],
        headers: dict[str, str],
    ) -> tuple[int, dict[str, str], Any]:
        """A finished report, from its link: to a request that carries
        its signature, and no other credential."""
        if 'authorization' in headers:
            return 400, {}, (
                b'<Error><Code>InvalidArgument</Code><Message>Only one auth '
                b'mechanism allowed</Message></Error>'
            )
        name = re.fullmatch(r'/sbom/([^/]+)\.spdx\.json', path)
        signed = self._links.get(query.get('X-Amz-Signature', ''))
        if (
            method != 'GET' or name is None or signed is None
            or signed[0] != name[1]
        ):
            return 403, {}, (
                b'<Error><Code>AccessDenied</Code><Message>Access Denied'
                b'</Message></Error>'
            )
        if self.clock() >= signed[1]:
            return 403, {}, (
                b'<Error><Code>AccessDenied</Code><Message>Request has '
                b'expired</Message></Error>'
            )
        report = self.reports.get(name[1])
        if report is None:
            return 404, {}, b'<Error><Code>NoSuchKey</Code></Error>'
        return 200, {}, report.document

    def _search(self, path: str, query: dict[str, str]) -> _Answer:
        found = [
            r for r in self.repos.values(
            ) if _found(r, query.get('q', ''))
        ]
        if query.get('sort') == 'stars':
            found.sort(
                key=lambda r: (r.stars, -r.id),
                reverse=query.get('order', 'desc') == 'desc',
            )
        else:
            found.sort(key=lambda r: r.id)
        per_page = min(int(query.get('per_page', 30)), 100)
        page = int(query.get('page', 1))
        if (page - 1) * per_page >= SEARCH_CAP:
            return _Answer(
                422, {
                    'message': 'Only the first 1000 search results are available',
                    'documentation_url': 'https://docs.github.com/rest/search',
                }, etag=False,
            )
        shown, links = _page(path, query, len(found), cap=SEARCH_CAP)
        return _Answer(
            200, {
                'total_count': len(found),
                'incomplete_results': False,
                'items': [{**r.rest(), 'score': 1.0} for r in found[shown]],
            }, links,
        )

    def _graphql(
        self, body: Mapping[str, Any], meter: Meter, cost: int,
    ) -> _Answer:
        query = str(body.get('query', ''))
        variables = body.get('variables') or {}
        if self.resolver is not None:
            data, errors = self.resolver(query, variables)
        elif 'ids' in variables:
            by_node = {r.node_id: r for r in self.repos.values()}
            nodes, errors = [], []
            for index, node_id in enumerate(variables['ids']):
                repo = by_node.get(node_id)
                nodes.append(repo.node() if repo else None)
                if repo is None:
                    errors.append({
                        'type': 'NOT_FOUND',
                        'path': ['nodes', index],
                        'message': (
                            'Could not resolve to a node with the global '
                            f"id of '{node_id}'"
                        ),
                    })
            data = {'nodes': nodes}
        else:
            data, errors = None, [{
                'message': 'The stand-in answers nodes(ids:) alone',
            }]
        if (
            'rateLimit' in query and isinstance(data, dict)
            and 'rateLimit' not in data
        ):
            # Where the bucket stands once this query is charged, which
            # it is as soon as it is answered.
            remaining = max(0, meter.remaining - cost)
            data = {
                **data, 'rateLimit': {
                    'cost': cost, 'limit': meter.limit,
                    'nodeCount': len(variables.get('ids') or ()),
                    'remaining': remaining, 'resetAt': stamp(meter.reset),
                    'used': meter.limit - remaining,
                },
            }
        answer: dict[str, Any] = {'data': data}
        if errors:
            answer['errors'] = errors
        return _Answer(200, answer, etag=False)

    def _rate_limit(self) -> dict[str, Any]:
        now = int(self.clock())
        resources = {
            bucket: {
                'limit': limit, 'remaining': limit, 'used': 0,
                'reset': now + window,
            }
            for bucket, (limit, window) in LIMITS.items()
        }
        return {'resources': resources, 'rate': resources['core']}


def _page(
    path: str, query: dict[str, str], total: int, cap: int | None,
) -> tuple[slice, dict[str, str]]:
    """The slice of `total` items a page is, and its `Link` header."""
    per_page = min(int(query.get('per_page', 30)), 100)
    page = int(query.get('page', 1))
    reachable = total if cap is None else min(total, cap)
    last = max(1, -(-reachable // per_page))
    links = []

    def link(number: int, rel: str) -> None:
        url = f'{API}{path}?' + urlencode({**query, 'page': str(number)})
        links.append(f'<{url}>; rel="{rel}"')

    if page < last:
        link(page + 1, 'next')
        link(last, 'last')
    if page > 1:
        link(1, 'first')
        link(page - 1, 'prev')
    start = (page - 1) * per_page
    end = min(start + per_page, reachable)
    return slice(start, max(start, end)), (
        {'Link': ', '.join(links)} if links else {}
    )


# -- the stand-in, held to GitHub's documentation -----------------------------

ONE = 'ghp_standin_token_one_000000000000000000'
TWO = 'ghp_standin_token_two_000000000000000000'


def ask(
    fake: FakeGitHub, method: str, path: str, *, token: str | None = ONE,
    **options: Any,
) -> httpx2.Response:
    """One request of the stand-in, as any client makes it."""
    headers = dict(options.pop('headers', {}))
    if token is not None:
        headers['Authorization'] = f'Bearer {token}'

    async def asking() -> httpx2.Response:
        async with httpx2.AsyncClient(
            transport=fake.transport(), base_url=API,
        ) as client:
            return await client.request(
                method, path, headers=headers, **options,
            )

    return asyncio.run(asking())


@pytest.fixture
def fake() -> FakeGitHub:
    fake = FakeGitHub()
    fake.token(ONE, 'alice')
    fake.token(TWO, 'bob')
    fake.add(
        Repo(
            1, 'octo', 'one', stars=5_000, releases=[
                Release('v2.0', '2026-09-10T00:00:00Z'),
                Release('v1.0', '2026-01-10T00:00:00Z'),
                Release('v0.1', '2025-01-10T00:00:00Z'),
            ],
        ),
    )
    fake.add(Repo(2, 'octo', 'two', stars=1_500, language='Go'))
    fake.add(Repo(3, 'octo', 'three', stars=900))
    return fake


class TestTheStandIn:
    """Every test of the collector believes it, so it is held to what
    GitHub's documentation says GitHub does."""

    def test_answers_a_repository_by_name_and_by_id(self, fake):
        by_name = ask(fake, 'GET', '/repos/octo/one')
        by_id = ask(fake, 'GET', '/repositories/1')
        assert by_name.status_code == by_id.status_code == 200
        assert by_name.json() == by_id.json()
        assert by_name.json()['node_id'] == fake.repos[1].node_id
        assert by_name.json()['stargazers_count'] == 5_000

    def test_says_the_buckets_standing_with_every_answer(self, fake):
        first = ask(fake, 'GET', '/repos/octo/one')
        second = ask(fake, 'GET', '/repos/octo/missing')
        assert first.headers['X-RateLimit-Resource'] == 'core'
        assert first.headers['X-RateLimit-Limit'] == '5000'
        assert first.headers['X-RateLimit-Remaining'] == '4999'
        assert first.headers['X-RateLimit-Used'] == '1'
        assert first.headers['X-RateLimit-Reset'] == str(int(START) + 3_600)
        # A 404 costs as much as a 200.
        assert second.status_code == 404
        assert second.headers['X-RateLimit-Remaining'] == '4998'

    def test_meters_an_account_and_shares_it_between_its_tokens(self, fake):
        fake.token('ghp_alices_second_token_0000000000000000', 'alice')
        ask(fake, 'GET', '/repos/octo/one', token=ONE)
        other = ask(
            fake, 'GET', '/repos/octo/one',
            token='ghp_alices_second_token_0000000000000000',
        )
        bobs = ask(fake, 'GET', '/repos/octo/one', token=TWO)
        assert other.headers['X-RateLimit-Remaining'] == '4998'
        assert bobs.headers['X-RateLimit-Remaining'] == '4999'

    def test_meters_graphql_and_search_apart(self, fake):
        ask(fake, 'GET', '/repos/octo/one')
        graphql = ask(
            fake, 'POST', '/graphql', json={
                'query': 'query($ids: [ID!]!) { nodes(ids: $ids) { id } }',
                'variables': {'ids': [fake.repos[1].node_id]},
            },
        )
        search = ask(fake, 'GET', '/search/repositories', params={'q': 'x'})
        assert graphql.headers['X-RateLimit-Resource'] == 'graphql'
        assert graphql.headers['X-RateLimit-Remaining'] == '4999'
        assert search.headers['X-RateLimit-Resource'] == 'search'
        assert search.headers['X-RateLimit-Limit'] == '30'
        assert search.headers['X-RateLimit-Remaining'] == '29'
        assert search.headers['X-RateLimit-Reset'] == str(int(START) + 60)

    def test_answers_a_matching_etag_with_a_free_304(self, fake):
        first = ask(fake, 'GET', '/repos/octo/one')
        etag = first.headers['ETag']
        assert etag.startswith('W/"')
        again = ask(
            fake, 'GET', '/repos/octo/one', headers={'If-None-Match': etag},
        )
        assert again.status_code == 304
        assert again.content == b''
        assert again.headers['X-RateLimit-Remaining'] == '4999'
        assert again.headers['ETag'] == etag
        assert [seen.billed for seen in fake.requests] == [True, False]

    def test_answers_a_stale_etag_with_the_document(self, fake):
        first = ask(fake, 'GET', '/repos/octo/one')
        fake.repos[1].stars += 1
        again = ask(
            fake, 'GET', '/repos/octo/one',
            headers={'If-None-Match': first.headers['ETag']},
        )
        assert again.status_code == 200
        assert again.json()['stargazers_count'] == 5_001
        assert again.headers['ETag'] != first.headers['ETag']

    def test_answers_if_modified_since_by_last_modified(self, fake):
        fake.document(
            '/repos/octo/one/readme', {'content': 'hi'}, etag=False,
            last_modified='Wed, 30 Sep 2026 00:00:00 GMT',
        )
        first = ask(fake, 'GET', '/repos/octo/one/readme')
        assert 'ETag' not in first.headers
        since = first.headers['Last-Modified']
        unchanged = ask(
            fake, 'GET', '/repos/octo/one/readme',
            headers={'If-Modified-Since': since},
        )
        earlier = ask(
            fake, 'GET', '/repos/octo/one/readme',
            headers={'If-Modified-Since': 'Tue, 29 Sep 2026 00:00:00 GMT'},
        )
        assert unchanged.status_code == 304
        assert earlier.status_code == 200

    def test_refuses_a_spent_bucket_until_its_window_ends(self, fake):
        fake.token(
            'ghp_small_token_00000000000000000000000', 'carol',
            core=(2, 3_600),
        )
        small = 'ghp_small_token_00000000000000000000000'
        ask(fake, 'GET', '/repos/octo/one', token=small)
        ask(fake, 'GET', '/repos/octo/one', token=small)
        refused = ask(fake, 'GET', '/repos/octo/one', token=small)
        assert refused.status_code == 403
        assert refused.headers['X-RateLimit-Remaining'] == '0'
        assert 'rate limit exceeded' in refused.json()['message']
        assert 'Retry-After' not in refused.headers
        assert fake.requests[-1].billed is False
        fake.clock.advance(3_600)
        again = ask(fake, 'GET', '/repos/octo/one', token=small)
        assert again.status_code == 200
        assert again.headers['X-RateLimit-Remaining'] == '1'

    def test_refuses_spent_graphql_with_a_200_that_says_rate_limited(
        self, fake,
    ):
        fake.token(
            'ghp_small_token_00000000000000000000000', 'carol',
            graphql=(1, 3_600),
        )
        small = 'ghp_small_token_00000000000000000000000'
        query = {'query': '{ viewer { login } }', 'variables': {'ids': []}}
        ask(fake, 'POST', '/graphql', token=small, json=query)
        refused = ask(fake, 'POST', '/graphql', token=small, json=query)
        assert refused.status_code == 200
        assert refused.headers['X-RateLimit-Remaining'] == '0'
        assert refused.json()['errors'][0]['type'] == 'RATE_LIMITED'

    @pytest.mark.parametrize('status', [403, 429])
    def test_refuses_a_secondary_limit_with_retry_after_until_it_passes(
        self, fake, status,
    ):
        fake.secondary(ONE, seconds=30, status=status)
        refused = ask(fake, 'GET', '/repos/octo/one')
        assert refused.status_code == status
        assert refused.headers['Retry-After'] == '30'
        assert 'secondary rate limit' in refused.json()['message']
        # Quota remains: it is not the primary limit.
        assert refused.headers['X-RateLimit-Remaining'] == '5000'
        # Another bucket, and another account, are not refused.
        assert ask(
            fake, 'POST', '/graphql', json={
                'query': '', 'variables': {'ids': []},
            },
        ).status_code == 200
        assert ask(
            fake, 'GET', '/repos/octo/one',
            token=TWO,
        ).status_code == 200
        fake.clock.advance(30)
        assert ask(fake, 'GET', '/repos/octo/one').status_code == 200

    def test_may_refuse_a_secondary_limit_without_retry_after(self, fake):
        fake.secondary(ONE, seconds=60, retry_after=False)
        refused = ask(fake, 'GET', '/repos/octo/one')
        assert refused.status_code == 403
        assert 'Retry-After' not in refused.headers
        assert 'secondary rate limit' in refused.json()['message']

    def test_refuses_a_token_it_does_not_know_with_401(self, fake):
        refused = ask(fake, 'GET', '/repos/octo/one', token='ghp_unknown')
        anonymous = ask(fake, 'GET', '/repos/octo/one', token=None)
        assert refused.status_code == anonymous.status_code == 401
        assert refused.json()['message'] == 'Bad credentials'

    def test_moves_a_renamed_repository_permanently_by_its_old_name(
        self, fake,
    ):
        fake.rename(1, 'octo', 'uno')
        moved = ask(fake, 'GET', '/repos/octo/one')
        assert moved.status_code == 301
        assert moved.headers['Location'] == f'{API}/repositories/1'
        assert ask(fake, 'GET', '/repos/octo/uno').status_code == 200
        releases = ask(fake, 'GET', '/repos/octo/one/releases')
        assert releases.headers['Location'] == f'{API}/repositories/1/releases'

    def test_answers_a_blocked_repository_451_and_a_missing_one_404(
        self, fake,
    ):
        fake.block('octo/two')
        blocked = ask(fake, 'GET', '/repos/octo/two')
        assert blocked.status_code == 451
        assert blocked.json()['block']['reason'] == 'dmca'
        assert ask(fake, 'GET', '/repos/octo/none').status_code == 404
        assert ask(fake, 'GET', '/repositories/99').status_code == 404

    def test_pages_releases_newest_first_with_links(self, fake):
        first = ask(
            fake, 'GET', '/repos/octo/one/releases',
            params={'per_page': 2},
        )
        assert [r['tag_name'] for r in first.json()] == ['v2.0', 'v1.0']
        assert 'rel="next"' in first.headers['Link']
        assert 'page=2' in first.headers['Link']
        second = ask(
            fake, 'GET', '/repos/octo/one/releases',
            params={'per_page': 2, 'page': 2},
        )
        assert [r['tag_name'] for r in second.json()] == ['v0.1']
        assert 'rel="next"' not in second.headers['Link']
        assert 'rel="prev"' in second.headers['Link']

    def test_searches_by_stars_sorted_by_stars(self, fake):
        found = ask(
            fake, 'GET', '/search/repositories', params={
                'q': 'stars:>=1000', 'sort': 'stars', 'order': 'desc',
            },
        )
        body = found.json()
        assert body['total_count'] == 2
        assert body['incomplete_results'] is False
        assert [item['full_name'] for item in body['items']] == [
            'octo/one', 'octo/two',
        ]
        ranged = ask(
            fake, 'GET', '/search/repositories', params={
                'q': 'language:Go stars:1000..2000',
            },
        )
        assert [item['id'] for item in ranged.json()['items']] == [2]

    def test_searches_the_first_1000_results_alone(self, fake):
        for number in range(10, 1_110):
            fake.add(Repo(number, 'many', f'r{number}', stars=10_000 + number))
        last = ask(
            fake, 'GET', '/search/repositories', params={
                'q': 'stars:>=10000', 'per_page': 100, 'page': 10,
            },
        )
        beyond = ask(
            fake, 'GET', '/search/repositories', params={
                'q': 'stars:>=10000', 'per_page': 100, 'page': 11,
            },
        )
        assert last.status_code == 200
        assert last.json()['total_count'] == 1_100
        assert len(last.json()['items']) == 100
        assert 'rel="next"' not in last.headers['Link']
        assert beyond.status_code == 422

    def test_resolves_nodes_by_id_and_a_missing_one_to_null(self, fake):
        answer = ask(
            fake, 'POST', '/graphql', json={
                'query': 'query($ids: [ID!]!) { nodes(ids: $ids) { id } }',
                'variables': {'ids': [fake.repos[1].node_id, 'R_gone']},
            },
        )
        body = answer.json()
        one, gone = body['data']['nodes']
        assert gone is None
        assert one['databaseId'] == 1
        assert one['defaultBranchRef']['target']['oid'] == 'a' * 40
        assert one['latestRelease'] == {
            'tagName': 'v2.0', 'publishedAt': '2026-09-10T00:00:00Z',
        }
        assert body['errors'][0]['type'] == 'NOT_FOUND'
        assert body['errors'][0]['path'] == ['nodes', 1]

    def test_charges_a_graphql_query_its_cost(self, fake):
        fake.graphql_cost = 3
        answer = ask(
            fake, 'POST', '/graphql', json={
                'query': '', 'variables': {'ids': []},
            },
        )
        assert answer.headers['X-RateLimit-Remaining'] == '4997'

    def test_says_what_a_graphql_query_cost_where_it_asks(self, fake):
        """`rateLimit`, as GitHub's GraphQL rate-limit page says a query
        may ask: its cost, and where the bucket stands after it, as the
        headers say too."""
        fake.graphql_cost = 2
        ids = [fake.repos[1].node_id, fake.repos[2].node_id]
        answer = ask(
            fake, 'POST', '/graphql', json={
                'query': (
                    'query($ids: [ID!]!) { rateLimit { cost limit nodeCount '
                    'remaining resetAt used } nodes(ids: $ids) { id } }'
                ),
                'variables': {'ids': ids},
            },
        )
        assert answer.json()['data']['rateLimit'] == {
            'cost': 2, 'limit': 5_000, 'nodeCount': 2, 'remaining': 4_998,
            'resetAt': stamp(START + 3_600), 'used': 2,
        }
        assert answer.headers['X-RateLimit-Remaining'] == '4998'
        unasked = ask(
            fake, 'POST', '/graphql', json={
                'query': 'query($ids: [ID!]!) { nodes(ids: $ids) { id } }',
                'variables': {'ids': ids},
            },
        )
        assert 'rateLimit' not in unasked.json()['data']

    def test_says_it_beside_a_resolvers_data_unless_the_resolver_does(
        self, fake,
    ):
        query = '{ rateLimit { cost remaining } viewer { login } }'
        fake.resolver = lambda query, variables: (
            {'viewer': {'login': 'alice'}}, [],
        )
        said = ask(fake, 'POST', '/graphql', json={'query': query})
        assert said.json()['data']['rateLimit']['cost'] == 1
        assert said.json()['data']['viewer'] == {'login': 'alice'}
        fake.resolver = lambda query, variables: (
            {'rateLimit': {'cost': 7, 'remaining': 1}}, [],
        )
        own = ask(fake, 'POST', '/graphql', json={'query': query})
        assert own.json()['data']['rateLimit'] == {'cost': 7, 'remaining': 1}

    def test_misreports_rate_limit_as_github_did(self, fake):
        """TODO.md: `GET /rate_limit` said 5,000/5,000 while a real
        answer's header said 3,156. A budget that reads it never
        stops."""
        for _ in range(3):
            ask(fake, 'GET', '/repos/octo/one')
        said = ask(fake, 'GET', '/rate_limit').json()
        assert said['resources']['core']['remaining'] == 5_000

    def test_gives_a_scripted_answer_in_place_of_its_own_once(self, fake):
        fake.script(
            Reply(502, {'message': 'Server Error'}),
            path='/repos/octo/one',
        )
        fake.script(Reply(200, b'<html>unicorn</html>'), token=TWO)
        assert ask(fake, 'GET', '/repos/octo/two').status_code == 200
        assert ask(fake, 'GET', '/repos/octo/one').status_code == 502
        assert ask(fake, 'GET', '/repos/octo/one').status_code == 200
        raw = ask(fake, 'GET', '/repos/octo/one', token=TWO)
        assert raw.content == b'<html>unicorn</html>'
        assert raw.headers['Content-Type'].startswith('text/html')
        assert [seen.billed for seen in fake.requests] == [
            True, False, True, False,
        ]

    def test_gives_a_scripted_answer_after_so_many_requests(self, fake):
        """To fail the third page of a search, or the second call of a
        sweep: what comes before is answered, and so is what is not the
        path scripted."""
        fake.script(
            Reply(502, {'message': 'Server Error'}), path='/repos/octo/one',
            after=2,
        )
        assert ask(fake, 'GET', '/repos/octo/one').status_code == 200
        assert ask(fake, 'GET', '/repos/octo/two').status_code == 200
        assert ask(fake, 'GET', '/repos/octo/one').status_code == 200
        assert ask(fake, 'GET', '/repos/octo/one').status_code == 502
        assert ask(fake, 'GET', '/repos/octo/one').status_code == 200

    def test_raises_what_a_scripted_answer_raises(self, fake):
        fake.script(Reply(0, raises=httpx2.ConnectError('refused')))
        with pytest.raises(httpx2.ConnectError):
            ask(fake, 'GET', '/repos/octo/one')

    def test_holds_requests_in_flight_until_let_go(self, fake):
        async def asking() -> list[int]:
            fake.gate = asyncio.Event()
            async with httpx2.AsyncClient(
                transport=fake.transport(), base_url=API,
                headers={'Authorization': f'Bearer {ONE}'},
            ) as client:
                sent = [
                    asyncio.ensure_future(client.get('/repos/octo/one'))
                    for _ in range(3)
                ]
                for _ in range(100):
                    if fake.flying[ONE] == 3:
                        break
                    await asyncio.sleep(0)
                assert fake.flying[ONE] == 3
                fake.gate.set()
                answers = await asyncio.gather(*sent)
            return [answer.status_code for answer in answers]

        assert asyncio.run(asking()) == [200, 200, 200]
        assert fake.peak[ONE] == 3
        assert fake.flying[ONE] == 0

    def test_keeps_no_authorization_in_what_it_recorded(self, fake):
        ask(fake, 'GET', '/repos/octo/one')
        assert fake.requests[0].token == ONE
        assert 'authorization' not in fake.requests[0].headers

    def test_records_the_host_each_request_went_to(self, fake):
        """Its transport takes a request for any host to it: what it
        records says whether one left the API."""
        ask(fake, 'GET', '/repos/octo/one')
        ask(fake, 'GET', 'https://elsewhere.example/repos/octo/one')
        assert [seen.host for seen in fake.requests] == [
            'api.github.com', 'elsewhere.example',
        ]

    def test_says_whether_a_release_is_a_draft_or_a_prerelease(self, fake):
        fake.repos[1].releases[0].prerelease = True
        fake.repos[1].releases[1].draft = True
        listed = ask(fake, 'GET', '/repositories/1/releases').json()
        assert [(r['prerelease'], r['draft']) for r in listed] == [
            (True, False), (False, True), (False, False),
        ]

    def test_answers_a_commit_it_knows_with_its_date(self, fake):
        sha = 'c' * 40
        fake.repos[1].commit_dates[sha] = '2026-07-07T07:07:07Z'
        known = ask(fake, 'GET', f'/repositories/1/commits/{sha}')
        assert known.status_code == 200
        assert known.json()['commit']['committer']['date'] == (
            '2026-07-07T07:07:07Z'
        )
        unknown = ask(fake, 'GET', f'/repos/octo/one/commits/{"d" * 40}')
        assert unknown.status_code == 422


#: A dependency graph as a finished report downloads it: an SPDX
#: document, without the `{"sbom": ...}` the synchronous endpoint wrapped
#: it in (ClickHouse/ClickBOM#119).
GRAPH = {
    'SPDXID': 'SPDXRef-DOCUMENT',
    'spdxVersion': 'SPDX-2.3',
    'creationInfo': {
        'created': '2026-09-14T03:56:20Z',
        'creators': ['Tool: GitHub.com-Dependency-Graph'],
    },
    'name': 'octo/one',
    'packages': [{
        'name': 'npm:left-pad',
        'SPDXID': 'SPDXRef-npm-left-pad-1.3.0',
        'versionInfo': '1.3.0',
        'externalRefs': [{
            'referenceCategory': 'PACKAGE-MANAGER',
            'referenceType': 'purl',
            'referenceLocator': 'pkg:npm/left-pad@1.3.0',
        }],
    }],
}

GENERATE = '/repos/octo/one/dependency-graph/sbom/generate-report'


def report_path(answer: httpx2.Response) -> str:
    """Where a report asked for is to be looked at, as a path on the
    API."""
    url = answer.json()['sbom_url']
    assert url.startswith(f'{API}/')
    return url.removeprefix(API)


class TestTheDependencyGraph:
    """The stand-in's dependency graph, as GitHub's REST reference says
    its report flow goes (#50): asked for, looked at until it is ready,
    then downloaded from a link off the API that GitHub signs."""

    def test_a_report_is_asked_for_and_says_where_to_look(self, fake):
        fake.repos[1].graph = GRAPH
        answer = ask(fake, 'GET', GENERATE)
        assert answer.status_code == 201
        path = report_path(answer)
        assert path.startswith(
            '/repos/octo/one/dependency-graph/sbom/fetch-report/',
        )
        # From the dependency graph's own bucket, not the REST API's.
        assert answer.headers['X-RateLimit-Resource'] == 'dependency_sbom'
        assert answer.headers['X-RateLimit-Limit'] == '200'
        assert answer.headers['X-RateLimit-Remaining'] == '199'
        core = ask(fake, 'GET', '/repos/octo/one')
        assert core.headers['X-RateLimit-Remaining'] == '4999'

    def test_a_repository_without_a_graph_is_404(self, fake):
        assert fake.repos[2].graph is None
        answer = ask(
            fake, 'GET', '/repos/octo/two/dependency-graph/sbom/generate-report',
        )
        assert answer.status_code == 404
        assert answer.headers['X-RateLimit-Resource'] == 'dependency_sbom'

    def test_a_report_is_202_until_ready_then_302_to_a_signed_link(
        self, fake,
    ):
        fake.repos[1].graph = GRAPH
        fake.report_seconds = 10
        path = report_path(ask(fake, 'GET', GENERATE))
        not_yet = ask(fake, 'GET', path)
        assert not_yet.status_code == 202
        assert not_yet.content == b''
        assert not_yet.headers['X-RateLimit-Resource'] == 'dependency_sbom'
        fake.clock.advance(10)
        ready = ask(fake, 'GET', path)
        assert ready.status_code == 302
        link = ready.headers['Location']
        assert link.startswith(f'{EXPORTS}/')
        assert 'X-Amz-Signature=' in link
        # Every look is billed, as the request for it was.
        assert ready.headers['X-RateLimit-Remaining'] == '197'

    def test_the_link_serves_the_graph_without_a_token(self, fake):
        fake.repos[1].graph = GRAPH
        link = ask(
            fake, 'GET', report_path(ask(fake, 'GET', GENERATE)),
        ).headers['Location']
        downloaded = ask(fake, 'GET', link, token=None)
        assert downloaded.status_code == 200
        assert downloaded.json() == GRAPH
        # Off the API: no bucket, and nothing billed.
        assert 'X-RateLimit-Resource' not in downloaded.headers
        seen = fake.requests[-1]
        assert (seen.host, seen.token, seen.billed) == (
            'sbom-exports.example', None, False,
        )

    def test_the_link_refuses_a_token_beside_its_signature(self, fake):
        """As a signed link refuses a second credential: whatever sends
        GitHub's token there has sent it off the API."""
        fake.repos[1].graph = GRAPH
        link = ask(
            fake, 'GET', report_path(ask(fake, 'GET', GENERATE)),
        ).headers['Location']
        refused = ask(fake, 'GET', link, token=ONE)
        assert refused.status_code == 400
        assert fake.requests[-1].token == ONE

    def test_the_link_expires(self, fake):
        fake.repos[1].graph = GRAPH
        fake.link_seconds = 60
        path = report_path(ask(fake, 'GET', GENERATE))
        link = ask(fake, 'GET', path).headers['Location']
        fake.clock.advance(60)
        assert ask(fake, 'GET', link, token=None).status_code == 403
        # Looked at again, the report is signed a new link.
        again = ask(fake, 'GET', path).headers['Location']
        assert again != link
        assert ask(fake, 'GET', again, token=None).status_code == 200

    def test_a_link_signed_for_another_report_is_refused(self, fake):
        fake.repos[1].graph = GRAPH
        link = ask(
            fake, 'GET', report_path(ask(fake, 'GET', GENERATE)),
        ).headers['Location']
        forged = link.replace('/sbom/', '/sbom/0-', 1)
        assert ask(fake, 'GET', forged, token=None).status_code == 403

    def test_a_report_is_of_the_graph_as_it_was_asked_for(self, fake):
        fake.repos[1].graph = GRAPH
        path = report_path(ask(fake, 'GET', GENERATE))
        fake.repos[1].graph = {**GRAPH, 'name': 'changed since'}
        link = ask(fake, 'GET', path).headers['Location']
        assert ask(fake, 'GET', link, token=None).json() == GRAPH

    @pytest.mark.parametrize('wrapped', [False, True])
    def test_stamps_each_report_as_github_does_when_asked(
        self, fake, wrapped,
    ):
        """When it was made, `creationInfo.created`, and a namespace of
        its own, `documentNamespace`: two reports of one graph differ in
        nothing else."""
        fake.repos[1].graph = {'sbom': GRAPH} if wrapped else GRAPH
        fake.stamp_reports = True

        def downloaded() -> Any:
            path = report_path(ask(fake, 'GET', GENERATE))
            link = ask(fake, 'GET', path).headers['Location']
            body = ask(fake, 'GET', link, token=None).json()
            return body['sbom'] if wrapped else body

        first = downloaded()
        fake.clock.advance(60)
        second = downloaded()
        for document, made in ((first, START), (second, START + 60)):
            info = {**GRAPH['creationInfo'], 'created': stamp(made)}
            assert document == {
                **GRAPH, 'creationInfo': info,
                'documentNamespace': document['documentNamespace'],
            }
            assert document['documentNamespace'].startswith(
                'https://github.com/octo/one/dependency_graph/sbom-',
            )
        assert first['documentNamespace'] != second['documentNamespace']
        # The graph itself is as it was.
        assert fake.repos[1].graph == ({'sbom': GRAPH} if wrapped else GRAPH)

    def test_a_report_it_does_not_have_is_404(self, fake):
        fake.repos[1].graph = GRAPH
        path = report_path(ask(fake, 'GET', GENERATE))
        assert ask(fake, 'GET', path + '0').status_code == 404
        # Nor is one repository's report looked at by another's name.
        other = path.replace('/octo/one/', '/octo/two/')
        assert ask(fake, 'GET', other).status_code == 404
        del fake.reports[path.rsplit('/', 1)[1]]
        assert ask(fake, 'GET', path).status_code == 404

    def test_serves_a_scripted_download_in_place_of_its_own(self, fake):
        fake.repos[1].graph = GRAPH
        link = ask(
            fake, 'GET', report_path(ask(fake, 'GET', GENERATE)),
        ).headers['Location']
        fake.script(
            Reply(500, b'<Error>InternalError</Error>'),
            path=httpx2.URL(link).path,
        )
        assert ask(fake, 'GET', link, token=None).status_code == 500
        assert ask(fake, 'GET', link, token=None).status_code == 200
