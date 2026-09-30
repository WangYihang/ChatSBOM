"""The collector's GitHub client (#156): REST, GraphQL and search, on
httpx2, each request on the token the budget manager chose for it.

- **REST** (`get`): a GET of the API, conditional where validators are
  kept (`ValidatorStore`, collector.sqlite's): `If-None-Match` with the
  ETag an earlier answer gave, or `If-Modified-Since` with its
  Last-Modified. A 304 is answered free, for a request made with a
  token, and says the document is as it was. collector.sqlite keeps
  validators and never documents, so a caller that no longer has what
  it was sent asks with `conditional=False`.
- **GraphQL** (`graphql`): `POST /graphql`, from the `graphql` bucket, a
  query held at the points it costs. What it could not resolve comes
  back with the data, as GitHub gives it.
- **Search** (`search`): a page of results, from the `search` bucket,
  or `code_search`; never conditional.

A refusal for a rate limit, 403 or 429, or GraphQL's 200 that says
RATE_LIMITED, is not an answer: the budget backs the token's bucket off,
and the request is asked again, with another token or once the bucket
has room, for as long as `wait` allows. A token GitHub does not take,
401, is left out, and the request asked again with another. Whatever
else goes wrong is one of `errors`' four.

A token goes to the API alone: a URL elsewhere is refused before
anything is sent, redirects are not followed, and no log line or error
holds a token (`tokens.scrub`). A caller may ask for a redirect as the
answer, which says where it points (`Answer.location`): a finished
dependency-graph report is a 302 to a link off the API, signed in its
query, which the caller fetches without a token (#162). Where it points
is in no log line or error.
"""
import json
import re
import time
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timezone
from types import TracebackType
from typing import Any
from typing import Protocol
from typing import Self
from urllib.parse import urlencode
from urllib.parse import urljoin
from urllib.parse import urlsplit

import httpx2
import structlog

from chatsbom.__version__ import __version__
from chatsbom.collector.budget import BudgetManager
from chatsbom.collector.budget import Lease
from chatsbom.collector.errors import Failed
from chatsbom.collector.errors import GitHubError
from chatsbom.collector.errors import Gone
from chatsbom.collector.errors import NotFound
from chatsbom.collector.state import Validators
from chatsbom.collector.tokens import scrub
from chatsbom.core.redact import redact_url

logger = structlog.get_logger('collector.github')

#: GitHub's API.
API = 'https://api.github.com'

#: The REST API's version, as GitHub asks to be told (`X-GitHub-Api-
#: Version`): what an answer looks like does not change under us.
API_VERSION = '2022-11-28'

#: What a request asks for, unless it says: GitHub's JSON.
MEDIA_TYPE = 'application/vnd.github+json'

#: How long a request may take: GitHub answers most at once, a GraphQL
#: query of a hundred nodes in seconds.
TIMEOUT = httpx2.Timeout(30.0, connect=10.0)

#: A connection that could not be made is tried again this often: no
#: request was sent, so nothing was billed.
CONNECT_RETRIES = 2

#: What an error quotes of what GitHub said, at most.
QUOTED = 300

#: The statuses of a redirect: moved.
REDIRECTS = frozenset({301, 302, 303, 307, 308})

#: One link of a `Link` header: `<url>; rel="next"`.
_LINK = re.compile(r'<([^>]*)>\s*;\s*rel="([^"]*)"')


class ValidatorStore(Protocol):
    """Where validators are kept, by the request they answered:
    `state.CollectorState`."""

    def validators(self, request: str) -> Validators | None:
        ...

    def keep_validators(
        self, request: str, validators: Validators,
        now: datetime | None = None,
    ) -> None:
        ...

    def drop_validators(self, request: str) -> None:
        ...


def request_key(
    method: str, path: str, params: Mapping[str, str], accept: str,
) -> str:
    """What validators are kept by: the request as GitHub answers it, its
    method, its path and query, in order, and the media type it asked
    for. Not the token: GitHub's validators answer for any."""
    query = urlencode(sorted(params.items()))
    return f'{method} {path}' + (f'?{query}' if query else '') + f' {accept}'


def bucket_of(path: str) -> str:
    """The bucket a request for `path`, on the API, draws from."""
    if path == '/graphql':
        return 'graphql'
    if path.startswith('/search/code'):
        return 'code_search'
    if path.startswith('/search/'):
        return 'search'
    return 'core'


@dataclass(frozen=True)
class Answer:
    """GitHub's answer to a GET: a document, a 304, or a redirect where
    the caller asked for one."""

    status: int
    headers: Mapping[str, str]
    #: Empty for a 304.
    content: bytes
    #: The request's URL, as a log shows one.
    url: str
    #: The name of the token it was asked with.
    token: str
    #: The bucket it drew from.
    bucket: str

    @property
    def not_modified(self) -> bool:
        """304: as it was when its validators were kept, and free."""
        return self.status == 304

    @property
    def links(self) -> dict[str, str]:
        """The pages its `Link` header names, by relation: `next`,
        `last`, `first`, `prev`."""
        return {
            relation: url
            for url, relations in _LINK.findall(self.headers.get('link', ''))
            for relation in relations.split()
        }

    @property
    def location(self) -> str | None:
        """Where a redirect points, its `Location` read against the
        request; None without one, or with one that is no URL. A link
        that may be signed, for the caller alone: never followed here,
        and never logged."""
        where = self.headers.get('location')
        if not where:
            return None
        try:
            return urljoin(self.url, where)
        except ValueError:
            return None

    def json(self) -> Any:
        try:
            return json.loads(self.content)
        except ValueError:
            raise Failed(
                f'GET {self.url}: {self.status}, and the answer is not JSON',
                status=self.status, url=self.url,
            ) from None


@dataclass(frozen=True)
class GraphQLAnswer:
    """GitHub's answer to a GraphQL query."""

    data: Mapping[str, Any]
    #: What it could not resolve, beside what it could: a node it has
    #: no more, as `NOT_FOUND`.
    errors: tuple[Mapping[str, Any], ...]
    token: str
    bucket: str
    #: The answer's headers: where its bucket stood once it was charged.
    headers: Mapping[str, str] = field(default_factory=dict)


def _message(response: httpx2.Response) -> str:
    """What GitHub said, as its JSON's `message`, or ''."""
    try:
        body = response.json()
    except ValueError:
        return ''
    message = body.get('message') if isinstance(body, dict) else None
    return message if isinstance(message, str) else ''


def _blocked(response: httpx2.Response) -> bool:
    """A 403 for a repository blocked, as its `block` says."""
    try:
        body = response.json()
    except ValueError:
        return False
    return isinstance(body, dict) and isinstance(body.get('block'), dict)


def _rate_limited(response: httpx2.Response, graphql: bool) -> bool:
    """Whether GitHub refused the request for a rate limit rather than
    answering it: 429; a 403 with nothing left, with `Retry-After`, or
    saying it is a rate limit, which a secondary limit may say alone;
    and, for GraphQL, a 200 whose error is RATE_LIMITED."""
    status = response.status_code
    if status == 429:
        return True
    if status == 403:
        headers = response.headers
        said = _message(response).lower()
        return (
            headers.get('x-ratelimit-remaining', '').strip() == '0'
            or 'retry-after' in headers
            or 'rate limit' in said
            or 'abuse' in said
        )
    if graphql and status == 200:
        try:
            body = response.json()
        except ValueError:
            return False
        errors = body.get('errors') if isinstance(body, dict) else None
        return isinstance(errors, list) and any(
            isinstance(error, dict) and error.get('type') == 'RATE_LIMITED'
            for error in errors
        )
    return False


class GitHubClient:
    """GitHub's API, on the tokens `budget` holds."""

    def __init__(
        self,
        budget: BudgetManager,
        *,
        validators: ValidatorStore | None = None,
        transport: httpx2.AsyncBaseTransport | None = None,
        base_url: str = API,
        timeout: httpx2.Timeout = TIMEOUT,
    ) -> None:
        parts = urlsplit(base_url)
        if parts.scheme not in ('http', 'https') or not parts.netloc:
            raise ValueError(f'not an http or https address: {base_url!r}')
        self.budget = budget
        self._validators = validators
        self._base = base_url.rstrip('/')
        self._origin = (parts.scheme, parts.netloc.lower())
        self._prefix = parts.path.rstrip('/')
        self._http = httpx2.AsyncClient(
            transport=(
                transport if transport is not None
                else httpx2.AsyncHTTPTransport(retries=CONNECT_RETRIES)
            ),
            timeout=timeout,
            follow_redirects=False,
            headers={
                'Accept': MEDIA_TYPE,
                'User-Agent': f'chatsbom/{__version__}',
                'X-GitHub-Api-Version': API_VERSION,
            },
        )

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
        await self._http.aclose()

    # -- the API ----------------------------------------------------------

    async def get(
        self,
        where: str,
        *,
        params: Mapping[str, str | int] | None = None,
        accept: str = MEDIA_TYPE,
        bucket: str | None = None,
        conditional: bool = True,
        redirect: bool = False,
        wait: float | None = None,
    ) -> Answer:
        """A GET of `where`, a path on the API or a URL there, as a
        `Link` gives one. Conditional where validators are kept, unless
        `conditional` is false; either way, the validators of a document
        answered whole are kept. A page of search results, which `Link`
        may name, is never conditional. `bucket` is the one GitHub meters
        it from, where the path does not say (`bucket_of`); `redirect`,
        that a redirect is the answer rather than `Gone`, and where it
        points is `Answer.location`; `wait`, how long to wait for a token
        with room, at most."""
        return await self._get(
            where, params=params, accept=accept, bucket=bucket,
            conditional=conditional, redirect=redirect, wait=wait,
        )

    async def search(
        self,
        what: str,
        q: str,
        *,
        sort: str | None = None,
        order: str | None = None,
        per_page: int = 100,
        page: int = 1,
        wait: float | None = None,
    ) -> Answer:
        """A page of `GET /search/<what>` for `q`: `total_count`,
        `incomplete_results` and `items`, and a `Link` to the next."""
        params: dict[str, str | int] = {'q': q}
        if sort is not None:
            params['sort'] = sort
        if order is not None:
            params['order'] = order
        params.update(per_page=per_page, page=page)
        return await self._get(
            f'/search/{what}', params=params, accept=MEDIA_TYPE, bucket=None,
            conditional=False, redirect=False, wait=wait,
        )

    async def graphql(
        self,
        query: str,
        variables: Mapping[str, Any] | None = None,
        *,
        cost: int = 1,
        wait: float | None = None,
    ) -> GraphQLAnswer:
        """`query` with `variables`, held at `cost` points while it is in
        flight: what GitHub charges it, which a query's `rateLimit {
        cost }` says, and which is to be measured on a live token before
        it is relied on (#128)."""
        target = httpx2.URL(f'{self._base}/graphql')
        response, lease, resource = await self._exchange(
            'POST', target, bucket='graphql', cost=cost, headers={},
            body={'query': query, 'variables': dict(variables or {})},
            wait=wait,
        )
        shown = redact_url(str(target))
        if response.status_code != 200:
            raise self._error('POST', shown, response)
        try:
            body = response.json()
        except ValueError:
            body = None
        data = body.get('data') if isinstance(body, dict) else None
        errors = body.get('errors') if isinstance(body, dict) else None
        errors = tuple(
            error for error in errors or () if isinstance(error, dict)
        )
        if not isinstance(data, dict):
            said = '; '.join(str(error.get('message', '')) for error in errors)
            raise Failed(
                f'POST {shown}: no data'
                + (f': {self._quote(said)}' if said else ''),
                status=200, url=shown,
            )
        return GraphQLAnswer(
            data=data, errors=errors, token=lease.token.label,
            bucket=resource, headers=response.headers,
        )

    # -- how it asks ------------------------------------------------------

    def _target(
        self, where: str, params: Mapping[str, str | int] | None,
    ) -> httpx2.URL:
        """`where` on the API, with `params`: never anywhere else."""
        if where.startswith('/') and not where.startswith('//'):
            url = f'{self._base}{where}'
        else:
            parts = urlsplit(where)
            if (
                (parts.scheme, parts.netloc.lower()) != self._origin
                or not parts.path.startswith(f'{self._prefix}/')
            ):
                raise ValueError(
                    f'not on {self._base}, where the tokens go and nowhere '
                    f'else: {redact_url(where)!r}',
                )
            url = where
        target = httpx2.URL(url)
        if params:
            target = target.copy_merge_params(
                {name: str(value) for name, value in params.items()},
            )
        return target

    async def _get(
        self,
        where: str,
        *,
        params: Mapping[str, str | int] | None,
        accept: str,
        bucket: str | None,
        conditional: bool,
        redirect: bool,
        wait: float | None,
    ) -> Answer:
        target = self._target(where, params)
        path = target.path[len(self._prefix):]
        key = request_key(
            'GET', path, dict(target.params.multi_items()), accept,
        )
        # A page of search results, however it is asked, by `search` or
        # by its `Link`, has no validators: asked again a week later it
        # would be answered 304, and nothing keeps the page.
        searching = path.startswith('/search/')
        store = None if searching else self._validators
        headers = {'Accept': accept}
        kept = store.validators(key) if store and conditional else None
        if kept is not None and kept.etag:
            headers['If-None-Match'] = kept.etag
        if kept is not None and kept.last_modified:
            headers['If-Modified-Since'] = kept.last_modified
        response, lease, resource = await self._exchange(
            'GET', target, bucket=bucket or bucket_of(path), cost=1,
            headers=headers, body=None, wait=wait,
        )
        shown = redact_url(str(target))
        status = response.status_code
        redirected = redirect and status in REDIRECTS
        if status == 304 or 200 <= status < 300 or redirected:
            if store is not None and status == 200:
                found = Validators(
                    response.headers.get('etag'),
                    response.headers.get('last-modified'),
                )
                now = datetime.fromtimestamp(self.budget.clock(), timezone.utc)
                store.keep_validators(key, found, now)
            if store is not None and redirected:
                store.drop_validators(key)
            return Answer(
                status=status, headers=response.headers,
                content=response.content, url=shown,
                token=lease.token.label, bucket=resource,
            )
        if store is not None and status in (404, 410, 451, *REDIRECTS):
            store.drop_validators(key)
        raise self._error('GET', shown, response)

    async def _exchange(
        self,
        method: str,
        target: httpx2.URL,
        *,
        bucket: str,
        cost: int,
        headers: Mapping[str, str],
        body: Any,
        wait: float | None,
    ) -> tuple[httpx2.Response, Lease, str]:
        """The request, asked until it is answered rather than refused:
        the answer, the lease it was answered on, and the bucket it drew
        from."""
        shown = redact_url(str(target))
        give_up = None if wait is None else self.budget.clock() + wait
        graphql = bucket == 'graphql' and method == 'POST'
        while True:
            lease = await self.budget.lease(
                bucket, cost=cost,
                wait=None if give_up is None
                else max(give_up - self.budget.clock(), 0.0),
            )
            async with lease:
                started = time.monotonic()
                response, failure = await self._send(
                    lease, method, target, headers, body,
                )
                if response is None:
                    lease.lost()
                    logger.debug(
                        'GitHub request failed', method=method, url=shown,
                        token=lease.token.label, error=failure,
                    )
                    raise Failed(f'{method} {shown}: {failure}', url=shown)
                status = response.status_code
                if status == 401:
                    lease.release()
                    self.budget.retire(
                        lease.token, 'GitHub answered 401, Bad credentials',
                    )
                    continue
                if _rate_limited(response, graphql):
                    lease.refused(response.headers)
                    continue
                lease.answered(response.headers, free=status == 304)
                resource = (
                    response.headers.get('x-ratelimit-resource') or bucket
                ).strip()
                logger.debug(
                    'GitHub answered', method=method, url=shown,
                    status=status, token=lease.token.label, bucket=resource,
                    remaining=response.headers.get('x-ratelimit-remaining'),
                    elapsed=f'{time.monotonic() - started:.3f}s',
                )
                return response, lease, resource

    async def _send(
        self,
        lease: Lease,
        method: str,
        target: httpx2.URL,
        headers: Mapping[str, str],
        body: Any,
    ) -> tuple[httpx2.Response | None, str]:
        """The answer, or why there was none, as an error may say it."""
        try:
            response = await self._http.request(
                method, target,
                headers={
                    **headers, 'Authorization': f'Bearer {lease.token.secret}',
                },
                json=body,
            )
        except httpx2.HTTPError as error:
            # Said without the exception, which quotes the request and
            # may quote its header.
            return None, f'{type(error).__name__}: {self._quote(str(error))}'
        return response, ''

    # -- what it says -----------------------------------------------------

    def _quote(self, text: str) -> str:
        """`text`, as an error or a log line may quote it: short, and
        without a token."""
        text = scrub(text, self.budget.tokens)
        return text if len(text) <= QUOTED else f'{text[:QUOTED]}...'

    def _error(
        self, method: str, shown: str, response: httpx2.Response,
    ) -> GitHubError:
        status = response.status_code
        said = self._quote(_message(response))
        text = f'{method} {shown}: {status}' + (f', {said}' if said else '')
        if status in REDIRECTS:
            location = response.headers.get('location')
            moved_to = (
                redact_url(str(response.url.join(location)))
                if location else None
            )
            return Gone(
                text + (f', to {moved_to}' if moved_to else ''),
                status=status, url=shown, moved_to=moved_to,
            )
        if status == 404:
            return NotFound(text, status=status, url=shown)
        if status in (410, 451) or (status == 403 and _blocked(response)):
            return Gone(text, status=status, url=shown)
        return Failed(text, status=status, url=shown)
