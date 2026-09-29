"""The page's reads, versioned by snapshot (#144).

The Worker answered `POST /api/q`, `{method, params}`, never to be kept,
and kept what it could itself, under a version it polled `meta` for
(web/src/d1/api.ts, cache.ts). Here the version is in the URL: a
snapshot's id is the hash of what it holds (#132), so an answer under
one is that answer for good, and anything between the page and the
service may keep it (#128, sections 2.4 and 2.5):

  /api/meta                   the current snapshot's id, and its
                              provenance, as `meta` answers it: kept a
                              minute, so a snapshot published is seen
                              within one
  /api/v/{snapshot}/{method}  one of the 21 methods of `Dataset`, by the
                              page's name for it, asked of that
                              snapshot, with its parameters as the query
                              string: kept for good

The page asks `meta` once, then each of its questions under the id it
was told: one request a panel, as it asked `/api/q`. A snapshot stays
served while `CURRENT` lists it, the current one and the two kept; one
it no longer lists answers 410, and the page asks `meta` again. When
WEB_SNAPSHOT names one snapshot's file, its own id is the only one.

**The parameters are the method's own.** Each name in the query string
is the page's name for one of them, `directOnly` for `direct_only`, and
each value is text, read as JSON would have carried it to the method: a
number where the method takes a count, true or false where it takes a
flag, and text everywhere else, so that a package called `true` is
still a name. The method then checks what it is given, as #142 checks
it for every caller; the route checks nothing twice. What it refuses
is a 400, in its own words. A name the method has no parameter for,
and one given twice, are refused before anything is opened.

An answer is JSON under the page's names (`jsonable`), and only an
answer is kept: a refusal is for its moment, and says `no-store`. Every
request counts against QUERY_RATE_LIMIT, as `/api/q`'s did.
"""
from __future__ import annotations

import hashlib
import inspect
import json
import re
import sqlite3
import typing
from collections.abc import Mapping
from collections.abc import Sequence
from contextlib import closing
from dataclasses import dataclass
from pathlib import Path
from typing import Literal

import structlog
from fastapi.responses import JSONResponse
from fastapi.responses import Response

from chatsbom.dataset import Dataset
from chatsbom.dataset import InvalidParameter
from chatsbom.dataset import jsonable
from chatsbom.dataset import open_dataset
from chatsbom.dataset import served
from chatsbom.dataset.open import connect
from chatsbom.dataset.open import current
from chatsbom.dataset.open import ID
from chatsbom.dataset.open import SUFFIX
from chatsbom.server.ratelimit import RateLimiter

logger = structlog.get_logger('queries')

#: A snapshot never changes, so neither does its answer to a URL.
IMMUTABLE = 'public, max-age=31536000, immutable'
#: Which snapshot is current may change with any pass, at most hourly:
#: a page is told within a minute.
META_AGE = 'max-age=60'
#: A refusal is for the moment it was made.
NO_STORE = 'no-store'

# What each refusal says, in English: the page says it by its status in
# the reader's language (web/src/i18n/strings.tsx, `queryRefused`).
TOO_MANY = 'Too many queries. Wait a moment.'
NO_DATASET = 'No dataset is configured on this deployment.'
UNREADABLE = 'The dataset cannot be read for a moment. Try again shortly.'
RETIRED = (
    'This snapshot of the dataset is no longer served. Reload the page.'
)
FAILED = 'The query could not be answered.'

#: A number as JSON writes one: what a count is read as.
JSON_NUMBER = re.compile(
    r'-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?',
)

#: How a parameter's text is read: as the JSON its method takes.
Kind = Literal['text', 'number', 'flag']


def camel(name: str) -> str:
    """A Python name as the page spells it: `direct_only`, `directOnly`."""
    return re.sub(r'_([a-z])', lambda match: match.group(1).upper(), name)


@dataclass(frozen=True)
class Parameter:
    """One of a method's parameters, as the query string gives it."""

    #: The method's own name for it.
    name: str
    kind: Kind
    #: Whether the method has no default for it.
    required: bool

    def read(self, text: str) -> object:
        """`text`, as JSON would have carried it to the method: what is
        not a number or a flag stays text, and the method refuses it as
        it refuses any other value of the wrong kind."""
        if self.kind == 'flag':
            return {'true': True, 'false': False}.get(text, text)
        if self.kind == 'number' and JSON_NUMBER.fullmatch(text):
            number: object = json.loads(text)
            return number
        return text


def _kind(hint: object) -> Kind:
    """What a parameter annotated `hint` takes: a flag is a bool, a
    count an int, and anything else a string."""
    kinds = typing.get_args(hint) or (hint,)
    if bool in kinds:
        return 'flag'
    if int in kinds:
        return 'number'
    return 'text'


@dataclass(frozen=True)
class Method:
    """One of `Dataset`'s methods, as a path of the API."""

    #: The method's own name.
    name: str
    #: Its parameters, by the page's names for them.
    parameters: Mapping[str, Parameter]

    def arguments(self, query: Sequence[tuple[str, str]]) -> dict[str, object]:
        """The method's arguments, from the query string's pairs.

        A parameter it cannot do without, left out, is given as None,
        which the method refuses as it refuses a JSON null: in its own
        words, as the Worker's endpoint said it.
        """
        given: dict[str, object] = {}
        for key, text in query:
            parameter = self.parameters.get(key)
            if parameter is None:
                raise InvalidParameter(f'Unknown parameter: "{key}"')
            if parameter.name in given:
                raise InvalidParameter(f'"{key}" is given twice')
            given[parameter.name] = parameter.read(text)
        for parameter in self.parameters.values():
            if parameter.required and parameter.name not in given:
                given[parameter.name] = None
        return given


def _method(name: str) -> Method:
    function = getattr(Dataset, name)
    hints = typing.get_type_hints(function)
    return Method(
        name=name,
        parameters={
            camel(parameter.name): Parameter(
                name=parameter.name,
                kind=_kind(hints[parameter.name]),
                required=parameter.default is inspect.Parameter.empty,
            )
            for parameter in inspect.signature(function).parameters.values()
            if parameter.name != 'self'
        },
    )


#: The methods, by the page's names: `Dataset`'s public methods, which
#: are the 21 of `DatasetQueries` and nothing else
#: (tests/dataset_contract_test.py).
METHODS: Mapping[str, Method] = {
    camel(name): _method(name)
    for name, _ in inspect.getmembers(Dataset, inspect.isfunction)
    if not name.startswith('_')
}


def own_id(path: Path) -> str:
    """The id of the snapshot file at `path`: the one its `meta` holds,
    which `snapshot build` named it by. A file that holds none, of D1's
    tables and the page table as the tests make one, is known by its
    bytes: the first sixteen hex digits of their SHA-256, so that other
    bytes are another snapshot to anything that keeps its answers."""
    with closing(connect(path)) as db:
        try:
            row = db.execute('SELECT snapshot FROM meta').fetchone()
        except sqlite3.OperationalError:
            # D1's `meta`, which has no such column.
            row = None
    if row is not None and isinstance(row[0], str) and ID.fullmatch(row[0]):
        return row[0]
    digest = hashlib.sha256()
    with path.open('rb') as file:
        for block in iter(lambda: file.read(1 << 20), b''):
            digest.update(block)
    return digest.hexdigest()[:16]


class Snapshots:
    """The snapshots WEB_SNAPSHOT serves: the directory they are
    published in, where `CURRENT` says which, as each request is
    answered; or one snapshot's file, whose id is read as the service
    starts."""

    def __init__(self, path: Path) -> None:
        self.path = path
        self._own = None if path.is_dir() else own_id(path)

    def current(self) -> tuple[str, Path]:
        """The current snapshot's id and file. OSError or ValueError when
        there is none."""
        if self._own is not None:
            return self._own, self.path
        found = current(self.path)
        return found.name.removesuffix(SUFFIX), found

    def find(self, snapshot: str) -> Path | None:
        """The file of the snapshot `snapshot` names, while it is
        served; None once it is not, or if it never was. OSError or
        ValueError when which are served cannot be read."""
        if self._own is not None:
            return self.path if snapshot == self._own else None
        if snapshot not in served(self.path):
            return None
        path = self.path / f'{snapshot}{SUFFIX}'
        return path if path.is_file() else None


def refused(status: int, message: str) -> JSONResponse:
    """A refusal, as the page's endpoint made them: `{"error": ...}`,
    kept by no one."""
    return JSONResponse(
        {'error': message}, status, headers={'Cache-Control': NO_STORE},
    )


class Reads:
    """The page's reads: of `snapshot`, WEB_SNAPSHOT, or of nothing when
    it is unset, and each counted against `limit`, QUERY_RATE_LIMIT.

    Blocking: each opens its snapshot, read-only, in the thread it runs
    in, and closes it, since a connection belongs to its thread. The
    open takes well under a millisecond.
    """

    def __init__(self, snapshot: Path | None, limit: RateLimiter) -> None:
        self.snapshots = None if snapshot is None else Snapshots(snapshot)
        self.limit = limit

    def meta(self, client: str) -> Response:
        """GET /api/meta, for `client`."""
        if not self.limit.admit(client):
            return refused(429, TOO_MANY)
        if self.snapshots is None:
            return refused(503, NO_DATASET)
        try:
            snapshot, path = self.snapshots.current()
            with open_dataset(path) as dataset:
                provenance = jsonable(dataset.meta())
        except (OSError, ValueError, sqlite3.Error) as error:
            logger.error('no snapshot to serve', error=str(error))
            return refused(503, UNREADABLE)
        return JSONResponse(
            {'snapshot': snapshot, **provenance},
            headers={'Cache-Control': META_AGE},
        )

    def read(
        self,
        client: str,
        snapshot: str,
        name: str,
        query: Sequence[tuple[str, str]],
    ) -> Response:
        """GET /api/v/{snapshot}/{name}, with `query`, for `client`."""
        if not self.limit.admit(client):
            return refused(429, TOO_MANY)
        method = METHODS.get(name)
        if method is None:
            return refused(404, f'Unknown method: {name}')
        if self.snapshots is None:
            return refused(503, NO_DATASET)
        try:
            path = self.snapshots.find(snapshot)
        except (OSError, ValueError) as error:
            logger.error('no snapshot to serve', error=str(error))
            return refused(503, UNREADABLE)
        if path is None:
            return refused(410, RETIRED)
        try:
            arguments = method.arguments(query)
            with open_dataset(path) as dataset:
                answer = getattr(dataset, method.name)(**arguments)
        except InvalidParameter as invalid:
            return refused(400, str(invalid))
        except (sqlite3.Error, OSError) as error:
            # Its text names tables and SQL: logged, never answered.
            logger.error(
                'query failed', method=name, snapshot=snapshot,
                error=str(error),
            )
            return refused(500, FAILED)
        return JSONResponse(
            jsonable(answer), headers={'Cache-Control': IMMUTABLE},
        )
