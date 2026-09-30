"""The Python dataset API, held to what D1 answered the contract suite.

The Worker's contract suite asked its two stores, D1 and ClickHouse,
the same questions about one corpus and expected one answer. The Python
port of those questions, `chatsbom/dataset/` (#138), answers the page
now, through `chatsbom web serve`, and the page was not to notice. So
every call that suite made of D1 was recorded with D1's answer
(`web/test/fixtures/contract/calls.json`), before the Worker and D1
were deleted (#151), and here each is asked of the Python over the same
export, `d1.sql` applied to a SQLite file, with what a snapshot adds to
D1's tables (#132, #165), and opened read-only as a snapshot is, and
has to come back as the same JSON. What #165 changed of D1's answers,
`meta`'s contract number and the edges' ambiguity, calls.json says
since, and the three calls it added, with answers worked out by hand
from `d1.sql`.

Pinned beside it: every method the page's client asks
(`DatasetClient`, `web/src/dataset/client.ts`) has a Python counterpart
and nothing else is one, the recording asks every one of them, and each
answer's fields are named as `dataset/types.ts` names them, which a
recording cannot show of an answer D1 never gave.
"""
from __future__ import annotations

import inspect
import json
import re
import sqlite3
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest
from pydantic import BaseModel

from chatsbom.dataset import Dataset
from chatsbom.dataset import jsonable
from chatsbom.dataset import open_dataset
from chatsbom.dataset import types
from chatsbom.snapshot.schema import add_dependants
from chatsbom.snapshot.schema import SCHEMA
from tests import golden

ROOT = Path(__file__).resolve().parents[1]
CONTRACT = ROOT / 'web/test/fixtures/contract'
CLIENT = ROOT / 'web/src/dataset/client.ts'
TYPES = ROOT / 'web/src/dataset/types.ts'

#: The calls, and D1's answers, in the order the recorder kept them.
CALLS: list[dict[str, Any]] = json.loads(
    (CONTRACT / 'calls.json').read_text(encoding='utf-8'),
)


#: The seed's edges' ambiguity, which D1's schema had nowhere to keep:
#: the warehouse's `mv_edge_ambiguity` of it, which `snapshot build`
#: copies into a snapshot (#165), as ClickHouse answered it before it was
#: deleted (`tests/golden/warehouse-contract.json`). Fifteen names,
#: `mail` the one of more than one ecosystem; fifteen edges, `mail-dev
#: -> mail` the one at such a name, which `agg_edges` leaves out since
#: `mail-dev` is no package; and expressjs/site's nine packages, the
#: most of any repository.
EDGE_AMBIGUITY: dict[str, Any] = golden.load('warehouse-contract.json')[
    'relations'
]['mv_edge_ambiguity']


def corpus(directory: Path) -> Path:
    """The contract's D1 export, applied to a SQLite file as D1 applies
    it, and closed, with what a snapshot of the same data has that D1's
    tables had not: the page table, made from their rows as `snapshot
    build` makes it, the edges' ambiguity the warehouse measured of the
    seed, and the snapshot's indexes, D1's among them. What a published
    snapshot of the same data is (#132)."""
    path = directory / 'contract.sqlite'
    columns = EDGE_AMBIGUITY['columns']
    with closing(sqlite3.connect(path)) as connection:
        connection.executescript(
            (CONTRACT / 'd1.sql').read_text(encoding='utf-8'),
        )
        add_dependants(connection)
        connection.execute(SCHEMA.table('agg_edge_ambiguity').ddl())
        connection.executemany(
            f"INSERT INTO agg_edge_ambiguity ({', '.join(columns)}) "
            f"VALUES ({', '.join('?' for _ in columns)})",
            EDGE_AMBIGUITY['rows'],
        )
        for index in SCHEMA.indexes:
            connection.execute(index.ddl())
        connection.commit()
    return path


def snake(name: str) -> str:
    """`dependentsOf` as Python spells it: `dependents_of`."""
    return re.sub(r'(?<!^)(?=[A-Z])', '_', name).lower()


def label(call: dict[str, Any]) -> str:
    params = ', '.join(
        json.dumps(param, sort_keys=True) for param in call['params']
    )
    return f"{call['method']}({params})"


def ask(dataset: Dataset, method: str, params: list[Any]) -> Any:
    """A recorded call, asked of the Python.

    The TypeScript method's parameters, in order: each positional one as
    it is, and an object of options — a dependant query, a tree's shape,
    the ranking's filters — as keywords, each named in snake_case. That
    is the whole of the mapping between the two signatures.
    """
    positional: list[Any] = []
    keywords: dict[str, Any] = {}
    for param in params:
        if isinstance(param, dict):
            keywords.update(
                {snake(key): value for key, value in param.items()},
            )
        else:
            positional.append(param)
    return getattr(dataset, snake(method))(*positional, **keywords)


def typescript_methods() -> list[str]:
    """What the page's client asks: `DatasetClient`'s public methods, as
    `web/src/dataset/client.ts` declares them.

    Parsed rather than imported: there is no Node in the Python test
    run. Each method's name opens a line of the class's body, two spaces
    in; a private one is marked so, and the constructor is none.
    """
    source = CLIENT.read_text(encoding='utf-8')
    body = source[source.index('export class DatasetClient {'):]
    body = body[:body.index('\n}\n')]
    return [
        name for name in re.findall(r'^  (\w+)\(', body, re.MULTILINE)
        if name != 'constructor'
    ]


def typescript_interfaces() -> dict[str, set[str]]:
    """Each interface of `web/src/dataset/types.ts`, and its fields."""
    source = TYPES.read_text(encoding='utf-8')
    return {
        match.group(1): set(
            re.findall(r'^  (\w+)\??:', match.group(2), re.MULTILINE),
        )
        for match in re.finditer(
            r'^export interface (\w+) \{\n(.*?)^\}',
            source,
            re.MULTILINE | re.DOTALL,
        )
    }


def python_methods() -> set[str]:
    """What `Dataset` offers a caller."""
    return {
        name
        for name, _ in inspect.getmembers(Dataset, inspect.isfunction)
        if not name.startswith('_')
    }


def python_answers() -> dict[str, type[BaseModel]]:
    """The answer types `chatsbom.dataset.types` declares."""
    return {
        name: model
        for name, model in vars(types).items()
        if isinstance(model, type)
        and issubclass(model, BaseModel)
        and model.__module__ == types.__name__
        and model is not types.Answer
    }


@pytest.fixture(scope='module')
def dataset(tmp_path_factory: pytest.TempPathFactory) -> Iterator[Dataset]:
    with open_dataset(corpus(tmp_path_factory.mktemp('contract'))) as opened:
        yield opened


class TestTheRecordedCalls:

    @pytest.mark.parametrize('call', CALLS, ids=label)
    def test_are_answered_as_d1_answered_them(
        self, dataset: Dataset, call: dict[str, Any],
    ) -> None:
        answer = jsonable(ask(dataset, call['method'], call['params']))
        assert answer == call['returns']
        # And as the same JSON, not only an equal value: `5.0 == 5` and
        # `True == 1` in Python, and the page would read either wrongly.
        assert json.dumps(answer, sort_keys=True) == json.dumps(
            call['returns'], sort_keys=True,
        )

    def test_ask_every_method_the_page_asks(self) -> None:
        # Else a method could drift with nothing here to see it.
        assert {call['method'] for call in CALLS} == set(typescript_methods())


class TestTheMethods:

    def test_the_client_parses(self) -> None:
        # If this breaks, the comparisons below are vacuous rather than
        # failing, so it is asserted by itself. `dependencyTree` and
        # `topPackages` are declared over several lines, and the client
        # has private methods, which the page's code alone calls.
        methods = typescript_methods()
        assert len(methods) >= 20
        assert {'dependentsOf', 'dependencyTree', 'topPackages', 'meta'} \
            <= set(methods)
        assert not {'call', 'send', 'ask', 'get', 'constructor'} \
            & set(methods)

    def test_each_the_page_asks_has_a_python_counterpart(self) -> None:
        missing = {snake(name) for name in typescript_methods()} \
            - python_methods()
        assert not missing, f'no Python counterpart: {sorted(missing)}'

    def test_nothing_else_is_offered(self) -> None:
        # The seam is at the method level: a `query(sql)`, or any method
        # the page cannot call, would move it.
        extra = python_methods() \
            - {snake(name) for name in typescript_methods()}
        assert not extra, f'not a method the page asks: {sorted(extra)}'


#: What the page reads with an answer that the service says beside it,
#: rather than the method: `/api/meta` names the snapshot with `meta`'s
#: answer (`chatsbom/server/queries.py`), and the page's `DatasetMeta`
#: is the two (#165).
SAID_BESIDE: dict[str, set[str]] = {'DatasetMeta': {'snapshot'}}


class TestTheAnswersNames:
    """Each answer's fields, as the page reads them."""

    def test_the_types_parse(self) -> None:
        interfaces = typescript_interfaces()
        assert len(interfaces) >= 20
        assert interfaces['Dependent'] >= {'observedAt', 'manifests'}

    def test_each_answer_type_has_a_python_counterpart(self) -> None:
        # `DependentQuery` is what the dependants methods are asked, and
        # Python takes it as keywords.
        assert set(python_answers()) == \
            set(typescript_interfaces()) - {'DependentQuery'}

    @pytest.mark.parametrize('name', sorted(python_answers()))
    def test_each_field_is_named_as_the_page_reads_it(self, name: str) -> None:
        model = python_answers()[name]
        wire = {
            field.serialization_alias or field_name
            for field_name, field in model.model_fields.items()
        }
        assert wire | SAID_BESIDE.get(name, set()) == \
            typescript_interfaces()[name]

    def test_what_the_service_says_beside_an_answer_is_not_the_answer_s(
        self,
    ) -> None:
        # Else the answer's would stand where the service's should:
        # `/api/meta` writes the answer after the snapshot's id.
        for name, beside in SAID_BESIDE.items():
            model = python_answers()[name]
            assert not beside & {
                field.serialization_alias or field_name
                for field_name, field in model.model_fields.items()
            }
