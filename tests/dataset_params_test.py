"""Each dataset method against its own parameters (#31, #138).

The Worker's endpoint for the page, `web/src/d1/api.ts` until #151,
refused a value before any store saw it: a count or a position that is
not a whole number at least its minimum, a string longer than what it
names, a flag that is not true or false. The Python API has no endpoint
in front of it, and every caller it is for, the web routes, the chat's
tools and the CLI, calls the methods directly: so each method refuses
what that endpoint refused, before it asks the snapshot anything. And
what the stores clamped (`web/src/dataset/shape.ts`), it clamps: past a
ceiling a value is not wrong, only more than anyone gets.

The cases are those of the Worker's `web/test/d1api.test.ts` and
`web/test/d1queries.test.ts`, asked of the contract's corpus through a
connection that keeps what it was asked.
"""
from __future__ import annotations

import math
import sqlite3
from collections.abc import Callable
from collections.abc import Iterator
from collections.abc import Sequence
from contextlib import closing
from typing import Any

import pytest

from chatsbom.dataset import Dataset
from chatsbom.dataset import InvalidParameter
from chatsbom.dataset import jsonable
from chatsbom.dataset.open import connect
from chatsbom.dataset.params import JAVASCRIPT_NAMES
from chatsbom.dataset.shape import MAX_OFFSET
from chatsbom.dataset.shape import shape_spread
from chatsbom.dataset.types import VersionShare
from tests.dataset_contract_test import corpus


class Spy:
    """A snapshot's connection that keeps each statement it is asked,
    and what it was bound with, and answers it for real."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection
        self.calls: list[tuple[str, list[Any]]] = []

    def execute(
        self, sql: str, parameters: Sequence[Any], /,
    ) -> sqlite3.Cursor:
        self.calls.append((sql, list(parameters)))
        return self.connection.execute(sql, parameters)

    @property
    def sql(self) -> str:
        """The last statement."""
        return self.calls[-1][0]

    @property
    def params(self) -> list[Any]:
        """What the last statement was bound with."""
        return self.calls[-1][1]


@pytest.fixture(scope='module')
def snapshot(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[sqlite3.Connection]:
    with closing(connect(corpus(tmp_path_factory.mktemp('params')))) as db:
        yield db


@pytest.fixture
def spy(snapshot: sqlite3.Connection) -> Spy:
    return Spy(snapshot)


@pytest.fixture
def dataset(spy: Spy) -> Dataset:
    return Dataset(spy)


def flat(sql: str) -> str:
    return ' '.join(sql.split())


class TestRefused:
    """Refused, and nothing asked: `d1api.test.ts`'s cases (#31)."""

    @pytest.mark.parametrize(
        ('method', 'args', 'keywords'),
        [
            pytest.param(
                'dependents_of', ('mail',), {'limit': -1},
                id='a negative limit',
            ),
            pytest.param(
                'dependents_of', ('mail',), {'limit': 2.5},
                id='a fractional limit',
            ),
            pytest.param(
                'dependents_of', ('mail',), {'limit': 2 ** 60},
                id='a limit too large to be exact',
            ),
            pytest.param(
                'dependents_of', ('mail',), {'limit': math.inf},
                id='a number JSON can only spell as infinity',
            ),
            pytest.param(
                'dependents_of', ('mail',), {'limit': math.nan},
                id='a number that is not one',
            ),
            pytest.param(
                'dependents_of', ('mail',), {'offset': -50},
                id='a negative offset',
            ),
            pytest.param(
                'dependents_of', ('mail',), {'offset': 0.5},
                id='a fractional offset',
            ),
            pytest.param(
                'search_packages', ('ma',), {'limit': 0},
                id='a limit of nothing',
            ),
            pytest.param(
                'version_spread', ('mail',), {'limit': -1},
                id='a negative version count',
            ),
            pytest.param(
                'license_shares', (), {'limit': -3},
                id='a negative licence count',
            ),
            pytest.param(
                'top_packages', (), {'limit': 1.5},
                id='a fractional ranking depth',
            ),
            pytest.param(
                'pulled_in_by', ('ms',), {'limit': -1},
                id='a negative edge count',
            ),
            pytest.param(
                'dependencies_of', ('ms',), {'limit': 7.5},
                id='a fractional edge count',
            ),
            pytest.param(
                'dependency_tree', ('ms',), {'children': 0},
                id='a tree of no children',
            ),
            pytest.param(
                'dependency_tree', ('ms',), {'children': -1},
                id='a tree of negative children',
            ),
            pytest.param(
                'dependency_tree', ('ms',), {'branch': 1.5},
                id='a tree with a fractional branch',
            ),
            pytest.param(
                'dependency_tree', ('ms',), {'branch': '99'},
                id='a tree bound given as text',
            ),
            # JSON's `true` is no number, and Python's `True` is an int.
            pytest.param(
                'license_shares', (), {'limit': True},
                id='a limit given as a flag',
            ),
            pytest.param(
                'top_packages', (), {'direct_only': 'yes'},
                id='a flag that is not true or false',
            ),
            pytest.param(
                'dependents_of', ('mail',), {'direct_only': 1},
                id='a flag given as a number',
            ),
            pytest.param(
                'dependents_of', ('',), {},
                id='no package name',
            ),
            pytest.param(
                'dependents_of', ({'$ne': None},), {},
                id='a package name that is not a string',
            ),
            pytest.param(
                'search_packages', ('',), {},
                id='no search term',
            ),
            pytest.param(
                'pulled_in_by', ('',), {},
                id='no name for the reverse edges',
            ),
            pytest.param(
                'dependencies_of', ('',), {},
                id='no name for the edges',
            ),
            pytest.param(
                'dependency_tree', ('',), {},
                id='no name for the tree',
            ),
            pytest.param(
                'dependents_of', ('x' * 257,), {},
                id='an over-long package name',
            ),
            pytest.param(
                'search_packages', ('x' * 257,), {},
                id='an over-long search term',
            ),
            pytest.param(
                'dependency_tree', ('x' * 257,), {},
                id='an over-long name for the tree',
            ),
            pytest.param(
                'dependents_of', ('mail',), {'language': 'x' * 65},
                id='an over-long repository language',
            ),
            pytest.param(
                'top_packages', (), {'ecosystem': 'x' * 65},
                id='an over-long ecosystem to rank',
            ),
            pytest.param(
                'relationship_split', ('x' * 65,), {},
                id='an over-long ecosystem to split',
            ),
            pytest.param(
                'count_dependents', ('mail',), {'type': 'x' * 65},
                id='an over-long type',
            ),
            pytest.param(
                'count_dependent_rows', ('mail',), {'language': 3},
                id='a language that is not a string',
            ),
        ],
    )
    def test_before_anything_is_asked(
        self,
        dataset: Dataset,
        spy: Spy,
        method: str,
        args: tuple[Any, ...],
        keywords: dict[str, Any],
    ) -> None:
        with pytest.raises(InvalidParameter):
            getattr(dataset, method)(*args, **keywords)
        assert spy.calls == []

    def test_says_which_parameter(self, dataset: Dataset) -> None:
        with pytest.raises(InvalidParameter, match='"limit"'):
            dataset.dependents_of('mail', limit=0)
        with pytest.raises(InvalidParameter, match='"name"'):
            dataset.ecosystems_for('x' * 257)
        with pytest.raises(InvalidParameter, match='"branch"'):
            dataset.dependency_tree('ms', branch=-1)

    def test_is_a_value_error(self) -> None:
        # So a caller that does not know this API's errors still reads
        # a bad argument as one.
        assert issubclass(InvalidParameter, ValueError)

    @pytest.mark.parametrize('name', sorted(JAVASCRIPT_NAMES))
    def test_an_ecosystem_named_after_a_javascript_built_in(
        self, dataset: Dataset, spy: Spy, name: str,
    ) -> None:
        # The endpoint refuses these, as no registry is called by a name
        # every JavaScript object has; while the page can be answered by
        # either service, both refuse the same values.
        for method in (
            'dependents_of', 'count_dependents',
            'count_dependent_rows',
        ):
            with pytest.raises(InvalidParameter, match='"type"'):
                getattr(dataset, method)('mail', type=name)
        with pytest.raises(InvalidParameter, match='"ecosystem"'):
            dataset.top_packages(ecosystem=name)
        with pytest.raises(InvalidParameter, match='"ecosystem"'):
            dataset.relationship_split(name)
        assert spy.calls == []

    def test_the_javascript_names_are_those_of_every_object(self) -> None:
        # `Object.getOwnPropertyNames(Object.prototype)`, in Node 22 and
        # 26: what `value in Object.prototype` finds in `api.ts`.
        assert JAVASCRIPT_NAMES == {
            '__defineGetter__', '__defineSetter__', '__lookupGetter__',
            '__lookupSetter__', '__proto__', 'constructor',
            'hasOwnProperty', 'isPrototypeOf', 'propertyIsEnumerable',
            'toLocaleString', 'toString', 'valueOf',
        }

    def test_the_counts_take_no_page(self, dataset: Dataset) -> None:
        # A count has no use for a page's size or its place, which the
        # TypeScript interface takes and drops: here they are not
        # parameters at all. Asked as an untyped caller would ask.
        counted: Any = dataset.count_dependents
        with pytest.raises(TypeError):
            counted('mail', limit=10)
        rows: Any = dataset.count_dependent_rows
        with pytest.raises(TypeError):
            rows('mail', offset=0)

    def test_no_method_takes_sql(self, dataset: Dataset) -> None:
        # Nor any parameter it does not declare.
        split: Any = dataset.relationship_split
        with pytest.raises(TypeError):
            split(sql='DROP TABLE artifacts')


class TestTaken:

    def test_a_string_at_its_cap(self, dataset: Dataset, spy: Spy) -> None:
        # 256 for a name, 64 for a language or an ecosystem.
        assert dataset.dependents_of(
            'x' * 256, type='y' * 64, language='z' * 64,
        ) == []
        assert len(spy.calls) == 1

    def test_a_length_counted_as_javascript_counts_it(
        self, dataset: Dataset,
    ) -> None:
        # In UTF-16 code units, as `value.length` does: a character past
        # the Basic Multilingual Plane is two. So the same name is, or is
        # not, too long in both services.
        assert dataset.ecosystems_for('\N{ROCKET}' * 128) == []
        with pytest.raises(InvalidParameter, match='"name"'):
            dataset.ecosystems_for('\N{ROCKET}' * 129)

    def test_not_half_a_character(self, dataset: Dataset, spy: Spy) -> None:
        # JSON can spell a lone surrogate, `"\ud800"`, and SQLite cannot
        # bind one: refused as what it is, not failed in the statement.
        with pytest.raises(InvalidParameter, match='"term" must be text'):
            dataset.search_packages('\ud800')
        assert spy.calls == []

    def test_a_whole_number_however_it_is_spelled(
        self, dataset: Dataset, spy: Spy,
    ) -> None:
        # JSON has one kind of number, and `2.0` is the whole number 2
        # to the page's endpoint as well: what `json.loads` gives a chat
        # tool's arguments.
        loose: Any = dataset.dependents_of
        assert len(loose('mail', limit=2.0, offset=0.0)) == 2
        assert spy.params[-2:] == [2, 0]
        assert all(type(value) is int for value in spy.params[-2:])

    def test_nothing_where_a_value_may_be_left_out(
        self, dataset: Dataset,
    ) -> None:
        # None is a value not given, as `undefined` and `null` are: JSON's
        # null, from an untyped caller, where a flag is expected too.
        loose: Any = dataset.dependents_of
        assert loose(
            'mail', type=None, language=None, direct_only=None,
            limit=None, offset=None,
        ) == dataset.dependents_of('mail')
        assert dataset.dependents_of('mail', type='', language='') \
            == dataset.dependents_of('mail')

    def test_an_ecosystem_the_table_does_not_map(
        self, dataset: Dataset, spy: Spy,
    ) -> None:
        # `github-action` is in the data and not in the ecosystem table:
        # the page offers it as itself, so it must come back working.
        assert dataset.dependents_of('actions/checkout', type='github-action') \
            == []
        assert 'github-action' in spy.params

    def test_every_call_the_page_and_the_chat_tools_make(
        self, dataset: Dataset, spy: Spy,
    ) -> None:
        dataset.dependents_of(
            'mail', type='gem', language='ruby', direct_only=True,
            limit=100, offset=200,
        )
        # The values arrive as they were sent, not re-clamped on the way.
        assert spy.calls[0][1] == ['mail', 'gem', 'ruby', 'direct', 100, 200]
        dataset.count_dependents('mail', direct_only=False)
        dataset.count_dependent_rows('mail')
        dataset.search_packages('mai', 8)
        dataset.top_packages(direct_only=True, ecosystem='npm', limit=100)
        dataset.license_shares(12)
        dataset.version_spread('mail', 10)
        dataset.pulled_in_by('ms', 15)
        dataset.dependencies_of('ms', 20)
        dataset.dependency_tree('body-parser', children=12, branch=3)
        dataset.relationship_split('maven')
        dataset.adoption_over_time('mail')
        dataset.ecosystems_for('mail')
        dataset.ecosystem_coverage()
        dataset.relationship_by_ecosystem()


class TestClamped:
    """Past a ceiling, as much as anyone gets: `d1queries.test.ts`."""

    def test_the_offset_as_the_other_store_must(
        self, dataset: Dataset, spy: Spy,
    ) -> None:
        # One bound for every store (#31): past it no package has rows,
        # and ClickHouse could not bind a larger one at all.
        assert dataset.dependents_of('mail', offset=10 ** 12) == []
        assert spy.params[-1] == MAX_OFFSET == 2 ** 32 - 1

    def test_the_rows_whatever_the_caller_asks_for(
        self, dataset: Dataset, spy: Spy,
    ) -> None:
        assert len(dataset.dependents_of('mail', limit=10_000)) == 8
        assert spy.params[-2] == 500
        dataset.search_packages('a', 10_000)
        assert spy.params[-1] == 500
        dataset.pulled_in_by('ms', 10_000)
        assert spy.params[1] == 500
        dataset.license_shares(10_000)
        assert spy.params == [500]
        dataset.top_packages(limit=10_000)
        assert spy.params[-1] == 500

    def test_the_tree_a_caller_asks_for(self, dataset: Dataset, spy: Spy) -> None:
        dataset.dependency_tree('body-parser', children=10_000, branch=10_000)
        # The first statement's LIMIT, and the second's per-parent rank.
        assert spy.calls[0][1][1] == 30
        assert spy.calls[1][1][-1] == 12

    def test_to_the_defaults(self, dataset: Dataset, spy: Spy) -> None:
        dataset.dependents_of('mail')
        assert spy.params[-2:] == [50, 0]
        dataset.search_packages('m')
        assert spy.params[-1] == 20
        dataset.dependencies_of('express')
        assert spy.params[-1] == 20
        dataset.pulled_in_by('debug')
        assert spy.params[-1] == 20
        dataset.license_shares()
        assert spy.params == [12]
        dataset.top_packages()
        assert spy.params == [0, '', 50]
        dataset.dependency_tree('express')
        assert spy.calls[-2][1][-1] == 14
        assert spy.calls[-1][1][-1] == 4

    def test_the_version_spread_to_ten_unless_asked(
        self, dataset: Dataset, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        # Sliced after the statement, so the depth is not in what the
        # snapshot is asked: it is what the spread is cut to.
        cut: list[int] = []

        def watched(rows: Sequence[VersionShare], limit: int) -> Any:
            cut.append(limit)
            return shape_spread(rows, limit)

        monkeypatch.setattr(
            'chatsbom.dataset.queries.shape_spread', watched,
        )
        dataset.version_spread('mail')
        dataset.version_spread('mail', 3)
        dataset.version_spread('mail', 10_000)
        assert cut == [10, 3, 500]

    def test_the_version_spread_after_it_is_asked(
        self, dataset: Dataset, spy: Spy,
    ) -> None:
        # The top ten *resolved* versions are not the resolved rows among
        # the top ten of everything, so the LIMIT cannot be in the SQL.
        spread = dataset.version_spread('laravel/framework', 1)
        assert spy.params == ['laravel/framework']
        assert 'LIMIT' not in spy.sql.upper()
        assert jsonable(spread) == {
            'versions': [
                {
                    'kind': 'resolved', 'version': 'v12.49.0',
                    'repositoryCount': 2,
                },
            ],
            'constrained': 3,
            'unversioned': 1,
        }


class TestBound:
    """What a method binds: never a value spliced into its statement."""

    HOSTILE = "mail'; DROP TABLE x;--"

    def test_a_name_rather_than_interpolating_it(
        self, dataset: Dataset, spy: Spy,
    ) -> None:
        asks: list[Callable[[str], object]] = [
            dataset.dependents_of, dataset.count_dependents,
            dataset.count_dependent_rows, dataset.ecosystems_for,
            dataset.version_spread, dataset.adoption_over_time,
            dataset.dependencies_of, dataset.pulled_in_by,
            dataset.search_packages,
        ]
        for ask in asks:
            ask(self.HOSTILE)
            assert 'DROP TABLE' not in spy.sql
            assert any('DROP TABLE' in str(value) for value in spy.params)

    def test_a_type_under_its_canonical_name(
        self, dataset: Dataset, spy: Spy,
    ) -> None:
        # As `kinds` and the page table store it: Syft's `php-composer`,
        # passed through, matched nothing at all.
        rows = dataset.dependents_of('laravel/framework', type='php-composer')
        assert 'composer' in spy.params
        assert 'php-composer' not in spy.params
        assert 'd.type = ?' in spy.sql
        assert len(rows) == 9

    def test_the_folded_language_lowercased(
        self, dataset: Dataset, spy: Spy,
    ) -> None:
        dataset.dependents_of('mail', language='Other')
        assert 'd.language_bucket = ?' in spy.sql
        assert 'other' in spy.params

    def test_declared_only_by_the_relationship(
        self, dataset: Dataset, spy: Spy,
    ) -> None:
        dataset.dependents_of('mail', direct_only=True)
        assert 'd.relationship = ?' in spy.sql
        assert 'direct' in spy.params

    def test_the_whole_corpus_or_one_ecosystem_lowercased(
        self, dataset: Dataset, spy: Spy,
    ) -> None:
        dataset.relationship_split()
        assert spy.params == ['']
        dataset.relationship_split('Ruby')
        assert spy.params == ['ruby']
        dataset.top_packages(ecosystem='Maven')
        assert spy.params == [0, 'maven', 50]
        dataset.top_packages(direct_only=True)
        assert spy.params == [1, '', 50]

    def test_a_search_anchored_at_the_start(
        self, dataset: Dataset, spy: Spy,
    ) -> None:
        # A leading wildcard cannot use the index on `packages.name`.
        dataset.search_packages('mai')
        assert spy.params[0] == 'mai%'

    def test_a_search_term_with_its_wildcards_escaped(
        self, dataset: Dataset, spy: Spy,
    ) -> None:
        # Otherwise `%` and `_` in what a reader typed become wildcards,
        # and the search quietly matches far more than they asked for.
        dataset.search_packages('100%_real\\')
        assert spy.params[0] == '100\\%\\_real\\\\%'
        assert 'ESCAPE' in spy.sql.upper()
        assert dataset.search_packages('m_') == []
        assert [match.name for match in dataset.search_packages('mini_')] \
            == ['mini_mime']
        # LIKE folds ASCII case, as it does on D1.
        assert dataset.search_packages('M') == dataset.search_packages('m')


class TestTheStatements:
    """What the statements must be for the page's speed, from
    `d1queries.test.ts`: what the answers cannot show."""

    @pytest.mark.parametrize(
        ('method', 'args', 'table'),
        [
            ('totals', (), 'agg_totals'),
            ('language_coverage', (), 'agg_language_coverage'),
            ('ecosystem_coverage', (), 'agg_ecosystem_coverage'),
            ('source_comparison', (), 'agg_source_comparison'),
            ('dependency_distribution', (), 'agg_dependency_buckets'),
            ('license_shares', (), 'licenses'),
            ('adoption_over_time', ('mail',), 'history'),
            ('top_packages', (), 'agg_top_packages'),
            ('relationship_split', (), 'agg_relationship_split'),
            ('relationship_by_ecosystem', (), 'agg_relationship_split'),
        ],
    )
    def test_an_aggregate_is_read_never_recomputed(
        self,
        dataset: Dataset,
        spy: Spy,
        method: str,
        args: tuple[Any, ...],
        table: str,
    ) -> None:
        # Aggregated live, the overview's panels read every one of six
        # million artifact rows by definition.
        getattr(dataset, method)(*args)
        assert f'FROM {table}' in flat(spy.sql)
        assert 'artifacts' not in spy.sql
        assert not any(
            word in spy.sql.upper() for word in ('GROUP BY', 'COUNT(', 'SUM(')
        )

    def test_a_count_carries_no_limit(self, dataset: Dataset, spy: Spy) -> None:
        dataset.count_dependents('mail')
        assert 'count(DISTINCT d.repository_id)' in spy.sql
        assert 'LIMIT' not in spy.sql.upper()

    def test_the_counts_filter_as_the_rows_do(
        self, dataset: Dataset, spy: Spy,
    ) -> None:
        # A count over other filters than the rows beside it looks
        # authoritative and disagrees with what the reader can see.
        def where(sql: str) -> str:
            clause = flat(sql)
            clause = clause[clause.index('WHERE'):]
            return clause.split(' GROUP BY')[0].split(' ORDER')[0]

        dataset.dependents_of(
            'mail', type='gem', language='ruby', direct_only=True,
        )
        rows = spy.calls[-1]
        dataset.count_dependents(
            'mail', type='gem', language='ruby', direct_only=True,
        )
        dependants = spy.calls[-1]
        dataset.count_dependent_rows(
            'mail', type='gem', language='ruby', direct_only=True,
        )
        counted = spy.calls[-1]
        assert where(dependants[0]) == where(rows[0])
        assert where(counted[0]) == where(rows[0])
        assert dependants[1] == counted[1] == rows[1][:-2]

    def test_the_tree_asks_its_first_hop_first(
        self, dataset: Dataset, spy: Spy,
    ) -> None:
        dataset.dependency_tree('body-parser', branch=3)
        assert len(spy.calls) == 2
        # Bound by the first hop's names, one placeholder each, and the
        # root, which alone is excluded, before the ranking.
        second, bound = spy.calls[1]
        assert bound == ['debug', 'qs', 'bytes', 'raw-body', 'body-parser', 3]
        assert flat(second).count('?') == 6
        assert flat(second).index('cc.name <> ?') \
            < flat(second).index('branch_rank <=')

    def test_the_tree_asks_nothing_more_of_a_leaf(
        self, dataset: Dataset, spy: Spy,
    ) -> None:
        tree = dataset.dependency_tree('left-pad')
        assert len(spy.calls) == 1
        assert jsonable(tree) == {
            'root': 'left-pad', 'children': [], 'grandchildren': [],
        }
