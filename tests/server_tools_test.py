"""What the model is told about its tools, and what it is handed back.

The chat's loop runs on the server now (#140), and its tools are the
dataset API (#138), called in-process: the page no longer runs them, so
no client supplies a tool's result. What they are and what they return
are the Worker's (`web/src/tools.ts`, `web/src/prompt.ts`), whose tests
(`web/test/tools.test.ts`) are ported here where they apply. Each part
of this has put a wrong number in an answer:

- `dependents_of` returned `count: rows.length`. The rows stop at a
  limit, so asked about `react` the model reported 50 dependants,
  where the store counts 5,095.
- The descriptions described other tools: a "substring" search that
  matches prefixes, and defaults the stores did not apply. They are
  checked here against what the Python dataset does.
- Results had no size. Two `dependents_of` calls at limit 500 were more
  text than a whole conversation was allowed.

New here: an ecosystem the model spells as a collector does
(`php-composer`) is the ecosystem the page names (`composer`): the
ranking lowercases it but does not canonicalise it (#142).
"""
import json
import re
import sqlite3
from collections.abc import Sequence
from pathlib import Path
from typing import Any

import pytest

from chatsbom.dataset import Dataset
from chatsbom.dataset import jsonable
from chatsbom.dataset import open_dataset
from chatsbom.dataset.types import Dependent
from chatsbom.dataset.types import PackageMatch
from chatsbom.dataset.types import PackagePopularity
from chatsbom.dataset.types import VersionShare
from chatsbom.dataset.types import VersionSpread
from chatsbom.models.relationship import RELATIONSHIPS
from chatsbom.server.prompt import SYSTEM_PROMPT
from chatsbom.server.prompt import TOOLS
from chatsbom.server.tools import call
from chatsbom.server.tools import execute
from chatsbom.server.tools import RESULT_CHARS
from chatsbom.server.tools import RESULT_ROWS
from chatsbom.server.tools import TOOL_NAMES
from chatsbom.server.tools import ToolError
from tests.dataset_contract_test import corpus


def dependant(index: int, repo: str | None = None) -> Dependent:
    """A dependant as the dataset returns one: a little over 200
    characters of JSON, and a long `repo` for the widest there can be."""
    name = repo or f'project-{index}'
    return Dependent(
        owner=f'owner-{index}',
        repo=name,
        stars=250_000 - index,
        version='18.3.1',
        url=f'https://github.com/owner-{index}/{name}',
        language='typescript',
        ecosystem='npm',
        relationship='direct' if index % 3 == 0 else 'transitive',
        observed_at='2026-09-13',
        manifests=1,
    )


class FakeDataset:
    """Answers as the dataset would, and records what it was asked.

    `dependents_of` honours the limit, 50 when none is given, as the
    dataset does; `search_packages` and `top_packages` return what they
    were handed whatever the limit.
    """

    def __init__(
        self,
        *,
        dependents: Sequence[Dependent] = (),
        total: int = 0,
        direct_total: int = 0,
        matches: Sequence[PackageMatch] = (),
        top: Sequence[PackagePopularity] = (),
        spread: VersionSpread | None = None,
    ) -> None:
        self.dependents = list(dependents)
        self.total = total
        self.direct_total = direct_total
        self.matches = list(matches)
        self.top = list(top)
        self.spread = spread or VersionSpread(
            versions=[], constrained=0, unversioned=0,
        )
        self.asked: dict[str, list[Any]] = {}

    def _note(self, method: str, *args: Any, **kwargs: Any) -> None:
        self.asked.setdefault(method, []).append((args, kwargs))

    def dependents_of(self, name: str, **kwargs: Any) -> list[Dependent]:
        self._note('dependents_of', name, **kwargs)
        limit = kwargs.get('limit')
        return self.dependents[:50 if limit is None else limit]

    def count_dependents(self, name: str, **kwargs: Any) -> int:
        self._note('count_dependents', name, **kwargs)
        return self.direct_total if kwargs.get('direct_only') else self.total

    def search_packages(
        self, term: str, limit: int | None = None,
    ) -> list[PackageMatch]:
        self._note('search_packages', term, limit)
        return self.matches

    def top_packages(self, **kwargs: Any) -> list[PackagePopularity]:
        self._note('top_packages', **kwargs)
        return self.top

    def version_spread(
        self, name: str, limit: int | None = None,
    ) -> VersionSpread:
        self._note('version_spread', name, limit)
        return self.spread


def run(dataset: Any, tool: str, /, **arguments: Any) -> dict[str, Any]:
    result: dict[str, Any] = execute(dataset, tool, arguments)
    return result


def sent(result: dict[str, Any]) -> str:
    """A result as the model receives it: JSON, as compact as the
    Worker's `JSON.stringify`."""
    return json.dumps(result, ensure_ascii=False, separators=(',', ':'))


REACT = [dependant(index) for index in range(200)]


class TestDependentsOf:
    def test_reports_how_many_depend_on_it_not_how_many_rows_it_shows(self):
        dataset = FakeDataset(
            dependents=REACT, total=5_095, direct_total=1_207,
        )

        result = run(dataset, 'dependents_of', name='react')

        assert result['total'] == 5_095
        assert result['direct_total'] == 1_207
        assert result['rows_shown'] == 50
        assert len(result['rows']) == 50
        assert 'truncated' not in result

    def test_counts_with_the_filters_the_rows_were_chosen_by(self):
        """A count over other filters than the rows beside it is worse
        than none: it looks authoritative and disagrees."""
        dataset = FakeDataset(dependents=REACT, total=118)

        run(
            dataset, 'dependents_of',
            name='mail', type='gem', language='ruby', limit=20,
        )

        filters = {'type': 'gem', 'language': 'ruby'}
        assert dataset.asked['dependents_of'] == [
            (('mail',), {**filters, 'direct_only': False, 'limit': 20}),
        ]
        assert dataset.asked['count_dependents'] == [
            (('mail',), {**filters, 'direct_only': False}),
            (('mail',), {**filters, 'direct_only': True}),
        ]

    def test_with_direct_only_counts_only_the_declared_and_once(self):
        dataset = FakeDataset(dependents=REACT, total=118, direct_total=17)

        result = run(dataset, 'dependents_of', name='mail', direct_only=True)

        assert result['total'] == 17
        # `total` already is the declared count; a second would repeat it.
        assert 'direct_total' not in result
        assert dataset.asked['count_dependents'] == [
            (('mail',), {'type': None, 'language': None, 'direct_only': True}),
        ]

    def test_puts_the_counts_first_where_the_model_reads_first(self):
        dataset = FakeDataset(dependents=REACT[:2], total=5, direct_total=1)
        result = run(dataset, 'dependents_of', name='react')
        assert list(result) == ['total', 'direct_total', 'rows_shown', 'rows']

    def test_hands_over_the_rows_as_the_page_reads_them(self):
        dataset = FakeDataset(dependents=REACT[:1], total=1)
        result = run(dataset, 'dependents_of', name='react')
        assert result['rows'] == [jsonable(REACT[0])]
        assert 'observedAt' in result['rows'][0]


class TestEveryResultIsBounded:
    def test_cuts_a_result_too_long_says_so_and_keeps_the_head(self):
        """The widest rows there can be. At the old ceiling of 500 rows
        this was over 200,000 characters: one result, larger than a
        whole conversation was allowed to be."""
        wide = [
            dependant(
                index,
                f'a-repository-name-as-long-as-github-allows-{index}'.ljust(
                    100, '-',
                ),
            )
            for index in range(500)
        ]
        dataset = FakeDataset(dependents=wide, total=5_095, direct_total=1_207)

        result = run(dataset, 'dependents_of', name='react', limit=500)

        assert len(sent(result)) <= RESULT_CHARS
        assert result['total'] == 5_095
        assert result['truncated'] is True
        shown, dropped = result['rows_shown'], result['rows_dropped']
        assert shown > 0 and dropped > 0
        # What was fetched is what was shown plus what was dropped, and
        # the rows kept are the head of the ranking: the most starred.
        assert shown + dropped == RESULT_ROWS
        assert result['rows'] == [jsonable(row) for row in wide[:shown]]

    def test_measures_length_as_javascript_counts_it(self):
        """In UTF-16 code units, as the Worker's `JSON.stringify(...)
        .length` did: a character past the Basic Multilingual Plane is
        two."""
        rocket = '\U0001F680'
        rows = [dependant(index, rocket * 90) for index in range(100)]
        dataset = FakeDataset(dependents=rows, total=100)

        result = run(dataset, 'dependents_of', name='react', limit=100)

        text = sent(result)
        assert len(text.encode('utf-16-le')) // 2 <= RESULT_CHARS
        assert result['truncated'] is True

    def test_cuts_rows_past_the_row_cap_whichever_tool_returned_them(self):
        matches = [
            PackageMatch(
                name=f'eslint-plugin-{index}', ecosystem=None,
                repository_count=150 - index, name_total=150 - index,
            )
            for index in range(150)
        ]
        dataset = FakeDataset(matches=matches)

        result = run(
            dataset, 'search_packages', fragment='eslint-plugin-', limit=75,
        )

        assert result['rows_shown'] == RESULT_ROWS
        assert result['truncated'] is True
        assert result['rows_dropped'] == 150 - RESULT_ROWS
        assert result['rows'] == [
            jsonable(row) for row in matches[:RESULT_ROWS]
        ]

    def test_leaves_a_result_that_fits_as_it_came(self):
        top = [
            PackagePopularity(
                name=f'package-{index}', repository_count=1_000 - index,
                direct_count=100 - index,
            )
            for index in range(30)
        ]
        dataset = FakeDataset(top=top)

        result = run(dataset, 'top_packages', direct_only=True)

        assert result == {'rows_shown': 30, 'rows': jsonable(top)}

    def test_asks_the_dataset_for_no_more_rows_than_a_result_may_carry(self):
        dataset = FakeDataset()

        run(dataset, 'dependents_of', name='react', limit=500)
        run(dataset, 'search_packages', fragment='react', limit=500)
        run(dataset, 'top_packages', limit=500)
        run(dataset, 'version_spread', name='react', limit=500)

        assert dataset.asked['dependents_of'][0][1]['limit'] == RESULT_ROWS
        assert dataset.asked['search_packages'] == [
            (('react', RESULT_ROWS), {}),
        ]
        assert dataset.asked['top_packages'][0][1]['limit'] == RESULT_ROWS
        assert dataset.asked['version_spread'] == [
            (('react', RESULT_ROWS), {}),
        ]

    @pytest.mark.parametrize(
        'limit,asked',
        [(0, 1), (-5, 1), (2.7, 2), (None, None), ('20', None), (True, None)],
    )
    def test_takes_a_limit_as_the_worker_did(self, limit, asked):
        """A number, floored and held within 1 to RESULT_ROWS; anything
        else, `true` among it, is no limit at all."""
        dataset = FakeDataset()
        run(dataset, 'search_packages', fragment='react', limit=limit)
        assert dataset.asked['search_packages'] == [(('react', asked), {})]

    def test_lists_a_version_spread_as_rows_beside_what_it_set_aside(self):
        versions = [
            VersionShare(
                kind='resolved', version='2.8.1',
                repository_count=40,
            ),
            VersionShare(
                kind='resolved', version='2.7.0',
                repository_count=12,
            ),
        ]
        dataset = FakeDataset(
            spread=VersionSpread(
                versions=versions, constrained=9, unversioned=3,
            ),
        )

        result = run(dataset, 'version_spread', name='mail')

        assert result == {
            'constrained': 9,
            'unversioned': 3,
            'rows_shown': 2,
            'rows': jsonable(versions),
        }


class TestWhatTheToolsRefuse:
    """A model's call is not trusted for its shape: every argument is
    checked again, and what cannot be run is said, for the model to
    recover from."""

    @pytest.mark.parametrize(
        'name,arguments,said',
        [
            ('dependents_of', {}, 'dependents_of requires a package name'),
            (
                'dependents_of', {'name': ''},
                'dependents_of requires a package name',
            ),
            (
                'dependents_of', {'name': 7},
                'dependents_of requires a package name',
            ),
            ('ecosystems_for', {}, 'ecosystems_for requires a package name'),
            ('search_packages', {}, 'search_packages requires a fragment'),
            ('version_spread', {}, 'version_spread requires a package name'),
            (
                'run_sql', {'sql': 'DROP TABLE packages'},
                'unknown tool: run_sql',
            ),
        ],
    )
    def test_what_cannot_be_run(self, name, arguments, said):
        with pytest.raises(ToolError, match=re.escape(said)):
            execute(fake(), name, arguments)

    def test_arguments_that_are_not_an_object(self):
        with pytest.raises(ToolError, match='not a JSON object'):
            execute(fake(), 'language_coverage', ['mail'])

    @pytest.mark.parametrize('arguments', [None, {}])
    def test_none_at_all_are_none_to_a_tool_that_takes_none(self, arguments):
        assert execute(fake(), 'top_packages', arguments) == {
            'rows_shown': 0, 'rows': [],
        }


def fake() -> Any:
    """A dataset with nothing in it, where the tools take a Dataset."""
    return FakeDataset()


@pytest.fixture(scope='module')
def snapshot(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """The contract's corpus, as a snapshot is published."""
    return corpus(tmp_path_factory.mktemp('snapshot'))


def called(snapshot: Path, name: str, arguments: str) -> dict[str, Any]:
    """A call as the loop makes it: by name, with the model's own text
    for its arguments, and what goes back to the model."""
    text = call(snapshot, name, arguments)
    parsed: dict[str, Any] = json.loads(text)
    return parsed


class TestACall:
    """A tool call as the loop makes it: the model's text, parsed,
    against the snapshot the question pinned, and what the model is
    handed back, as text."""

    def test_answers_from_the_snapshot(self, snapshot):
        result = called(snapshot, 'ecosystems_for', '{"name": "mail"}')
        assert result == {
            'rows_shown': 3,
            'rows': [
                {'type': 'gem', 'repositoryCount': 3, 'directCount': 3},
                {'type': 'maven', 'repositoryCount': 1, 'directCount': 1},
                {'type': 'pypi', 'repositoryCount': 1, 'directCount': 0},
            ],
        }

    def test_is_sent_as_compact_json(self, snapshot):
        text = call(snapshot, 'dependents_of', '{"name": "mail", "limit": 1}')
        assert text == sent(json.loads(text))

    @pytest.mark.parametrize('spelled', ['php-composer', 'Composer', 'COMPOSER'])
    def test_ranks_an_ecosystem_however_the_model_spells_it(
        self, snapshot, spelled,
    ):
        """The ranking lowercases an ecosystem and stops there, so the
        collector's `php-composer` found nothing (#142): here each
        spelling is the page's `composer`."""
        result = called(
            snapshot, 'top_packages', json.dumps({'ecosystem': spelled}),
        )
        assert [row['name'] for row in result['rows']] == ['laravel/framework']

    def test_scopes_dependants_to_an_ecosystem_however_it_is_spelled(
        self, snapshot,
    ):
        exact = called(
            snapshot, 'dependents_of', '{"name": "mail", "type": "gem"}',
        )
        loud = called(
            snapshot, 'dependents_of', '{"name": "mail", "type": "GEM"}',
        )
        assert loud == exact
        assert exact['total'] == 3

    @pytest.mark.parametrize(
        'arguments,said',
        [
            ('{"name": "mail"', 'not JSON'),
            ('{"name": NaN}', 'not JSON'),
            ('', 'requires a package name'),
            ('["mail"]', 'not a JSON object'),
            ('{"name": "' + 'x' * 300 + '"}', 'at most 256 characters'),
        ],
    )
    def test_says_what_went_wrong_for_the_model_to_recover_from(
        self, snapshot, arguments, said,
    ):
        """Reported back, never dropped: the model can recover, and a
        call left unanswered is a malformed conversation."""
        result = called(snapshot, 'dependents_of', arguments)
        assert list(result) == ['error']
        assert said in result['error']

    def test_says_so_when_the_snapshot_cannot_answer(self, tmp_path):
        """And names nothing of why: the log does."""
        broken = tmp_path / 'broken.sqlite'
        with sqlite3.connect(broken) as db:
            db.execute('CREATE TABLE unrelated (x)')
        db.close()

        result = called(broken, 'language_coverage', '{}')

        assert result == {'error': 'The dataset could not answer this call.'}

    def test_leaves_nothing_open(self, snapshot, recwarn):
        for _ in range(3):
            call(snapshot, 'ecosystems_for', '{"name": "mail"}')
        assert not [w for w in recwarn if w.category is ResourceWarning]


# ---- the descriptions, against what the dataset does ------------------

#: Rows carrying every column any statement below reads.
PLENTY = [
    {
        'owner': f'owner-{index}',
        'repo': f'project-{index}',
        'stars': 1_000 - index,
        'url': f'https://github.com/owner-{index}/project-{index}',
        'language': 'ruby',
        'relationship': 'direct',
        'observed_on': '2026-09-13',
        'manifests': 1,
        'type': 'gem',
        'ecosystem': 'gem',
        'name': f'package-{index}',
        'version': f'1.0.{index}',
        'listed': f'1.0.{index}',
        'version_kind': 'resolved',
        'repository_count': 1_000 - index,
        'repositoryCount': 1_000 - index,
        'direct_count': 1,
        'directCount': 1,
    }
    for index in range(600)
]


class Plentiful:
    """A snapshot holding more of everything than any limit, applying a
    statement's limit as SQLite would: the placeholders before its
    first `LIMIT ?` or `rank <= ?` say which bound value it is."""

    def execute(self, sql: str, parameters: Sequence[Any], /) -> Any:
        at = re.search(r'LIMIT \?|rank <= \?', sql)
        rows = PLENTY
        if at is not None:
            rows = PLENTY[:parameters[sql[:at.start()].count('?')]]
        columns = list(PLENTY[0])
        return FakeCursor(
            columns, [tuple(row[c] for c in columns) for row in rows],
        )


class FakeCursor:
    """What the dataset reads of a cursor: the names in its description,
    and its rows."""

    def __init__(self, columns: list[str], rows: list[tuple[Any, ...]]) -> None:
        self.description = [(column,) for column in columns]
        self._rows = rows

    def fetchall(self) -> list[tuple[Any, ...]]:
        return self._rows

    def close(self) -> None:
        pass


#: How many rows the dataset gives each tool that names no limit: its
#: own default, since the tools pass none (checked below).
DATASET_DEFAULT = {
    'dependents_of': lambda dataset: len(dataset.dependents_of('react')),
    'search_packages': lambda dataset: len(dataset.search_packages('package-')),
    'top_packages': lambda dataset: len(dataset.top_packages()),
    'version_spread': lambda dataset: len(dataset.version_spread('react').versions),
}

DEFINITIONS = {tool['function']['name']: tool['function'] for tool in TOOLS}


def described_limit(name: str) -> str:
    properties = DEFINITIONS[name]['parameters']['properties']
    return str(properties.get('limit', {}).get('description', ''))


class TestTheDescriptions:
    def test_describe_each_tool_there_is_and_no_other(self):
        """A tool the loop could run and the model was never told of
        could not be called; one described and not run would fail."""
        assert [tool['function']['name'] for tool in TOOLS] == list(TOOL_NAMES)
        assert TOOL_NAMES == (
            'dependents_of', 'ecosystems_for', 'search_packages',
            'top_packages', 'version_spread', 'language_coverage',
            'ecosystem_coverage',
        )

    def test_are_function_tools_as_the_chat_completions_api_takes_them(self):
        for tool in TOOLS:
            assert tool['type'] == 'function'
            assert set(tool['function']) == {
                'name', 'description', 'parameters',
            }
            parameters = tool['function']['parameters']
            assert parameters['type'] == 'object'
            assert parameters['additionalProperties'] is False
            assert set(parameters['required']) <= set(parameters['properties'])

    def test_cover_every_tool_that_takes_a_limit(self):
        taking = [
            name for name, tool in DEFINITIONS.items()
            if 'limit' in tool['parameters']['properties']
        ]
        assert sorted(taking) == sorted(DATASET_DEFAULT)

    @pytest.mark.parametrize('name', sorted(DATASET_DEFAULT))
    def test_state_the_default_the_dataset_applies(self, name):
        stated = re.search(r'default (\d+)', described_limit(name))
        assert stated, described_limit(name)
        assert DATASET_DEFAULT[name](Dataset(Plentiful())) == int(stated[1])

    @pytest.mark.parametrize('name', sorted(DATASET_DEFAULT))
    def test_state_the_cap_the_rows_are_held_to(self, name):
        assert f'at most {RESULT_ROWS}' in described_limit(name)

    def test_hold_only_because_the_tools_pass_no_limit_of_their_own(self):
        dataset = FakeDataset()

        run(dataset, 'dependents_of', name='react')
        run(dataset, 'search_packages', fragment='react')
        run(dataset, 'top_packages')
        run(dataset, 'version_spread', name='react')

        assert dataset.asked['dependents_of'][0][1]['limit'] is None
        assert dataset.asked['search_packages'] == [(('react', None), {})]
        assert dataset.asked['top_packages'][0][1]['limit'] is None
        assert dataset.asked['version_spread'] == [(('react', None), {})]

    def test_say_search_packages_matches_a_prefix_which_is_what_it_does(
        self, snapshot,
    ):
        with open_dataset(snapshot) as dataset:
            assert [m.name for m in dataset.search_packages('ma')] == ['mail']
            assert dataset.search_packages('ail') == []
        search = DEFINITIONS['search_packages']
        assert 'prefix' in search['description']
        assert 'prefix' in search['parameters']['properties']['fragment'][
            'description'
        ]
        assert 'substring to match' not in json.dumps(search)

    def test_say_which_dependents_of_number_is_the_count(self):
        description = DEFINITIONS['dependents_of']['description']
        assert re.search(r'`total`[^.]*the real count', description)
        assert '`direct_total`' in description
        assert '`rows` is a sample' in description


class TestTheSystemPrompt:
    def test_names_every_relationship(self):
        assert ' | '.join(RELATIONSHIPS) in SYSTEM_PROMPT

    def test_states_the_bounds_a_result_is_held_to(self):
        assert f'{RESULT_ROWS} rows and {RESULT_CHARS} characters' in (
            SYSTEM_PROMPT
        )

    def test_says_earlier_answers_are_no_source_of_numbers(self):
        """They come from the page, as its reader's own record: nothing
        checks that the model wrote them."""
        said = ' '.join(SYSTEM_PROMPT.split())
        assert 'earlier questions' in said
        assert 'take no number from them' in said
