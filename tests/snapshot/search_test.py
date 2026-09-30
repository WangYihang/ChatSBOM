"""The search box reads a range of an index, not every name (#165).

`search_packages` asks for the names that begin with a term, `name LIKE
'term%'`, which SQLite matches without regard to case (ASCII's), as D1
did. So `idx_packages_name`, which orders names as bytes, cannot serve
it, and every keystroke read every name. A snapshot has a second index
on the names, in SQLite's NOCASE order, the one its LIKE matches in, and
the same statement becomes a range of it.

The answers must not change: without regard to case, `%` and `_` a
reader typed taken as themselves, and ranked as before. So each is
asked of a snapshot written as `snapshot build` writes one, and of a
copy without that index, which answers as every snapshot did before it.
"""
from __future__ import annotations

import shutil
import sqlite3
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest

from chatsbom.dataset import Dataset
from chatsbom.dataset.open import connect
from chatsbom.dataset.types import PackageMatch
from chatsbom.snapshot.write import write
from tests.snapshot.conftest import artifact
from tests.snapshot.conftest import Corpus
from tests.snapshot.conftest import FEB
from tests.snapshot.conftest import repository
from tests.snapshot.conftest import warehouse

#: The index the search reads.
NOCASE = 'idx_packages_name_nocase'

#: Each name, and the repositories that depend on it: names that differ
#: in case alone, that hold a `%` or a `_`, or begin with one, or with
#: what sorts between `Z` and `a`, and letters past ASCII, which SQLite
#: does not fold.
NAMES: dict[str, tuple[int, ...]] = {
    'rack': (1, 2, 3),
    'Rack': (1, 2),
    'RACK_ENV': (1,),
    'rack-test': (2,),
    'rackup': (3,),
    'ra%ck': (1,),
    'ra_ck': (2,),
    'raXck': (3,),
    'r\\ack': (1,),
    '\\back': (2,),
    '%percent': (2,),
    '_under': (3,),
    '`tick': (1,),
    '^caret': (2,),
    '[bracket': (3,),
    'Ärger': (1, 2),
    'ärger': (3,),
    'Zeta': (1,),
    'zeta': (2, 3),
}

#: What a reader might type: each name's start in each case, the
#: wildcards alone and inside a term, the escape, and the characters at
#: the edges of what NOCASE folds.
TERMS = (
    'r', 'R', 'ra', 'RA', 'Ra', 'rA', 'rack', 'RACK', 'Rack', 'RACK_',
    'rack_', 'ra%', 'ra_', 'r\\', '%', '_', '\\', '`', '^', '[', 'ä', 'Ä',
    'z', 'Z', 'zeta', 'ZETA', 'x', '-',
)


def search_corpus() -> Corpus:
    """Three repositories of one scan each, depending on `NAMES`."""
    return Corpus(
        repositories=[
            repository(id, 'acme', f'app{id}', 100 * id, 'Ruby')
            for id in (1, 2, 3)
        ],
        artifacts=[
            artifact(
                id, name, '1.0.0', 'gem', observed_at=FEB, commit=f'c{id}',
            )
            for name, ids in NAMES.items() for id in ids
        ],
    )


@pytest.fixture(scope='module')
def snapshot(tmp_path_factory: pytest.TempPathFactory) -> Path:
    directory = tmp_path_factory.mktemp('search')
    return write(
        warehouse(directory / 'warehouse.duckdb', search_corpus()),
        directory / 'snapshots',
    ).path


@pytest.fixture(scope='module')
def unindexed(
    snapshot: Path, tmp_path_factory: pytest.TempPathFactory,
) -> Path:
    """The same snapshot without the index, as every snapshot was before
    it: the answers it gives are the ones to keep."""
    copy = tmp_path_factory.mktemp('unindexed') / 'snapshot.sqlite'
    shutil.copyfile(snapshot, copy)
    copy.chmod(0o644)
    with closing(sqlite3.connect(copy)) as connection:
        connection.execute(f'DROP INDEX IF EXISTS {NOCASE}')
        connection.commit()
    return copy


class Planned:
    """A snapshot's connection that keeps the plan of each statement it
    is asked, and answers it."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection
        #: Each statement's plan, a step a line.
        self.plans: list[list[str]] = []

    def execute(self, sql: str, parameters: Any, /) -> sqlite3.Cursor:
        self.plans.append([
            str(step[-1]) for step in self.connection.execute(
                f'EXPLAIN QUERY PLAN {sql}', parameters,
            ).fetchall()
        ])
        return self.connection.execute(sql, parameters)


@pytest.fixture
def planned(snapshot: Path) -> Iterator[Planned]:
    with closing(connect(snapshot)) as connection:
        yield Planned(connection)


def names(path: Path, term: str) -> list[tuple[str, int]]:
    """The search's answer for `term`: each name and its count."""
    with closing(connect(path)) as connection:
        return [
            (match.name, match.repository_count)
            for match in Dataset(connection).search_packages(term, 500)
        ]


class TestThePlan:

    @pytest.mark.parametrize('term', ['ra', 'RA', '%', '_', '\\', 'Ä'])
    def test_is_a_range_of_the_nocase_index(
        self, planned: Planned, term: str,
    ) -> None:
        Dataset(planned).search_packages(term)
        [steps] = planned.plans
        assert f'SEARCH p USING INDEX {NOCASE} (name>? AND name<?)' in steps
        assert 'SCAN p' not in steps

    def test_was_a_scan_of_every_name_without_it(
        self, unindexed: Path,
    ) -> None:
        with closing(connect(unindexed)) as connection:
            planned = Planned(connection)
            Dataset(planned).search_packages('ra')
        assert 'SCAN p' in planned.plans[0]

    def test_the_index_is_the_names_in_nocase_order(
        self, snapshot: Path,
    ) -> None:
        with closing(connect(snapshot)) as connection:
            [(ddl,)] = connection.execute(
                'SELECT sql FROM sqlite_master WHERE name = ?', [NOCASE],
            ).fetchall()
        assert ddl.endswith('packages(name COLLATE NOCASE)')


class TestTheAnswers:

    @pytest.mark.parametrize('term', TERMS)
    def test_are_what_they_were(
        self, snapshot: Path, unindexed: Path, term: str,
    ) -> None:
        assert names(snapshot, term) == names(unindexed, term)

    def test_are_not_vacuous(self, snapshot: Path) -> None:
        """Each case the comparison covers has something in it."""
        assert all(names(snapshot, term) for term in TERMS[:-2])
        assert names(snapshot, 'x') == names(snapshot, '-') == []

    def test_ignore_case_and_rank_by_repositories(
        self, snapshot: Path,
    ) -> None:
        # Most depended upon first, then by name as bytes: `R` before
        # `r`, and `%` and `X` before `_`, which is before `c`.
        ranked = [
            ('rack', 3), ('Rack', 2), ('RACK_ENV', 1), ('ra%ck', 1),
            ('raXck', 1), ('ra_ck', 1), ('rack-test', 1), ('rackup', 1),
        ]
        for term in ('ra', 'RA', 'Ra', 'rA'):
            assert names(snapshot, term) == ranked, term

    def test_take_a_wildcard_as_itself(self, snapshot: Path) -> None:
        assert names(snapshot, 'ra%') == [('ra%ck', 1)]
        assert names(snapshot, 'ra_') == [('ra_ck', 1)]
        assert names(snapshot, 'RACK_') == [('RACK_ENV', 1)]
        assert names(snapshot, '%') == [('%percent', 1)]
        assert names(snapshot, '_') == [('_under', 1)]
        assert names(snapshot, 'r\\') == [('r\\ack', 1)]
        assert names(snapshot, '\\') == [('\\back', 1)]

    def test_fold_ascii_alone(self, snapshot: Path) -> None:
        """As SQLite's LIKE does, and D1's did: `Ä` is not `ä`."""
        assert names(snapshot, 'Ä') == [('Ärger', 2)]
        assert names(snapshot, 'ä') == [('ärger', 1)]

    def test_are_the_answers_the_page_reads(self, snapshot: Path) -> None:
        with closing(connect(snapshot)) as connection:
            [match] = Dataset(connection).search_packages('zet', 1)
        assert match == PackageMatch(
            name='zeta', ecosystem=None, repository_count=2, name_total=2,
        )
