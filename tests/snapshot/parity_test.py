"""A snapshot answers the contract suite as D1 did (#132).

The seed the contract suite and `d1.sql` are made from
(`web/test/fixtures/contract/build.py`) goes into a warehouse as the
warehouse's own parity check loads it, and a snapshot is written from
that warehouse. Then:

- every call the contract suite made of D1, recorded with D1's answer
  in `calls.json`, is asked of `Dataset` over the snapshot, and has to
  come back as the same JSON;
- and the snapshot's tables are `d1.sql`'s, row for row and id for id,
  and its page table the one made of `d1.sql`'s by the same statement.

Where a snapshot is not D1 by design, the difference is named here,
said why, and held exactly: each is a function that makes D1's rows or
answer into the snapshot's, and nothing else is let through.
"""
from __future__ import annotations

import json
from collections.abc import Callable
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest

from chatsbom.__version__ import __version__
from chatsbom.dataset import Dataset
from chatsbom.dataset import jsonable
from chatsbom.dataset import open_dataset
from chatsbom.dataset.open import connect
from chatsbom.snapshot.schema import DEPENDANTS
from chatsbom.snapshot.schema import META
from chatsbom.snapshot.schema import REPOSITORIES
from chatsbom.snapshot.schema import SCHEMA
from chatsbom.snapshot.write import write
from tests.dataset_contract_test import ask
from tests.dataset_contract_test import CALLS
from tests.dataset_contract_test import corpus
from tests.dataset_contract_test import label
from tests.snapshot.conftest import contract_corpus
from tests.snapshot.conftest import warehouse

Row = tuple[Any, ...]
Rows = list[Row]


@pytest.fixture(scope='module')
def snapshot(tmp_path_factory: pytest.TempPathFactory) -> Path:
    directory = tmp_path_factory.mktemp('contract')
    return write(
        warehouse(directory / 'warehouse.duckdb', contract_corpus()),
        directory / 'snapshots',
    ).path


@pytest.fixture(scope='module')
def dataset(snapshot: Path) -> Iterator[Dataset]:
    with open_dataset(snapshot) as opened:
        yield opened


# -- the answers ----------------------------------------------------------


def built_by_this_code(answer: Any) -> Any:
    """`meta().generator` is the build that wrote the file: `d1.sql`'s is
    the code that exported it, the snapshot's the code that wrote it.
    Equal while both are this version; the recording is not made again
    for a new one, and the snapshot is."""
    return {**answer, 'generator': f'chatsbom/{__version__}'}


#: The recorded calls whose answer a snapshot gives otherwise, by
#: method, and how. Every other answer is D1's as it was recorded.
EXPLAINED: dict[str, Callable[[Any], Any]] = {
    'meta': built_by_this_code,
}


class TestTheRecordedCalls:

    @pytest.mark.parametrize('call', CALLS, ids=label)
    def test_are_answered_as_d1_answered_them(
        self, dataset: Dataset, call: dict[str, Any],
    ) -> None:
        expected = EXPLAINED.get(call['method'], lambda same: same)(
            call['returns'],
        )
        answer = jsonable(ask(dataset, call['method'], call['params']))
        assert answer == expected
        assert json.dumps(answer, sort_keys=True) == json.dumps(
            expected, sort_keys=True,
        )

    def test_every_explained_method_is_asked(self) -> None:
        assert set(EXPLAINED) <= {call['method'] for call in CALLS}


# -- the tables -----------------------------------------------------------


def contents(path: Path, table: str, columns: list[str]) -> Rows:
    """A table's `columns`, its rows in the order they were written, a
    page table's in its key's."""
    order = 'rowid' if table != DEPENDANTS.name else DEPENDANTS.primary_key
    with closing(connect(path)) as connection:
        return connection.execute(
            f"SELECT {', '.join(columns)} FROM {table} ORDER BY {order}",
        ).fetchall()


Explain = Callable[[Rows], Rows]

#: The columns of `meta` D1's had. A snapshot's says more of itself: its
#: id, the version, the corpus and the rows, none of which `d1.sql` has.
D1_META = META.column_names[:4]


def compare(
    d1: Path,
    snapshot: Path,
    explained: dict[str, Explain],
) -> dict[str, int]:
    """Every table, D1's against the snapshot's, row for row in the
    order each was written, after what `explained` makes of D1's rows:
    the page table, which each has made of its own tables by the one
    statement, and of `meta` the columns D1's had. The rows of each
    table."""
    compared = {}
    for table in SCHEMA.tables:
        columns = D1_META if table is META else table.column_names
        theirs = contents(d1, table.name, columns)
        expected = explained.get(table.name, lambda same: same)(theirs)
        ours = contents(snapshot, table.name, columns)
        assert ours == expected, table.name
        compared[table.name] = len(ours)
    return compared


def dated(ids: dict[int, str]) -> Explain:
    """`repositories.observed_at` of a repository with no dependency is
    the day `db index` wrote its row in D1's, which says when the indexer
    ran and not when anything was seen, and which the warehouse does not
    keep. A snapshot dates it by its newest current scan, or leaves it
    empty when there is none. Neither is served: no answer reads the
    date of a repository with no dependency."""
    column = REPOSITORIES.column_names.index('observed_at')

    def explain(rows: Rows) -> Rows:
        return [
            (*row[:column], ids[row[0]], *row[column + 1:])
            if row[0] in ids else row
            for row in rows
        ]

    return explain


def generator(rows: Rows) -> Rows:
    """`meta.generator`, as `built_by_this_code` says."""
    return [(f'chatsbom/{__version__}', *row[1:]) for row in rows]


class TestTheTables:

    def test_are_d1_sql_s(self, snapshot: Path, tmp_path: Path) -> None:
        """`d1.sql` is D1's export of the same seed. golang/tools was
        never scanned, and `db index` wrote its row on 11 February."""
        d1 = corpus(tmp_path)
        columns = REPOSITORIES.column_names
        [tools] = [
            row for row in contents(d1, 'repositories', columns)
            if row[0] == 12
        ]
        assert tools[columns.index('observed_at')] == '2026-02-11'

        compared = compare(
            d1, snapshot,
            {'repositories': dated({12: ''}), 'meta': generator},
        )
        # Not agreement on nothing: every table has rows.
        assert {name for name, count in compared.items() if count} == {
            table.name for table in SCHEMA.tables
        }
