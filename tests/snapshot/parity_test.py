"""A snapshot answers the contract suite as D1 does (#132).

The seed the contract suite and `d1.sql` were recorded from
(`tests/contract_seed.py`) goes into a warehouse as the warehouse's
golden test loads it, and a snapshot is written from that warehouse.
Then:

- every call the contract suite made of D1, recorded with D1's answer
  in `calls.json`, is asked of `Dataset` over the snapshot, and has to
  come back as the same JSON;
- the snapshot's tables are `d1.sql`'s, row for row and id for id, and
  its page table the one made of `d1.sql`'s by the same statement;
- and a snapshot of a synthetic corpus has the rows `export d1` of it
  had, as recorded from ClickHouse before it was deleted (#153).

Where a snapshot is not `export d1` by design, the difference is named
here, said why, and held exactly: each is a function that makes D1's
rows or answer into the snapshot's, and nothing else is let through.
"""
from __future__ import annotations

import json
from collections.abc import Callable
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path
from typing import Any

import duckdb
import pytest

from chatsbom.__version__ import __version__
from chatsbom.dataset import Dataset
from chatsbom.dataset import jsonable
from chatsbom.dataset import open_dataset
from chatsbom.dataset.open import connect
from chatsbom.export.d1 import D1_SCHEMA
from chatsbom.snapshot.schema import DEPENDANTS
from chatsbom.snapshot.write import write
from tests import golden
from tests.dataset_contract_test import ask
from tests.dataset_contract_test import CALLS
from tests.dataset_contract_test import corpus
from tests.dataset_contract_test import label
from tests.snapshot.conftest import contract_corpus
from tests.snapshot.conftest import Corpus
from tests.snapshot.conftest import warehouse
from tests.warehouse.conftest import synthetic

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
    """A table's rows, in the order they were written, by the columns
    D1 declares: a page table's are its key's."""
    order = 'rowid' if table != DEPENDANTS.name else DEPENDANTS.primary_key
    with closing(connect(path)) as connection:
        return connection.execute(
            f"SELECT {', '.join(columns)} FROM {table} ORDER BY {order}",
        ).fetchall()


Explain = Callable[[Rows], Rows]


def compare(
    exported: Path,
    snapshot: Path,
    explained: dict[str, Explain],
) -> dict[str, int]:
    """Every table of D1's schema, `export d1`'s against the snapshot's,
    row for row in the order each was written, after what `explained`
    makes of D1's rows; and the page table, which each has made of its
    own tables by the one statement. The rows of each table."""
    compared = {}
    for table in (*D1_SCHEMA.tables, DEPENDANTS):
        columns = table.column_names
        theirs = contents(exported, table.name, columns)
        expected = explained.get(table.name, lambda same: same)(theirs)
        ours = contents(snapshot, table.name, columns)
        assert ours == expected, table.name
        compared[table.name] = len(ours)
    return compared


def dated(ids: dict[int, str]) -> Explain:
    """`repositories.observed_at` of a repository with no dependency is
    the day `db index` wrote its row in `export d1`, which says when the
    indexer ran and not when anything was seen, and which the warehouse
    does not keep. A snapshot dates it by its newest current scan, or
    leaves it empty when there is none. Neither is served: no answer
    reads the date of a repository with no dependency."""
    column = D1_SCHEMA.table('repositories').column_names.index('observed_at')

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
        """`d1.sql` is `export d1` of the same seed. golang/tools was
        never scanned, and `db index` wrote its row on 11 February."""
        d1 = corpus(tmp_path)
        columns = D1_SCHEMA.table('repositories').column_names
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
            table.name for table in (*D1_SCHEMA.tables, DEPENDANTS)
        }


# -- against `export d1`, as it was recorded -------------------------------


def months(path: Path, relation: str) -> Rows:
    """A warehouse's monthly series, as D1's `history` holds it: by name,
    source and month, of every named package."""
    with duckdb.connect(str(path), read_only=True) as con:
        return con.execute(
            'SELECT name, month, source, repositories, direct_repositories '
            f"FROM {relation} WHERE name != '' ORDER BY name, source, month",
        ).fetchall()


class TestAgainstExportD1:
    """`export d1` of a synthetic corpus: every source, history, repeats,
    names across ecosystems, more than twelve languages, and repositories
    outside the corpus (`tests/warehouse/conftest.py`).

    As ClickHouse made it, kept in `tests/golden/
    snapshot-synthetic.json`. Recorded on 2026-09-30 at 119be7f from
    ClickHouse 25.12.11.4 on 127.0.0.1:8123, with DuckDB 1.5.6 and
    Python 3.12, by this test as it stood then, turned into a recorder:
    the corpus into a `chatsbom_test_*` database by `seed`, `export d1`
    of it applied to SQLite as D1 applies it, with the page table made
    of its rows, and each table read in the order written. It was kept
    after a snapshot of the same rows agreed with it as below. The date
    `export d1` gave a repository with no dependency, the day `db index`
    wrote its row, is not kept: it said when the recording ran.

    Of the contract seed, `export d1` is `d1.sql`, which `TestTheTables`
    holds a snapshot to.
    """

    def test_of_a_synthetic_corpus(self, tmp_path: Path) -> None:
        repositories, artifacts, edges, ids = synthetic()
        store = warehouse(
            tmp_path / 'warehouse.duckdb',
            Corpus(
                repositories=repositories, artifacts=artifacts, edges=edges,
                corpus=ids,
            ),
        )
        snapshot = write(store, tmp_path / 'snapshots').path
        recorded = golden.load('snapshot-synthetic.json')['tables']

        # The adoption series: `export d1`'s counted a repository in the
        # months of its scans, the warehouse's `mv_package_month`; the
        # snapshot's in every month between two scans that both show the
        # package (Q9). Here they differ.
        scans = months(store, 'mv_package_month')
        intervals = months(store, 'mv_package_month_intervals')
        assert scans != intervals
        assert golden.holds(
            'history', recorded['history'], golden.ordered(scans),
        )

        columns = D1_SCHEMA.table('repositories').column_names
        undependent = columns.index('total_dependencies')
        never_scanned = {
            row[0] for row in recorded['repositories']['rows']
            if not row[undependent]
        }
        assert never_scanned
        explained: dict[str, Explain] = {
            'history': lambda rows: intervals,
            'repositories': dated(dict.fromkeys(never_scanned, '')),
            'meta': generator,
        }
        compared = {}
        for table in (*D1_SCHEMA.tables, DEPENDANTS):
            kept = recorded[table.name]
            assert kept['columns'] == table.column_names, table.name
            ours = golden.ordered(
                contents(snapshot, table.name, table.column_names),
            )
            if table.name in explained:
                theirs = [tuple(row) for row in kept.get('rows', [])]
                expected = golden.ordered(explained[table.name](theirs))
                assert ours == expected, table.name
            else:
                assert golden.holds(table.name, kept, ours), (
                    golden.mismatch(table.name, kept, ours)
                )
            compared[table.name] = len(ours)
        assert compared['artifacts'] > 1000
        # Not agreement on nothing: every table has rows.
        assert all(compared.values()), compared
