"""A snapshot answers the contract suite as D1 does (#132).

The seed the contract suite and `d1.sql` are made from
(`web/test/fixtures/contract/build.py`) goes into a warehouse as the
warehouse's own parity check loads it, and a snapshot is written from
that warehouse. Then:

- every call the contract suite made of D1, recorded with D1's answer
  in `calls.json`, is asked of `Dataset` over the snapshot, and has to
  come back as the same JSON;
- the snapshot's tables are `d1.sql`'s, row for row and id for id;
- and, with a ClickHouse server, `export d1` of the same seed and of a
  synthetic corpus, made now, has the snapshot's rows too.

Where a snapshot is not `export d1` by design, the difference is named
here, said why, and held exactly: each is a function that makes D1's
rows or answer into the snapshot's, and nothing else is let through.
"""
from __future__ import annotations

import json
import sqlite3
from collections.abc import Callable
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path
from typing import Any

import duckdb
import pytest

from chatsbom.__version__ import __version__
from chatsbom.core.config import DatabaseConfig
from chatsbom.core.repository import QueryRepository
from chatsbom.dataset import Dataset
from chatsbom.dataset import jsonable
from chatsbom.dataset import open_dataset
from chatsbom.dataset.open import connect
from chatsbom.export.d1 import D1_SCHEMA
from chatsbom.export.d1 import export_d1
from chatsbom.snapshot.write import write
from tests.conftest import CLICKHOUSE_HOST
from tests.conftest import CLICKHOUSE_PASSWORD
from tests.conftest import CLICKHOUSE_PORT
from tests.conftest import CLICKHOUSE_USER
from tests.conftest import requires_clickhouse
from tests.dataset_contract_test import ask
from tests.dataset_contract_test import CALLS
from tests.dataset_contract_test import corpus
from tests.dataset_contract_test import label
from tests.snapshot.conftest import contract
from tests.snapshot.conftest import contract_corpus
from tests.snapshot.conftest import Corpus
from tests.snapshot.conftest import warehouse
from tests.warehouse.parity_test import seed as seed_clickhouse
from tests.warehouse.parity_test import synthetic

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
    D1 declares."""
    with closing(connect(path)) as connection:
        return connection.execute(
            f"SELECT {', '.join(columns)} FROM {table} ORDER BY rowid",
        ).fetchall()


Explain = Callable[[Rows], Rows]


def compare(
    exported: Path,
    snapshot: Path,
    explained: dict[str, Explain],
) -> dict[str, int]:
    """Every table of D1's schema, `export d1`'s against the snapshot's,
    row for row in the order each was written, after what `explained`
    makes of D1's rows; the rows of each table."""
    compared = {}
    for table in D1_SCHEMA.tables:
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
        # Not agreement on nothing: every table but the empty ones.
        assert {name for name, count in compared.items() if count} == {
            table.name for table in D1_SCHEMA.tables
        }


# -- against `export d1`, made now ----------------------------------------


def config(database: str) -> DatabaseConfig:
    return DatabaseConfig(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD, database=database,
    )


def exported(database: str, directory: Path) -> Path:
    """`export d1` of `database`, its scripts applied to a SQLite file in
    the order of their names, as D1 applies them."""
    with QueryRepository(config(database)) as query:
        result = export_d1(query, directory)
    path = directory / 'd1.sqlite'
    with closing(sqlite3.connect(path)) as connection:
        for name in sorted(result.files):
            connection.executescript(
                (directory / name).read_text(encoding='utf-8'),
            )
        connection.commit()
    return path


def indexed_on(database: str, ids: set[int]) -> dict[int, str]:
    """The day `db index` wrote each repository's row, as `export d1`
    dates it."""
    with QueryRepository(config(database)) as query:
        return {
            int(row['id']): str(row['day'])
            for row in query.stream_rows(
                "SELECT id, formatDateTime(updated_at, '%Y-%m-%d', 'UTC') "
                'AS day FROM repositories FINAL',
            )
            if int(row['id']) in ids
        }


def undependent(path: Path) -> set[int]:
    """The repositories of a D1 file with no dependency."""
    with closing(connect(path)) as connection:
        return {
            int(id) for (id,) in connection.execute(
                'SELECT id FROM repositories WHERE total_dependencies = 0',
            ).fetchall()
        }


def months(path: Path, relation: str) -> Rows:
    """A warehouse's monthly series, as D1's `history` holds it: by name,
    source and month, of every named package."""
    with duckdb.connect(str(path), read_only=True) as con:
        return con.execute(
            'SELECT name, month, source, repositories, direct_repositories '
            f"FROM {relation} WHERE name != '' ORDER BY name, source, month",
        ).fetchall()


@requires_clickhouse
class TestAgainstExportD1:

    def test_of_the_contract_seed(
        self, clickhouse_db: str, tmp_path: Path,
    ) -> None:
        contract().seed(clickhouse_db)
        d1 = exported(clickhouse_db, tmp_path / 'd1')
        snapshot = write(
            warehouse(tmp_path / 'warehouse.duckdb', contract_corpus()),
            tmp_path / 'snapshots',
        ).path
        compare(
            d1, snapshot,
            {'repositories': dated({12: ''}), 'meta': generator},
        )

    def test_of_a_synthetic_corpus(
        self, clickhouse_db: str, tmp_path: Path,
    ) -> None:
        """Every source, history, repeats, names across ecosystems, more
        than twelve languages, and repositories outside the corpus."""
        repositories, artifacts, edges, ids = synthetic()
        seed_clickhouse(clickhouse_db, repositories, artifacts, edges)
        d1 = exported(clickhouse_db, tmp_path / 'd1')
        store = warehouse(
            tmp_path / 'warehouse.duckdb',
            Corpus(
                repositories=repositories, artifacts=artifacts, edges=edges,
                corpus=ids,
            ),
        )
        snapshot = write(store, tmp_path / 'snapshots').path

        # The adoption series: `export d1`'s counts a repository in the
        # months of its scans, the snapshot's in every month between two
        # scans that both show the package (Q9). Each is the warehouse's
        # relation of that name, which `tests/warehouse/` holds to
        # ClickHouse and to the rule; here they differ.
        scans = months(store, 'mv_package_month')
        intervals = months(store, 'mv_package_month_intervals')
        assert scans != intervals
        assert contents(
            d1, 'history', D1_SCHEMA.table('history').column_names,
        ) == scans

        never_scanned = undependent(d1)
        assert never_scanned
        compared = compare(
            d1, snapshot, {
                'history': lambda rows: intervals,
                'repositories': dated(dict.fromkeys(never_scanned, '')),
                'meta': generator,
            },
        )
        assert compared['artifacts'] > 1000
        # And `export d1` dates those by when their rows were written.
        written = indexed_on(clickhouse_db, never_scanned)
        column = D1_SCHEMA.table('repositories').column_names.index(
            'observed_at',
        )
        assert {
            row[0]: row[column] for row in contents(
                d1, 'repositories',
                D1_SCHEMA.table('repositories').column_names,
            ) if row[0] in never_scanned
        } == written
