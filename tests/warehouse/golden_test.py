"""The warehouse, held to what ClickHouse answered on the same input.

Until #153 the warehouse stood beside the ClickHouse server, and was
held to it relation by relation on three inputs (#131, #147): the
corpus, its language buckets and the current facts; each repository's
releases, what its row says of them, and the ref of its current Syft
scan; and every rollup. ClickHouse's rollups were themselves held to
answers computed another way by `scripts/verify_rollups.py`. The server
is gone, and what it answered is kept (`tests/golden/
warehouse-*.json`), and the warehouse is held to that.

**How the fixtures were recorded.** On 2026-09-30, at 119be7f, against
ClickHouse 25.12.11.4 on 127.0.0.1:8123, with DuckDB 1.5.6 and Python
3.12, by the parity tests of the time turned into a recorder (kept with
#153's pull request, not in the tree). Each input went into a
`chatsbom_test_*` database of its own, made and dropped for it, as `db
index` and `db edges` left a database: the contract corpus and the
synthetic one by `seed`, the store by `db index --from-files` after each
of its two collections and `db edges` after the second. Each relation
was asked of ClickHouse and of the warehouse of the same input as
`chatsbom/warehouse/parity.py` asked it, compared as multisets, and kept
only where the two agreed, which they did, on all 22 of each input;
`verify_rollups.py` reported no failure on ClickHouse for the contract
corpus and the synthetic one. The rows are `tests/golden.py`'s
canonical JSON, sorted, and a release's assets without their download
counts, which a store's release list does not keep (#147).

What differed by design between the two engines never showed on these
inputs, and the record of it went with `parity.py`: which scan and which
graph are current, when a commit was scanned, the corpus, what history
is kept, and what an unparsable document costs.
"""
from __future__ import annotations

import json
from collections.abc import Callable
from collections.abc import Iterator
from typing import Any

import duckdb
import pytest

from chatsbom.warehouse import connect
from chatsbom.warehouse.rollups import derive
from chatsbom.warehouse.rollups import ROLLUPS
from chatsbom.warehouse.rows import load
from tests import golden
from tests.warehouse.conftest import Build
from tests.warehouse.conftest import contract_rows
from tests.warehouse.conftest import first_collection
from tests.warehouse.conftest import second_collection
from tests.warehouse.conftest import Store
from tests.warehouse.conftest import synthetic
from tests.warehouse.conftest import with_releases

Row = tuple[Any, ...]

#: How a record is asked of the warehouse, where it is not a table of
#: its name: of the corpus, and a scan's ref where ClickHouse kept it on
#: the repository's row.
RECORDS: dict[str, str] = {
    'releases': (
        'SELECT {columns} FROM releases '
        'WHERE repository_id IN (SELECT id FROM corpus)'
    ),
    'repository_releases': (
        'SELECT {columns} FROM repositories '
        'WHERE id IN (SELECT id FROM corpus)'
    ),
    'refs': (
        'SELECT repository_id, ref, ref_type, commit_sha '
        "FROM current_scans WHERE source = 'syft'"
    ),
}

#: The warehouse's own rollup, which ClickHouse never had (Q9).
OWN = frozenset({'mv_package_month_intervals'})


def without_download_counts(row: Row) -> Row:
    """A `releases` row with its assets as a store's release list keeps
    them: no download count, keys in order. `release_assets` is the
    tenth column."""
    assets = row[9]
    try:
        parsed = json.loads(assets)
    except (TypeError, ValueError):
        return row
    if isinstance(parsed, list):
        parsed = [
            {k: v for k, v in asset.items() if k != 'download_count'}
            if isinstance(asset, dict) else asset
            for asset in parsed
        ]
    return (*row[:9], json.dumps(parsed, sort_keys=True), *row[10:])


NORMAL: dict[str, Callable[[Row], Row]] = {
    'releases': without_download_counts,
}


def asked(
    warehouse: duckdb.DuckDBPyConnection,
    name: str,
    columns: list[str],
) -> list[golden.Row]:
    """A relation of the warehouse, as its fixture keeps it."""
    sql = RECORDS.get(name, 'SELECT {columns} FROM ' + name).format(
        columns=', '.join(columns),
    )
    normal = NORMAL.get(name, lambda row: row)
    return golden.multiset(
        normal(tuple(row)) for row in warehouse.execute(sql).fetchall()
    )


def held(
    warehouse: duckdb.DuckDBPyConnection,
    fixture: str,
    empty: tuple[str, ...] = (),
) -> None:
    """Every relation the fixture keeps, as the warehouse has it; and
    every one but those `empty` names has rows: not agreement on
    nothing."""
    recorded = golden.load(fixture)
    relations = recorded['relations']
    assert list(relations) == sorted(recorded['order'])
    failures = []
    for name in recorded['order']:
        kept = relations[name]
        rows = asked(warehouse, name, kept['columns'])
        if not golden.holds(name, kept, rows):
            failures.append(golden.mismatch(name, kept, rows))
        if name not in empty:
            assert kept['count'], name
    assert not failures, '\n'.join(failures)


def test_every_rollup_but_the_warehouse_s_own_is_recorded() -> None:
    """A rollup ClickHouse had and the warehouse dropped, or one the
    warehouse added without a record to hold it to, fails here."""
    for fixture in (
        'warehouse-contract.json', 'warehouse-synthetic.json',
        'warehouse-store.json',
    ):
        order = golden.load(fixture)['order']
        assert order[:6] == [
            'corpus', 'language_buckets', 'facts', 'releases',
            'repository_releases', 'refs',
        ]
        assert order[6:] == [
            name for name, _ in ROLLUPS if name not in OWN
        ], fixture


@pytest.fixture
def in_memory() -> Iterator[Callable[..., duckdb.DuckDBPyConnection]]:
    """A warehouse of rows, derived in memory, as the parity check
    made one."""
    opened: list[duckdb.DuckDBPyConnection] = []

    def make(*rows: Any, **options: Any) -> duckdb.DuckDBPyConnection:
        con = connect(':memory:')
        opened.append(con)
        load(con, *rows, **options)
        derive(con)
        return con

    yield make
    for con in opened:
        con.close()


def test_the_contract_corpus(
    in_memory: Callable[..., duckdb.DuckDBPyConnection],
) -> None:
    repositories, artifacts, edges = contract_rows()
    warehouse = in_memory(repositories, artifacts, edges, corpus=None)
    # The contract seeds no release.
    held(warehouse, 'warehouse-contract.json', empty=('releases',))


def test_a_synthetic_corpus(
    in_memory: Callable[..., duckdb.DuckDBPyConnection],
) -> None:
    repositories, artifacts, edges, corpus = synthetic()
    releases = with_releases(repositories)
    warehouse = in_memory(
        repositories, artifacts, edges, corpus=corpus, releases=releases,
    )
    held(warehouse, 'warehouse-synthetic.json')
    (buckets,), = warehouse.execute(
        "SELECT count(*) FROM mv_language_coverage WHERE language = 'other'",
    ).fetchall()
    assert buckets == 1


@pytest.fixture
def collected(store: Store) -> Store:
    """The store as its two collections left it."""
    first_collection(store)
    second_collection(store)
    return store


def test_a_store(collected: Store, built: Build) -> None:
    held(built(), 'warehouse-store.json')


def test_the_store_has_what_the_record_needs(
    collected: Store, built: Build,
) -> None:
    """The store case is not agreement on nothing: history, a dependency
    that went, every source, and a repository outside the corpus."""
    warehouse = built()
    assert warehouse.execute(
        'SELECT source, count(*) FROM scans GROUP BY source ORDER BY source',
    ).fetchall() == [('github-depgraph', 3), ('manifest', 7), ('syft', 7)]
    assert warehouse.execute(
        "SELECT count(*) FROM facts WHERE source = 'manifest'",
    ).fetchone() == (4,)
    assert warehouse.execute(
        "SELECT name FROM mv_package_month WHERE month = '2026-01' "
        'ORDER BY name',
    ).fetchall() == [('js-tokens',), ('lodash',), ('react',)]


class TestTheFixturesHold:
    """What the fixtures catch: a mistake the parity check caught, made
    again, fails here too."""

    def test_the_oldest_scan_taken_as_current(
        self,
        in_memory: Callable[..., duckdb.DuckDBPyConnection],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        from chatsbom.warehouse import rollups

        monkeypatch.setattr(
            rollups, 'CURRENT_SCANS',
            rollups.CURRENT_SCANS.replace('observed_at DESC', 'observed_at'),
        )
        monkeypatch.setattr(
            rollups, 'CURRENT', tuple(
                (
                    name, rollups.CURRENT_SCANS if name == 'current_scans'
                    else sql,
                )
                for name, sql in rollups.CURRENT
            ),
        )
        repositories, artifacts, edges, corpus = synthetic()
        warehouse = in_memory(
            repositories, artifacts, edges, corpus=corpus,
            releases=with_releases(repositories),
        )
        with pytest.raises(AssertionError, match='facts'):
            held(warehouse, 'warehouse-synthetic.json')

    def test_months_made_in_another_zone(
        self,
        in_memory: Callable[..., duckdb.DuckDBPyConnection],
    ) -> None:
        repositories, artifacts, edges, corpus = synthetic()
        warehouse = in_memory(
            repositories, artifacts, edges, corpus=corpus,
            releases=with_releases(repositories),
        )
        # The months of UTC+8, as a warehouse made them before every
        # connection worked in UTC (#120).
        warehouse.execute('DROP TABLE mv_package_month')
        shifted = dict(ROLLUPS)['mv_package_month'].replace(
            'strftime(s.observed_at,',
            'strftime(s.observed_at + INTERVAL 8 HOUR,',
        )
        assert shifted != dict(ROLLUPS)['mv_package_month']
        warehouse.execute(shifted)
        with pytest.raises(AssertionError, match='mv_package_month'):
            held(warehouse, 'warehouse-synthetic.json')
