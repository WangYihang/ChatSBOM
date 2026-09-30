"""The Parquet export from the warehouse is `export parquet`'s (#148).

Each input goes into a `chatsbom_test_*` database of its own, which
`clickhouse_db` makes and drops, and into a warehouse, and both are
exported: `export parquet` as it runs, from ClickHouse, and `export
parquet --from warehouse`. Three inputs:

- the seed the contract suite and `d1.sql` are made from
  (`web/test/fixtures/contract/build.py`), each of whose rows is a case
  two readers of this data have disagreed on;
- a synthetic corpus of 300 repositories: every source, history,
  repeats, names across ecosystems, more than twelve languages, and
  repositories outside the corpus (`tests/warehouse/parity_test.py`);
- a store on disk, indexed by `db index --from-files` after each of two
  collections, against `warehouse build` of the store after both: the
  parsers' own rows, with the manifests read for each scan's verdicts.

Every table is compared row by row, in the order each export wrote it,
and file by file. Where the export from the warehouse is not `export
parquet` by design, the difference is named here, said why, and held
exactly: a function makes ClickHouse's rows into the warehouse's, and
nothing else is let through. The same rows are the same bytes: a table
whose rows agree is the same file, under the same content-addressed
name, and one that differs by design is the file its explained rows
make when written by the same writer.
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from collections.abc import Iterator
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

import duckdb
import pytest

from chatsbom.core.config import DatabaseConfig
from chatsbom.core.repository import QueryRepository
from chatsbom.export.parquet import _arrow_schema
from chatsbom.export.parquet import _write_rows
from chatsbom.export.parquet import export_dataset
from chatsbom.export.parquet import export_warehouse
from chatsbom.export.parquet import ExportResult
from chatsbom.export.parquet import MANIFEST_NAME
from chatsbom.export.queries import repository_freshness
from chatsbom.export.schema import EXPORT_SCHEMA
from chatsbom.warehouse.build import build
from tests.conftest import CLICKHOUSE_HOST
from tests.conftest import CLICKHOUSE_PASSWORD
from tests.conftest import CLICKHOUSE_PORT
from tests.conftest import CLICKHOUSE_USER
from tests.conftest import requires_clickhouse
from tests.snapshot.conftest import contract
from tests.snapshot.conftest import contract_corpus
from tests.snapshot.conftest import Corpus
from tests.snapshot.conftest import warehouse
from tests.warehouse.conftest import Store
from tests.warehouse.conftest import TODAY
from tests.warehouse.parity_test import A
from tests.warehouse.parity_test import first_collection
from tests.warehouse.parity_test import second_collection
from tests.warehouse.parity_test import seed as seed_clickhouse
from tests.warehouse.parity_test import synthetic

pytestmark = requires_clickhouse

pa = pytest.importorskip('pyarrow')
pq = pytest.importorskip('pyarrow.parquet')

Rows = list[dict[str, Any]]
Explain = Callable[[Rows], Rows]


def config(database: str) -> DatabaseConfig:
    return DatabaseConfig(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD, database=database,
    )


def from_clickhouse(database: str, directory: Path) -> ExportResult:
    """`export parquet` of `database`, as it runs."""
    with QueryRepository(config(database)) as query:
        return export_dataset(query, directory)


def rows(result: ExportResult, table: str) -> Rows:
    """A table's rows, in the order the export wrote them."""
    read: Rows = pq.read_table(
        result.directory / result.files[table],
    ).to_pylist()
    return read


def manifest(result: ExportResult) -> dict[str, Any]:
    said: dict[str, Any] = json.loads(
        (result.directory / MANIFEST_NAME).read_text(encoding='utf-8'),
    )
    return said


def written(table: str, content: Rows, directory: Path) -> str:
    """The digest of the file `content` makes, written as the export
    writes a table."""
    declared = _arrow_schema(EXPORT_SCHEMA.table(table))
    path = directory / f'{table}.parquet'
    _write_rows(
        [pa.RecordBatch.from_pylist(content, schema=declared)], path,
        declared,
    )
    return hashlib.sha256(path.read_bytes()).hexdigest()


def compare(
    clickhouse: ExportResult,
    ours: ExportResult,
    explained: dict[str, Explain],
    scratch: Path,
) -> dict[str, int]:
    """Every table: ClickHouse's rows, after what `explained` makes of
    them, against the warehouse's, row for row in the order written;
    each file the one those rows make; and the manifests, but for which
    files they name. The rows of each table."""
    compared = {}
    scratch.mkdir()
    for table in EXPORT_SCHEMA.tables:
        name = table.name
        expected = explained.get(name, lambda same: same)(
            rows(clickhouse, name),
        )
        assert rows(ours, name) == expected, name
        file = ours.files[name]
        if name in explained:
            assert written(name, expected, scratch) == ours.checksums[file]
        else:
            # Not only the same rows: the same bytes, and name.
            assert file == clickhouse.files[name], name
            assert ours.checksums[file] == clickhouse.checksums[file]
        compared[name] = len(expected)

    theirs, said = manifest(clickhouse), manifest(ours)
    assert said['rowCounts'] == compared
    for key in ('schemaVersion', 'generator'):
        assert said[key] == theirs[key], key
    assert said['schema'] == EXPORT_SCHEMA.to_dict(files=ours.files)
    # The span of the rows compared, as ClickHouse's is of its own.
    assert said['freshness'] == repository_freshness(
        explained.get('repositories', lambda same: same)(
            rows(clickhouse, 'repositories'),
        ),
    )
    return compared


# -- what differs by design -------------------------------------------------


def dated(dates: dict[int, str]) -> Explain:
    """`observed_at` of the repositories in `dates`, as the warehouse
    dates them.

    One with no dependency: `export parquet` has the day `db index` wrote
    its row, which says when the indexer ran rather than when anything
    was seen, and the warehouse keeps no such day. The export from it
    dates the repository by its newest current scan, or not at all when
    it has none, as the snapshot does. Neither is in the manifest's span
    (`repository_freshness`).

    One whose manifests the store had the day before Syft made the
    commit's document: see `scanned`."""
    def explain(content: Rows) -> Rows:
        return [
            {**row, 'observed_at': dates[row['id']]}
            if row['id'] in dates else row
            for row in content
        ]
    return explain


def undependent(clickhouse: ExportResult) -> set[int]:
    """The repositories ClickHouse's export has no dependency of."""
    return {
        row['id'] for row in rows(clickhouse, 'repositories')
        if not row['total_dependencies']
    }


def indexed_on(database: str, ids: set[int]) -> dict[int, str]:
    """The day `db index` wrote each repository's row: what `export
    parquet` dates one with no dependency by."""
    with QueryRepository(config(database)) as query:
        return {
            int(row['id']): str(row['day'])
            for row in query.stream_rows(
                "SELECT id, formatDateTime(updated_at, '%Y-%m-%d', 'UTC') "
                'AS day FROM repositories FINAL',
            )
            if int(row['id']) in ids
        }


def newest_scan(path: Path, ids: set[int]) -> dict[int, str]:
    """Each repository's newest current scan's day, or '' for one with
    none: what the export from the warehouse dates one with no
    dependency by."""
    with duckdb.connect(str(path), read_only=True) as con:
        seen = dict(
            con.execute(
                "SELECT repository_id, strftime(max(observed_at), '%Y-%m-%d') "
                'FROM current_scans GROUP BY repository_id',
            ).fetchall(),
        )
    return {i: str(seen.get(i, '')) for i in ids}


def months(path: Path, relation: str) -> Rows:
    """A warehouse's monthly series of every named package, as the
    history table holds it."""
    with duckdb.connect(str(path), read_only=True) as con:
        return [
            {
                'name': name, 'month': month, 'source': source,
                'repository_count': count, 'direct_count': direct,
            }
            for name, month, source, count, direct in con.execute(
                'SELECT name, month, source, repositories, '
                f"direct_repositories FROM {relation} WHERE name != '' "
                'ORDER BY name, source, month',
            ).fetchall()
        ]


def intervals(path: Path, clickhouse: ExportResult) -> Explain:
    """Adoption over time: `export parquet` counts a repository in the
    months of its scans, the warehouse's `mv_package_month`, which
    `tests/warehouse/parity_test.py` holds to ClickHouse's; the export
    from the warehouse in every month between two scans that both show
    the package (owner decision Q9 on #128), as the snapshot serves it."""
    assert rows(clickhouse, 'history') == months(path, 'mv_package_month')
    return lambda content: months(path, 'mv_package_month_intervals')


# -- the inputs ---------------------------------------------------------------


class TestTheContractSeed:

    def test_is_export_parquets(
        self, clickhouse_db: str, tmp_path: Path,
    ) -> None:
        """golang/tools was never scanned, and the seed says `db index`
        wrote its row on 11 February. Its history is the same by either
        count: no month falls between two scans that show a package."""
        contract().seed(clickhouse_db)
        theirs = from_clickhouse(clickhouse_db, tmp_path / 'clickhouse')
        path = warehouse(tmp_path / 'warehouse.duckdb', contract_corpus())
        ours = export_warehouse(path, tmp_path / 'warehouse')

        assert undependent(theirs) == {12}
        assert indexed_on(clickhouse_db, {12}) == {12: '2026-02-11'}
        assert months(path, 'mv_package_month') == months(
            path, 'mv_package_month_intervals',
        )
        compared = compare(
            theirs, ours, {'repositories': dated({12: ''})},
            tmp_path / 'scratch',
        )
        # Not agreement on nothing: every table has rows.
        assert all(compared.values()), compared
        assert manifest(ours)['freshness'] == manifest(theirs)['freshness']


class TestASyntheticCorpus:

    def test_is_export_parquets(
        self, clickhouse_db: str, tmp_path: Path,
    ) -> None:
        repositories, artifacts, edges, ids = synthetic()
        seed_clickhouse(clickhouse_db, repositories, artifacts, edges)
        theirs = from_clickhouse(clickhouse_db, tmp_path / 'clickhouse')
        path = warehouse(
            tmp_path / 'warehouse.duckdb',
            Corpus(
                repositories=repositories, artifacts=artifacts, edges=edges,
                corpus=ids,
            ),
        )
        ours = export_warehouse(path, tmp_path / 'warehouse')

        never_scanned = undependent(theirs)
        assert never_scanned
        assert set(indexed_on(clickhouse_db, never_scanned)) == never_scanned
        assert newest_scan(path, never_scanned) == dict.fromkeys(
            never_scanned, '',
        )
        # The two counts differ on this corpus: the test is not of a
        # difference that never shows.
        assert months(path, 'mv_package_month') != months(
            path, 'mv_package_month_intervals',
        )
        compared = compare(
            theirs, ours, {
                'repositories': dated(dict.fromkeys(never_scanned, '')),
                'history': intervals(path, theirs),
            },
            tmp_path / 'scratch',
        )
        assert compared['artifacts'] > 1000
        assert manifest(ours)['freshness'] == manifest(theirs)['freshness']


def utc_day() -> str:
    return datetime.now(timezone.utc).date().isoformat()


def day_of(path: Path) -> str:
    """The UTC day a file was last written."""
    return datetime.fromtimestamp(
        path.stat().st_mtime, timezone.utc,
    ).date().isoformat()


def scanned(store: Store, repository_id: int, commit: str) -> tuple[str, str]:
    """The days a commit's Syft document was made, and its manifests
    first fetched: `export parquet` dates the commit's scan by the first,
    and the warehouse by the earlier of the two (`store._first_had`), so
    that `sbom generate` writing an older commit's document again after
    an upgrade of Syft moves nothing (`warehouse/parity.py`)."""
    document = day_of(store.paths.sbom_file(repository_id, commit))
    root = store.paths.content_root(repository_id, commit)
    manifests = min(day_of(path) for path in root.rglob('*') if path.is_file())
    return document, min(document, manifests)


@pytest.fixture
def store(tmp_path: Path) -> Store:
    """`data/` in the directory `db_command` indexes."""
    return Store(tmp_path / 'data')


@pytest.fixture
def indexed(store: Store, db_command: Any) -> Iterator[tuple[Store, str]]:
    """ClickHouse as `db index --from-files` left it after each of two
    collections (`tests/warehouse/parity_test.py`'s); and the UTC day it
    began on."""
    began = utc_day()
    first_collection(store)
    db_command('index', '--from-files')
    second_collection(store)
    db_command('index', '--from-files')
    yield store, began


class TestAStore:

    def test_through_both_engines_is_export_parquets(
        self, indexed: tuple[Store, str], clickhouse_db: str, tmp_path: Path,
    ) -> None:
        """Beside what the other inputs show: the manifests read for a
        scan's verdicts, its ecosystems from them, and the refs of the
        records, each through the parsers both engines share.

        acme/bare was never scanned. acme/dart's and acme/pods'
        manifests were fetched late on the day before Syft scanned them:
        `export parquet` dates each scan by the document, a day after the
        warehouse's date, and dart is the oldest of the scanned, so the
        manifest's span begins a day earlier too."""
        store, began = indexed
        theirs = from_clickhouse(clickhouse_db, tmp_path / 'clickhouse')
        path = tmp_path / 'warehouse.duckdb'
        build(store.paths, path, today=TODAY)
        ours = export_warehouse(path, tmp_path / 'warehouse')

        undated = undependent(theirs)
        assert undated == {6}
        assert set(indexed_on(clickhouse_db, undated).values()) <= {
            began, utc_day(),
        }
        days = {i: scanned(store, i, A) for i in (3, 4)}
        assert days == {
            3: ('2026-02-03', '2026-02-02'), 4: ('2026-02-04', '2026-02-03'),
        }
        dates = {
            r['id']: r['observed_at']
            for r in rows(theirs, 'repositories')
        }
        assert {i: dates[i] for i in days} == {
            i: d for i, (d, _) in days.items()
        }
        compare(
            theirs, ours, {
                'repositories': dated({
                    **newest_scan(path, undated),
                    **{i: first for i, (_, first) in days.items()},
                }),
                'history': intervals(path, theirs),
            },
            tmp_path / 'scratch',
        )
        assert manifest(theirs)['freshness']['observedFrom'] == '2026-02-03'
        assert manifest(ours)['freshness']['observedFrom'] == '2026-02-02'
        sources = {
            row['repo']: row['manifest_sources']
            for row in rows(ours, 'repositories')
        }
        assert sources['web'] == ['package.json']
