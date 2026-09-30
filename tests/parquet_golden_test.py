"""The Parquet export is what `export parquet` wrote from ClickHouse.

`export parquet` read the ClickHouse server until #153, and the warehouse
too from #148, where the export from each was held to the other, row for
row and file for file. The server is gone, and what `export parquet`
wrote from it is kept (`tests/golden/parquet-*.json`), each
table's rows in the order written; the export from the warehouse is
held to that. Three inputs (`tests/warehouse/conftest.py`):

- the seed the contract suite and `d1.sql` were recorded from, each of
  whose rows is a case two readers of this data have disagreed on;
- a synthetic corpus of 300 repositories: every source, history,
  repeats, names across ecosystems, more than twelve languages, and
  repositories outside the corpus;
- a store on disk as two collections leave it: the parsers' own rows,
  with the manifests read for each scan's verdicts.

**How the fixtures were recorded.** On 2026-09-30, at 119be7f, against
ClickHouse 25.12.11.4 on 127.0.0.1:8123, with DuckDB 1.5.6, pyarrow
25.0.1 and Python 3.12, by this test as it stood then turned into a
recorder
(kept with #153's pull request, not in the tree). Each input went into a
`chatsbom_test_*` database of its own, as `db index` left one: the
contract seed and the synthetic corpus by `seed`, the store by `db index
--from-files` after each of its two collections. `export parquet` of it
was read back table by table, and kept after the export from the
warehouse of the same input agreed with it as below: row for row, and
where no difference is explained, byte for byte under the same name.
The date ClickHouse gave a repository with no dependency, the day `db
index` wrote its row, is not kept: it said when the recording ran.

Where the export from the warehouse is not what ClickHouse's was by
design, the difference is named here, said why, and held exactly: a
function makes ClickHouse's rows into the warehouse's, and nothing else
is let through. Each file is the one its rows make, written by the
export's own writer.
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Callable
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

import duckdb
import pytest

from chatsbom.__version__ import __version__
from chatsbom.export.parquet import _arrow_schema
from chatsbom.export.parquet import _write_rows
from chatsbom.export.parquet import export_warehouse
from chatsbom.export.parquet import ExportResult
from chatsbom.export.parquet import MANIFEST_NAME
from chatsbom.export.parquet import repository_freshness
from chatsbom.export.schema import EXPORT_SCHEMA
from chatsbom.warehouse.build import build
from tests import golden
from tests.snapshot.conftest import contract_corpus
from tests.snapshot.conftest import Corpus
from tests.snapshot.conftest import warehouse
from tests.warehouse.conftest import A
from tests.warehouse.conftest import first_collection
from tests.warehouse.conftest import second_collection
from tests.warehouse.conftest import Store
from tests.warehouse.conftest import synthetic
from tests.warehouse.conftest import TODAY

pa = pytest.importorskip('pyarrow')
pq = pytest.importorskip('pyarrow.parquet')

Rows = list[dict[str, Any]]
Explain = Callable[[Rows], Rows]


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


def as_kept(table: str, content: Rows) -> list[golden.Row]:
    """A table's rows as a fixture keeps them."""
    columns = EXPORT_SCHEMA.table(table).column_names
    return golden.ordered(tuple(r[c] for c in columns) for r in content)


def kept_rows(recorded: dict[str, Any], table: str) -> Rows:
    """The rows ClickHouse's export wrote of a table kept whole."""
    kept = recorded['tables'][table]
    return [dict(zip(kept['columns'], values)) for values in kept['rows']]


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
    fixture: str,
    ours: ExportResult,
    explained: dict[str, Explain],
    scratch: Path,
) -> dict[str, int]:
    """Every table: ClickHouse's rows, as recorded and after what
    `explained` makes of them, against the warehouse's, row for row in
    the order written; each file the one its rows make; and the
    manifest, but for which files it names. The rows of each table."""
    recorded = golden.load(fixture)
    compared = {}
    scratch.mkdir()
    for table in EXPORT_SCHEMA.tables:
        name = table.name
        kept = recorded['tables'][name]
        assert kept['columns'] == table.column_names, name
        content = rows(ours, name)
        found = as_kept(name, content)
        if name in explained:
            theirs = kept_rows(recorded, name) if 'rows' in kept else []
            assert found == as_kept(name, explained[name](theirs)), name
        else:
            assert golden.holds(name, kept, found), (
                golden.mismatch(name, kept, found)
            )
        assert written(name, content, scratch) == (
            ours.checksums[ours.files[name]]
        ), name
        compared[name] = len(found)

    said = manifest(ours)
    assert said['rowCounts'] == compared
    assert said['schemaVersion'] == recorded['schemaVersion']
    assert said['generator'] == f'chatsbom/{__version__}'
    assert said['schema'] == EXPORT_SCHEMA.to_dict(files=ours.files)
    # The span of the rows compared.
    assert said['freshness'] == repository_freshness(
        explained.get('repositories', lambda same: same)(
            kept_rows(recorded, 'repositories'),
        ),
    )
    return compared


# -- what differs by design -------------------------------------------------


def dated(dates: dict[int, str]) -> Explain:
    """`observed_at` of the repositories in `dates`, as the warehouse
    dates them.

    One with no dependency: `export parquet` from ClickHouse had the day
    `db index` wrote its row, which said when the indexer ran rather than
    when anything was seen, and the warehouse keeps no such day. The
    export from it dates the repository by its newest current scan, or
    not at all when it has none, as the snapshot does. Neither is in the
    manifest's span (`repository_freshness`).

    One whose manifests the store had the day before Syft made the
    commit's document: see `scanned`."""
    def explain(content: Rows) -> Rows:
        return [
            {**row, 'observed_at': dates[row['id']]}
            if row['id'] in dates else row
            for row in content
        ]
    return explain


def undependent(fixture: str) -> set[int]:
    """The repositories ClickHouse's export had no dependency of."""
    return {
        row['id'] for row in kept_rows(golden.load(fixture), 'repositories')
        if not row['total_dependencies']
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


def intervals(path: Path, fixture: str) -> Explain:
    """Adoption over time: `export parquet` from ClickHouse counted a
    repository in the months of its scans, the warehouse's
    `mv_package_month`, which `tests/warehouse/golden_test.py` holds to
    ClickHouse's; the export from the warehouse in every month between
    two scans that both show the package (owner decision Q9 on #128), as
    the snapshot serves it."""
    kept = golden.load(fixture)['tables']['history']
    scans = as_kept('history', months(path, 'mv_package_month'))
    assert golden.holds('history', kept, scans), (
        golden.mismatch('history', kept, scans)
    )
    return lambda content: months(path, 'mv_package_month_intervals')


# -- the inputs ---------------------------------------------------------------


class TestTheContractSeed:

    def test_is_export_parquets(self, tmp_path: Path) -> None:
        """golang/tools was never scanned. Its history is the same by
        either count: no month falls between two scans that show a
        package."""
        fixture = 'parquet-contract.json'
        path = warehouse(tmp_path / 'warehouse.duckdb', contract_corpus())
        ours = export_warehouse(path, tmp_path / 'warehouse')

        assert undependent(fixture) == {12}
        assert months(path, 'mv_package_month') == months(
            path, 'mv_package_month_intervals',
        )
        compared = compare(
            fixture, ours, {'repositories': dated({12: ''})},
            tmp_path / 'scratch',
        )
        # Not agreement on nothing: every table has rows.
        assert all(compared.values()), compared
        assert manifest(ours)['freshness'] == (
            golden.load(fixture)['freshness']
        )


class TestASyntheticCorpus:

    def test_is_export_parquets(self, tmp_path: Path) -> None:
        fixture = 'parquet-synthetic.json'
        repositories, artifacts, edges, ids = synthetic()
        path = warehouse(
            tmp_path / 'warehouse.duckdb',
            Corpus(
                repositories=repositories, artifacts=artifacts, edges=edges,
                corpus=ids,
            ),
        )
        ours = export_warehouse(path, tmp_path / 'warehouse')

        never_scanned = undependent(fixture)
        assert never_scanned
        assert newest_scan(path, never_scanned) == dict.fromkeys(
            never_scanned, '',
        )
        # The two counts differ on this corpus: the test is not of a
        # difference that never shows.
        assert months(path, 'mv_package_month') != months(
            path, 'mv_package_month_intervals',
        )
        compared = compare(
            fixture, ours, {
                'repositories': dated(dict.fromkeys(never_scanned, '')),
                'history': intervals(path, fixture),
            },
            tmp_path / 'scratch',
        )
        assert compared['artifacts'] > 1000
        assert manifest(ours)['freshness'] == (
            golden.load(fixture)['freshness']
        )


@pytest.fixture
def store(tmp_path: Path) -> Store:
    return Store(tmp_path / 'data')


def day_of(path: Path) -> str:
    """The UTC day a file was last written."""
    return datetime.fromtimestamp(
        path.stat().st_mtime, timezone.utc,
    ).date().isoformat()


def scanned(store: Store, repository_id: int, commit: str) -> tuple[str, str]:
    """The days a commit's Syft document was made, and its manifests
    first fetched: `export parquet` from ClickHouse dated the commit's
    scan by the first, and the warehouse by the earlier of the two
    (`store._first_had`), so that `sbom generate` writing an older
    commit's document again after an upgrade of Syft moves nothing."""
    document = day_of(store.paths.sbom_file(repository_id, commit))
    root = store.paths.content_root(repository_id, commit)
    manifests = min(day_of(path) for path in root.rglob('*') if path.is_file())
    return document, min(document, manifests)


class TestAStore:

    def test_is_export_parquets(self, store: Store, tmp_path: Path) -> None:
        """Beside what the other inputs show: the manifests read for a
        scan's verdicts, its ecosystems from them, and the refs of the
        records, each through the parsers.

        acme/bare was never scanned. acme/dart's and acme/pods'
        manifests were fetched late on the day before Syft scanned them:
        `export parquet` from ClickHouse dated each scan by the document,
        a day after the warehouse's date, and dart is the oldest of the
        scanned, so the manifest's span begins a day earlier too."""
        fixture = 'parquet-store.json'
        first_collection(store)
        second_collection(store)
        path = tmp_path / 'warehouse.duckdb'
        build(store.paths, path, today=TODAY)
        ours = export_warehouse(path, tmp_path / 'warehouse')

        undated = undependent(fixture)
        assert undated == {6}
        days = {i: scanned(store, i, A) for i in (3, 4)}
        assert days == {
            3: ('2026-02-03', '2026-02-02'), 4: ('2026-02-04', '2026-02-03'),
        }
        recorded = kept_rows(golden.load(fixture), 'repositories')
        assert {
            row['id']: row['observed_at'] for row in recorded
            if row['id'] in days
        } == {i: d for i, (d, _) in days.items()}
        compare(
            fixture, ours, {
                'repositories': dated({
                    **newest_scan(path, undated),
                    **{i: first for i, (_, first) in days.items()},
                }),
                'history': intervals(path, fixture),
            },
            tmp_path / 'scratch',
        )
        assert golden.load(fixture)['freshness']['observedFrom'] == (
            '2026-02-03'
        )
        assert manifest(ours)['freshness']['observedFrom'] == '2026-02-02'
        sources = {
            row['repo']: row['manifest_sources']
            for row in rows(ours, 'repositories')
        }
        assert sources['web'] == ['package.json']
