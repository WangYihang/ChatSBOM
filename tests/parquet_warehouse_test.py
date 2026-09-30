"""`export parquet --from warehouse`: the Parquet export, read from the
warehouse rather than from ClickHouse (#148).

Phase 2d of #128 (Q11: a weekly public Parquet export), and what phase
5 needs before the ClickHouse server goes: the same four tables, the
same contract (`EXPORT_SCHEMA`, version 8), the same content-addressed
files and checksummed manifest, written by the same writer a row group
at a time, from `export parquet`'s queries ported to DuckDB. Nothing but
the warehouse is read, and no server is reached.

Whether the files are `export parquet`'s from ClickHouse, table by
table and row by row, is `parquet_parity_test.py`'s.
"""
from __future__ import annotations

import json
from collections.abc import Iterator
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Any

import duckdb
import pytest
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.__version__ import __version__
from chatsbom.core.container import Container
from chatsbom.export.parquet import export_warehouse
from chatsbom.export.parquet import MANIFEST_NAME
from chatsbom.export.queries import ExportStopped
from chatsbom.export.schema import EXPORT_SCHEMA
from chatsbom.export.warehouse import QUERIES
from chatsbom.warehouse import connect
from chatsbom.warehouse import schema
from chatsbom.warehouse.rollups import derive
from chatsbom.warehouse.rows import load
from chatsbom.warehouse.writer import Scan
from chatsbom.warehouse.writer import Writer
from tests.parquet_export_test import WRITTEN_SCHEMAS
from tests.snapshot.conftest import artifact
from tests.snapshot.conftest import at
from tests.snapshot.conftest import Corpus
from tests.snapshot.conftest import repository
from tests.snapshot.conftest import shop

pa = pytest.importorskip('pyarrow')
pq = pytest.importorskip('pyarrow.parquet')

runner = CliRunner()

#: When gems' one scan was made.
MAY = at(2026, 5, 20, 7)


def gems_scan() -> Scan:
    """acme/gems' Syft scan, whose verdicts two manifests gave: the
    audit trail `manifest_sources` carries."""
    return Scan(
        repository_id=5, source='syft', input_key='g1', tool='',
        observed_at=MAY, ref='v3.0.0', commit_sha='g1',
        manifest_sources=('Gemfile', 'gems.gemspec'), ecosystems=('gem',),
        rows=[
            artifact(
                5, 'rake', '13.2.1', 'gem', observed_at=MAY, commit='g1',
                ref='v3.0.0', relationship='direct', licenses=['MIT'],
                found_by='gemfile-lock-cataloger',
            ),
        ],
    )


def store() -> Corpus:
    """The snapshot tests' shop, and a fifth repository of the corpus:
    one whose scan read manifests for its verdicts."""
    corpus = shop()
    corpus.repositories.append(repository(5, 'acme', 'gems', 200, 'Ruby'))
    assert corpus.corpus is not None
    corpus.corpus.add(5)
    return corpus


def warehouse_of(
    path: Path, corpus: Corpus, scans: tuple[Scan, ...] = (),
) -> Path:
    """A warehouse file of `corpus`, and of `scans` beside its rows,
    derived as a pass derives it, and closed."""
    with connect(path) as con:
        load(
            con, corpus.repositories, corpus.artifacts, corpus.edges,
            corpus=corpus.corpus,
        )
        with Writer(con) as writer:
            for scan in scans:
                writer.scan(scan)
            for repository_id, source, key, instant in corpus.empty:
                syft = source == 'syft'
                writer.scan(
                    Scan(
                        repository_id=repository_id, source=source,
                        input_key=key, tool='', observed_at=instant,
                        ref='main' if syft else '',
                        commit_sha=key if syft else '',
                    ),
                )
        derive(con)
        con.execute('CHECKPOINT')
    return path


@pytest.fixture
def warehouse(tmp_path: Path) -> Path:
    return warehouse_of(tmp_path / 'warehouse.duckdb', store(), (gems_scan(),))


def table(directory: Path, name: str) -> list[dict[str, Any]]:
    """An exported table's rows, from the one file of it there."""
    [path] = sorted(directory.glob(f'{name}-*.parquet'))
    rows: list[dict[str, Any]] = pq.read_table(path).to_pylist()
    return rows


def manifest(directory: Path) -> dict[str, Any]:
    loaded: dict[str, Any] = json.loads(
        (directory / MANIFEST_NAME).read_text(encoding='utf-8'),
    )
    return loaded


def asked(path: Path, sql: str) -> list[tuple[Any, ...]]:
    with duckdb.connect(str(path), read_only=True) as con:
        return con.execute(sql).fetchall()


# -- what the files hold ------------------------------------------------------


REPOSITORY = {
    'url': '', 'description': '', 'license_spdx_id': '',
    'pushed_at': '2026-09-01', 'manifest_sources': [],
}


class TestTheRepositories:

    def test_are_the_corpus_by_stars(
        self, warehouse: Path, tmp_path: Path,
    ) -> None:
        """Each repository of the corpus, scanned or not, and no other:
        other/gone is not in it."""
        export_warehouse(warehouse, tmp_path / 'out')
        rows = table(tmp_path / 'out', 'repositories')
        assert [(r['id'], r['stars']) for r in rows] == [
            (2, 500), (1, 300), (5, 200), (3, 100),
        ]

    def test_hold_what_export_parquet_says_of_each(
        self, warehouse: Path, tmp_path: Path,
    ) -> None:
        export_warehouse(warehouse, tmp_path / 'out')
        rows = {r['id']: r for r in table(tmp_path / 'out', 'repositories')}
        assert rows[1] == {
            **REPOSITORY, 'id': 1, 'owner': 'acme', 'repo': 'app',
            'stars': 300, 'language': 'ruby', 'github_language': 'Ruby',
            'language_bucket': 'ruby', 'ecosystems': ['gem'],
            'url': 'https://github.com/acme/app', 'description': 'An app',
            'license_spdx_id': 'MIT',
            # Its newest current scan, the graph's: when we last looked.
            'observed_at': '2026-09-13',
            # Its current Syft scan's, March's.
            'sbom_ref': 'v2.0.0', 'sbom_commit_sha': 'c2',
            # rack, declared; and puma.
            'direct_dependencies': 1, 'total_dependencies': 2,
        }
        assert rows[2] == {
            **REPOSITORY, 'id': 2, 'owner': 'acme', 'repo': 'web',
            'stars': 500, 'language': 'javascript',
            'github_language': 'JavaScript', 'language_bucket': 'javascript',
            # Syft's `php-composer`, as the ecosystem it is.
            'ecosystems': ['composer', 'gem', 'npm'],
            'url': 'https://github.com/acme/web',
            'description': 'Ünïcode — the web 🕸', 'pushed_at': '2026-08-01',
            # Its graph was fetched later and saw nothing.
            'observed_at': '2026-02-01',
            'sbom_ref': 'main', 'sbom_commit_sha': 'w1',
            'direct_dependencies': 2, 'total_dependencies': 4,
        }
        assert rows[5] == {
            **REPOSITORY, 'id': 5, 'owner': 'acme', 'repo': 'gems',
            'stars': 200, 'language': 'ruby', 'github_language': 'Ruby',
            'language_bucket': 'ruby', 'ecosystems': ['gem'],
            'url': 'https://github.com/acme/gems', 'observed_at': '2026-05-20',
            'sbom_ref': 'v3.0.0', 'sbom_commit_sha': 'g1',
            'direct_dependencies': 1, 'total_dependencies': 1,
            'manifest_sources': ['Gemfile', 'gems.gemspec'],
        }

    def test_one_with_no_dependency_is_dated_by_its_newest_scan(
        self, warehouse: Path, tmp_path: Path,
    ) -> None:
        """acme/idle was scanned once and saw nothing. `export parquet`
        from ClickHouse dates it by the day `db index` wrote its row,
        which says when the indexer ran; the warehouse keeps no such day,
        and dates it as the snapshot does (`snapshot/tables.py`)."""
        export_warehouse(warehouse, tmp_path / 'out')
        rows = {r['id']: r for r in table(tmp_path / 'out', 'repositories')}
        assert rows[3] == {
            **REPOSITORY, 'id': 3, 'owner': 'acme', 'repo': 'idle',
            'stars': 100, 'language': '', 'github_language': '',
            'language_bucket': 'none', 'ecosystems': [],
            'url': 'https://github.com/acme/idle', 'observed_at': '2026-04-02',
            'sbom_ref': 'main', 'sbom_commit_sha': 'i1',
            'direct_dependencies': 0, 'total_dependencies': 0,
        }

    def test_one_never_scanned_is_not_dated(self, tmp_path: Path) -> None:
        corpus = store()
        corpus.empty = [
            scan for scan in corpus.empty if scan[0] != 3
        ]
        path = warehouse_of(tmp_path / 'w.duckdb', corpus)
        export_warehouse(path, tmp_path / 'out')
        rows = {r['id']: r for r in table(tmp_path / 'out', 'repositories')}
        assert (rows[3]['observed_at'], rows[3]['sbom_commit_sha']) == ('', '')


class TestTheArtifacts:

    def test_are_the_facts_in_the_lookup_order(
        self, warehouse: Path, tmp_path: Path,
    ) -> None:
        """One row per fact, sorted by every column a fact is distinct
        in, name first, so that no two rows tie and a lookup by name
        touches few row groups."""
        export_warehouse(warehouse, tmp_path / 'out')
        columns = EXPORT_SCHEMA.table('artifacts').column_names
        rows = [
            tuple(r[c] for c in columns)
            for r in table(tmp_path / 'out', 'artifacts')
        ]
        assert rows == asked(
            warehouse,
            f"SELECT {', '.join(columns)} FROM facts ORDER BY name, "
            'repository_id, version, type, found_by, relationship, source, '
            'version_kind',
        )
        assert [r[1] for r in rows] == [
            'laravel/framework', 'left-pad', 'lodash', 'puma', 'rack',
            'rack', 'rack', 'rake',
        ]


class TestTheLicences:

    def test_are_keyed_by_licence_and_ecosystem(
        self, warehouse: Path, tmp_path: Path,
    ) -> None:
        """Every licence a package carries, the unknown one kept as
        empty, by type as Syft or the graph spelt it, widest first."""
        export_warehouse(warehouse, tmp_path / 'out')
        assert table(tmp_path / 'out', 'licenses') == [
            {
                'license': 'MIT', 'type': 'gem', 'package_count': 2,
                'repository_count': 3,
            },
            # puma, and the graph's rack, which carries none.
            {
                'license': '', 'type': 'gem', 'package_count': 2,
                'repository_count': 1,
            },
            {
                'license': 'MIT', 'type': 'npm', 'package_count': 1,
                'repository_count': 1,
            },
            {
                'license': 'MIT', 'type': 'php-composer', 'package_count': 1,
                'repository_count': 1,
            },
            # rack's second licence, beside its first.
            {
                'license': 'Ruby', 'type': 'gem', 'package_count': 1,
                'repository_count': 1,
            },
            {
                'license': 'WTFPL', 'type': 'npm', 'package_count': 1,
                'repository_count': 1,
            },
        ]

    def test_are_the_widest_five_hundred(self, tmp_path: Path) -> None:
        corpus = Corpus(
            repositories=[repository(1, 'acme', 'many', 10, 'Go')],
            artifacts=[
                artifact(
                    1, f'pkg-{k:03}', '1.0', 'go-module',
                    observed_at=at(2026, 3, 1), commit='m1',
                    licenses=[f'L-{k:03}'],
                )
                for k in range(520)
            ],
            corpus={1},
        )
        path = warehouse_of(tmp_path / 'w.duckdb', corpus)
        export_warehouse(path, tmp_path / 'out')
        rows = table(tmp_path / 'out', 'licenses')
        assert len(rows) == 500
        assert rows[0]['license'] == 'L-000' and rows[-1]['license'] == 'L-499'


class TestTheHistory:

    def test_counts_a_repository_in_every_month_a_package_held(
        self, warehouse: Path, tmp_path: Path,
    ) -> None:
        """Adoption over time by intervals, owner decision Q9 on #128, as
        the snapshot serves it: acme/app's January and March scans both
        show rack, so it counts in February too, beside acme/web's."""
        export_warehouse(warehouse, tmp_path / 'out')
        rack = [
            (r['month'], r['source'], r['repository_count'], r['direct_count'])
            for r in table(tmp_path / 'out', 'history') if r['name'] == 'rack'
        ]
        assert rack == [
            ('2026-09', 'github-depgraph', 1, 1),
            ('2026-01', 'syft', 1, 0),
            ('2026-02', 'syft', 2, 0),
            ('2026-03', 'syft', 1, 1),
        ]

    def test_is_the_intervals_rollup_of_every_named_package(
        self, warehouse: Path, tmp_path: Path,
    ) -> None:
        export_warehouse(warehouse, tmp_path / 'out')
        columns = EXPORT_SCHEMA.table('history').column_names
        assert [
            tuple(r[c] for c in columns)
            for r in table(tmp_path / 'out', 'history')
        ] == asked(
            warehouse,
            'SELECT name, month, source, repositories, direct_repositories '
            "FROM mv_package_month_intervals WHERE name != '' "
            'ORDER BY name, source, month',
        )


# -- the files and the manifest ----------------------------------------------


class TestTheFiles:

    def test_are_one_a_table_named_after_their_content(
        self, warehouse: Path, tmp_path: Path,
    ) -> None:
        result = export_warehouse(warehouse, tmp_path / 'out')
        assert sorted(p.name for p in (tmp_path / 'out').iterdir()) == sorted(
            [*result.files.values(), MANIFEST_NAME],
        )
        for name, file in result.files.items():
            assert file == f'{name}-{result.checksums[file][:8]}.parquet'

    def test_keep_the_schema_the_files_have_always_had(
        self, warehouse: Path, tmp_path: Path,
    ) -> None:
        """DuckDB's Arrow is its own types, unsigned ids and lists whose
        items are `l`: cast to the contract on the way out, as
        ClickHouse's is."""
        export_warehouse(warehouse, tmp_path / 'out')
        for declared in EXPORT_SCHEMA.tables:
            [path] = sorted((tmp_path / 'out').glob(f'{declared.name}-*'))
            written = pq.read_schema(path)
            assert [
                (f.name, str(f.type)) for f in written
            ] == WRITTEN_SCHEMAS[declared.name], declared.name
            assert all(f.nullable for f in written)

    def test_the_same_warehouse_is_the_same_bytes(
        self, warehouse: Path, tmp_path: Path,
    ) -> None:
        first = export_warehouse(warehouse, tmp_path / 'a')
        second = export_warehouse(warehouse, tmp_path / 'b')
        assert first.checksums == second.checksums
        assert (tmp_path / 'a' / MANIFEST_NAME).read_bytes() == (
            tmp_path / 'b' / MANIFEST_NAME
        ).read_bytes()

    def test_whatever_order_its_rows_were_written_in(
        self, warehouse: Path, tmp_path: Path,
    ) -> None:
        """Every query orders its rows totally: the files are named
        after their bytes, and the rows' order is in them."""
        corpus = store()
        corpus.repositories.reverse()
        corpus.artifacts.reverse()
        corpus.empty.reverse()
        reversed_ = warehouse_of(tmp_path / 'r.duckdb', corpus, (gems_scan(),))
        assert export_warehouse(warehouse, tmp_path / 'a').checksums == (
            export_warehouse(reversed_, tmp_path / 'b').checksums
        )

    def test_are_written_in_row_groups_of_the_declared_size(
        self, warehouse: Path, tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        monkeypatch.setattr('chatsbom.export.parquet.ROW_GROUP_SIZE', 3)
        export_warehouse(warehouse, tmp_path / 'out')
        [path] = sorted((tmp_path / 'out').glob('artifacts-*.parquet'))
        metadata = pq.ParquetFile(path).metadata
        assert [
            metadata.row_group(i).num_rows
            for i in range(metadata.num_row_groups)
        ] == [3, 3, 2]

    def test_an_empty_warehouse_still_exports_every_table(
        self, tmp_path: Path,
    ) -> None:
        path = tmp_path / 'w.duckdb'
        with connect(path) as con:
            schema.create(con)
            derive(con)
        result = export_warehouse(path, tmp_path / 'out')
        assert result.row_counts == dict.fromkeys(
            ('repositories', 'artifacts', 'licenses', 'history'), 0,
        )
        for declared in EXPORT_SCHEMA.tables:
            [file] = sorted((tmp_path / 'out').glob(f'{declared.name}-*'))
            assert pq.read_schema(file).names == declared.column_names

    def test_a_changed_warehouse_leaves_no_superseded_file(
        self, warehouse: Path, tmp_path: Path,
    ) -> None:
        before = export_warehouse(warehouse, tmp_path / 'out').files
        changed = warehouse_of(tmp_path / 'changed.duckdb', store())
        result = export_warehouse(changed, tmp_path / 'out')
        assert sorted(p.name for p in (tmp_path / 'out').iterdir()) == sorted(
            [*result.files.values(), MANIFEST_NAME],
        )
        assert result.files['repositories'] != before['repositories']

    def test_the_warehouse_is_only_read(
        self, warehouse: Path, tmp_path: Path,
    ) -> None:
        before = warehouse.read_bytes()
        export_warehouse(warehouse, tmp_path / 'out')
        assert warehouse.read_bytes() == before
        assert sorted(p.name for p in warehouse.parent.glob('warehouse*')) == [
            'warehouse.duckdb',
        ]


class TestTheManifest:

    def test_describes_the_export_as_export_parquet_does(
        self, warehouse: Path, tmp_path: Path,
    ) -> None:
        result = export_warehouse(warehouse, tmp_path / 'out')
        said = manifest(tmp_path / 'out')
        assert said == {
            'schemaVersion': EXPORT_SCHEMA.version,
            'generator': f'chatsbom/{__version__}',
            'rowCounts': result.row_counts,
            # Of the repositories with dependencies: acme/idle's date,
            # which is no observation of one, is not in the span.
            'freshness': {
                'observedFrom': '2026-02-01', 'observedTo': '2026-09-13',
            },
            'files': [
                {
                    'name': name, 'bytes': result.sizes[name],
                    'sha256': result.checksums[name],
                }
                for name in sorted(result.checksums)
            ],
            'schema': EXPORT_SCHEMA.to_dict(files=result.files),
        }
        assert result.row_counts == {
            'repositories': 4, 'artifacts': 8, 'licenses': 6,
            'history': result.row_counts['history'],
        }

    def test_names_files_that_hold_what_it_says(
        self, warehouse: Path, tmp_path: Path,
    ) -> None:
        import hashlib

        export_warehouse(warehouse, tmp_path / 'out')
        for entry in manifest(tmp_path / 'out')['files']:
            data = (tmp_path / 'out' / entry['name']).read_bytes()
            assert len(data) == entry['bytes']
            assert hashlib.sha256(data).hexdigest() == entry['sha256']


# -- streamed ------------------------------------------------------------------


FACTS = 60_000


def bulk(path: Path, facts: int = FACTS) -> Path:
    """A warehouse of `facts` facts, 600 repositories' Syft scans of a
    hundred rows each, the scans made in SQL: written row by row through
    the writer they would take longer than the export."""
    with connect(path) as con:
        schema.create(con)
        repositories = facts // 100
        with Writer(con) as writer:
            writer.extend(
                'repositories', (
                    {
                        'id': r, 'owner': 'o', 'repo': f'r{r}', 'stars': r,
                        'snapshot': 'all-2026-09-01',
                    }
                    for r in range(1, repositories + 1)
                ),
            )
        con.execute(
            f'INSERT INTO corpus SELECT r FROM range(1, {repositories} + 1) '
            'AS t(r)',
        )
        con.execute(
            'INSERT INTO scans (scan_id, repository_id, source, input_key, '
            'tool, observed_at, ref, ref_type, commit_sha, document, '
            'manifest_sources, ecosystems, observations) '
            "SELECT r, r, 'syft', 'c' || r, '', TIMESTAMP '2026-03-01', "
            "'main', '', 'c' || r, '', [], ['npm'], 100 "
            f'FROM range(1, {repositories} + 1) AS t(r)',
        )
        con.execute(
            'INSERT INTO observations (scan_id, position, repository_id, '
            'source, artifact_id, name, version, type, purl, found_by, '
            'licenses, relationship, version_kind) '
            "SELECT s, k, s, 'syft', 'a' || k, 'package-' || k, "
            "'1.' || (s % 10) || '.0', 'npm', '', 'javascript-cataloger', "
            "['MIT'], 'transitive', 'resolved' "
            f'FROM range(1, {repositories} + 1) AS a(s), range(100) AS b(k)',
        )
        derive(con)
        con.execute('CHECKPOINT')
    return path


#: Each table's query, by what it is.
TABLES = {sql: name for name, sql in QUERIES.items()}


@dataclass
class Tally:
    """Rows DuckDB has handed over and rows written, by the export."""

    read: int = 0
    written: int = 0
    #: Rows read and not yet written, as each batch was handed over.
    in_flight: list[int] = field(default_factory=list)
    #: Each batch's rows, by table.
    batches: dict[str, list[int]] = field(default_factory=dict)
    #: Queries sent, by table.
    queries: list[str] = field(default_factory=list)


@pytest.fixture
def tally(monkeypatch: pytest.MonkeyPatch) -> Tally:
    """Every batch the warehouse hands the export, and every row the
    Parquet writer is given, counted; a row group of 5,000 rows, and
    batches of 1,000."""
    from chatsbom.export import parquet

    counted = Tally()
    batches = parquet._batches
    write_table = pq.ParquetWriter.write_table

    def reading(con: duckdb.DuckDBPyConnection, sql: str) -> Iterator[Any]:
        name = TABLES[sql]
        counted.queries.append(name)
        for batch in batches(con, sql):
            counted.read += batch.num_rows
            counted.in_flight.append(counted.read - counted.written)
            counted.batches.setdefault(name, []).append(batch.num_rows)
            yield batch

    def writing(writer: Any, written: Any, row_group_size: Any = None) -> None:
        counted.written += written.num_rows
        write_table(writer, written, row_group_size=row_group_size)

    monkeypatch.setattr(parquet, '_batches', reading)
    monkeypatch.setattr(pq.ParquetWriter, 'write_table', writing)
    monkeypatch.setattr(parquet, 'ROW_GROUP_SIZE', 5_000)
    monkeypatch.setattr(parquet, 'BATCH', 1_000)
    return counted


class TestStreamed:

    def test_each_query_runs_once(self, tally: Tally, tmp_path: Path) -> None:
        export_warehouse(bulk(tmp_path / 'w.duckdb'), tmp_path / 'out')
        assert sorted(tally.queries) == sorted(
            t.name for t in EXPORT_SCHEMA.tables
        )

    def test_duckdb_hands_the_rows_over_a_batch_at_a_time(
        self, tally: Tally, tmp_path: Path,
    ) -> None:
        """Never a table: the facts come in sixty batches."""
        result = export_warehouse(
            bulk(tmp_path / 'w.duckdb'), tmp_path / 'out',
        )
        assert result.row_counts['artifacts'] == FACTS
        assert tally.batches['artifacts'] == [1_000] * 60

    def test_batches_are_written_as_they_arrive(
        self, tally: Tally, tmp_path: Path,
    ) -> None:
        """Never more than a row group and a batch read and not yet
        written."""
        export_warehouse(bulk(tmp_path / 'w.duckdb'), tmp_path / 'out')
        assert tally.read == tally.written
        assert max(tally.in_flight) <= 5_000 + 1_000

    def test_a_stream_that_breaks_off_names_its_table_and_leaves_nothing(
        self, warehouse: Path, tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """DuckDB can stop a query partway, out of memory or of disk to
        spill to: the error says which table, and what to look at, and
        no file of the table or manifest is left to name it."""
        from chatsbom.export import parquet

        batches = parquet._batches

        def breaking(
            con: duckdb.DuckDBPyConnection, sql: str,
        ) -> Iterator[Any]:
            for batch in batches(con, sql):
                yield batch
                if TABLES[sql] == 'artifacts':
                    raise duckdb.OutOfMemoryException('Out of Memory Error')

        monkeypatch.setattr(parquet, '_batches', breaking)
        monkeypatch.setattr(parquet, 'BATCH', 2)
        with pytest.raises(ExportStopped, match='artifacts') as stopped:
            export_warehouse(warehouse, tmp_path / 'out')
        assert 'CHATSBOM_DUCKDB_MEMORY_LIMIT' in str(stopped.value)
        left = sorted(p.name for p in (tmp_path / 'out').iterdir())
        assert not [name for name in left if 'artifacts' in name], left
        assert MANIFEST_NAME not in left


# -- the command ----------------------------------------------------------------


@pytest.fixture
def here(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Path]:
    """A warehouse where the command looks for one, `data/`, in the
    working directory; and no ClickHouse to reach, which fails a test
    that tries."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr('chatsbom.core.config._config', None)
    monkeypatch.setattr(Container, '_instance', None)

    def refuse(*args: object, **kwargs: object) -> None:
        raise AssertionError('ClickHouse was reached')

    monkeypatch.setattr(
        'chatsbom.commands.export.parquet.check_clickhouse_connection',
        refuse,
    )
    monkeypatch.setattr(Container, 'get_export_repository', refuse)
    (tmp_path / 'data').mkdir()
    warehouse_of(
        tmp_path / 'data' / 'warehouse.duckdb', store(), (gems_scan(),),
    )
    yield tmp_path


def said(output: str) -> str:
    return ' '.join(output.split())


class TestTheCommand:

    def test_exports_the_warehouse_beside_the_store(self, here: Path) -> None:
        result = runner.invoke(
            app, ['export', 'parquet', '--from', 'warehouse'],
        )

        assert result.exit_code == 0, result.output
        out = here / 'dist' / 'data'
        described = manifest(out)
        assert described['rowCounts']['repositories'] == 4
        assert sorted(p.name for p in out.iterdir()) == sorted(
            [entry['name'] for entry in described['files']] + [MANIFEST_NAME],
        )
        # What it wrote, with each file's rows, is its output; what it
        # was doing meanwhile is on stderr, with the logs (#114).
        for entry in described['files']:
            assert entry['name'] in result.stdout
        assert 'Exporting data/warehouse.duckdb' in said(result.stderr)
        assert 'Exporting' not in result.stdout

    def test_reads_and_writes_where_it_is_told(
        self, here: Path, tmp_path: Path,
    ) -> None:
        other = warehouse_of(tmp_path / 'other.duckdb', store())
        result = runner.invoke(
            app, [
                'export', 'parquet', '--from', 'warehouse',
                '--warehouse', str(other), '--output', str(tmp_path / 'out'),
            ],
        )
        assert result.exit_code == 0, result.output
        assert manifest(tmp_path / 'out')['rowCounts']['repositories'] == 4
        assert not (here / 'dist').exists()

    def test_without_a_warehouse_it_says_so(
        self, here: Path, tmp_path: Path,
    ) -> None:
        missing = tmp_path / 'missing.duckdb'
        result = runner.invoke(
            app, [
                'export', 'parquet', '--from', 'warehouse',
                '--warehouse', str(missing),
            ],
        )
        assert result.exit_code == 1
        assert result.stdout == ''
        assert 'no warehouse' in said(result.stderr).lower()
        assert 'warehouse build' in said(result.stderr)
        assert not missing.exists()
        assert not (here / 'dist').exists()

    def test_a_warehouse_is_read_only_from_the_warehouse(
        self, here: Path,
    ) -> None:
        """`--warehouse` names what `--from warehouse` reads. Given it
        alone, the export would read ClickHouse, and the files would not
        be of the warehouse named."""
        result = runner.invoke(
            app, [
                'export', 'parquet', '--warehouse', 'data/warehouse.duckdb',
            ],
        )
        assert result.exit_code == 1
        assert result.stdout == ''
        assert '--from warehouse' in said(result.stderr)
        assert not (here / 'dist').exists()

    def test_what_it_does_not_catch_is_reported_on_stderr(
        self, here: Path,
    ) -> None:
        """As `handle_errors` reports it for every command (#124): a file
        that is no warehouse, here."""
        (here / 'data' / 'warehouse.duckdb').write_bytes(b'not DuckDB')
        result = runner.invoke(
            app, ['export', 'parquet', '--from', 'warehouse'],
        )
        assert isinstance(result.exception, SystemExit), repr(result.exception)
        assert result.exit_code == 1
        assert result.stdout == ''
        assert 'Unexpected Error' in result.stderr
