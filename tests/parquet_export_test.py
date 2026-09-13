"""Parquet export: the payload the dashboard downloads."""
import json

import pytest

from chatsbom.core.schema import ARTIFACTS
from chatsbom.core.schema import REPOSITORIES
from chatsbom.export.parquet import export_dataset
from chatsbom.export.schema import EXPORT_SCHEMA
from chatsbom.models.relationship import DIRECT
from chatsbom.models.relationship import TRANSITIVE
from tests.conftest import requires_clickhouse
from tests.repository_query_test import artifact_row
from tests.repository_query_test import repo_row

pytestmark = requires_clickhouse

pa = pytest.importorskip('pyarrow')
pq = pytest.importorskip('pyarrow.parquet')


@pytest.fixture
def seeded(ingest, query):
    repos = [
        repo_row(id=1, owner='mastodon', repo='mastodon', stars=300),
        repo_row(id=2, owner='rails', repo='rails', stars=200),
    ]
    artifacts = [
        artifact_row(repository_id=1, artifact_id='a1', relationship=DIRECT),
        artifact_row(
            repository_id=1, artifact_id='a2',
            name='mini_mime', relationship=TRANSITIVE,
        ),
        artifact_row(
            repository_id=2, artifact_id='a3',
            relationship=TRANSITIVE,
        ),
    ]
    ingest.insert_batch(
        REPOSITORIES.name, REPOSITORIES.rows(repos), REPOSITORIES.column_names,
    )
    ingest.insert_batch(
        ARTIFACTS.name, ARTIFACTS.rows(artifacts), ARTIFACTS.column_names,
    )
    return query


def test_export_writes_a_file_per_table(seeded, tmp_path):
    result = export_dataset(seeded, tmp_path)
    for table in EXPORT_SCHEMA.tables:
        assert (tmp_path / f'{table.name}.parquet').exists()
    assert result.row_counts == {'repositories': 2, 'artifacts': 3}


def test_parquet_columns_match_the_declared_schema(seeded, tmp_path):
    export_dataset(seeded, tmp_path)
    for table in EXPORT_SCHEMA.tables:
        written = pq.read_schema(tmp_path / f'{table.name}.parquet')
        assert written.names == table.column_names, table.name


def test_relationship_values_are_within_the_enum(seeded, tmp_path):
    export_dataset(seeded, tmp_path)
    column = pq.read_table(
        tmp_path / 'artifacts.parquet', columns=['relationship'],
    )['relationship'].to_pylist()
    allowed = set(EXPORT_SCHEMA.table('artifacts').column('relationship').enum)
    assert set(column) <= allowed


def test_repositories_carry_dependency_counts(seeded, tmp_path):
    export_dataset(seeded, tmp_path)
    rows = pq.read_table(tmp_path / 'repositories.parquet').to_pylist()
    by_repo = {r['repo']: r for r in rows}
    assert by_repo['mastodon']['total_dependencies'] == 2
    assert by_repo['mastodon']['direct_dependencies'] == 1


def test_artifacts_are_sorted_for_row_group_pruning(seeded, tmp_path):
    export_dataset(seeded, tmp_path)
    names = pq.read_table(
        tmp_path / 'artifacts.parquet', columns=['name'],
    )['name'].to_pylist()
    assert names == sorted(names), 'sorted by name keeps lookups cheap'


def test_manifest_describes_every_file(seeded, tmp_path):
    export_dataset(seeded, tmp_path)
    manifest = json.loads((tmp_path / 'manifest.json').read_text())

    assert manifest['schemaVersion'] == EXPORT_SCHEMA.version
    assert manifest['rowCounts'] == {'repositories': 2, 'artifacts': 3}
    files = {f['name']: f for f in manifest['files']}
    assert set(files) == {'repositories.parquet', 'artifacts.parquet'}
    for entry in files.values():
        assert entry['bytes'] > 0
        assert len(entry['sha256']) == 64


def test_manifest_records_the_generator_version(seeded, tmp_path):
    export_dataset(seeded, tmp_path)
    manifest = json.loads((tmp_path / 'manifest.json').read_text())
    assert manifest['generator'].startswith('chatsbom/')


def test_export_is_reproducible(seeded, tmp_path):
    a = export_dataset(seeded, tmp_path / 'a')
    b = export_dataset(seeded, tmp_path / 'b')
    assert a.checksums == b.checksums, 'same input must give the same bytes'


def test_empty_database_still_produces_valid_files(query, tmp_path):
    result = export_dataset(query, tmp_path)
    assert result.row_counts == {'repositories': 0, 'artifacts': 0}
    for table in EXPORT_SCHEMA.tables:
        written = pq.read_schema(tmp_path / f'{table.name}.parquet')
        assert written.names == table.column_names


def test_export_creates_the_output_directory(seeded, tmp_path):
    out = tmp_path / 'nested' / 'deep'
    export_dataset(seeded, out)
    assert (out / 'manifest.json').exists()
