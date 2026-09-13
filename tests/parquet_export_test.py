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


def test_unknown_licences_are_reported_not_hidden(seeded, tmp_path):
    """'We do not know' is a finding about SBOM quality."""
    export_dataset(seeded, tmp_path)
    rows = pq.read_table(tmp_path / 'licenses.parquet').to_pylist()
    assert rows, 'the licence table must not be empty for seeded data'
    assert all('license' in r for r in rows)


def test_history_carries_the_monthly_series(seeded, tmp_path):
    export_dataset(seeded, tmp_path)
    rows = pq.read_table(tmp_path / 'history.parquet').to_pylist()
    mail = [r for r in rows if r['name'] == 'mail']
    assert mail, 'the monthly series is what a snapshot cannot answer'
    assert all(len(r['month']) == 7 for r in mail), 'YYYY-MM'


def test_export_writes_a_file_per_table(seeded, tmp_path):
    result = export_dataset(seeded, tmp_path)
    for table in EXPORT_SCHEMA.tables:
        assert (tmp_path / f'{table.name}.parquet').exists()
    assert result.row_counts['repositories'] == 2
    assert result.row_counts['artifacts'] == 3
    assert 'history' in result.row_counts


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
    assert manifest['rowCounts']['repositories'] == 2
    assert manifest['rowCounts']['artifacts'] == 3
    files = {f['name']: f for f in manifest['files']}
    assert set(files) == {
        'repositories.parquet', 'artifacts.parquet',
        'licenses.parquet', 'history.parquet',
    }
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
    assert result.row_counts == {
        'repositories': 0, 'artifacts': 0, 'licenses': 0, 'history': 0,
    }
    for table in EXPORT_SCHEMA.tables:
        written = pq.read_schema(tmp_path / f'{table.name}.parquet')
        assert written.names == table.column_names


def test_export_creates_the_output_directory(seeded, tmp_path):
    out = tmp_path / 'nested' / 'deep'
    export_dataset(seeded, out)
    assert (out / 'manifest.json').exists()


def test_manifest_sources_reach_the_export(ingest, query, tmp_path):
    """The audit trail behind every direct/transitive verdict.

    This column was declared and then exported as a literal empty array,
    so a `transitive` label was indistinguishable from an unexamined one
    for anyone reading the Parquet.
    """
    ingest.insert_batch(
        REPOSITORIES.name,
        REPOSITORIES.rows([
            repo_row(
                id=5, owner='o', repo='r',
                manifest_sources=['Gemfile', 'x.gemspec'],
            ),
        ]),
        REPOSITORIES.column_names,
    )
    export_dataset(query, tmp_path)
    rows = pq.read_table(
        tmp_path / 'repositories.parquet', columns=['manifest_sources'],
    )['manifest_sources'].to_pylist()
    assert rows == [['Gemfile', 'x.gemspec']]


def test_repository_with_no_manifests_exports_an_empty_list(
    ingest, query, tmp_path,
):
    ingest.insert_batch(
        REPOSITORIES.name,
        REPOSITORIES.rows([repo_row(id=6, manifest_sources=[])]),
        REPOSITORIES.column_names,
    )
    export_dataset(query, tmp_path)
    rows = pq.read_table(
        tmp_path / 'repositories.parquet', columns=['manifest_sources'],
    )['manifest_sources'].to_pylist()
    assert rows == [[]]
