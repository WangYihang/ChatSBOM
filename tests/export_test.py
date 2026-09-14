"""CSV export: streamed, with framework resolved by dict lookup."""
import csv

import pytest

from chatsbom.commands.db.export import COLUMNS
from chatsbom.commands.db.export import export_rows
from chatsbom.commands.db.export import write_csv
from chatsbom.core.schema import ARTIFACTS
from chatsbom.core.schema import REPOSITORIES
from chatsbom.models.framework_index import FrameworkIndex
from chatsbom.models.relationship import DIRECT
from chatsbom.models.relationship import TRANSITIVE
from tests.conftest import requires_clickhouse
from tests.repository_query_test import artifact_row
from tests.repository_query_test import repo_row

pytestmark = requires_clickhouse


@pytest.fixture
def index() -> FrameworkIndex:
    return FrameworkIndex.build()


@pytest.fixture
def seeded(ingest, query):
    repos = [
        repo_row(id=1, owner='gin-app', repo='api', stars=900, language='go'),
        repo_row(id=2, owner='plain', repo='lib', stars=100, language='go'),
    ]
    artifacts = [
        artifact_row(
            repository_id=1, artifact_id='a1',
            name='github.com/gin-gonic/gin', relationship=DIRECT,
        ),
        artifact_row(
            repository_id=1, artifact_id='a2',
            name='golang.org/x/sys', relationship=TRANSITIVE,
        ),
        artifact_row(
            repository_id=2, artifact_id='a3',
            name='golang.org/x/sys', relationship=TRANSITIVE,
        ),
    ]
    ingest.insert_batch(
        REPOSITORIES.name, REPOSITORIES.rows(repos), REPOSITORIES.column_names,
    )
    ingest.insert_batch(
        ARTIFACTS.name, ARTIFACTS.rows(artifacts), ARTIFACTS.column_names,
    )
    return query


def _as_dicts(rows):
    return [dict(zip(COLUMNS, row)) for row in rows]


def test_export_detects_the_framework(seeded, index):
    rows = _as_dicts(export_rows(seeded, index))
    by_repo = {r['repo']: r for r in rows}
    assert by_repo['api']['framework'] == 'gin'
    assert by_repo['lib']['framework'] == ''


def test_export_counts_direct_and_total_dependencies(seeded, index):
    by_repo = {r['repo']: r for r in _as_dicts(export_rows(seeded, index))}
    assert by_repo['api']['direct_dependencies'] == 1
    assert by_repo['api']['total_dependencies'] == 2
    assert by_repo['lib']['direct_dependencies'] == 0


def test_export_is_ordered_by_stars(seeded, index):
    rows = _as_dicts(export_rows(seeded, index))
    assert [r['repo'] for r in rows] == ['api', 'lib']


def test_web_only_drops_repositories_without_a_framework(seeded, index):
    rows = _as_dicts(export_rows(seeded, index, web_only=True))
    assert [r['repo'] for r in rows] == ['api']


def test_export_includes_every_repository_even_with_no_artifacts(
    ingest, query, index,
):
    ingest.insert_batch(
        REPOSITORIES.name,
        REPOSITORIES.rows([repo_row(id=9, owner='empty', repo='repo')]),
        REPOSITORIES.column_names,
    )
    rows = _as_dicts(export_rows(query, index))
    assert [r['repo'] for r in rows] == ['repo']
    assert rows[0]['total_dependencies'] == 0


def test_export_streams_rather_than_materialising(seeded, index):
    """export_rows is a generator, so the first row arrives before the last."""
    rows = export_rows(seeded, index)
    assert next(iter(rows)) is not None


def test_write_csv_writes_a_header_and_counts_rows(seeded, index, tmp_path):
    out = tmp_path / 'projects.csv'
    written = write_csv(out, export_rows(seeded, index))

    assert written == 2
    with open(out, newline='') as f:
        parsed = list(csv.DictReader(f))
    assert list(parsed[0]) == COLUMNS
    assert parsed[0]['repo'] == 'api'


def test_write_csv_creates_missing_directories(seeded, index, tmp_path):
    out = tmp_path / 'nested' / 'dir' / 'projects.csv'
    assert write_csv(out, export_rows(seeded, index)) == 2
    assert out.exists()
