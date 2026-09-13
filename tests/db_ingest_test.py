"""Ingestion tests: column contracts, SBOM provenance, and stats accuracy."""
import json

import pytest

from chatsbom.core.schema import ARTIFACTS
from chatsbom.core.schema import REPOSITORIES
from chatsbom.models.repository import Repository
from chatsbom.services.db_service import DbService


FULL_SHA = '8a79c788a54745c467cf6a1a9d438c9c91881001'


def make_repo(**overrides):
    data = {
        'id': 4321,
        'owner': 'discourse',
        'name': 'discourse',
        'stargazers_count': 46265,
        'html_url': 'https://github.com/discourse/discourse',
        'language': 'ruby',
        'default_branch': 'main',
        'download_target': {
            'ref': 'v3.2.0',
            'ref_type': 'release',
            'commit_sha': FULL_SHA,
            'commit_sha_short': FULL_SHA[:7],
        },
    }
    data.update(overrides)
    return Repository.model_validate(data)


@pytest.fixture
def service():
    return DbService()


class FakeIngestionRepository:
    """Records inserts so tests can assert on what would reach ClickHouse."""

    def __init__(self):
        self.batches: list[tuple[str, list, list]] = []

    def insert_batch(self, table, data, columns):
        self.batches.append((table, data, columns))

    def rows_for(self, table: str) -> list[dict]:
        """Re-key recorded rows by column name."""
        out: list[dict] = []
        for name, data, columns in self.batches:
            if name != table:
                continue
            out.extend(dict(zip(columns, row)) for row in data)
        return out


# --- column contract -------------------------------------------------------

def test_parse_repository_returns_column_keyed_mapping(service):
    row = service.parse_repository(make_repo())
    assert set(row) == set(REPOSITORIES.columns)
    assert row['owner'] == 'discourse'
    assert row['stars'] == 46265


def test_parse_repository_output_projects_cleanly(service):
    """The mapping must satisfy the table contract exactly."""
    REPOSITORIES.row(service.parse_repository(make_repo()))


def test_repository_sbom_provenance_from_download_target(service):
    row = service.parse_repository(make_repo())
    assert row['sbom_ref'] == 'v3.2.0'
    assert row['sbom_ref_type'] == 'release'
    assert row['sbom_commit_sha'] == FULL_SHA
    assert row['sbom_commit_sha_short'] == FULL_SHA[:7]


# --- the off-by-one regression --------------------------------------------

def test_artifacts_inherit_full_sha_and_real_ref(service, tmp_path):
    """Artifacts used to get sbom_ref_type and the 7-char sha instead."""
    sbom = tmp_path / 'sbom.json'
    sbom.write_text(
        json.dumps({
            'artifacts': [{
                'id': 'abc123',
                'name': 'mail',
                'version': '2.9.0',
                'type': 'gem',
                'purl': 'pkg:gem/mail@2.9.0',
                'foundBy': 'ruby-gemfile-cataloger',
                'licenses': [{'value': 'MIT'}],
            }],
        }),
    )

    repo_row = service.parse_repository(make_repo())
    artifacts = service.parse_artifacts(sbom, repo_id=4321, repo_row=repo_row)

    assert len(artifacts) == 1
    art = artifacts[0]
    assert art['sbom_ref'] == 'v3.2.0', 'must be the ref, not the ref type'
    assert art['sbom_commit_sha'] == FULL_SHA, 'must be the full sha'
    assert art['name'] == 'mail'
    assert art['licenses'] == ['MIT']
    ARTIFACTS.row(art)


def test_parse_artifacts_normalises_license_shapes(service, tmp_path):
    sbom = tmp_path / 'sbom.json'
    sbom.write_text(
        json.dumps({
            'artifacts': [{
                'name': 'x', 'version': '1', 'type': 'gem',
                'licenses': [
                    {'value': 'MIT'},
                    {'spdxExpression': 'Apache-2.0'},
                    {'name': 'BSD-3-Clause'},
                    'ISC',
                    {},
                ],
            }],
        }),
    )
    row = service.parse_artifacts(
        sbom, 1, service.parse_repository(make_repo()),
    )[0]
    assert row['licenses'] == ['MIT', 'Apache-2.0', 'BSD-3-Clause', 'ISC']


def test_parse_artifacts_missing_file_is_empty(service, tmp_path):
    assert service.parse_artifacts(
        tmp_path / 'nope.json', 1, service.parse_repository(make_repo()),
    ) == []


def test_parse_artifacts_reports_unreadable_sbom(service, tmp_path):
    bad = tmp_path / 'sbom.json'
    bad.write_text('{not json')
    with pytest.raises(ValueError, match='sbom'):
        service.parse_artifacts(
            bad, 1, service.parse_repository(make_repo()),
        )


# --- stats accuracy --------------------------------------------------------

def _write_list(tmp_path, sbom_path, count=1):
    repo = make_repo().model_dump(mode='json')
    repo['sbom_path'] = str(sbom_path)
    p = tmp_path / 'list.jsonl'
    with open(p, 'w') as f:
        for i in range(count):
            repo['id'] = 4321 + i
            f.write(json.dumps(repo) + '\n')
    return p


def test_artifact_count_is_not_doubled(service, tmp_path):
    sbom = tmp_path / 'sbom.json'
    sbom.write_text(
        json.dumps({
            'artifacts': [
                {'name': 'a', 'version': '1', 'type': 'gem'},
                {'name': 'b', 'version': '2', 'type': 'gem'},
                {'name': 'c', 'version': '3', 'type': 'gem'},
            ],
        }),
    )
    fake = FakeIngestionRepository()
    stats = service.ingest_from_list(_write_list(tmp_path, sbom), fake)

    assert stats.repos == 1
    assert stats.artifacts == 3, 'was reported as 6'
    assert stats.failed == 0
    assert len(fake.rows_for('artifacts')) == 3


def test_repository_without_sbom_path_is_skipped_not_failed(service, tmp_path):
    repo = make_repo().model_dump(mode='json')
    repo.pop('sbom_path', None)
    p = tmp_path / 'list.jsonl'
    p.write_text(json.dumps(repo) + '\n')

    fake = FakeIngestionRepository()
    stats = service.ingest_from_list(p, fake)

    assert stats.repos == 1
    assert stats.artifacts == 0
    assert stats.skipped == 1
    assert stats.failed == 0


def test_ingest_honours_limit(service, tmp_path):
    sbom = tmp_path / 'sbom.json'
    sbom.write_text(json.dumps({'artifacts': []}))
    fake = FakeIngestionRepository()
    stats = service.ingest_from_list(
        _write_list(tmp_path, sbom, count=10), fake, limit=3,
    )
    assert stats.repos == 3


def test_progress_callback_fires_once_per_repository(service, tmp_path):
    sbom = tmp_path / 'sbom.json'
    sbom.write_text(json.dumps({'artifacts': [{'name': 'a', 'type': 'gem'}]}))
    seen = []
    service.ingest_from_list(
        _write_list(tmp_path, sbom, count=4),
        FakeIngestionRepository(),
        progress_callback=lambda: seen.append(1),
    )
    assert len(seen) == 4
