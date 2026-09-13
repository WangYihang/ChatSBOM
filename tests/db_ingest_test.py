"""Ingestion tests: column contracts, SBOM provenance, and stats accuracy."""
import json

import pytest

from chatsbom.core.manifest import resolve_relationships
from chatsbom.core.schema import ARTIFACTS
from chatsbom.core.schema import REPOSITORIES
from chatsbom.models.language import Language
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


# --- dependency relationship ----------------------------------------------

def test_artifacts_are_marked_direct_or_transitive(service, tmp_path):
    content = tmp_path / 'content'
    content.mkdir()
    (content / 'Gemfile').write_text("gem 'mail'\n")

    sbom = tmp_path / 'sbom.json'
    sbom.write_text(
        json.dumps({
            'artifacts': [
                {'name': 'mail', 'version': '2.9.0', 'type': 'gem'},
                {'name': 'mini_mime', 'version': '1.1', 'type': 'gem'},
            ],
        }),
    )

    deps = resolve_relationships(content, Language.RUBY)
    rows = service.parse_artifacts(
        sbom, 1, service.parse_repository(make_repo()), direct_deps=deps,
    )
    by_name = {r['name']: r['relationship'] for r in rows}
    assert by_name == {'mail': 'direct', 'mini_mime': 'transitive'}


def test_artifacts_default_to_unknown_relationship(service, tmp_path):
    sbom = tmp_path / 'sbom.json'
    sbom.write_text(
        json.dumps(
            {'artifacts': [{'name': 'mail', 'type': 'gem'}]},
        ),
    )
    rows = service.parse_artifacts(
        sbom, 1, service.parse_repository(make_repo()),
    )
    assert rows[0]['relationship'] == 'unknown'


def test_ingest_classifies_relationships_from_local_content(service, tmp_path):
    content = tmp_path / 'content'
    content.mkdir()
    (content / 'Gemfile').write_text("gem 'mail'\n")

    sbom = tmp_path / 'sbom.json'
    sbom.write_text(
        json.dumps({
            'artifacts': [
                {'name': 'mail', 'type': 'gem'},
                {'name': 'mini_mime', 'type': 'gem'},
            ],
        }),
    )

    repo = make_repo().model_dump(mode='json')
    repo['sbom_path'] = str(sbom)
    repo['local_content_path'] = str(content)
    listing = tmp_path / 'list.jsonl'
    listing.write_text(json.dumps(repo) + '\n')

    fake = FakeIngestionRepository()
    service.ingest_from_list(listing, fake)

    rows = {r['name']: r['relationship'] for r in fake.rows_for('artifacts')}
    assert rows == {'mail': 'direct', 'mini_mime': 'transitive'}


def test_ingest_without_local_content_leaves_relationship_unknown(service, tmp_path):
    sbom = tmp_path / 'sbom.json'
    sbom.write_text(
        json.dumps(
            {'artifacts': [{'name': 'mail', 'type': 'gem'}]},
        ),
    )
    repo = make_repo().model_dump(mode='json')
    repo['sbom_path'] = str(sbom)
    repo.pop('local_content_path', None)
    listing = tmp_path / 'list.jsonl'
    listing.write_text(json.dumps(repo) + '\n')

    fake = FakeIngestionRepository()
    service.ingest_from_list(listing, fake)
    assert fake.rows_for('artifacts')[0]['relationship'] == 'unknown'


def test_ingest_unknown_language_does_not_break_classification(service, tmp_path):
    sbom = tmp_path / 'sbom.json'
    sbom.write_text(json.dumps({'artifacts': [{'name': 'x', 'type': 'gem'}]}))
    content = tmp_path / 'content'
    content.mkdir()

    repo = make_repo(language='Brainfuck').model_dump(mode='json')
    repo['sbom_path'] = str(sbom)
    repo['local_content_path'] = str(content)
    listing = tmp_path / 'list.jsonl'
    listing.write_text(json.dumps(repo) + '\n')

    fake = FakeIngestionRepository()
    stats = service.ingest_from_list(listing, fake)
    assert stats.failed == 0
    assert fake.rows_for('artifacts')[0]['relationship'] == 'unknown'


# --- the repository list must not shrink ----------------------------------

def test_the_sbom_ledger_decides_which_repositories_are_ingested(
    service, tmp_path,
):
    """A partial depgraph ledger must not shrink the corpus.

    `db index` preferred the depgraph ledger, described in a comment as a
    superset. `github depgraph --limit 120` makes it a *subset*: Java went
    from 1,215 indexed repositories to 87, silently, because the shorter
    ledger became the input list.
    """
    sbom = tmp_path / 'sbom.json'
    sbom.write_text(json.dumps({'artifacts': [{'name': 'a', 'type': 'gem'}]}))

    def record(repo_id: int) -> dict:
        row = make_repo(id=repo_id).model_dump(mode='json')
        row['sbom_path'] = str(sbom)
        return row

    full = tmp_path / 'sbom.jsonl'
    full.write_text('\n'.join(json.dumps(record(i)) for i in range(1, 6)))

    partial = tmp_path / 'depgraph.jsonl'
    partial.write_text(json.dumps(record(1)))

    fake = FakeIngestionRepository()
    stats = service.ingest_from_list(full, fake, depgraph_index=partial)

    assert stats.repos == 5, 'every repository in the SBOM ledger'


def test_depgraph_documents_are_attached_where_present(service, tmp_path):
    sbom = tmp_path / 'sbom.json'
    sbom.write_text(json.dumps({'artifacts': [{'name': 'a', 'type': 'gem'}]}))

    depgraph = tmp_path / 'dg.json'
    depgraph.write_text(
        json.dumps({
            'sbom': {
                'packages': [{
                    'SPDXID': 'p1', 'name': 'org.x:y',
                    'externalRefs': [{
                        'referenceType': 'purl',
                        'referenceLocator': 'pkg:maven/org.x/y',
                    }],
                }],
            },
        }),
    )

    covered = make_repo(id=1).model_dump(mode='json')
    covered['sbom_path'] = str(sbom)
    covered['depgraph_path'] = str(depgraph)

    uncovered = make_repo(id=2).model_dump(mode='json')
    uncovered['sbom_path'] = str(sbom)

    full = tmp_path / 'sbom.jsonl'
    full.write_text(f'{json.dumps(covered)}\n{json.dumps(uncovered)}\n')

    index = tmp_path / 'depgraph.jsonl'
    index.write_text(json.dumps(covered) + '\n')

    fake = FakeIngestionRepository()
    stats = service.ingest_from_list(full, fake, depgraph_index=index)

    assert stats.repos == 2
    sources = {r['source'] for r in fake.rows_for('artifacts')}
    assert sources == {'syft', 'github-depgraph'}


def test_a_missing_depgraph_index_is_not_an_error(service, tmp_path):
    sbom = tmp_path / 'sbom.json'
    sbom.write_text(json.dumps({'artifacts': []}))
    row = make_repo().model_dump(mode='json')
    row['sbom_path'] = str(sbom)
    listing = tmp_path / 'l.jsonl'
    listing.write_text(json.dumps(row) + '\n')

    stats = service.ingest_from_list(
        listing, FakeIngestionRepository(),
        depgraph_index=tmp_path / 'absent.jsonl',
    )
    assert stats.repos == 1
