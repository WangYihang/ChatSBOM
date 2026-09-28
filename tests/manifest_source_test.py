"""`source = 'manifest'`: what Gradle build files declare (owner decision D1).

Syft 1.41.2 produced no artifact from 40 of 40 sampled Gradle-only
projects, and GitHub's graph for halo lists no Spring starter, so these
rows are the only source that says a Gradle-only repository uses Spring
Boot. They are declared, never resolved, versions.
"""
import json
from datetime import datetime
from datetime import timezone

from chatsbom.models.provenance import CONSTRAINT
from chatsbom.models.provenance import MANIFEST
from chatsbom.models.provenance import UNVERSIONED
from chatsbom.services.db_service import DbService
from chatsbom.services.db_service import ecosystems_of
from tests.db_ingest_test import FakeIngestionRepository
from tests.db_ingest_test import FULL_SHA
from tests.db_ingest_test import ledger_records
from tests.db_ingest_test import make_repo

BUILD = """
plugins {
    id 'org.springframework.boot' version '3.2.4'
}
dependencies {
    implementation 'org.springframework.boot:spring-boot-starter-web'
    implementation 'org.projectlombok:lombok'
    implementation libs.guava
}
"""

CATALOG = '[libraries]\nguava = "com.google.guava:guava:33.0.0-jre"\n'

SCANNED = datetime(2026, 9, 20, 4, 0, tzinfo=timezone.utc)


def rows(manifests, sha=FULL_SHA):
    service = DbService()
    repo_row = service.parse_repository(make_repo())
    repo_row['sbom_commit_sha'] = sha
    return service.parse_manifests(
        manifests, 4321, repo_row, observed_at=SCANNED,
    )


def test_a_gradle_declaration_is_a_manifest_row():
    found = {
        r['name']: r for r in rows([
            ('gradle/libs.versions.toml', CATALOG),
            ('app/build.gradle', BUILD),
        ])
    }
    web = found['spring-boot-starter-web']
    assert web['source'] == MANIFEST
    assert web['type'] == 'maven'
    assert web['relationship'] == 'direct'
    assert web['purl'] == (
        'pkg:maven/org.springframework.boot/spring-boot-starter-web@3.2.4'
    )
    assert (web['version'], web['version_kind']) == ('3.2.4', CONSTRAINT)
    assert web['found_by'] == 'chatsbom-gradle'
    assert web['sbom_commit_sha'] == FULL_SHA
    assert web['sbom_ref'] == 'v3.2.0'
    assert web['observed_at'] == SCANNED
    assert web['artifact_id'].startswith('app/build.gradle#')

    lombok = found['lombok']
    assert (lombok['version'], lombok['version_kind']) == ('', UNVERSIONED)

    guava = found['guava']
    assert guava['found_by'] == 'chatsbom-gradle-catalog'
    assert guava['version'] == '33.0.0-jre'


def test_a_declared_version_is_never_resolved():
    assert {
        r['version_kind'] for r in rows([('build.gradle', BUILD)])
    } <= {CONSTRAINT, UNVERSIONED}


def test_a_pom_gives_no_manifest_row():
    """Syft's `java-pom-cataloger` reads it: a row here would count it
    twice."""
    assert rows([('pom.xml', '<project/>')]) == []


def test_no_scan_commit_means_no_row():
    """A row is current by the scan's commit, and forgotten by it."""
    assert rows([('build.gradle', BUILD)], sha='') == []


def test_the_ingest_writes_all_three_sources(tmp_path):
    content = tmp_path / 'content'
    (content / 'app').mkdir(parents=True)
    (content / 'app' / 'build.gradle').write_text(BUILD)
    (content / 'ui').mkdir()
    (content / 'ui' / 'package.json').write_text(
        '{"dependencies": {"vue": "^3"}}',
    )
    sbom = tmp_path / 'sbom.json'
    sbom.write_text(
        json.dumps({
            'artifacts': [{'name': 'vue', 'version': '3.4.0', 'type': 'npm'}],
        }),
    )
    graph = tmp_path / 'graph.json'
    graph.write_text(
        json.dumps({
            'sbom': {
                'creationInfo': {'created': '2026-09-14T00:00:00Z'},
                'packages': [{
                    'SPDXID': 'p1', 'name': 'com.google.guava:guava',
                    'externalRefs': [{
                        'referenceType': 'purl',
                        'referenceLocator': 'pkg:maven/com.google.guava/guava',
                    }],
                }],
            },
        }),
    )
    record = make_repo(language='TypeScript').model_dump(mode='json')
    record['sbom_path'] = str(sbom)
    record['depgraph_path'] = str(graph)
    record['local_content_path'] = str(content)
    listing = tmp_path / 'list.jsonl'
    listing.write_text(json.dumps(record) + '\n')

    fake = FakeIngestionRepository()
    stats = DbService().ingest_from_list(ledger_records(listing), fake)

    assert stats.failed == 0
    written = fake.rows_for('artifacts')
    assert {r['source'] for r in written} == {
        'syft', 'github-depgraph', 'manifest',
    }
    declared = {r['name'] for r in written if r['source'] == 'manifest'}
    assert declared == {'spring-boot-starter-web', 'lombok'}, (
        'libs.guava has no catalog here, so it is not guessed'
    )
    [repository] = fake.rows_for('repositories')
    assert repository['ecosystems'] == ['maven', 'npm']
    assert repository['manifest_sources'] == [
        'app/build.gradle', 'ui/package.json',
    ]
    # Syft's scan, and the manifests with it, are at the record's commit.
    assert {
        r['sbom_commit_sha'] for r in written if r['source'] != 'github-depgraph'
    } == {FULL_SHA}


def test_ecosystems_come_from_artifacts_and_manifests():
    assert ecosystems_of(
        [
            {'type': 'java-archive'}, {'type': 'binary'},
            {'type': '', 'purl': 'pkg:golang/x/y'},
        ],
        [('ui/package.json', None), ('README.md', None)],
    ) == ['go', 'maven', 'npm']
