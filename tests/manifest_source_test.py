"""`source = 'manifest'`: what Gradle build files declare (owner decision D1).

Syft 1.41.2 produced no artifact from 40 of 40 sampled Gradle-only
projects, 1.52.0 reads no Gradle file either, and GitHub's graph for
halo lists no Spring starter, so these rows are the only source that
says a Gradle-only repository uses Spring Boot. They are declared,
never resolved, versions.
"""
import json
from datetime import datetime
from datetime import timezone

from chatsbom.core.documents import DEPGRAPH
from chatsbom.core.documents import FILE_MANIFESTS
from chatsbom.core.documents import FILES
from chatsbom.core.documents import SYFT
from chatsbom.core.manifest import relationships_from
from chatsbom.core.manifest import sources_of
from chatsbom.models.provenance import CONSTRAINT
from chatsbom.models.provenance import MANIFEST
from chatsbom.models.provenance import UNVERSIONED
from chatsbom.services.db_service import DbService
from chatsbom.services.db_service import ecosystems_of
from tests.db_ingest_test import FULL_SHA
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


def test_a_repository_has_all_three_sources(tmp_path):
    """A commit's Syft document and the manifests beside it, and the
    dependency graph, as the warehouse reads a repository
    (`warehouse/store.py`)."""
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
    service = DbService()
    repo = make_repo(language='TypeScript')
    manifests = FILE_MANIFESTS.for_repository(repo.id, str(content))
    by_ecosystem = relationships_from(manifests)
    syft_rows, declared_rows = service.scan_rows(
        FILES.get(SYFT, repo.id, str(sbom)), manifests, repo.id,
        {'sbom_ref': 'v3.2.0', 'sbom_commit_sha': FULL_SHA}, by_ecosystem,
    )
    document = FILES.get(DEPGRAPH, repo.id, str(graph))
    assert document is not None
    graph_rows = service.parse_dependency_graph(
        document, repo.id, service.parse_repository(repo),
    )

    written = [*syft_rows, *declared_rows, *graph_rows]
    assert {r['source'] for r in written} == {
        'syft', 'github-depgraph', 'manifest',
    }
    declared = {r['name'] for r in declared_rows}
    assert declared == {'spring-boot-starter-web', 'lombok'}, (
        'libs.guava has no catalog here, so it is not guessed'
    )
    assert ecosystems_of(written, manifests) == ['maven', 'npm']
    assert sources_of(by_ecosystem) == ['app/build.gradle', 'ui/package.json']
    # Syft's scan, and the manifests with it, are at the commit.
    assert {
        r['sbom_commit_sha'] for r in [*syft_rows, *declared_rows]
    } == {FULL_SHA}


def test_ecosystems_come_from_artifacts_and_manifests():
    assert ecosystems_of(
        [
            {'type': 'java-archive'}, {'type': 'binary'},
            {'type': '', 'purl': 'pkg:golang/x/y'},
        ],
        [('ui/package.json', None), ('README.md', None)],
    ) == ['go', 'maven', 'npm']


def test_a_podspec_dependency_is_a_manifest_row():
    """A CocoaPods library's spec (#55 pilot), stored like a Gradle
    declaration: direct, its requirement a constraint."""
    spec = (
        "Pod::Spec.new do |s|\n  s.name = 'Kit'\n"
        "  s.dependency 'Alamofire', '~> 5.0'\n  s.dependency 'SnapKit'\nend\n"
    )
    found = {r['name']: r for r in rows([('Kit.podspec', spec)])}

    alamofire = found['Alamofire']
    assert alamofire['source'] == MANIFEST
    assert alamofire['type'] == 'cocoapods'
    assert alamofire['relationship'] == 'direct'
    assert (alamofire['version'], alamofire['version_kind']) == (
        '~> 5.0', CONSTRAINT,
    )
    assert alamofire['found_by'] == 'chatsbom-podspec'
    assert alamofire['purl'].startswith('pkg:cocoapods/Alamofire@')
    assert alamofire['sbom_commit_sha'] == FULL_SHA
    assert found['SnapKit']['version_kind'] == UNVERSIONED
    assert ecosystems_of(rows([('Kit.podspec', spec)])) == ['cocoapods']


def test_buildsrc_constants_become_manifest_rows():
    """ZacSweers/CatchUp: every dependency a `deps.*` constant."""
    found = rows([
        (
            'buildSrc/src/main/kotlin/dependencies.kt',
            'object deps { object okhttp { '
            'const val core = "com.squareup.okhttp3:okhttp:3.10.0" } }',
        ),
        (
            'app/build.gradle.kts',
            'dependencies {\n  implementation(deps.okhttp.core)\n}\n',
        ),
    ])
    assert [(r['name'], r['version'], r['found_by']) for r in found] == [
        ('okhttp', '3.10.0', 'chatsbom-gradle-constant'),
    ]
