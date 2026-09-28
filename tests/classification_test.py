"""Direct or transitive is judged per ecosystem, never by language (#55).

`relationships_from(read, Language(repo.language))` judged every
artifact against the manifests of the repository's *language*: a
repository GitHub labels TypeScript with a Maven backend had its Maven
artifacts judged against `package.json` (so `unknown` or, worse,
`transitive`), and a Java-labelled one had its npm front end judged
against its poms.
"""
import json

from chatsbom.core.ecosystems import artifact_ecosystem
from chatsbom.core.manifest import classify
from chatsbom.core.manifest import DIRECT
from chatsbom.core.manifest import relationships_from
from chatsbom.core.manifest import TRANSITIVE
from chatsbom.core.manifest import UNKNOWN
from chatsbom.services.db_service import DbService
from tests.db_ingest_test import FakeIngestionRepository
from tests.db_ingest_test import FULL_SHA
from tests.db_ingest_test import ledger_records
from tests.db_ingest_test import make_repo

POM = """
<project xmlns="http://maven.apache.org/POM/4.0.0">
  <dependencies>
    <dependency>
      <groupId>org.springframework.boot</groupId>
      <artifactId>spring-boot-starter-web</artifactId>
    </dependency>
  </dependencies>
</project>
"""

PACKAGE_JSON = '{"dependencies": {"react": "^18"}}'


def test_the_ecosystem_is_the_artifacts_type_else_its_purl():
    assert artifact_ecosystem('java-archive') == 'maven'
    assert artifact_ecosystem('go-module') == 'go'
    assert artifact_ecosystem('python') == 'pypi'
    assert artifact_ecosystem('', 'pkg:golang/github.com/go-chi/chi') == 'go'
    assert artifact_ecosystem('', 'pkg:maven/org.x/y@1') == 'maven'
    assert artifact_ecosystem() is None


def test_each_artifact_is_judged_in_its_own_ecosystem():
    by_ecosystem = relationships_from([
        ('app/server/pom.xml', POM),
        ('app/client/package.json', PACKAGE_JSON),
    ])
    assert set(by_ecosystem) == {'maven', 'npm'}

    assert classify(
        by_ecosystem, 'spring-boot-starter-web', 'java-archive',
    ) == DIRECT
    assert classify(by_ecosystem, 'spring-core', 'java-archive') == TRANSITIVE
    assert classify(by_ecosystem, 'react', 'npm') == DIRECT
    assert classify(by_ecosystem, 'loose-envify', 'npm') == TRANSITIVE
    # The same name in another ecosystem is not declared by these.
    assert classify(by_ecosystem, 'react', 'java-archive') == TRANSITIVE


def test_an_ecosystem_with_no_manifest_is_unknown():
    """A Maven artifact with no pom or Gradle build to judge it by."""
    by_ecosystem = relationships_from([('package.json', PACKAGE_JSON)])
    assert classify(by_ecosystem, 'guava', 'java-archive') == UNKNOWN
    assert classify(by_ecosystem, 'x', 'binary') == UNKNOWN
    assert classify({}, 'react', 'npm') == UNKNOWN


def test_a_typescript_labelled_repository_gets_maven_verdicts(tmp_path):
    """Stirling-PDF and appsmith are TypeScript to GitHub."""
    content = tmp_path / 'content'
    (content / 'app' / 'server').mkdir(parents=True)
    (content / 'app' / 'server' / 'pom.xml').write_text(POM)
    (content / 'app' / 'client').mkdir(parents=True)
    (content / 'app' / 'client' / 'package.json').write_text(PACKAGE_JSON)

    sbom = tmp_path / 'sbom.json'
    sbom.write_text(
        json.dumps({
            'artifacts': [
                {
                    'name': 'spring-boot-starter-web', 'type': 'java-archive',
                    'purl': 'pkg:maven/org.springframework.boot/spring-boot-starter-web@3.2.0',
                },
                {'name': 'spring-core', 'type': 'java-archive'},
                {'name': 'react', 'type': 'npm'},
            ],
        }),
    )
    record = make_repo(language='TypeScript').model_dump(mode='json')
    record['sbom_path'] = str(sbom)
    record['local_content_path'] = str(content)
    listing = tmp_path / 'list.jsonl'
    listing.write_text(json.dumps(record) + '\n')

    fake = FakeIngestionRepository()
    DbService().ingest_from_list(ledger_records(listing), fake)

    verdicts = {
        r['name']: r['relationship'] for r in fake.rows_for('artifacts')
    }
    assert verdicts == {
        'spring-boot-starter-web': DIRECT,
        'spring-core': TRANSITIVE,
        'react': DIRECT,
    }
    [repository] = fake.rows_for('repositories')
    assert repository['manifest_sources'] == [
        'app/client/package.json', 'app/server/pom.xml',
    ]
    assert repository['ecosystems'] == ['maven', 'npm']
    assert repository['sbom_commit_sha'] == FULL_SHA


def test_a_gradle_catalog_reference_is_a_declaration_now():
    """`libs.x.y` left a Gradle build incomplete, so every Maven name no
    other file declared was `unknown`. With the catalog read it is a
    name, and the rest are transitive."""
    catalog = (
        '[libraries]\n'
        'spring-boot-starter-web = '
        '{ module = "org.springframework.boot:spring-boot-starter-web" }\n'
    )
    by_ecosystem = relationships_from([
        ('gradle/libs.versions.toml', catalog),
        (
            'app/build.gradle.kts',
            'dependencies { implementation(libs.spring.boot.starter.web) }\n',
        ),
    ])
    maven = by_ecosystem['maven']
    assert maven.sources == ('app/build.gradle.kts',), 'the catalog is context'
    assert maven.incomplete == ()
    assert maven.relationship_of('spring-boot-starter-web') == DIRECT
    assert maven.relationship_of('spring-web') == TRANSITIVE
