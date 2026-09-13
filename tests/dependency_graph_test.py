"""Adapting GitHub's dependency-graph SPDX into our artifact rows.

Shapes here are copied from live responses for macrozheng/mall (Maven,
which Syft reports 8 packages for) and mikel/mail (a gemspec library,
which Syft reports nothing for).
"""
import pytest

from chatsbom.models.provenance import CONSTRAINT
from chatsbom.models.provenance import DEPGRAPH
from chatsbom.models.provenance import UNVERSIONED
from chatsbom.models.relationship import DIRECT
from chatsbom.services.dependency_graph_service import parse_spdx_document


MAVEN_SBOM = {
    'sbom': {
        'spdxVersion': 'SPDX-2.3',
        'SPDXID': 'SPDXRef-DOCUMENT',
        'name': 'com.github.macrozheng/mall',
        'packages': [
            {
                'SPDXID': 'SPDXRef-Repository',
                'name': 'macrozheng/mall',
                'externalRefs': [{
                    'referenceType': 'purl',
                    'referenceCategory': 'PACKAGE-MANAGER',
                    'referenceLocator': 'pkg:github/macrozheng/mall',
                }],
            },
            {
                'SPDXID': 'SPDXRef-maven-1',
                'name': 'org.springframework.boot:spring-boot-starter-amqp',
                'versionInfo': None,
                'externalRefs': [{
                    'referenceType': 'purl',
                    'referenceCategory': 'PACKAGE-MANAGER',
                    'referenceLocator': 'pkg:maven/org.springframework.boot/spring-boot-starter-amqp',
                }],
            },
            {
                'SPDXID': 'SPDXRef-actions-1',
                'name': 'actions/checkout',
                'versionInfo': 'v4',
                'externalRefs': [{
                    'referenceType': 'purl',
                    'referenceCategory': 'PACKAGE-MANAGER',
                    'referenceLocator': 'pkg:githubactions/actions/checkout@v4',
                }],
            },
        ],
        'relationships': [
            {
                'spdxElementId': 'SPDXRef-DOCUMENT',
                'relationshipType': 'DESCRIBES',
                'relatedSpdxElement': 'SPDXRef-Repository',
            },
            {
                'spdxElementId': 'SPDXRef-Repository',
                'relationshipType': 'DEPENDS_ON',
                'relatedSpdxElement': 'SPDXRef-maven-1',
            },
        ],
    },
}

GEM_SBOM = {
    'sbom': {
        'spdxVersion': 'SPDX-2.3',
        'packages': [
            {
                'SPDXID': 'SPDXRef-gem-mini-mime',
                'name': 'mini_mime',
                'versionInfo': '>= 0',
                'externalRefs': [{
                    'referenceType': 'purl',
                    'referenceCategory': 'PACKAGE-MANAGER',
                    'referenceLocator': 'pkg:gem/mini_mime',
                }],
            },
        ],
        'relationships': [],
    },
}


def test_maven_packages_are_extracted():
    rows = parse_spdx_document(MAVEN_SBOM)
    names = {r['name'] for r in rows}
    assert 'org.springframework.boot:spring-boot-starter-amqp' in names


def test_the_repository_itself_is_not_a_dependency():
    rows = parse_spdx_document(MAVEN_SBOM)
    assert 'macrozheng/mall' not in {r['name'] for r in rows}


def test_github_actions_are_not_software_dependencies():
    """A workflow's actions are not what the project depends on."""
    rows = parse_spdx_document(MAVEN_SBOM)
    assert 'actions/checkout' not in {r['name'] for r in rows}
    assert all(r['type'] != 'githubactions' for r in rows)


def test_ecosystem_comes_from_the_purl():
    rows = parse_spdx_document(MAVEN_SBOM)
    row = next(r for r in rows if r['name'].startswith('org.springframework'))
    assert row['type'] == 'maven'
    assert row['purl'].startswith('pkg:maven/')


def test_everything_reported_is_a_declared_dependency():
    """GitHub's graph is flat: root DEPENDS_ON each package, no tree."""
    rows = parse_spdx_document(MAVEN_SBOM)
    assert rows
    assert all(r['relationship'] == DIRECT for r in rows)


def test_source_is_recorded():
    rows = parse_spdx_document(MAVEN_SBOM)
    assert all(r['source'] == DEPGRAPH for r in rows)


def test_missing_version_is_unversioned_not_empty_resolved():
    rows = parse_spdx_document(MAVEN_SBOM)
    row = next(r for r in rows if r['name'].startswith('org.springframework'))
    assert row['version'] == ''
    assert row['version_kind'] == UNVERSIONED


def test_constraint_versions_are_marked_as_such():
    """`>= 0` must never be charted as if it were a resolved version."""
    rows = parse_spdx_document(GEM_SBOM)
    assert rows[0]['version'] == '>= 0'
    assert rows[0]['version_kind'] == CONSTRAINT


def test_artifact_id_is_the_spdx_id():
    rows = parse_spdx_document(MAVEN_SBOM)
    assert any(r['artifact_id'] == 'SPDXRef-maven-1' for r in rows)


def test_empty_document_yields_nothing():
    assert parse_spdx_document({'sbom': {'packages': []}}) == []


def test_missing_sbom_key_is_an_error():
    with pytest.raises(ValueError, match='sbom'):
        parse_spdx_document({'unexpected': True})


def test_packages_without_a_purl_are_skipped():
    """Without a purl there is no ecosystem, so the row is unusable."""
    rows = parse_spdx_document({
        'sbom': {'packages': [{'SPDXID': 'x', 'name': 'mystery'}]},
    })
    assert rows == []
