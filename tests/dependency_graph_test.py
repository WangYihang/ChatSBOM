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


# --- transport failures are expected in a batch ---------------------------

class FakeSession:
    def __init__(self, behaviour):
        self._behaviour = behaviour
        self.calls = 0

    def get(self, url, **kwargs):
        self.calls += 1
        result = self._behaviour
        if isinstance(result, Exception):
            raise result
        return result


class FakeResponse:
    def __init__(self, status_code: int, payload=None):
        self.status_code = status_code
        self._payload = payload

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(f'status {self.status_code}')


def service_with(behaviour):
    from types import SimpleNamespace
    from chatsbom.services.dependency_graph_service import DependencyGraphService
    github = SimpleNamespace(session=FakeSession(behaviour))
    return DependencyGraphService(github)


def test_missing_graph_returns_none():
    assert service_with(FakeResponse(404)).fetch('o', 'r') is None


def test_server_error_returns_none():
    """spring-boot answers 500 'Request timed out' for this endpoint."""
    assert service_with(FakeResponse(502)).fetch('o', 'r') is None


def test_retry_exhaustion_returns_none_rather_than_killing_the_batch():
    """The shared session retries 5xx and then raises RetryError.

    A persistent 500 therefore never reaches the status check — it arrives
    as an exception, which used to abort the whole language.
    """
    import requests
    from requests.exceptions import RetryError
    assert service_with(RetryError('max retries')).fetch('o', 'r') is None
    assert service_with(
        requests.ConnectionError(
            'reset',
        ),
    ).fetch('o', 'r') is None
    assert service_with(requests.Timeout('slow')).fetch('o', 'r') is None


def test_successful_fetch_returns_the_payload():
    payload = {'sbom': {'packages': []}}
    assert service_with(FakeResponse(200, payload)).fetch('o', 'r') == payload


def test_artifacts_for_absent_graph_is_none():
    assert service_with(FakeResponse(404)).artifacts_for('o', 'r') is None


def test_artifacts_for_malformed_payload_is_none():
    assert service_with(
        FakeResponse(200, {'nope': 1}),
    ).artifacts_for('o', 'r') is None
