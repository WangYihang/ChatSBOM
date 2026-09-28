"""Adapting GitHub's dependency-graph SPDX into our artifact rows.

Shapes here are copied from live responses for macrozheng/mall (Maven,
which Syft reports 8 packages for) and mikel/mail (a gemspec library,
which Syft reports nothing for).
"""
import pytest
import requests
from requests.exceptions import RetryError

from chatsbom.models.provenance import CONSTRAINT
from chatsbom.models.provenance import DEPGRAPH
from chatsbom.models.provenance import UNVERSIONED
from chatsbom.models.relationship import DIRECT
from chatsbom.models.relationship import TRANSITIVE
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


#: A graph with a second hop, which MAVEN_SBOM does not have.
#:
#: Its absence is why `test_everything_reported_is_a_declared_dependency`
#: passed for as long as it did: the only package that survives the
#: ecosystem filter there is a root dependency, so the fixture satisfied
#: "everything is declared" and "declared means the root depends on it"
#: equally well, and the assertion tested neither.
NESTED_SBOM = {
    'sbom': {
        'spdxVersion': 'SPDX-2.3',
        'SPDXID': 'SPDXRef-DOCUMENT',
        'packages': [
            {
                'SPDXID': 'SPDXRef-Repository',
                'name': 'expressjs/express',
                'externalRefs': [{
                    'referenceType': 'purl',
                    'referenceCategory': 'PACKAGE-MANAGER',
                    'referenceLocator': 'pkg:github/expressjs/express',
                }],
            },
            {
                'SPDXID': 'SPDXRef-npm-body-parser',
                'name': 'body-parser',
                'versionInfo': '1.20.2',
                'externalRefs': [{
                    'referenceType': 'purl',
                    'referenceCategory': 'PACKAGE-MANAGER',
                    'referenceLocator': 'pkg:npm/body-parser@1.20.2',
                }],
            },
            {
                # Reached only through body-parser: inherited, not
                # chosen. This is the row the old code mislabelled.
                'SPDXID': 'SPDXRef-npm-bytes',
                'name': 'bytes',
                'versionInfo': '3.1.2',
                'externalRefs': [{
                    'referenceType': 'purl',
                    'referenceCategory': 'PACKAGE-MANAGER',
                    'referenceLocator': 'pkg:npm/bytes@3.1.2',
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
                'relatedSpdxElement': 'SPDXRef-npm-body-parser',
            },
            {
                'spdxElementId': 'SPDXRef-npm-body-parser',
                'relationshipType': 'DEPENDS_ON',
                'relatedSpdxElement': 'SPDXRef-npm-bytes',
            },
        ],
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


def test_a_root_dependency_is_declared():
    rows = parse_spdx_document(MAVEN_SBOM)
    assert rows
    assert all(r['relationship'] == DIRECT for r in rows)


def test_a_package_reached_through_another_is_inherited():
    """The graph is not flat, and recording it as flat cost the
    dashboard its central claim.

    Every package used to be `direct`, on the belief written into the
    code as "the graph is flat, so everything in it was declared". The
    same wrong reading had already been corrected once in
    `core/edges.py` — measured across 420 documents, 94.4% of Go edges
    and 93.4% of JavaScript edges run between packages, not out of the
    root.

    Weighted by package count over 400 stored documents, 17.3% of
    packages are root dependencies and 82.7% are reached through
    another. So roughly 11 million of 13,263,227 rows claimed to be
    declared when they were inherited, the headline read "70.7% of
    dependency records are declared outright" against a truer 14.0%,
    and the declared-only ranking returned `semver, debug, ms, glob,
    which` — npm plumbing nobody chooses — where the resolved closures
    give `typescript, eslint, prettier, react`.
    """
    rows = {
        r['name']: r['relationship']
        for r in parse_spdx_document(NESTED_SBOM)
    }
    assert rows['body-parser'] == DIRECT, 'the root depends on it'
    assert rows['bytes'] == TRANSITIVE, 'only body-parser depends on it'


def test_a_document_with_no_relationships_claims_nothing_declared():
    """The conservative direction.

    Claiming a dependency was declared is the error that misleads, so a
    document that does not say falls to inherited. None of the 250
    stored documents sampled was actually relationship-free, so this is
    a guard rather than a common path.
    """
    rows = parse_spdx_document(GEM_SBOM)
    assert rows
    assert all(r['relationship'] == TRANSITIVE for r in rows)


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


# --- one of four outcomes, never None ------------------------------------
#
# `fetch` used to answer None for a missing graph, a server error, a
# dropped connection and a refused token alike, so a batch recorded all
# four as "no graph". 890 refusals were counted among "3,133 with no graph
# published" that way.

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
    def __init__(self, status_code: int, payload=None, headers=None):
        self.status_code = status_code
        self._payload = payload
        self.headers = headers or {}

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f'status {self.status_code}')


def service_with(behaviour):
    from types import SimpleNamespace
    from chatsbom.services.dependency_graph_service import DependencyGraphService
    session = FakeSession(behaviour)
    # Both of GitHubService's sessions. Which one `fetch` must go through
    # is pinned end to end, in depgraph_command_test.
    github = SimpleNamespace(session=session, plain_session=session)
    return DependencyGraphService(github)


def test_missing_graph_is_absent():
    """404: never built, or switched off — this endpoint answers both so."""
    result = service_with(FakeResponse(404)).fetch('o', 'r')
    assert result.absent
    assert not result.failed


def test_server_error_is_a_failure_not_a_missing_graph():
    """spring-boot answers 500 'Request timed out' for this endpoint."""
    result = service_with(FakeResponse(502)).fetch('o', 'r')
    assert result.failed
    assert not result.absent


def test_retry_exhaustion_is_a_failure_rather_than_killing_the_batch():
    """The shared session retries 5xx and then raises RetryError.

    A persistent 500 therefore never reaches the status check — it arrives
    as an exception, which used to abort the whole language.
    """
    for error in (
        RetryError('max retries'),
        requests.ConnectionError('reset'),
        requests.Timeout('slow'),
    ):
        result = service_with(error).fetch('o', 'r')
        assert result.failed, error
        assert not result.absent, error


@pytest.mark.parametrize(
    'status,headers',
    [
        (429, {}),
        (403, {'X-RateLimit-Remaining': '0'}),
        (403, {'X-RateLimit-Remaining': '4000', 'Retry-After': '60'}),
    ],
    ids=['429', '403-no-quota-left', '403-secondary-limit'],
)
def test_a_refused_token_is_not_a_missing_graph(status, headers):
    """A refusal is about the token, not the repository — and every
    later request with the same token would be refused the same way."""
    result = service_with(
        FakeResponse(status, headers=headers),
    ).fetch('o', 'r')
    assert result.rate_limited
    assert not result.absent


def test_a_forbidden_repository_is_a_failure_not_a_missing_graph():
    """A 403 with quota left and no Retry-After refuses the repository,
    not the token. It does not say there is no graph, either."""
    result = service_with(
        FakeResponse(403, headers={'X-RateLimit-Remaining': '4000'}),
    ).fetch('o', 'r')
    assert result.failed
    assert not result.absent


def test_successful_fetch_returns_the_payload():
    payload = {'sbom': {'packages': []}}
    result = service_with(FakeResponse(200, payload)).fetch('o', 'r')
    assert result.changed
    assert result.payload == payload


def test_a_body_that_is_not_a_document_is_a_failure():
    """Nothing to store, and no evidence that there is no graph."""
    result = service_with(FakeResponse(200, ['unexpected'])).fetch('o', 'r')
    assert result.failed
    assert not result.absent
    assert result.payload is None


def test_artifacts_for_absent_graph_is_none():
    assert service_with(FakeResponse(404)).artifacts_for('o', 'r') is None


def test_artifacts_for_malformed_payload_is_none():
    assert service_with(
        FakeResponse(200, {'nope': 1}),
    ).artifacts_for('o', 'r') is None
