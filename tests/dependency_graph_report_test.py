"""The same graph, whichever way GitHub gave it (#50).

The synchronous endpoint answered `{"sbom": {...}}`, and the old
pipeline stored that. It closes after 2026-11-13, and the collector
asks for reports instead (`collector/depgraph.py`): "the SBOM in SPDX
JSON format", and seen to be the document alone, without the wrapper
(ClickHouse/ClickBOM#119). The collector puts the wrapper back
(`document_of`), so that a graph it keeps is stored as the synchronous
endpoint's answer would have been. The store holds both, and
`warehouse build` reads them the same: the same rows, edges and
instant.

The old pipeline's fetch, whose tests these were beside its flows',
went with it (#171); the collector's is collector_depgraph_test's.
"""
from __future__ import annotations

import json
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest

from chatsbom.collector.depgraph import document_of
from chatsbom.core.documents import DEPGRAPH
from chatsbom.core.documents import FILES
from chatsbom.core.edges import edges_in
from chatsbom.core.fs import atomic_write_text
from chatsbom.models.relationship import DIRECT
from chatsbom.models.relationship import TRANSITIVE
from chatsbom.services.db_service import DbService
from chatsbom.services.db_service import graph_observed_at
from chatsbom.services.dependency_graph_service import parse_spdx_document

#: What the synchronous endpoint answers: GitHub's own example from the
#: REST reference (`dependency-graph-export-sbom-response`), with what a
#: real repository adds to it — a package reached only through another,
#: a Maven one declared without a version, and a workflow action. The
#: SPDX document is wrapped in `sbom`.
EXPORTED: dict[str, Any] = {
    'sbom': {
        'SPDXID': 'SPDXRef-DOCUMENT',
        'spdxVersion': 'SPDX-2.3',
        'creationInfo': {
            # In another zone, with a fraction of a second, as a document
            # may state it: 03:56:20 UTC, to the second.
            'created': '2026-09-14T11:56:20.924281+08:00',
            'creators': ['Tool: GitHub.com-Dependency-Graph'],
        },
        'name': 'github/example',
        'dataLicense': 'CC0-1.0',
        'documentNamespace': (
            'https://spdx.org/spdxdocs/protobom/'
            '15e41dd2-f961-4f4d-b8dc-f8f57ad70d57'
        ),
        'packages': [
            {
                'name': 'rails',
                'SPDXID': 'SPDXRef-Package',
                'versionInfo': '1.0.0',
                'downloadLocation': 'NOASSERTION',
                'filesAnalyzed': False,
                'licenseConcluded': 'MIT',
                'licenseDeclared': 'MIT',
                'copyrightText': 'Copyright (c) 1985 GitHub.com',
                'externalRefs': [{
                    'referenceCategory': 'PACKAGE-MANAGER',
                    'referenceType': 'purl',
                    'referenceLocator': 'pkg:gem/rails@1.0.0',
                }],
            },
            {
                'name': 'github/example',
                'SPDXID': 'SPDXRef-Repository',
                'versionInfo': 'main',
                'downloadLocation': 'NOASSERTION',
                'filesAnalyzed': False,
                'externalRefs': [{
                    'referenceCategory': 'PACKAGE-MANAGER',
                    'referenceType': 'purl',
                    'referenceLocator': 'pkg:github/example@main',
                }],
            },
            {
                'name': 'npm:body-parser',
                'SPDXID': 'SPDXRef-npm-body-parser-1.20.2',
                'versionInfo': '1.20.2',
                'downloadLocation': 'NOASSERTION',
                'filesAnalyzed': False,
                'licenseConcluded': 'MIT',
                'externalRefs': [{
                    'referenceCategory': 'PACKAGE-MANAGER',
                    'referenceType': 'purl',
                    'referenceLocator': 'pkg:npm/body-parser@1.20.2',
                }],
            },
            {
                'name': 'npm:bytes',
                'SPDXID': 'SPDXRef-npm-bytes-3.1.2',
                'versionInfo': '3.1.2',
                'downloadLocation': 'NOASSERTION',
                'filesAnalyzed': False,
                'licenseConcluded': 'NOASSERTION',
                'externalRefs': [{
                    'referenceCategory': 'PACKAGE-MANAGER',
                    'referenceType': 'purl',
                    'referenceLocator': 'pkg:npm/bytes@3.1.2',
                }],
            },
            {
                'name': (
                    'maven:org.springframework.boot:spring-boot-starter-web'
                ),
                'SPDXID': 'SPDXRef-maven-spring-boot-starter-web',
                'downloadLocation': 'NOASSERTION',
                'filesAnalyzed': False,
                'externalRefs': [{
                    'referenceCategory': 'PACKAGE-MANAGER',
                    'referenceType': 'purl',
                    'referenceLocator': (
                        'pkg:maven/org.springframework.boot/'
                        'spring-boot-starter-web'
                    ),
                }],
            },
            {
                'name': 'actions:actions/checkout',
                'SPDXID': 'SPDXRef-githubactions-actions-checkout-4',
                'versionInfo': '4',
                'downloadLocation': 'NOASSERTION',
                'filesAnalyzed': False,
                'externalRefs': [{
                    'referenceCategory': 'PACKAGE-MANAGER',
                    'referenceType': 'purl',
                    'referenceLocator': 'pkg:githubactions/actions/checkout@4',
                }],
            },
        ],
        'relationships': [
            {
                'relationshipType': 'DEPENDS_ON',
                'spdxElementId': 'SPDXRef-Repository',
                'relatedSpdxElement': 'SPDXRef-Package',
            },
            {
                'relationshipType': 'DEPENDS_ON',
                'spdxElementId': 'SPDXRef-Repository',
                'relatedSpdxElement': 'SPDXRef-npm-body-parser-1.20.2',
            },
            {
                'relationshipType': 'DEPENDS_ON',
                'spdxElementId': 'SPDXRef-npm-body-parser-1.20.2',
                'relatedSpdxElement': 'SPDXRef-npm-bytes-3.1.2',
            },
            {
                'relationshipType': 'DEPENDS_ON',
                'spdxElementId': 'SPDXRef-Repository',
                'relatedSpdxElement': 'SPDXRef-maven-spring-boot-starter-web',
            },
            {
                'relationshipType': 'DEPENDS_ON',
                'spdxElementId': 'SPDXRef-Repository',
                'relatedSpdxElement': 'SPDXRef-githubactions-actions-checkout-4',
            },
            {
                'relationshipType': 'DESCRIBES',
                'spdxElementId': 'SPDXRef-DOCUMENT',
                'relatedSpdxElement': 'SPDXRef-Repository',
            },
        ],
    },
}

#: The same repository's report, as the asynchronous flow has been seen
#: to download it: "the SBOM in SPDX JSON format", without the wrapper.
DOWNLOADED: dict[str, Any] = EXPORTED['sbom']

#: The instant both state.
CREATED = datetime(2026, 9, 14, 3, 56, 20, tzinfo=timezone.utc)


# --- the same graph, whichever way it came ---------------------------------------

@pytest.fixture
def both(tmp_path) -> tuple[Path, Path]:
    """The synchronous endpoint's answer, as the old pipeline stored it,
    and the report, as the collector keeps it: each as the store writes
    a document (`depgraph_store.store`), `json.dumps` of it, whole."""
    kept = document_of(DOWNLOADED)
    assert kept is not None

    paths = tmp_path / 'sync.spdx.json', tmp_path / 'async.spdx.json'
    for path, payload in zip(paths, (EXPORTED, kept)):
        atomic_write_text(path, json.dumps(payload, ensure_ascii=False))
    return paths


def test_both_flows_store_the_same_bytes(both):
    old, new = both
    assert new.read_bytes() == old.read_bytes()


def test_both_flows_parse_into_the_same_artifact_rows(both):
    old, new = (FILES.get(DEPGRAPH, 1, str(path)) for path in both)
    assert old is not None and new is not None

    rows = parse_spdx_document(new.body)
    assert rows == parse_spdx_document(old.body)
    # And they are the rows a graph should give: the repository and its
    # workflow action are not dependencies, the root's own are declared,
    # and a package reached through another is inherited.
    assert {row['name']: row['relationship'] for row in rows} == {
        'rails': DIRECT,
        'npm:body-parser': DIRECT,
        'npm:bytes': TRANSITIVE,
        'maven:org.springframework.boot:spring-boot-starter-web': DIRECT,
    }


def test_both_flows_give_the_same_edges(both):
    old, new = (json.loads(path.read_text()) for path in both)
    assert edges_in(new) == edges_in(old) == {('npm:body-parser', 'npm:bytes')}


def test_both_flows_are_observed_at_the_instant_the_document_states(both):
    """#22 keys graph rows by `creationInfo.created`, and the report
    states it just as the export did."""
    old, new = (FILES.get(DEPGRAPH, 1, str(path)) for path in both)
    assert graph_observed_at(new) == graph_observed_at(old) == CREATED


def test_both_flows_index_into_the_same_rows(both):
    old, new = (FILES.get(DEPGRAPH, 1, str(path)) for path in both)
    assert old is not None and new is not None
    repository = {'default_branch': 'main', 'sbom_commit_sha': 'c' * 40}
    service = DbService()

    rows = service.parse_dependency_graph(new, 7, repository)
    assert rows == service.parse_dependency_graph(old, 7, repository)
    assert {row['observed_at'] for row in rows} == {CREATED}
