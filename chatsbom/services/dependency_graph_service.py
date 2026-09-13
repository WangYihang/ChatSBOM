"""GitHub's dependency graph as a second SBOM source.

Syft reads lockfiles, so Maven and Composer projects — which often ship
none — come back nearly empty: 0 packages for spring-boot, 0 for
elasticsearch, 0 for ghidra. GitHub parses the manifests server-side and
reports 303, 107 and 147 for those same repositories.

What it returns is narrower than Syft's output in two ways that must be
carried through rather than glossed over:

* the graph is **flat** — the document DESCRIBES the repository, and the
  repository DEPENDS_ON each package, with no tree — so every row is a
  *declared* dependency, never a transitive one;
* `versionInfo` is the manifest's constraint (`>= 0`) or absent, not a
  resolution, so it is classified rather than stored as if exact.

Two package kinds are dropped: the repository's own `pkg:github/...`
entry, which is the SPDX document subject rather than a dependency, and
`pkg:githubactions/...` entries, which are workflow steps rather than
anything the project ships.
"""
import json
from collections.abc import Iterator
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import structlog

from chatsbom.models.provenance import classify_version
from chatsbom.models.provenance import DEPGRAPH
from chatsbom.models.relationship import DIRECT
from chatsbom.services.github_service import GitHubService

logger = structlog.get_logger('dependency_graph')

#: purl types that describe the repository or its CI, not its dependencies.
EXCLUDED_ECOSYSTEMS = frozenset({'github', 'githubactions'})

PURL_REFERENCE_TYPE = 'purl'


def parse_spdx_document(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Project a dependency-graph SPDX document into artifact row mappings.

    Rows carry the same column names `db_service.parse_artifacts` emits,
    so both sources land in one table.
    """
    sbom = payload.get('sbom')
    if not isinstance(sbom, Mapping):
        raise ValueError('response has no sbom document')

    rows: list[dict[str, Any]] = []
    for package in sbom.get('packages') or []:
        if not isinstance(package, Mapping):
            continue

        purl = _purl_of(package)
        if purl is None:
            # No purl means no ecosystem, which makes the row unusable.
            continue

        ecosystem = _ecosystem_of(purl)
        if ecosystem in EXCLUDED_ECOSYSTEMS:
            continue

        name = str(package.get('name') or '').strip()
        if not name:
            continue

        version, version_kind = classify_version(package.get('versionInfo'))

        rows.append({
            'artifact_id': str(package.get('SPDXID') or ''),
            'name': name,
            'version': version,
            'version_kind': version_kind,
            'type': ecosystem,
            'purl': purl,
            'found_by': 'github-dependency-graph',
            'licenses': _licenses_of(package),
            # The graph is flat, so everything in it was declared.
            'relationship': DIRECT,
            'source': DEPGRAPH,
        })

    return rows


def _purl_of(package: Mapping[str, Any]) -> str | None:
    for ref in package.get('externalRefs') or []:
        if not isinstance(ref, Mapping):
            continue
        if ref.get('referenceType') == PURL_REFERENCE_TYPE:
            locator = ref.get('referenceLocator')
            if locator:
                return str(locator)
    return None


def _ecosystem_of(purl: str) -> str:
    """`pkg:maven/group/artifact@1.0` -> `maven`."""
    without_scheme = purl.split(':', 1)[-1]
    return without_scheme.split('/', 1)[0].lower()


def _licenses_of(package: Mapping[str, Any]) -> list[str]:
    concluded = package.get('licenseConcluded')
    if not concluded or concluded in {'NOASSERTION', 'NONE'}:
        return []
    return [str(concluded)]


class DependencyGraphService:
    """Fetches and caches GitHub dependency-graph SBOMs."""

    #: The synchronous endpoint is scheduled to close after 2026-11-13 in
    #: favour of an asynchronous generate/fetch report pair. Keeping the
    #: path in one place so the swap is a single edit.
    ENDPOINT = 'https://api.github.com/repos/{owner}/{repo}/dependency-graph/sbom'

    def __init__(self, github: GitHubService):
        self.github = github

    def fetch(self, owner: str, repo: str) -> dict[str, Any] | None:
        """The raw SPDX document, or None when GitHub has no data for it.

        Large repositories make this endpoint time out server-side (it
        answers 500 "Request timed out" for spring-boot), and repositories
        with the dependency graph disabled answer 404. Neither is fatal to
        a batch run, so both return None.
        """
        url = self.ENDPOINT.format(owner=owner, repo=repo)
        response = self.github.session.get(url, timeout=60)

        if response.status_code == 404:
            logger.info(
                'No dependency graph', repo=f'{owner}/{repo}',
            )
            return None
        if response.status_code >= 500:
            logger.warning(
                'Dependency graph unavailable',
                repo=f'{owner}/{repo}', status=response.status_code,
            )
            return None

        response.raise_for_status()
        payload = response.json()
        return payload if isinstance(payload, dict) else None

    def artifacts_for(self, owner: str, repo: str) -> list[dict[str, Any]] | None:
        """Artifact rows for a repository, or None when there is no graph."""
        payload = self.fetch(owner, repo)
        if payload is None:
            return None
        try:
            return parse_spdx_document(payload)
        except ValueError as e:
            logger.warning(
                'Malformed dependency graph',
                repo=f'{owner}/{repo}', error=str(e),
            )
            return None


def load_artifacts(path: Path) -> Iterator[dict[str, Any]]:
    """Artifact rows from a stored dependency-graph document."""
    if not path.exists():
        return
    try:
        payload = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError) as e:
        raise ValueError(f"unreadable dependency graph {path}: {e}") from e
    yield from parse_spdx_document(payload)
