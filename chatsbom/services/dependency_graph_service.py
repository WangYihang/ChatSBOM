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
from dataclasses import replace
from pathlib import Path
from typing import Any

import structlog

from chatsbom.core.conditional import conditional_get
from chatsbom.core.conditional import ConditionalResult
from chatsbom.models.provenance import classify_version
from chatsbom.models.provenance import DEPGRAPH
from chatsbom.models.relationship import DIRECT
from chatsbom.models.relationship import TRANSITIVE
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

    declared = _root_dependencies(sbom)
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

        spdx_id = str(package.get('SPDXID') or '')
        rows.append({
            'artifact_id': spdx_id,
            'name': name,
            'version': version,
            'version_kind': version_kind,
            'type': ecosystem,
            'purl': purl,
            'found_by': 'github-dependency-graph',
            'licenses': _licenses_of(package),
            # Declared if the document says the repository depends on it
            # directly, inherited otherwise. See `_root_dependencies`
            # for why this is not simply DIRECT.
            'relationship': DIRECT if spdx_id in declared else TRANSITIVE,
            'source': DEPGRAPH,
        })

    return rows


def _root_dependencies(sbom: Mapping[str, Any]) -> set[str]:
    """SPDX ids the repository itself depends on.

    Every package in one of these documents used to be recorded as
    `direct`, on the belief — written into the code as "the graph is
    flat, so everything in it was declared" — that GitHub's dependency
    graph reports manifests only. It does not, and the same wrong
    reading had already been corrected once in `core/edges.py`: measured
    across 420 documents, 94.4% of Go edges and 93.4% of JavaScript
    edges run between packages rather than out of the root.

    What that cost is specific. Weighted by package count over 400
    documents, **17.3%** of the packages are root dependencies and 82.7%
    are reached through another package — so about 11 million of the
    13,263,227 stored rows claimed to be declared when they were
    inherited. The dashboard's headline read "70.7% of dependency
    records are declared outright" against a truer 14.0%, and its
    declared-only ranking returned `semver, debug, ms, glob, which` —
    npm plumbing nobody chooses — where the same ranking over resolved
    closures gives `typescript, eslint, prettier, react`.

    The distinction is in the document: the root is whatever `DESCRIBES`
    points at, and a `DEPENDS_ON` leaving the root names a declared
    dependency. A document with no relationships at all yields an empty
    set, and every package in it falls to `transitive` — the
    conservative direction, since claiming a dependency was declared is
    the error that misleads. None of the 250 documents sampled was
    actually relationship-free.
    """
    relationships = sbom.get('relationships') or []
    if not isinstance(relationships, list):
        return set()

    roots = {
        r['relatedSpdxElement']
        for r in relationships
        if isinstance(r, Mapping)
        and r.get('relationshipType') == 'DESCRIBES'
        and r.get('relatedSpdxElement')
    }
    return {
        str(r['relatedSpdxElement'])
        for r in relationships
        if isinstance(r, Mapping)
        and r.get('relationshipType') == 'DEPENDS_ON'
        and r.get('spdxElementId') in roots
        and r.get('relatedSpdxElement')
    }


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
    """Fetches GitHub dependency-graph SBOMs."""

    #: The synchronous endpoint is scheduled to close after 2026-11-13 in
    #: favour of an asynchronous generate/fetch report pair. Keeping the
    #: path in one place so the swap is a single edit.
    ENDPOINT = 'https://api.github.com/repos/{owner}/{repo}/dependency-graph/sbom'

    def __init__(self, github: GitHubService):
        self.github = github
        #: Rate-limited requests sent. `chatsbom run` bounds a pass by
        #: the sum of the services' own counters rather than keeping a
        #: second tally that would drift from theirs.
        self.requests = 0

    def fetch(self, owner: str, repo: str) -> ConditionalResult:
        """GitHub's answer for one repository, as the outcome it was.

        This used to return None for everything but a document, so a
        batch recorded a refused token as "no graph": 890 answers of 429
        were counted among "3,133 with no graph published". The four
        outcomes a batch has to tell apart:

        * `changed` — the SPDX document, in `payload`;
        * `absent` — 404, no graph. A repository with the dependency
          graph switched off answers this endpoint the same way;
        * `rate_limited` — the token was refused, which says nothing
          about the repository;
        * `failed` — anything else. Large repositories make this endpoint
          time out server-side (500 "Request timed out" for spring-boot),
          and the session retries 5xx and then raises `RetryError`, so a
          persistent 500 arrives as a transport error. A body that is not
          a JSON object is a failure too: nothing to store, and no
          evidence that there is no graph.

        Never raises for any of them: none is fatal to a batch over
        thousands of repositories, and the caller decides which one stops
        it.

        Through `plain_session`. The cached `session` sleeps through a 429
        carrying `Retry-After` and asks again, three times, then raises a
        transport error — a refusal nobody could recognise as one. What
        its cache held was a second copy of every document the command
        stores anyway, and a week's memory of each 404: a re-run now asks
        again about a repository that had no graph, which is also how it
        notices one switched on.
        """
        name = f'{owner}/{repo}'
        self.requests += 1
        result = conditional_get(
            self.github.plain_session,
            self.ENDPOINT.format(owner=owner, repo=repo),
            timeout=60,
        )
        if result.changed and not isinstance(result.payload, dict):
            result = replace(
                result,
                payload=None,
                error=f'not an SPDX document: {type(result.payload).__name__}',
            )

        if result.absent:
            logger.info('No dependency graph', repo=name)
        elif result.rate_limited:
            logger.warning(
                'Dependency graph refused: rate limited',
                repo=name, status=result.status,
                remaining=result.rate_limit.remaining,
            )
        elif not result.changed:
            logger.warning(
                'Dependency graph unavailable',
                repo=name, status=result.status, error=result.error,
            )
        return result

    def artifacts_for(self, owner: str, repo: str) -> list[dict[str, Any]] | None:
        """Artifact rows for a repository, or None without a usable graph.

        None covers every outcome but a document — including a refused
        token. A batch has to tell those apart, so it uses `fetch`.
        """
        result = self.fetch(owner, repo)
        if not result.changed:
            return None
        try:
            return parse_spdx_document(result.payload)
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
