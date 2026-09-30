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

GitHub serves the document two ways, and the first is closing down: the
synchronous endpoint answered with it, which the old pipeline asked, and
from 2026-11-13 there is only the asynchronous pair, which generates a
report and serves it once ready. The collector asks for reports
(`collector/depgraph.py`), and keeps each as the synchronous endpoint
would have answered, so a graph stored either way is read the same:
here, by `warehouse build` (`DbService.parse_dependency_graph`). The
old pipeline's fetch went with that pipeline (#171).
"""
from collections.abc import Mapping
from typing import Any

from chatsbom.models.provenance import classify_version
from chatsbom.models.provenance import DEPGRAPH
from chatsbom.models.relationship import DIRECT
from chatsbom.models.relationship import TRANSITIVE

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
