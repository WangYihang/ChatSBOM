"""Package-to-package dependency edges, read from the stored SPDX.

These are dataset content, not an export format, which is why they live
here rather than in `export/d1.py` where they were written. Two stores
need them now — the D1 export writes them into `agg_edges`, and the
ClickHouse-backed dashboard reads them from a table of its own — and a
copy per store would let the two describe different graphs.

The edges are *not* derivable from the `artifacts` table: that records
what a repository depends on, not what its packages depend on each
other. They come from the raw documents the collector already stored.
"""
from __future__ import annotations

import json
from collections import Counter
from collections.abc import Mapping
from datetime import datetime
from datetime import timezone
from pathlib import Path

import structlog

logger = structlog.get_logger(__name__)


def edges_in(document: Mapping[str, object]) -> set[tuple[str, str]]:
    """Package-to-package dependency edges in one SPDX document.

    GitHub's dependency graph does carry these — an earlier reading of
    this data concluded it did not, from a single small PHP sample where
    every edge happened to start at the repository root. Measured
    properly across 420 documents, 94.4% of Go edges and 93.4% of
    JavaScript edges run between packages.

    Three things are deliberately dropped.

    **Edges out of the root.** Those say "this repository depends on X",
    which is what the artifacts table already records. Including them
    would double-count and would mix two different claims in one table.

    **Versions.** `mail 2.8.1 -> mini_mime 1.1.5` becomes
    `mail -> mini_mime`. The question a reader has is which packages
    pull in which, and keeping versions multiplies the rows without
    answering it any better.

    **Duplicates within a document.** A repository counts once for a
    pair however many times its lockfile expresses it, so the stored
    count is "repositories", not "occurrences".
    """
    sbom = document.get('sbom', document)
    if not isinstance(sbom, Mapping):
        return set()

    relationships = sbom.get('relationships') or []
    packages = sbom.get('packages') or []
    if not isinstance(relationships, list) or not isinstance(packages, list):
        return set()

    names = {
        p['SPDXID']: str(p.get('name', ''))
        for p in packages
        if isinstance(p, Mapping) and p.get('SPDXID')
    }
    roots = {
        r['relatedSpdxElement']
        for r in relationships
        if isinstance(r, Mapping)
        and r.get('relationshipType') == 'DESCRIBES'
        and r.get('relatedSpdxElement')
    }

    edges: set[tuple[str, str]] = set()
    for relationship in relationships:
        if not isinstance(relationship, Mapping):
            continue
        if relationship.get('relationshipType') != 'DEPENDS_ON':
            continue
        source = relationship.get('spdxElementId')
        target = relationship.get('relatedSpdxElement')
        if source in roots:
            continue
        parent, child = names.get(source), names.get(target)
        if parent and child:
            edges.add((parent, child))
    return edges


#: Where the collector writes dependency-graph documents.
DEPGRAPH_ROOT = Path('data/09-github-depgraph')


class EdgeCounts(Counter):
    """Pair counts, plus when the documents behind them were collected.

    The date travels with the counts because a stored edge row has to
    carry one, and deriving it at the call site would mean either a
    clock — which makes a recount look like a fresh observation — or a
    second walk of the same 24,936 documents.
    """

    #: Newest `creationInfo.created` seen, falling back to file mtimes.
    observed_at: datetime = datetime(1970, 1, 1)
    #: Documents actually read.
    documents: int = 0


def collect_edges(root: Path = DEPGRAPH_ROOT) -> EdgeCounts:
    """Count, per package pair, how many repositories show that edge.

    Walks the raw SPDX documents rather than the database, because the
    edges are not in the database: `artifacts` records what a repository
    depends on, not what its packages depend on each other. Nothing has
    to be re-collected — the documents are already on disk.

    The root is a parameter so a test does not walk a real collection —
    23,890 documents took 74 seconds per test before it was.

    Aggregating here rather than storing per-repository rows is a
    deliberate 6.5x reduction, measured: 30,530,533 raw edges against
    4,691,332 distinct pairs. The raw form would be 1,206 MB in SQLite
    and would answer "what does this one repository's tree look like",
    a question whose answer is a 6,635-node graph nobody can read.
    """
    counts = EdgeCounts()
    if not root.exists():
        logger.warning('No dependency-graph documents', path=str(root))
        return counts

    newest = datetime(1970, 1, 1)
    documents = 0
    for path in root.rglob('*.json'):
        try:
            with path.open(encoding='utf-8') as handle:
                document = json.load(handle)
        except (OSError, json.JSONDecodeError):
            # One unreadable document is not a reason to lose the rest.
            logger.warning('Unreadable depgraph document', path=str(path))
            continue
        documents += 1
        newest = max(newest, _collected_at(path, document))
        for edge in edges_in(document):
            counts[edge] += 1

    counts.observed_at = newest
    counts.documents = documents
    logger.info(
        'Collected dependency edges',
        documents=documents,
        pairs=len(counts),
        observed_at=str(newest),
    )
    return counts


def _collected_at(path: Path, document: Mapping[str, object]) -> datetime:
    """When this document was produced, per the document itself.

    GitHub writes `creationInfo.created` beside
    `Tool: GitHub.com-Dependency-Graph`, which is the graph's own view of
    when it was generated. The file's mtime is the fallback. Never the
    clock: re-counting the same documents must not make the edges look
    newly observed.
    """
    sbom = document.get('sbom', document)
    if isinstance(sbom, Mapping):
        info = sbom.get('creationInfo')
        if isinstance(info, Mapping):
            created = info.get('created')
            if isinstance(created, str):
                try:
                    return datetime.fromisoformat(
                        created.replace('Z', '+00:00'),
                    ).astimezone(timezone.utc).replace(tzinfo=None)
                except ValueError:
                    pass
    try:
        return datetime.fromtimestamp(
            path.stat().st_mtime, tz=timezone.utc,
        ).replace(tzinfo=None)
    except OSError:
        return datetime(1970, 1, 1)
