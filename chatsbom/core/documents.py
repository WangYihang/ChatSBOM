"""Where a collector's document comes from.

`db index` read the collectors' JSON straight off disk, which made the
files load-bearing: `data/` is 31 GB, it is the only copy, and the
transform could not be re-run without it. `db raw` landed the same
documents in `raw_documents` at 1.92 GiB, so there are now two places a
document can come from and the transform should not care which.

That is all this module is: one value object for a document that has
been read, and two ways of reading one.

    FILES.get(SYFT, repo_id, path)          # from data/07-sbom
    RawDocuments(client).get(SYFT, repo_id) # from raw_documents

Both answer with the same `Document`, including the same
`observed_at` — the timestamp says when the document was *collected*,
and moving where it is stored must not change it. For a dependency
graph that is what the document itself states; for a Syft SBOM, which
carries no timestamp, it is the file's mtime, which `db raw` copied
into `fetched_at` for exactly this reason.
"""
from __future__ import annotations

import json
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from typing import Protocol

import structlog

from chatsbom.core.instants import mtime
from chatsbom.core.instants import stated
from chatsbom.core.instants import utc

logger = structlog.get_logger('documents')

#: The two kinds, spelled as `raw_documents.kind` stores them.
SYFT = 'syft'
DEPGRAPH = 'github-depgraph'


@dataclass(frozen=True)
class Document:
    """A collector's document, parsed, with when it was collected.

    `origin` is only ever used in messages — a path, or a description of
    the row it came out of. It exists because "unreadable sbom" with no
    subject has cost real debugging time.
    """

    body: Mapping[str, Any]
    observed_at: datetime
    origin: str


class DocumentSource(Protocol):
    """Somewhere documents can be read from.

    `path` is what the ledger recorded, and is meaningless to a source
    that reads from the database — it is accepted and ignored there so
    the caller does not have to know which source it holds.
    """

    def get(
        self,
        kind: str,
        repository_id: int,
        path: str | None = None,
    ) -> Document | None:
        """The document, or None when this source does not have it.

        Raises ValueError when the document exists but cannot be parsed:
        a corrupt document is a fact about the data worth failing on,
        while an absent one is normal — the dependency graph covers
        whatever it has reached.
        """
        ...


class FileDocuments:
    """Documents read from the paths the ledgers recorded."""

    def get(
        self,
        kind: str,
        repository_id: int,
        path: str | None = None,
    ) -> Document | None:
        if not path:
            return None
        target = Path(path)
        if not target.exists():
            return None
        try:
            body = json.loads(target.read_text(encoding='utf-8'))
        except (OSError, json.JSONDecodeError) as error:
            raise ValueError(f"unreadable {kind} {target}: {error}") from error
        if not isinstance(body, dict):
            raise ValueError(f"unreadable {kind} {target}: not an object")
        return Document(
            body=body,
            observed_at=observed_at(body, mtime(target)),
            origin=str(target),
        )


class RawDocuments:
    """Documents read from the `raw_documents` landing zone.

    One point query per document. The table is ordered by
    `(kind, repository_id, sha256)`, so each is a primary-key prefix
    lookup rather than a scan, and the newest copy wins: the same
    repository collected twice is two rows, distinguished by content
    hash, and only the latest describes it now.
    """

    def __init__(self, client: Any) -> None:
        self._client = client

    def get(
        self,
        kind: str,
        repository_id: int,
        path: str | None = None,
    ) -> Document | None:
        rows = self._client.query(
            'SELECT body, fetched_at FROM raw_documents '
            'WHERE kind = {kind:String} '
            'AND repository_id = {repository_id:UInt64} '
            'ORDER BY fetched_at DESC LIMIT 1',
            parameters={'kind': kind, 'repository_id': repository_id},
        ).result_rows
        if not rows:
            return None
        raw, fetched_at = rows[0]
        origin = f"raw_documents {kind}/{repository_id}"
        try:
            body = json.loads(raw)
        except json.JSONDecodeError as error:
            raise ValueError(f"unreadable {origin}: {error}") from error
        if not isinstance(body, dict):
            raise ValueError(f"unreadable {origin}: not an object")
        return Document(
            body=body,
            observed_at=observed_at(body, fetched_at),
            origin=origin,
        )


#: The ordinary source. Stateless, so one instance is enough.
FILES = FileDocuments()


def observed_at(body: Mapping[str, Any], fallback: datetime) -> datetime:
    """When the document says it was produced, else `fallback`.

    Two sources of truth, in order of authority:

    - what the document states. GitHub's SPDX carries
      `creationInfo.created` — `2026-09-14T03:56:20Z`, alongside
      `Tool: GitHub.com-Dependency-Graph` — which is the graph's own
      view of when it was produced and beats any timestamp this side of
      the wire;
    - the fallback. Syft's output carries no timestamp at all — its
      `descriptor` names the tool and version and nothing else — so for
      those this is all there is.

    Never `now()`. `observed_at` defaulted to the ingest time once, and
    a rebuild reset all 19,361,638 rows to the moment it ran: the syft
    documents were collected 2026-02-11 and the graphs 2026-09-14, and
    the table claimed 2026-09-14 for every row.
    """
    said = _stated_creation(body)
    when = stated(said)
    if when is None:
        if said:
            logger.warning(
                'Unparsable creation timestamp, falling back',
                stated=said,
            )
        return utc(fallback)
    return when


def _stated_creation(body: Mapping[str, Any]) -> str | None:
    """`creationInfo.created`, for the documents that have one.

    Read forgivingly: a document this cannot make sense of is still
    ingested, so a surprise here must fall through to the fallback
    rather than raise.
    """
    sbom = body.get('sbom', body)
    if not isinstance(sbom, dict):
        return None
    info = sbom.get('creationInfo')
    if not isinstance(info, dict):
        return None
    created = info.get('created')
    return created if isinstance(created, str) else None
