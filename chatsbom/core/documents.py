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

import hashlib
import json
from collections.abc import Iterator
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from pathlib import Path
from pathlib import PurePosixPath
from typing import Any
from typing import Protocol

import structlog

from chatsbom.core.instants import mtime
from chatsbom.core.instants import stated
from chatsbom.core.instants import utc

logger = structlog.get_logger('documents')

#: The kinds, spelled as `raw_documents.kind` stores them.
SYFT = 'syft'
DEPGRAPH = 'github-depgraph'
#: One row per manifest *file*, not per repository -- see `db raw`.
CONTENT = 'content'
#: The accumulated repository record: metadata, releases, download target.
REPO = 'repo'
#: `GET /repos/{owner}/{repo}` as `github repo` last fetched it. Fresher
#: than the record, and the reason the overlay exists.
REPO_METADATA = 'repo-metadata'

#: Depth of a stored manifest's content root:
#: `<language>/<owner>/<repo>/<ref>/<sha>`. Everything after it is the
#: manifest's path inside the repository.
CONTENT_PREFIX_DEPTH = 5


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


class ManifestSource(Protocol):
    """Somewhere a repository's declared manifests can be read from.

    Separate from `DocumentSource` because the unit differs: a
    repository has one SBOM and one dependency graph, but many
    manifests, and what the caller needs is all of them together.
    """

    def for_repository(
        self,
        repository_id: int,
        content_dir: str | None = None,
    ) -> list[tuple[str, str]]:
        """`(path within the repository, text)`, in no particular order.

        Empty when there is nothing stored, which is the ordinary case:
        a repository whose manifests were never downloaded has no
        declared set, and every dependency of it stays `unknown`.
        """
        ...


class FileManifests:
    """Manifests read from the directory `content` wrote."""

    def __init__(self, max_bytes: int = 0) -> None:
        # 0 means "whatever manifest.py's own limit is", so the cap
        # lives in one place rather than being restated here.
        self._max_bytes = max_bytes

    def for_repository(
        self,
        repository_id: int,
        content_dir: str | None = None,
    ) -> list[tuple[str, str]]:
        if not content_dir:
            return []
        root = Path(content_dir)
        if not root.is_dir():
            return []
        from chatsbom.core.manifest import MAX_MANIFEST_BYTES
        cap = self._max_bytes or MAX_MANIFEST_BYTES
        out: list[tuple[str, str]] = []
        for path in sorted(root.rglob('*')):
            if not path.is_file():
                continue
            try:
                if path.stat().st_size > cap:
                    continue
                from chatsbom.core.manifest import _decoded
                text = _decoded(path)
            except (OSError, UnicodeDecodeError) as error:
                logger.debug(
                    'Unreadable manifest', path=str(path), error=str(error),
                )
                continue
            out.append((str(path.relative_to(root)), text))
        return out


class RawManifests:
    """Manifests read from `raw_documents`.

    One query per repository, on the `(kind, repository_id)` prefix of
    the sort key. The stored `path` is the full path on disk, for
    tracing a row back; what the parser needs is the part inside the
    repository, so the fixed
    `<language>/<owner>/<repo>/<ref>/<sha>` prefix is stripped.

    Deriving it rather than storing it a second time is deliberate: the
    two would drift, and the one that drifted would be the one nothing
    checked.
    """

    def __init__(self, client: Any, content_dir: str | Path = '') -> None:
        self._client = client
        self._content_dir = str(content_dir)

    def for_repository(
        self,
        repository_id: int,
        content_dir: str | None = None,
    ) -> list[tuple[str, str]]:
        rows = self._client.query(
            'SELECT path, body FROM raw_documents '
            'WHERE kind = {kind:String} '
            'AND repository_id = {repository_id:UInt64}',
            parameters={'kind': CONTENT, 'repository_id': repository_id},
        ).result_rows
        out: list[tuple[str, str]] = []
        for path, body in rows:
            out.append((self._inside(str(path)), body))
        return out

    def _inside(self, stored: str) -> str:
        """The manifest's path within its repository.

        Falls back to the basename rather than raising: a row whose
        path does not sit under the configured content directory still
        names a manifest, and the parser only needs the filename to
        pick a reader. Losing the directory costs detail in `sources`,
        which is better than dropping the manifest.
        """
        parts = PurePosixPath(stored).parts
        if self._content_dir:
            root = PurePosixPath(self._content_dir).parts
            if parts[:len(root)] == root:
                parts = parts[len(root):]
        if len(parts) > CONTENT_PREFIX_DEPTH:
            return '/'.join(parts[CONTENT_PREFIX_DEPTH:])
        return parts[-1] if parts else stored


class RecordSource(Protocol):
    """Where the repository records to ingest come from.

    The list *and* the records, because they are the same read: what
    decides which repositories are ingested is what the source has
    records for.
    """

    def records(
        self,
        language: str,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """The repository records for one language, newest copy each."""
        ...


class LedgerRecords:
    """Records read from the JSONL ledgers, as the pipeline wrote them."""

    def __init__(self, sbom_list: Path, metadata_list: Path | None) -> None:
        self._sbom_list = sbom_list
        self._metadata_list = metadata_list

    def records(
        self,
        language: str,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        fresher = _fresh_metadata(self._metadata_list)
        seen = 0
        with self._sbom_list.open(encoding='utf-8') as handle:
            for line in handle:
                if not line.strip():
                    continue
                if limit is not None and seen >= limit:
                    return
                seen += 1
                record = json.loads(line)
                update = fresher.get(record.get('id'))
                yield {**record, **update} if update else record


class RawRecords:
    """Records read from `raw_documents`.

    One query per language for the records and one for the metadata
    overlay, rather than one query per repository: there are 28,069 of
    them and the transform wants them in a stream, not 28,069 round
    trips.

    The overlay is applied here, the same way and for the same reason
    the ledger path applies it -- the record carries metadata from when
    the SBOM was generated, so without it a `github repo` refresh never
    reaches the database.
    """

    def __init__(self, client: Any) -> None:
        self._client = client

    def records(
        self,
        language: str,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        fresher = {
            repository_id: _wanted(body)
            for repository_id, body in self._newest(REPO_METADATA, language)
        }
        seen = 0
        for repository_id, body in self._newest(REPO, language):
            if limit is not None and seen >= limit:
                return
            seen += 1
            update = fresher.get(repository_id)
            yield {**body, **update} if update else body

    def _newest(
        self,
        kind: str,
        language: str,
    ) -> Iterator[tuple[int, dict[str, Any]]]:
        """One row per repository: the newest copy of `kind`.

        `LIMIT 1 BY repository_id` after ordering by `fetched_at`
        descending, because a repository collected twice is two rows
        distinguished by content hash and only the latest describes it
        now.

        Scoped by the **ledger file** the row came from, not by the
        `language` field inside the record. Those are different things,
        and reading the record's field instead was wrong: GitHub reports
        `github/choosealicense.com` as HTML — it is a Jekyll site — but
        it sits in the Ruby corpus, so filtering on the record dropped
        its metadata overlay and the transform served January's 4,034
        stars instead of September's 4,197.

        Which language a repository belongs to is the pipeline's
        judgement, recorded in which ledger it was written to, and
        `path` already carries that.
        """
        # The language filter is in SQL, not in Python. Filtering after
        # the fetch means transferring every `repo` row for every
        # language -- 5.16 GiB of stored records, nine times -- and the
        # first version of this did exactly that and did not finish.
        suffix = f'/{language}.jsonl' if language else ''
        rows = self._client.query(
            'SELECT repository_id, path, body FROM raw_documents '
            'WHERE kind = {kind:String} '
            'AND (({suffix:String} = \'\') OR endsWith(path, {suffix:String})) '
            'ORDER BY fetched_at DESC '
            'LIMIT 1 BY repository_id',
            parameters={'kind': kind, 'suffix': suffix},
        ).result_rows
        for repository_id, path, raw in rows:
            try:
                body = json.loads(raw)
            except json.JSONDecodeError as error:
                logger.warning(
                    'Unreadable record',
                    kind=kind, repository_id=repository_id,
                    error=str(error),
                )
                continue
            if not isinstance(body, dict):
                continue
            yield int(repository_id), body


#: Metadata fields that go stale on their own, and only those.
#:
#: A blanket merge would also overwrite `sbom_path` and
#: `sbom_commit_sha`, which describe *this* SBOM and must keep pointing
#: at the commit that was actually scanned -- a fresher `pushed_at`
#: beside a stale `sbom_commit_sha` is the truth, and the panel says so.
FRESH_FIELDS: tuple[str, ...] = (
    'stars', 'pushed_at', 'description', 'license_spdx_id',
    'license_name', 'topics', 'is_archived', 'is_fork',
    'fork_count', 'watchers_count', 'disk_usage',
    'default_branch', 'has_releases', 'total_releases',
    'latest_release_tag', 'latest_release_published_at',
    'vulnerability_alerts_count',
)


def _wanted(raw: str | dict[str, Any]) -> dict[str, Any]:
    """Just the fields that go stale, from a stored metadata record."""
    body = raw if isinstance(raw, dict) else json.loads(raw)
    if not isinstance(body, dict):
        return {}
    return {k: body[k] for k in FRESH_FIELDS if k in body}


def _fresh_metadata(index: Path | None) -> dict[int, dict[str, Any]]:
    """repository id -> newer metadata, from a JSONL ledger."""
    if index is None or not index.exists():
        return {}
    fresh: dict[int, dict[str, Any]] = {}
    with index.open(encoding='utf-8') as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            repository_id = record.get('id')
            if not isinstance(repository_id, int):
                continue
            update = _wanted(record)
            if update:
                fresh[repository_id] = update
    return fresh


class RecordStore:
    """Writes a repository record into the landing zone.

    `db raw` derives `kind='repo'` rows from the `07-sbom` ledger, which
    is fine while that ledger carries the whole record — and is exactly
    what has to stop. A record in `07-sbom/ruby.jsonl` is 63.1 KiB of
    which 98% is `all_releases`, and each of four stages appends its own
    copy, so the release list is on disk four times for 21 of the 22 GB
    of ledgers.

    The ledgers can only slim down once something else keeps the record,
    which is this. A stage that updates a repository writes it here, and
    the ledger keeps the one thing it is actually good at: a greppable
    line saying which repository reached which stage.

    Keyed on content: the same record written twice is one row, so a
    stage that changed nothing costs nothing.
    """

    def __init__(self, client: Any) -> None:
        self._client = client

    def remember(
        self,
        repository: Mapping[str, Any],
        ledger: str | Path,
        taken_at: datetime | None = None,
    ) -> bool:
        """Store `repository` as the current `repo` record.

        `ledger` is the JSONL path this record belongs to, and it is not
        decoration: `RawRecords` scopes a language by that path, because
        which language a repository belongs to is the pipeline's
        judgement rather than anything in the record. A row written with
        the wrong ledger is a row the transform will not find.

        `taken_at` defaults to now, which is right *here* and wrong
        almost everywhere else in this project: this record is being
        produced at this moment, unlike a document collected in
        February whose timestamp must come from the document. See
        `chatsbom/core/instants.py`.
        """
        repository_id = repository.get('id')
        if not isinstance(repository_id, int):
            return False
        body = json.dumps(
            dict(repository), sort_keys=True, separators=(',', ':'),
        )
        digest = hashlib.sha256(body.encode('utf-8')).hexdigest()
        self._client.insert(
            'raw_documents',
            [[
                REPO,
                repository_id,
                str(ledger),
                digest,
                utc(taken_at or datetime.now(timezone.utc)),
                body,
            ]],
            column_names=[
                'kind', 'repository_id', 'path', 'sha256', 'fetched_at',
                'body',
            ],
        )
        return True


def stage_input(
    container: Any,
    language: str,
    fallback: Path,
    from_raw: bool,
    limit: int | None = None,
) -> list[Any]:
    """The repositories a collection stage should work on.

    Every stage read the previous stage's JSONL ledger, which is why
    each ledger carries the whole record and why there are four copies
    of every release list on disk. It is also why the middle of the
    pipeline does not currently run: `03-github-release` and
    `04-github-commit` are not on this machine at all, so `github
    commit`, `github tree` and `github content` find no input and stop.

    With `from_raw` the records come from `raw_documents` instead, so a
    stage depends on the landing zone rather than on whichever ledger
    happened to survive.

    Returns `Repository` objects either way. A record that will not
    validate is dropped with a warning rather than failing the stage —
    one bad row should not cost a language.
    """
    from chatsbom.models.repository import Repository

    if not from_raw:
        from chatsbom.core.storage import load_jsonl
        repos = load_jsonl(fallback)
        return repos[:limit] if limit else repos

    client = container.get_ingestion_repository().client
    out: list[Any] = []
    for record in RawRecords(client).records(language, limit):
        try:
            out.append(Repository.model_validate(record))
        except Exception as error:  # noqa: BLE001 - dropped, not fatal
            logger.warning(
                'Unusable stored record',
                repository_id=record.get('id'), error=str(error),
            )
    return out


#: The ordinary sources. Stateless, so one instance of each is enough.
FILES = FileDocuments()
FILE_MANIFESTS = FileManifests()


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
