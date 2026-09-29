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
from collections.abc import Callable
from collections.abc import Iterable
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

from chatsbom.core.depgraph_store import stamp_of
from chatsbom.core.depgraph_store import stamp_of_path
from chatsbom.core.instants import mtime
from chatsbom.core.instants import stated
from chatsbom.core.instants import utc
from chatsbom.core.layout import content_inside
from chatsbom.core.layout import relocate
from chatsbom.models.repository import license_fields

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

#: Depth of a stored manifest's content root below `06-github-content`:
#: `<repository_id>/<sha>` (`core/layout.py`). Everything after it is the
#: manifest's path inside the repository.
CONTENT_PREFIX_DEPTH = 2
#: The same in the language-keyed layout it replaced,
#: `<language>/<owner>/<repo>/<ref>/<sha>`: rows landed before
#: `data migrate-layout` rewrote their paths.
LEGACY_CONTENT_PREFIX_DEPTH = 5


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
    #: For a dependency graph kept by `core/depgraph_store`: the default
    #: branch and the HEAD sha it was fetched at. '' for a legacy graph,
    #: which recorded neither, and for every other kind.
    ref: str = ''
    commit_sha: str = ''


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
        commit_sha: str | None = None,
    ) -> Document | None:
        """The document, or None when this source does not have it.

        Raises ValueError when the document exists but cannot be parsed:
        a corrupt document is a fact about the data worth failing on,
        while an absent one is normal — the dependency graph covers
        whatever it has reached.

        `commit_sha` is the scan a Syft SBOM is read for: its document,
        and no other commit's. A graph is not asked for by commit — it
        describes the default branch when it was fetched, so the newest
        is the current one.
        """
        ...

    def observations(
        self,
        kind: str,
        wanted: Mapping[int, str | None],
    ) -> dict[int, datetime]:
        """When each document `get` would return says it was produced.

        `wanted` maps a repository id to the path its ledger recorded,
        as `get` takes them. A document this source does not have, or
        cannot read, is left out: the ingest writes nothing for it.

        The pre-pass behind `IngestionRepository.forget_graphs`. A
        dependency graph is named by this instant, so the rows of one
        indexed before are found by it and dropped before it is written
        again. It must therefore be `get(...).observed_at` exactly: a
        copy the forget misses is a graph counted twice.
        """
        ...


class FileDocuments:
    """Documents read from the paths the ledgers recorded."""

    def get(
        self,
        kind: str,
        repository_id: int,
        path: str | None = None,
        commit_sha: str | None = None,
    ) -> Document | None:
        # `commit_sha` has nothing to narrow here: `path` is the one the
        # record names, written for its own download target, as
        # `FileManifests` reads the one commit directory it is given.
        if not path:
            return None
        target = Path(path)
        if not target.exists():
            # Recorded before `data migrate-layout` moved it under the
            # repository's id.
            target = relocate(path, repository_id)
        if not target.exists():
            return None
        try:
            body = json.loads(target.read_text(encoding='utf-8'))
        except (OSError, json.JSONDecodeError) as error:
            raise ValueError(f"unreadable {kind} {target}: {error}") from error
        if not isinstance(body, dict):
            raise ValueError(f"unreadable {kind} {target}: not an object")
        ref, commit_sha = (
            stamp_of_path(target) if kind == DEPGRAPH else ('', '')
        )
        return Document(
            body=body,
            observed_at=observed_at(body, mtime(target)),
            origin=str(target),
            ref=ref,
            commit_sha=commit_sha,
        )

    def observations(
        self,
        kind: str,
        wanted: Mapping[int, str | None],
    ) -> dict[int, datetime]:
        # Each file read as `get` reads it: this is the fallback path,
        # and one parse per graph more is what exactness costs here.
        seen: dict[int, datetime] = {}
        for repository_id, path in wanted.items():
            try:
                document = self.get(kind, repository_id, path)
            except ValueError:
                continue
            if document is not None:
                seen[repository_id] = document.observed_at
        return seen


#: Newest first, and the same choice every time. Two copies landed with
#: the same `fetched_at` were picked between arbitrarily, and `get` and
#: `observations` must pick the same one.
_NEWEST_FIRST = 'ORDER BY fetched_at DESC, sha256 DESC'

#: Repositories per `observations` query: the ids are bound as one
#: array parameter, which travels in the URL.
_OBSERVATIONS_CHUNK = 1000

#: `_stated_creation`, asked of a stored body on the server, so that
#: `observations` transfers a date per document rather than the
#: document. `JSONExtractString` answers '' for a missing path, a value
#: that is not a string and an object that is not one, as
#: `_stated_creation` answers None; `documents_test.py` holds the two
#: to agreeing on every shape.
_STATED_CREATION_SQL = (
    "if(JSONHas(body, 'sbom'), "
    "JSONExtractString(body, 'sbom', 'creationInfo', 'created'), "
    "JSONExtractString(body, 'creationInfo', 'created'))"
)


class RawDocuments:
    """Documents read from the `raw_documents` landing zone.

    One point query per document. The table is ordered by
    `(kind, repository_id, sha256)`, so each is a primary-key prefix
    lookup rather than a scan, and the newest copy wins: the same
    repository collected twice is two rows, distinguished by content
    hash, and only the latest describes it now.

    **A Syft SBOM is the scan's own.** Reading the newest of every SBOM
    a repository landed, one generated for an earlier commit and landed
    after this commit's was read as this scan and stamped with its
    commit. The landed path names the commit it was generated at —
    `07-sbom/<repository_id>/<sha>/sbom.json`, as
    `RawManifests` uses — so a commit narrows the query to its own. A
    record with no download target has no scan to narrow to and reads
    the newest, as before.
    """

    def __init__(self, client: Any) -> None:
        self._client = client

    def get(
        self,
        kind: str,
        repository_id: int,
        path: str | None = None,
        commit_sha: str | None = None,
    ) -> Document | None:
        parameters: dict[str, Any] = {
            'kind': kind, 'repository_id': repository_id,
        }
        scope = ''
        if commit_sha:
            scope = 'AND position(path, {commit:String}) > 0 '
            parameters['commit'] = f'/{commit_sha}/'
        rows = self._client.query(
            'SELECT body, fetched_at, path FROM raw_documents '
            'WHERE kind = {kind:String} '
            'AND repository_id = {repository_id:UInt64} '
            f'{scope}{_NEWEST_FIRST} LIMIT 1',
            parameters=parameters,
        ).result_rows
        if not rows:
            return None
        raw, fetched_at, *rest = rows[0]
        landed = rest[0] if rest else ''

        origin = f"raw_documents {kind}/{repository_id}"
        try:
            body = json.loads(raw)
        except json.JSONDecodeError as error:
            raise ValueError(f"unreadable {origin}: {error}") from error
        if not isinstance(body, dict):
            raise ValueError(f"unreadable {origin}: not an object")
        # The stamp is in the landed path's directory name; `meta.json`
        # was not landed, so the branch is not here, only the commit.
        stamp = (
            stamp_of(PurePosixPath(str(landed)).parent.name)
            if kind == DEPGRAPH and landed else None
        )
        return Document(
            body=body,
            observed_at=observed_at(body, fetched_at),
            origin=origin,
            commit_sha=stamp[1] if stamp else '',
        )

    def observations(
        self,
        kind: str,
        wanted: Mapping[int, str | None],
    ) -> dict[int, datetime]:
        # One query per thousand repositories, and the date read on the
        # server, rather than `get` for each: on 1,000 synthetic graphs of
        # 243 KiB that is 0.45 s against 9.6 s, and a point query that
        # returned only the date would still pay 3.8 ms a repository on
        # the round trip. The same row as `get` picks, by the same
        # order, and the date made from it as `observed_at` makes it.
        ids = sorted(wanted)
        seen: dict[int, datetime] = {}
        for start in range(0, len(ids), _OBSERVATIONS_CHUNK):
            rows = self._client.query(
                'SELECT repository_id, '
                f'{_STATED_CREATION_SQL} AS created, fetched_at '
                'FROM raw_documents '
                'WHERE kind = {kind:String} '
                'AND repository_id IN {ids:Array(UInt64)} '
                f'{_NEWEST_FIRST} LIMIT 1 BY repository_id',
                parameters={
                    'kind': kind,
                    'ids': ids[start:start + _OBSERVATIONS_CHUNK],
                },
            ).result_rows
            for repository_id, created, fetched_at in rows:
                seen[int(repository_id)] = _dated(created or None, fetched_at)
        return seen


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
        commit_sha: str | None = None,
    ) -> list[tuple[str, str | None]]:
        """`(path within the repository, text)`, in no particular order.

        Empty when there is nothing stored, which is the ordinary case:
        a repository whose manifests were never downloaded has no
        declared set, and every dependency of it stays `unknown`.

        The text is None for a file that is there but cannot be read.
        It is passed on rather than left out: what it declares is
        unseen, so `relationships_from` counts it as incomplete, and a
        name no other manifest declares is `unknown`, not `transitive`.

        `commit_sha` is the scan the verdicts are for. Its manifests
        are the declared set, and no other commit's: a package only an
        older commit declared is not `direct` in this one.
        """
        ...


class FileManifests:
    """Manifests read from the directory `content` wrote.

    Already one commit's: `content` writes
    `<repository_id>/<sha>`, a directory per commit, and
    `content_dir` is the one the record's own download target produced.
    So `commit_sha` has nothing left to narrow here. It is not checked
    against the directory name either, which is a layout, not a
    contract: a directory named otherwise is still the one the record
    points at.
    """

    def __init__(self, max_bytes: int = 0) -> None:
        # 0 means "whatever manifest.py's own limit is", so the cap
        # lives in one place rather than being restated here.
        self._max_bytes = max_bytes

    def for_repository(
        self,
        repository_id: int,
        content_dir: str | None = None,
        commit_sha: str | None = None,
    ) -> list[tuple[str, str | None]]:
        if not content_dir:
            return []
        root = Path(content_dir)
        if not root.is_dir():
            # Recorded before `data migrate-layout` moved it.
            root = relocate(content_dir, repository_id)
        if not root.is_dir():
            return []
        from chatsbom.core.manifest import MAX_MANIFEST_BYTES
        from chatsbom.core.manifest import read_manifest
        cap = self._max_bytes or MAX_MANIFEST_BYTES
        out: list[tuple[str, str | None]] = []
        for path in sorted(root.rglob('*')):
            if not path.is_file():
                continue
            # None when too large, unreadable or undecodable: kept, so
            # the judgement sees a manifest it could not read.
            out.append((str(path.relative_to(root)), read_manifest(path, cap)))
        return out


class RawManifests:
    """Manifests read from `raw_documents`.

    One query per repository, on the `(kind, repository_id)` prefix of
    the sort key. The stored `path` is the full path on disk, for
    tracing a row back; what the parser needs is the part inside the
    repository, so the fixed
    `06-github-content/<repository_id>/<sha>` prefix is stripped (or,
    for a row landed before `data migrate-layout`, the old
    `<language>/<owner>/<repo>/<ref>/<sha>`).

    Deriving it rather than storing it a second time is deliberate: the
    two would drift, and the one that drifted would be the one nothing
    checked.

    **One commit's manifests, not every one landed.** A repository
    collected twice has both commits' files here, and reading them all
    made the declared set a union across scans: a package only an old
    commit declared came out `direct` in the new one. The `<sha>` in the
    stored path says which commit a file belongs to, and the query
    selects the scan's own by it, so no other commit's are even
    transferred.

    One consequence of the table's key, `(kind, repository_id,
    sha256)`: a manifest that did not change between two commits is one
    row once merged, the copy with the later `fetched_at`. That is the
    file's mtime, and the newer commit's copy is written later, so the
    survivor sits under the commit that has it now. A `data/` restored
    with older mtimes than it was collected with would reverse that.

    Without a commit, as for a record with no download target, every
    manifest is read, as before: there is no scan to narrow to.
    """

    def __init__(self, client: Any, content_dir: str | Path = '') -> None:
        self._client = client
        self._content_dir = str(content_dir)

    def for_repository(
        self,
        repository_id: int,
        content_dir: str | None = None,
        commit_sha: str | None = None,
    ) -> list[tuple[str, str | None]]:
        parameters: dict[str, Any] = {
            'kind': CONTENT, 'repository_id': repository_id,
        }
        scope = ''
        if commit_sha:
            # A directory of the stored path, wherever the content root
            # sits: `<...>/<ref>/<sha>/Gemfile` holds `/<sha>/`, and a
            # 40-character sha appears nowhere else by accident.
            scope = ' AND position(path, {commit:String}) > 0'
            parameters['commit'] = f'/{commit_sha}/'
        rows = self._client.query(
            'SELECT path, body FROM raw_documents '
            'WHERE kind = {kind:String} '
            'AND repository_id = {repository_id:UInt64}' + scope,
            parameters=parameters,
        ).result_rows
        out: list[tuple[str, str | None]] = []
        for path, body in rows:
            out.append((self._inside(str(path)), _landed(str(path), body)))
        return out

    def _inside(self, stored: str) -> str:
        """The manifest's path within its repository.

        Falls back to the basename rather than raising: a row whose
        path does not sit under the configured content directory still
        names a manifest, and the parser only needs the filename to
        pick a reader. Losing the directory costs detail in `sources`,
        which is better than dropping the manifest.
        """
        # Either layout, wherever the stage root sits in the path:
        # `06-github-content/<id>/<sha>/...` as `db raw` lands it now,
        # `.../06-github-content/<lang>/<o>/<r>/<ref>/<sha>/...` before.
        inside = content_inside(stored)
        if inside is not None:
            return inside
        parts = PurePosixPath(stored).parts
        if self._content_dir:
            root = PurePosixPath(self._content_dir).parts
            if parts[:len(root)] == root:
                parts = parts[len(root):]
        depth = (
            CONTENT_PREFIX_DEPTH if parts and parts[0].isdigit()
            else LEGACY_CONTENT_PREFIX_DEPTH
        )
        if len(parts) > depth:
            return '/'.join(parts[depth:])
        return parts[-1] if parts else stored


def _landed(origin: str, body: str) -> str | None:
    """A manifest's text as `db raw` stored it, judged as the file is.

    `db raw` stores `bytes.decode('utf-8', 'replace')`, which is not
    always what `read_manifest` makes of the same file:

    - a UTF-8 byte-order mark arrives as U+FEFF, which `_decoded` slices
      off a file. It is sliced off here too, or a package.json does not
      parse and the first name in a requirements.txt starts with it;
    - a byte that is not UTF-8 arrives as U+FFFD. Read off disk, such a
      file is unreadable, and so it is here: what it declared cannot be
      recovered from the replacement characters, and parsing what is
      left would invent names. That includes a UTF-16 file, whose mark
      `_decoded` honours but whose text the landing already replaced;
    - a manifest over MAX_MANIFEST_BYTES is not read, whichever source
      it comes from.

    None is a manifest that could not be read, which
    `relationships_from` counts as incomplete rather than leaving out.
    """
    from chatsbom.core.manifest import MAX_MANIFEST_BYTES
    if '\ufffd' in body:
        logger.debug('Undecodable manifest', origin=origin)
        return None
    if len(body.encode('utf-8')) > MAX_MANIFEST_BYTES:
        logger.debug('Manifest too large', origin=origin)
        return None
    return body.removeprefix('\ufeff')


class RecordSource(Protocol):
    """Where the repository records to ingest come from.

    The list *and* the records, because they are the same read: what
    decides which repositories are ingested is what the source has
    records for. `TrackedRecords` makes the ledger's list the master
    instead, and a source of this kind what fills it in.

    No language: which list a record was filed under selects nothing
    any more (#55). Every call yields the same records in the same
    order, since `db index` reads a source three times (the scans, the
    graphs, the ingest) and the three must agree under a `limit`.
    """

    def records(
        self,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """The repository records, newest copy each."""
        ...


class LedgerRecords:
    """Records read from the JSONL ledgers, as the pipeline wrote them.

    Every list given, in order; a repository listed more than once --
    in two lists, or on two lines of one -- is its last line, the newest
    the pipeline appended.
    """

    def __init__(
        self,
        sbom_lists: Path | Iterable[Path],
        metadata_lists: Path | Iterable[Path] | None = None,
    ) -> None:
        self._sbom_lists = _paths(sbom_lists)
        self._metadata_lists = _paths(metadata_lists)

    def records(
        self,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        fresher: dict[int, dict[str, Any]] = {}
        for listing in self._metadata_lists:
            for key, value in _fresh_metadata(listing).items():
                fresher.setdefault(key, value)
        newest: dict[Any, dict[str, Any]] = {}
        for listing in self._sbom_lists:
            if not listing.exists():
                continue
            with listing.open(encoding='utf-8') as handle:
                for line in handle:
                    if not line.strip():
                        continue
                    record = json.loads(line)
                    listed = record.get('id')
                    newest.pop(listed, None)
                    newest[listed] = record
        for seen, record in enumerate(newest.values()):
            if limit is not None and seen >= limit:
                return
            repository_id = record.get('id')
            update = fresher.get(repository_id) if isinstance(
                repository_id, int,
            ) else None
            yield {**record, **update} if update else record


def _paths(value: Path | Iterable[Path] | None) -> list[Path]:
    if value is None:
        return []
    if isinstance(value, (str, Path)):
        return [Path(value)]
    return [Path(v) for v in value]


class RawRecords:
    """Records read from `raw_documents`.

    One query for the records and one for the metadata overlay, rather
    than one query per repository: there are tens of thousands of them
    and the transform wants them in a stream, not as many round trips.

    The overlay is applied here, the same way and for the same reason
    the ledger path applies it -- the record carries metadata from when
    the SBOM was generated, so without it a `github repo` refresh never
    reaches the database.
    """

    def __init__(self, client: Any) -> None:
        self._client = client

    def records(
        self,
        limit: int | None = None,
        language: str | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Every repository's newest record, or with `language` only the
        ones filed under that list (`stage_input`'s scoping)."""
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

    def metadata(self, ids: Iterable[int]) -> dict[int, dict[str, Any]]:
        """The newest `repo-metadata` of each of `ids`, whole.

        What a tracked repository with no record is indexed from: the
        repository resource `github repo` fetched, which is everything
        a `repositories` row needs but the scan.
        """
        wanted = sorted(set(ids))
        found: dict[int, dict[str, Any]] = {}
        for start in range(0, len(wanted), _OBSERVATIONS_CHUNK):
            rows = self._client.query(
                'SELECT repository_id, body FROM raw_documents '
                'WHERE kind = {kind:String} '
                'AND repository_id IN {ids:Array(UInt64)} '
                f'{_NEWEST_FIRST} LIMIT 1 BY repository_id',
                parameters={
                    'kind': REPO_METADATA,
                    'ids': wanted[start:start + _OBSERVATIONS_CHUNK],
                },
            ).result_rows
            for repository_id, raw in rows:
                try:
                    body = json.loads(raw)
                except json.JSONDecodeError:
                    continue
                if isinstance(body, dict):
                    found[int(repository_id)] = body
        return found

    def newest_with(
        self,
        why_not: Callable[[Mapping[str, Any]], str | None],
    ) -> Iterator[tuple[int, dict[str, Any] | None, str | None]]:
        """Each repository's newest record that `why_not` takes, in order
        of id: `(id, record or None, why its newest was not taken)`, the
        last None when its newest was.

        `why_not` says why a record will not do, or None when it will.
        The copies of a repository are read newest first, as `records`
        orders them, and only as far as the first that will: most
        repositories' newest does, so their one body is all that is
        transferred. A chunk of repositories at a time, as `_newest`
        reads bodies.
        """
        copies = self._client.query(
            'SELECT repository_id, sha256, max(fetched_at) AS taken '
            'FROM raw_documents WHERE kind = {kind:String} '
            'GROUP BY repository_id, sha256 '
            'ORDER BY repository_id, taken DESC, sha256 DESC',
            parameters={'kind': REPO},
        ).result_rows
        newest_first: dict[int, list[str]] = {}
        for repository_id, sha, _ in copies:
            newest_first.setdefault(int(repository_id), []).append(str(sha))
        ids = list(newest_first)
        for start in range(0, len(ids), _BODIES_CHUNK):
            chunk = ids[start:start + _BODIES_CHUNK]
            taken: dict[int, dict[str, Any]] = {}
            why: dict[int, str | None] = {}
            at = {repository_id: 0 for repository_id in chunk}
            while at:
                bodies = self._bodies(
                    REPO, [(i, newest_first[i][n]) for i, n in at.items()],
                )
                further: dict[int, int] = {}
                for repository_id, position in at.items():
                    sha = newest_first[repository_id][position]
                    body = bodies.get((repository_id, sha))
                    said = 'unreadable' if body is None else why_not(body)
                    if position == 0:
                        why[repository_id] = said
                    if said is None and body is not None:
                        taken[repository_id] = body
                    elif position + 1 < len(newest_first[repository_id]):
                        further[repository_id] = position + 1
                at = further
            for repository_id in chunk:
                yield repository_id, taken.get(repository_id), why[repository_id]

    def _bodies(
        self, kind: str, pairs: list[tuple[int, str]],
    ) -> dict[tuple[int, str], dict[str, Any]]:
        """The bodies of `(repository_id, sha256)` copies, parsed; one
        that is not a JSON object is left out."""
        rows = self._client.query(
            'SELECT repository_id, sha256, body FROM raw_documents '
            'WHERE kind = {kind:String} '
            'AND (repository_id, sha256) IN {pairs:Array(Tuple(UInt64, String))} '
            'LIMIT 1 BY repository_id, sha256',
            parameters={'kind': kind, 'pairs': pairs},
        ).result_rows
        found: dict[tuple[int, str], dict[str, Any]] = {}
        for repository_id, sha, raw in rows:
            try:
                body = json.loads(raw)
            except json.JSONDecodeError:
                continue
            if isinstance(body, dict):
                found[(int(repository_id), str(sha))] = body
        return found

    def _newest(
        self,
        kind: str,
        language: str | None = None,
    ) -> Iterator[tuple[int, dict[str, Any]]]:
        """One row per repository: the newest copy of `kind`.

        `LIMIT 1 BY repository_id` after ordering by `fetched_at`
        descending, because a repository collected twice is two rows
        distinguished by content hash and only the latest describes it
        now. In order of id, so that every read yields the same order.

        With `language`, scoped by the **ledger file** the row came
        from, `<language>.jsonl`, not by the `language` field inside the
        record: which list a repository was collected from is the
        pipeline's judgement (`github/choosealicense.com` is HTML to
        GitHub and sat in the Ruby list). `db index` no longer scopes at
        all: a record filed under `07-sbom/index.jsonl`, for a
        repository tracked with no language, is read like any other.
        """
        suffix = f'/{language}.jsonl' if language else ''
        # Which copy first, without the bodies, then the bodies a chunk
        # at a time. The records are 5.16 GiB: one query for all of
        # them held every body in memory at once, where a query per
        # language held an eighth of it.
        newest = self._client.query(
            'SELECT repository_id, '
            'argMax(sha256, (fetched_at, sha256)) AS newest '
            'FROM raw_documents '
            'WHERE kind = {kind:String} '
            'AND (({suffix:String} = \'\') OR endsWith(path, {suffix:String})) '
            'GROUP BY repository_id ORDER BY repository_id',
            parameters={'kind': kind, 'suffix': suffix},
        ).result_rows
        wanted = [(int(rid), str(sha)) for rid, sha in newest]
        for start in range(0, len(wanted), _BODIES_CHUNK):
            chunk = wanted[start:start + _BODIES_CHUNK]
            rows = self._client.query(
                'SELECT repository_id, body FROM raw_documents '
                'WHERE kind = {kind:String} '
                'AND (repository_id, sha256) IN {pairs:Array(Tuple(UInt64, String))} '
                'LIMIT 1 BY repository_id',
                parameters={'kind': kind, 'pairs': chunk},
            ).result_rows
            bodies = {int(rid): raw for rid, raw in rows}
            for repository_id, _ in chunk:
                raw = bodies.get(repository_id)
                if raw is None:
                    continue
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
                yield repository_id, body


#: Records per query for their bodies: a record is about 190 KiB, most
#: of it the release list, so 200 is some 40 MiB in flight.
_BODIES_CHUNK = 200


class TrackedRecords:
    """Every repository the ledger tracks, whether or not it has a record.

    `db index` mastered on the records: a repository was indexed only
    once a walk of the whole chain had filed one. The ~28 k repositories
    a search snapshot seeded, and any whose scan failed, never got a
    `repositories` row, so their dependency graphs, fetched and landed,
    never reached `artifacts`, and every coverage ratio was measured
    against the repositories that had succeeded (#55 §4.11).

    Now the ledger is the list. Each tracked repository is its newest
    record where it has one, and otherwise a record made from what is
    known of it: the repository resource `github repo` last fetched
    (`metadata`), else the ledger's own row (name, stars, default
    branch, GitHub's language). Such a record has no download target,
    so it has no scan, but it still gets its row, its dependency graph
    and its releases, if any.

    A record whose repository the ledger does not track is still
    yielded: it was indexed before, and dropping it would delete a
    repository from the dataset because of a ledger that has not been
    seeded with it. `only` narrows to some ids (`--repos-file`).
    """

    def __init__(
        self,
        records: RecordSource,
        tracked: Mapping[int, Any] | None,
        metadata: Any = None,
        only: set[int] | None = None,
    ) -> None:
        self._records = records
        self._tracked = tracked or {}
        self._metadata = metadata
        self._only = only

    def records(
        self,
        limit: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        seen: set[int] = set()
        count = 0
        for record in self._records.records():
            repository_id = record.get('id')
            if self._only is not None and repository_id not in self._only:
                continue
            if limit is not None and count >= limit:
                return
            if isinstance(repository_id, int):
                seen.add(repository_id)
            count += 1
            yield self._stated(record)
        missing = [
            i for i in sorted(self._tracked)
            if i not in seen and (self._only is None or i in self._only)
        ]
        if limit is not None:
            missing = missing[:max(0, limit - count)]
        fetched = self._metadata(missing) if self._metadata and missing else {}
        for repository_id in missing:
            yield self._minimal(repository_id, fetched.get(repository_id))

    def _stated(self, record: dict[str, Any]) -> dict[str, Any]:
        """The record, with the ledger's GitHub language where it has one
        and the snapshot that lists it (which selects the corpus).

        And what the record left blank that the ledger knows: its stars
        and URL. A record `chatsbom run` filed before it started from
        the ledger has the model's placeholders -- stars 0, no URL -- and
        one with no metadata document to overlay kept them in the index
        (#55 pilot). Only blanks: stars the record states are newer.

        The default branch is the ledger's whenever it has one: the
        commit stage keeps it as `git ls-remote --symref` last said
        (`Ledger.observe_default_branch`), which is newer than any
        record, and a record filed before that has `'main'` for a
        placeholder, not a blank.
        """
        row = self._tracked.get(record.get('id'))  # type: ignore[arg-type]
        if not row:
            return record
        extra: dict[str, Any] = {}
        language = getattr(row, 'github_language', '')
        snapshot = getattr(row, 'snapshot', '')
        if language:
            extra['github_language'] = language
        if snapshot:
            extra['snapshot'] = snapshot
        stars = getattr(row, 'stars', None)
        if stars and not (record.get('stars') or record.get('stargazers_count')):
            extra['stars'] = stars
        branch = getattr(row, 'default_branch', '')
        if branch:
            extra['default_branch'] = branch
        if not (record.get('url') or record.get('html_url')):
            extra['url'] = f'https://github.com/{row.owner}/{row.repo}'
        return {**record, **extra} if extra else record

    def _minimal(
        self,
        repository_id: int,
        metadata: Mapping[str, Any] | None,
    ) -> dict[str, Any]:
        row = self._tracked[repository_id]
        record: dict[str, Any] = {
            'id': repository_id,
            'owner': row.owner,
            'name': row.repo,
            'html_url': f'https://github.com/{row.owner}/{row.repo}',
        }
        if row.stars is not None:
            record['stargazers_count'] = row.stars
        if row.default_branch:
            record['default_branch'] = row.default_branch
        if row.github_language:
            record['language'] = row.github_language
        if metadata:
            record.update(metadata)
            record['id'] = repository_id
        if row.github_language:
            record['github_language'] = row.github_language
        if getattr(row, 'snapshot', ''):
            record['snapshot'] = row.snapshot
        return record


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
    # Not stale, but a record `chatsbom run` files starts from the ledger,
    # which has no creation date: without it here, every repository it
    # collected was indexed as created in 1970 (#55 pilot, 82 of 82).
    'created_at',
    # GitHub's own licence object travels with the two fields read from
    # it. Left behind, the record's older object would refill any field
    # the newer one leaves empty.
    'license',
)


def _wanted(raw: str | dict[str, Any]) -> dict[str, Any]:
    """Just the fields that go stale, from a stored metadata record."""
    body = raw if isinstance(raw, dict) else json.loads(raw)
    if not isinstance(body, dict):
        return {}
    return {
        **{k: body[k] for k in FRESH_FIELDS if k in body},
        # Read as the model reads them. A metadata record written before
        # the fields were filled carries only the object, and the
        # overlay has to state them outright to replace what the record
        # already holds.
        **license_fields(body),
    }


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
    for record in RawRecords(client).records(limit, language=language):
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
    return _dated(_stated_creation(body), fallback)


def _dated(said: str | None, fallback: datetime) -> datetime:
    """What `said` states, else `fallback`: `observed_at`'s judgement,
    shared with `RawDocuments.observations`, which reads `said` on the
    server."""
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
