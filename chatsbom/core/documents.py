"""Where a collector's document comes from: the store, `data/`.

One value object for a document that has been read, and the readers of
the store the warehouse is built with (`warehouse/store.py`):

    FILES.get(SYFT, repo_id, path)                # a document, off disk
    FILE_MANIFESTS.for_repository(repo_id, root)  # a commit's manifests
    TrackedRecords(LedgerRecords(lists), ledger)  # the repositories

A `Document` carries its `observed_at`, which says when it was
*collected*: for a dependency graph, what the document itself states;
for a Syft SBOM, which carries no timestamp, the file's mtime.

`db raw` landed the same documents in ClickHouse's `raw_documents` too,
and `db index` could read them from there as well as off disk; both went
with the server (#153), and the store is the one copy.
"""
from __future__ import annotations

import json
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from typing import Protocol

import structlog

from chatsbom.core.depgraph_store import stamp_of_path
from chatsbom.core.instants import mtime
from chatsbom.core.instants import stated
from chatsbom.core.instants import utc
from chatsbom.core.layout import relocate
from chatsbom.models.repository import license_fields

logger = structlog.get_logger('documents')

#: The kinds of document, as `get` is asked for them.
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
    #: For a dependency graph kept by `core/depgraph_store`: the default
    #: branch and the HEAD sha it was fetched at. '' for a legacy graph,
    #: which recorded neither, and for every other kind.
    ref: str = ''
    commit_sha: str = ''


class DocumentSource(Protocol):
    """Somewhere documents can be read from: the store's files, `FILES`.

    `path` is what the ledger recorded.
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


class RecordSource(Protocol):
    """Where the repository records come from.

    The list *and* the records, because they are the same read: which
    repositories are read is what the source has records for.
    `TrackedRecords` makes the ledger's list the master instead, and a
    source of this kind what fills it in.

    No language: which list a record was filed under selects nothing
    any more (#55). Every call yields the same records in the same
    order.
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
    seeded with it. `only` narrows to some ids.
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
    # Not stale, but a record `chatsbom run` filed started from the
    # ledger, which had no creation date: without it here, every
    # repository it collected was indexed as created in 1970 (#55 pilot,
    # 82 of 82).
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
    which `RawDocuments.observations` shared on the server until #153."""
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
