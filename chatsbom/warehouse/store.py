"""The store, read with the parsers `db index` used.

`db index` read each repository's newest record and the one commit it
named; ClickHouse kept the scans of earlier passes because it never
forgot them. The warehouse is rebuilt from the store alone, so it reads
every scan the store still holds:

- every commit under `07-sbom/<id>/` or `06-github-content/<id>/`: its
  Syft document, judged against the same commit's manifests, and what
  those manifests declare (`DbService.scan_rows`, the rule `db index`
  paired them by);
- every fetch of the dependency graph under `09-github-depgraph/<id>/`,
  and the graph kept from before every fetch was
  (`DbService.parse_dependency_graph`).

Which repositories, and what is known of each, is what `db index
--from-files` read, from the same sources: the records in the `07-sbom`
lists with `02-github-repo`'s fresher metadata, and every repository a
complete search snapshot lists (`TrackedRecords`), each projected by
`DbService.parse_repository`. The newest complete search snapshot
(`core/catalog.py`) is the corpus.

A commit is dated by when the store first had it, the earliest of its
tree, its manifests and its Syft document (`_first_had`), where `db
index` dated it by the document alone: the SBOM stage writes the
documents again, an older commit's too, after an upgrade of Syft.

A document that cannot be parsed is left out, and counted: one
corrupt file costs its own scan. `db index` dropped the whole
repository for it instead.

**The releases and the refs are the decisions'.** The release and
commit stages keep what they decide in the store (#147,
`core/decisions.py`). A scan's ref is the one the commit decision that
resolved to its commit says, an older scan's too, the newest such where
two did. A repository's releases, the latest stable one, and its
download target are those of the newest push whose commit the store
has a scan of: `db index` read the record `chatsbom run` landed at the
end of the last walk to reach one, and a newer push whose walk is still
under way is not read yet. Where the store has a scan of no decided
commit, they are the newest push's, with the download target of the
newest push whose key is resolved. Where the store has no decision, or
a list it cannot read (counted), the record's own stand.

**A repository with no record** is what `github repo` last fetched of
it, the newest `02-github-repo` line, whole: its flags (archived, fork,
template, mirror), description, licence, topics, dates and counts, as
`db index` read them from `repo-metadata` (#181). Over that, what the
snapshots say of it, its name, stars, language and default branch, as
the ledger that listed them did until it went with the old pipeline
(#171). With no line either, the snapshots' alone.
"""
from __future__ import annotations

import os
from collections.abc import Iterator
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from dataclasses import replace
from datetime import date
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

import structlog

from chatsbom.__version__ import __version__
from chatsbom.core import catalog
from chatsbom.core import decisions
from chatsbom.core import depgraph_store
from chatsbom.core.catalog import Tracked
from chatsbom.core.config import PathConfig
from chatsbom.core.documents import DEPGRAPH
from chatsbom.core.documents import Document
from chatsbom.core.documents import FILE_MANIFESTS
from chatsbom.core.documents import FILES
from chatsbom.core.documents import LedgerRecords
from chatsbom.core.documents import SYFT
from chatsbom.core.documents import TrackedRecords
from chatsbom.core.edges import EdgeCounts
from chatsbom.core.edges import edges_in
from chatsbom.core.fs import looks_like_whole_json_object
from chatsbom.core.instants import UNSET
from chatsbom.core.instants import utc
from chatsbom.core.layout import is_sha
from chatsbom.core.layout import landed
from chatsbom.core.manifest import relationships_from
from chatsbom.core.manifest import sources_of
from chatsbom.models.provenance import MANIFEST
from chatsbom.models.repository import Repository
from chatsbom.services.db_service import DbService
from chatsbom.services.db_service import ecosystems_of
from chatsbom.services.db_service import graph_observed_at
from chatsbom.warehouse.writer import Scan

logger = structlog.get_logger('warehouse')

#: The tool of a commit's manifests: the parsers are this package's
#: (`core/gradle.py`, `core/podspec.py`), so its version is theirs.
MANIFEST_TOOL = f'chatsbom@{__version__}'

#: A graph's tool when the document names none.
GRAPH_TOOL = 'github-dependency-graph'

#: The input key of the graph kept from before every fetch was.
LEGACY_KEY = depgraph_store.LEGACY


@dataclass
class Repositories:
    """What one repository contributes to the warehouse."""

    row: dict[str, Any]
    releases: list[dict[str, Any]]
    scans: list[Scan]


@dataclass
class Decided:
    """What the store's decisions say of one repository."""

    #: A record's fields, to lay over its record.
    record: dict[str, Any] = field(default_factory=dict)
    #: The ref each commit was resolved from, and its type, by commit.
    refs: dict[str, tuple[str, str]] = field(default_factory=dict)


@dataclass
class Universe:
    """Which repositories there are, and which are the corpus."""

    #: The snapshot the corpus is, or '' when the store has none.
    corpus: str
    #: Its repositories, or None for every repository.
    ids: frozenset[int] | None
    #: What the snapshots say of each, by id: the `TrackedRecords`
    #: master list.
    tracked: dict[int, Tracked] = field(default_factory=dict)


class StoreReader:
    """One pass over the store at `paths`, as it stands on `today`."""

    def __init__(self, paths: PathConfig, today: date) -> None:
        self.paths = paths
        self.today = today
        self.service = DbService()
        #: Documents that could not be parsed, and so were left out.
        self.unreadable = 0
        #: Repositories with outputs in the store and no metadata.
        self.unnamed = 0
        #: The count `db edges` made, of each repository's newest graph.
        self.edges = EdgeCounts()
        self._edges_seen = UNSET

    # -- the universe -----------------------------------------------------

    def universe(self) -> Universe:
        """The corpus, and the list `TrackedRecords` masters on.

        Every repository a complete search snapshot lists, as the newest
        of them to list it says: so a repository the universe no longer
        holds keeps its row, and counts where its scans do, as it did
        while the ledger, which the old pipeline seeded from each
        snapshot, listed them (#171). The newest complete snapshot is
        the corpus. A repository only the records name is still in the
        list (`TrackedRecords`).
        """
        tracked: dict[int, Tracked] = {}
        newest: catalog.Snapshot | None = None
        ids: frozenset[int] | None = None
        for snapshot in catalog.snapshots(self.paths.search_dir):
            if not snapshot.complete(self.today):
                continue
            listed = catalog.read_snapshot(snapshot)
            tracked.update(listed.repositories)
            newest, ids = snapshot, frozenset(listed.repositories)
        return Universe(
            corpus=newest.name if newest is not None else '',
            ids=ids,
            tracked=tracked,
        )

    def history(self) -> Iterator[dict[str, Any]]:
        """A `repository_history` row per repository per dated snapshot."""
        for snapshot in catalog.snapshots(self.paths.search_dir):
            listed = catalog.read_snapshot(snapshot)
            day = datetime.combine(snapshot.day, datetime.min.time())
            complete = snapshot.complete(self.today)
            for repository_id, tracked in listed.repositories.items():
                pushed = listed.pushed_at.get(repository_id)
                yield {
                    'id': repository_id,
                    'snapshot': snapshot.name,
                    'observed_at': day.replace(tzinfo=timezone.utc),
                    'complete': complete,
                    'owner': tracked.owner,
                    'repo': tracked.repo,
                    'stars': tracked.stars,
                    'github_language': tracked.github_language,
                    'default_branch': tracked.default_branch,
                    'pushed_at': utc(pushed) if pushed else None,
                }

    # -- the repositories ---------------------------------------------------

    def repositories(self, universe: Universe) -> Iterator[Repositories]:
        """Each repository the records or the list name, with every scan
        the store holds of it."""
        paths = self.paths
        found = LedgerRecords(
            sorted(paths.sbom_dir.glob('*.jsonl')),
            sorted(paths.repo_dir.glob('*.jsonl')),
        )
        # A listed repository with no record is what `github repo` last
        # fetched of it, as `db index` read it from `repo-metadata`
        # (#181), else what the list says.
        records = TrackedRecords(
            found, universe.tracked or None, metadata=found.metadata,
        )
        commits = self._commits()
        graphs = self._graphs()
        for data in records.records():
            repository_id = data.get('id')
            decided = self._decided(
                repository_id,
                commits.get(repository_id, set())
                if isinstance(repository_id, int) else set(),
            )
            try:
                repo = Repository.model_validate({**data, **decided.record})
            except Exception as error:  # noqa: BLE001 - counted, not raised
                logger.warning(
                    'Unusable record', repository_id=repository_id,
                    error=str(error),
                )
                self.unreadable += 1
                continue
            row = self.service.parse_repository(repo)
            scans = [
                *self._commit_scans(
                    repo, row, commits.pop(repo.id, ()), decided.refs,
                ),
                *self._graph_scans(repo.id, row, graphs.pop(repo.id, [])),
            ]
            yield Repositories(
                row=row, releases=self.service.parse_releases(repo),
                scans=scans,
            )
        # A repository nothing names is outside every answer, but `db
        # edges` counts every graph in the store, and so does this.
        self.unnamed = len(set(commits) | set(graphs))
        for repository_id, kept in sorted(graphs.items()):
            newest = kept[-1][1]
            try:
                document = FILES.get(DEPGRAPH, repository_id, str(newest))
            except ValueError as error:
                self._unreadable(newest, error)
                continue
            if document is not None:
                self._count_edges(document)
        self.edges.observed_at = self._edges_seen

    def _decided(self, repository_id: object, scanned: set[str]) -> Decided:
        """What the store's decisions say of one repository, whose scans
        are of the commits `scanned` (the module's docstring says how
        they are read)."""
        if not isinstance(repository_id, int) or isinstance(repository_id, bool):
            return Decided()
        keyed = decisions.resolutions(self.paths, repository_id)
        # As text: a ref git holds as bytes that are not UTF-8 is kept
        # in the decision as git has it (`decisions.readable`).
        refs = {
            resolution.commit_sha: decisions.readable(
                (resolution.ref, resolution.ref_type),
            )
            for resolution in sorted(
                (found for resolved in keyed.values() for found in resolved),
                key=decisions.resolved_at,
            )
        }
        newest: decisions.Chain | None = None
        collected: decisions.Chain | None = None
        target: decisions.CommitDecision | None = None
        for chain in decisions.chains(self.paths, repository_id, keyed):
            newest = newest or chain
            if chain.commit is None:
                continue
            target = target or chain.commit
            if chain.commit.commit_sha in scanned:
                collected = chain
                break
            if not scanned:
                # No scan to look for: the newest, and the newest resolved.
                break
        taken = collected or newest
        if taken is None:
            return Decided(refs=refs)
        releases = decisions.release_list(
            self.paths, repository_id, taken.release.releases,
        )
        if releases is None:
            self._unreadable(
                decisions.list_path(
                    self.paths, repository_id, taken.release.releases,
                ),
                ValueError('the release list its decision names'),
            )
        made = decisions.as_record(replace(taken, releases=releases))
        if taken.commit is None and target is not None:
            made['download_target'] = decisions.readable(
                target.download_target,
            )
        return Decided(made, refs)

    def _commits(self) -> dict[int, set[str]]:
        """repository id -> each commit the store has a Syft document or
        manifests of."""
        found: dict[int, set[str]] = {}
        for root, marker in (
            (self.paths.sbom_dir, 'sbom.json'),
            (self.paths.content_dir, None),
        ):
            for repository_id, directory in _numbered(root):
                for scan in _children(directory):
                    if not is_sha(scan.name):
                        continue
                    if marker is not None and not (scan / marker).is_file():
                        continue
                    found.setdefault(repository_id, set()).add(scan.name)
        return found

    def _graphs(self) -> dict[int, list[tuple[str, Path]]]:
        """repository id -> `(input key, document)` of each whole graph
        kept of it, oldest first: the legacy one, then every fetch."""
        found: dict[int, list[tuple[str, Path]]] = {}
        for repository_id, directory in _numbered(self.paths.depgraph_dir):
            kept: list[tuple[str, Path]] = []
            legacy = directory / LEGACY_KEY / depgraph_store.DOCUMENT
            if looks_like_whole_json_object(legacy):
                kept.append((LEGACY_KEY, legacy))
            kept += [
                (fetch.directory.name, fetch.document)
                for fetch in depgraph_store.fetches(
                    self.paths.depgraph_dir, repository_id,
                )
            ]
            if kept:
                found[repository_id] = kept
        return found

    def _commit_scans(
        self,
        repo: Repository,
        row: Mapping[str, Any],
        commits: set[str] | tuple[()],
        refs: Mapping[str, tuple[str, str]],
    ) -> Iterator[Scan]:
        """Each commit's Syft scan, where it has a document, and its
        manifests' scan, which it always has: a commit whose manifests
        declare nothing replaces the declarations of the one before.

        A scan's ref is the one `refs` has for its commit, the commit
        decisions', else the download target's where it names the
        commit, else none."""
        paths = self.paths
        target = repo.download_target
        for sha in sorted(commits):
            sbom_path = paths.sbom_file(repo.id, sha)
            sbom: Document | None = None
            if sbom_path.is_file():
                try:
                    sbom = FILES.get(SYFT, repo.id, str(sbom_path))
                except ValueError as error:
                    self._unreadable(sbom_path, error)
            content = paths.content_root(repo.id, sha)
            manifests = (
                FILE_MANIFESTS.for_repository(repo.id, str(content))
                if content.is_dir() else []
            )
            if sbom is not None:
                # The commit's instant, which both its scans and every
                # row the parsers make of them carry: see `_first_had`.
                sbom = replace(
                    sbom, observed_at=_first_had(
                        sbom, content, paths.tree_file(repo.id, sha).parent,
                    ),
                )
            # The layout names a scan by its commit: its ref is what a
            # commit decision, or the newest record, says of the commit.
            ref, ref_type = refs.get(sha) or (
                (target.ref, target.ref_type)
                if target is not None and target.commit_sha == sha
                else ('', '')
            )
            scan_row = {'sbom_ref': ref, 'sbom_commit_sha': sha}
            by_ecosystem = relationships_from(manifests) if manifests else {}
            syft_rows, declared = self.service.scan_rows(
                sbom, manifests, repo.id, scan_row, by_ecosystem,
            )
            read = tuple(sources_of(by_ecosystem))
            if sbom is not None:
                yield Scan(
                    repository_id=repo.id, source=SYFT, input_key=sha,
                    tool=_syft_tool(sbom.body),
                    observed_at=utc(sbom.observed_at),
                    ref=ref, ref_type=ref_type, commit_sha=sha,
                    document=landed(sbom_path), manifest_sources=read,
                    ecosystems=ecosystems_of(syft_rows, manifests),
                    rows=syft_rows,
                )
            yield Scan(
                repository_id=repo.id, source=MANIFEST, input_key=sha,
                tool=MANIFEST_TOOL,
                observed_at=utc(sbom.observed_at if sbom else None),
                ref=ref, ref_type=ref_type, commit_sha=sha,
                document=landed(content) if content.is_dir() else '',
                manifest_sources=read,
                ecosystems=ecosystems_of(declared, manifests),
                rows=declared,
            )

    def _graph_scans(
        self,
        repository_id: int,
        row: Mapping[str, Any],
        kept: list[tuple[str, Path]],
    ) -> Iterator[Scan]:
        """Each graph kept of the repository. The newest fetch, or the
        legacy graph where there is none, is the one counted into
        `edges`, as `db edges` counted it."""
        for position, (key, path) in enumerate(kept):
            try:
                document = FILES.get(DEPGRAPH, repository_id, str(path))
                if document is None:
                    continue
                rows = self.service.parse_dependency_graph(
                    document, repository_id, row,
                )
            except ValueError as error:
                self._unreadable(path, error)
                continue
            if position == len(kept) - 1:
                self._count_edges(document)
            yield Scan(
                repository_id=repository_id, source=DEPGRAPH,
                input_key=key, tool=_graph_tool(document.body),
                observed_at=graph_observed_at(document),
                # The graph's own, as its rows have them.
                ref=document.ref or row['default_branch'],
                commit_sha=document.commit_sha or row['sbom_commit_sha'],
                document=landed(path),
                ecosystems=ecosystems_of(rows),
                rows=rows,
            )

    def _count_edges(self, document: Document) -> None:
        for edge in edges_in(document.body):
            self.edges[edge] += 1
        self.edges.documents += 1
        self._edges_seen = max(self._edges_seen, utc(document.observed_at))

    def _unreadable(self, path: Path, error: Exception) -> None:
        logger.warning('Unreadable document', path=str(path), error=str(error))
        self.unreadable += 1


def _first_had(sbom: Document, *roots: Path) -> datetime:
    """When the store first had a commit: the earliest of its Syft
    document's instant and the mtimes of the files under `roots`, its
    content root and its tree's directory.

    Not the document's alone, which is what `db index` dated a scan by,
    because the document is not written once. After an upgrade of Syft,
    the SBOM stage writes every stored root's document again, an older
    commit's too, in the order it walks them (`staleness`), and
    dated by those, an older commit would be the newest scan of about
    half the repositories that keep two, and its packages would move to
    the month of the upgrade. The tree is listed and the manifests are
    written when the commit is the download target, just before its
    first scan, and nothing writes an older commit's again. The tree
    dates a commit whose content root is empty, which has no manifest
    to date it by (#180).

    A commit with manifests and no document keeps the unset date, as
    `db index` gave its declarations: the scan that follows will date
    it, and until then the commit before it stays the current one, as
    its record, which is only written once the scan is, says.
    """
    earliest = sbom.observed_at
    for root in roots:
        for entry in _files(root):
            try:
                seconds = entry.stat().st_mtime
            except OSError:
                continue
            earliest = min(
                earliest,
                utc(datetime.fromtimestamp(seconds, tz=timezone.utc)),
            )
    return utc(earliest)


def _files(root: Path) -> Iterator[os.DirEntry[str]]:
    """Every file under `root`, as `root.rglob('*')` and `Path.is_file`
    found them: a link to a file is one, and a link to a directory is
    not walked into. A directory that cannot be listed has none.

    From `os.scandir`'s entries, which say from the listing what they
    are, and ask the file system once for a file's `stat`, where the
    rglob asked twice, and once again for each entry what it was
    (#187)."""
    pending = [str(root)]
    while pending:
        try:
            with os.scandir(pending.pop()) as entries:
                listed = list(entries)
        except OSError:
            continue
        for entry in listed:
            if entry.is_dir(follow_symlinks=False):
                pending.append(entry.path)
            elif entry.is_file():
                yield entry


def _numbered(root: Path) -> Iterator[tuple[int, Path]]:
    """`(repository id, directory)` of each `<id>/` under a stage root,
    in id order; anything else there is not a repository's."""
    found = [child for child in _children(root) if child.name.isdigit()]
    for child in sorted(found, key=lambda c: int(c.name)):
        yield int(child.name), child


def _children(directory: Path) -> list[Path]:
    """The directories in `directory`, a link to one among them, in the
    order the file system lists them; none where it cannot be listed.

    By `os.scandir`, whose entries say what they are from the listing
    itself (`d_type`): asking each by `stat`, as `Path.is_dir` does,
    is a seek per entry on a disk that turns (#187). Only a link is
    asked, to follow it."""
    try:
        with os.scandir(directory) as entries:
            return [Path(entry.path) for entry in entries if entry.is_dir()]
    except OSError:
        return []


def _syft_tool(body: Mapping[str, Any]) -> str:
    """`syft@<version>`, as Syft's `descriptor` names itself."""
    descriptor = body.get('descriptor')
    if isinstance(descriptor, Mapping):
        name = str(descriptor.get('name') or 'syft')
        version = descriptor.get('version')
        return f'{name}@{version}' if version else name
    return 'syft'


def _graph_tool(body: Mapping[str, Any]) -> str:
    """The tool the SPDX document names among its creators."""
    sbom = body.get('sbom', body)
    info = sbom.get('creationInfo') if isinstance(sbom, Mapping) else None
    creators = info.get('creators') if isinstance(info, Mapping) else None
    for creator in creators if isinstance(creators, list) else ():
        if isinstance(creator, str) and creator.startswith('Tool:'):
            return creator.removeprefix('Tool:').strip()
    return GRAPH_TOOL
