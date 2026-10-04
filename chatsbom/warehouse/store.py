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
from collections.abc import Callable
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
from typing import TypeGuard

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
from chatsbom.core.edges import edges_in
from chatsbom.core.fs import files_under
from chatsbom.core.fs import looks_like_whole_json_object
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
from chatsbom.warehouse import carry
from chatsbom.warehouse.prefetch import Prefetcher
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
class Unit:
    """What one repository contributes to the warehouse, read by its id.

    Its row, releases and scans, where it has a record that can be read;
    and either way the edges of its newest graph, and how many of its
    documents could not be read. A repository whose record cannot be
    read is read in two parts, its record and then what else the store
    has of it, as `db index` read it: two units of one id.
    """

    #: Its id; None for a record whose id is not a number.
    id: int | None
    row: dict[str, Any] | None = None
    releases: list[dict[str, Any]] = field(default_factory=list)
    scans: list[Scan] = field(default_factory=list)
    #: The package pairs its newest graph shows (`core/edges.py`), and
    #: that graph's instant: None where no graph of it is counted.
    edges: frozenset[tuple[str, str]] = frozenset()
    graph_seen: datetime | None = None
    #: Its documents that could not be parsed, and so were left out.
    unreadable: int = 0
    #: It has outputs in the store and no metadata.
    unnamed: bool = False
    #: Its record, as `carry.record_digest` says it; '' for none.
    record: str = ''
    #: Not read: what the warehouse before has of it stands
    #: (`carry.py`), and every field above but `id` and `record` is
    #: empty.
    carried: bool = False
    #: Not read, the pass's time being up (`build.py`): not in what it
    #: writes either. Every field above but `id` and `record` is empty.
    skipped: bool = False


#: Whether a repository is as the warehouse before read it, by its id,
#: its record and the directories its id names (`carry.Previous`).
Unchanged = Callable[[int, str, frozenset[str]], bool]


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
        #: Each stage root's repository directories, by id, as
        #: `_numbered` lists them.
        self._listed: dict[Path, dict[int, list[Path]]] = {}
        self._commits_found: dict[int, set[str]] = {}
        self._graphs_found: dict[int, list[tuple[str, Path]]] = {}
        #: The ids whose outputs a repository was read with.
        self._consumed: set[int] = set()
        #: The ids a record names.
        self._recorded: set[int] = set()
        #: Each repository's directories, as they were before it was
        #: read (`carry.walk`), by id, until the pass takes them.
        self.states: dict[int, carry.State] = {}
        self._walked: set[int] = set()
        #: Whether what this pass read can be carried by the next: not
        #: when a record's id is not a number, which `Repository` reads
        #: as one, and so may take another record's outputs.
        self.carryable = True

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

    def list_roots(self) -> None:
        """List each stage root a repository is read from, once."""
        paths = self.paths
        for root in (
            paths.release_dir, paths.commit_dir, paths.tree_dir,
            paths.content_dir, paths.sbom_dir, paths.depgraph_dir,
        ):
            listed: dict[int, list[Path]] = {}
            for repository_id, directory in _numbered(root):
                listed.setdefault(repository_id, []).append(directory)
            self._listed[root] = listed

    def tops(self, repository_id: int) -> frozenset[str]:
        """The directories the repository's id names in the stage roots,
        relative to the data directory."""
        base = self.paths.base_data_dir
        return frozenset(
            str(directory.relative_to(base))
            for listed in self._listed.values()
            for directory in listed.get(repository_id, ())
        )

    def units(
        self,
        universe: Universe,
        unchanged: Unchanged | None = None,
        enough: Callable[[], bool] | None = None,
    ) -> Iterator[Unit]:
        """Each repository the records or the list name, with every scan
        the store holds of it, in the records' order; then each the store
        has outputs of and nothing names.

        A repository's outputs are read when it is: they are the
        directories of its id, and the stage roots are listed once, by
        `_numbered`, to find them. A record whose id is not a number
        stands for the repository its id is read as (`Repository`), and
        takes the outputs of that id, unless another record took them
        first; that is what `db index` did with them.

        A repository `unchanged` says is as the warehouse before read it
        is not read, but carried (`carry.py`). Never where a record's id
        is not a number: which outputs are whose is then not the
        repository's own to say.

        Once `enough` says so, a repository that is not carried is not
        read either, but skipped: all of it, a repository whose record
        cannot be read included, whose second part is read whenever its
        first was.
        """
        paths = self.paths
        found = LedgerRecords(
            sorted(paths.sbom_dir.glob('*.jsonl')),
            sorted(paths.repo_dir.glob('*.jsonl')),
        )
        # A listed repository with no record is what `github repo` last
        # fetched of it, as `db index` read it from `repo-metadata`
        # (#181), else what the list says.
        records = list(
            TrackedRecords(
                found, universe.tracked or None, metadata=found.metadata,
            ).records(),
        )
        if not self._listed:
            self.list_roots()
        if not all(_plain(data.get('id')) for data in records):
            self.carryable = False
            unchanged = None
        digests = [carry.record_digest(data) for data in records]
        listed = set().union(
            *(
                self._listed[root] for root in (
                    paths.sbom_dir, paths.content_dir, paths.depgraph_dir,
                )
            ),
        )
        # What the pass will read, in order, for threads to read ahead of
        # it (`prefetch.py`): what a record names, then what none does.
        recorded = {data.get('id') for data in records}
        read: list[int] = []
        for data, record in zip(records, digests):
            key = data.get('id')
            if _plain(key) and not (
                unchanged is not None
                and unchanged(key, record, self.tops(key))
            ):
                read.append(key)
        read += [
            key for key in sorted(listed - recorded)
            if not (
                unchanged is not None and unchanged(key, '', self.tops(key))
            )
        ]
        with Prefetcher(
            [self._directories(key) for key in read],
            stat_only=[paths.tree_dir],
        ) as ahead:
            for data, record in zip(records, digests):
                unit = self._named(data, record, unchanged, enough)
                if not (unit.carried or unit.skipped):
                    ahead.advance()
                yield unit
            # A repository nothing names is outside every answer, but `db
            # edges` counts every graph in the store, and so does this.
            for repository_id in sorted(listed - self._consumed):
                if (
                    unchanged is not None
                    and repository_id not in self._recorded
                    and unchanged(repository_id, '', self.tops(repository_id))
                ):
                    yield Unit(repository_id, carried=True)
                    continue
                if (
                    enough is not None and repository_id not in self._recorded
                    and enough()
                ):
                    yield Unit(repository_id, skipped=True)
                    continue
                ahead.advance()
                yield self._unnamed(repository_id)

    def _directories(self, repository_id: int) -> list[Path]:
        """The directories the repository's id names in the stage roots."""
        return sorted(
            directory
            for listed in self._listed.values()
            for directory in listed.get(repository_id, ())
        )

    def _capture(self, repository_id: int) -> None:
        """The repository's directories, before anything in them is read."""
        if repository_id in self._walked:
            return
        self._walked.add(repository_id)
        self.states[repository_id] = carry.walk(
            self.paths.base_data_dir, self._directories(repository_id),
        )

    def _named(
        self,
        data: dict[str, Any],
        record: str,
        unchanged: Unchanged | None,
        enough: Callable[[], bool] | None = None,
    ) -> Unit:
        """A repository a record names, and what the store has of it;
        `record` is its digest (`carry.record_digest`)."""
        before = self.unreadable
        repository_id = data.get('id')
        if _plain(repository_id):
            self._recorded.add(repository_id)
            if unchanged is not None and unchanged(
                repository_id, record, self.tops(repository_id),
            ):
                self._consumed.add(repository_id)
                return Unit(repository_id, record=record, carried=True)
            if enough is not None and enough():
                self._consumed.add(repository_id)
                return Unit(repository_id, record=record, skipped=True)
            self._capture(repository_id)
        decided = self._decided(
            repository_id,
            self._commits_of(repository_id)
            if isinstance(repository_id, int)
            and repository_id not in self._consumed else set(),
        )
        key = repository_id if _plain(repository_id) else None
        try:
            repo = Repository.model_validate({**data, **decided.record})
        except Exception as error:  # noqa: BLE001 - counted, not raised
            logger.warning(
                'Unusable record', repository_id=repository_id,
                error=str(error),
            )
            self.unreadable += 1
            return Unit(
                key, unreadable=self.unreadable - before, record=record,
            )
        unit = Unit(repo.id, record=record)
        commits: set[str] | tuple[()] = ()
        graphs: list[tuple[str, Path]] = []
        if repo.id not in self._consumed:
            self._capture(repo.id)
            commits, graphs = self._commits_of(
                repo.id,
            ), self._graphs_of(repo.id)
            self._consumed.add(repo.id)
        unit.row = self.service.parse_repository(repo)
        unit.releases = self.service.parse_releases(repo)
        unit.scans = [
            *self._commit_scans(repo, unit.row, commits, decided.refs),
            *self._graph_scans(unit, repo.id, unit.row, graphs),
        ]
        unit.unreadable = self.unreadable - before
        return unit

    def _unnamed(self, repository_id: int) -> Unit:
        """A repository the store has outputs of and no record names:
        none of its scans is read, but its newest graph is counted."""
        before = self.unreadable
        self._capture(repository_id)
        commits = self._commits_of(repository_id)
        graphs = self._graphs_of(repository_id)
        self._consumed.add(repository_id)
        unit = Unit(repository_id, unnamed=bool(commits or graphs))
        if graphs:
            newest = graphs[-1][1]
            try:
                document = FILES.get(DEPGRAPH, repository_id, str(newest))
            except ValueError as error:
                self._unreadable(newest, error)
            else:
                if document is not None:
                    _count_edges(unit, document)
        unit.unreadable = self.unreadable - before
        return unit

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

    def _commits_of(self, repository_id: int) -> set[str]:
        """Each commit the store has a Syft document or manifests of."""
        if repository_id in self._commits_found:
            return self._commits_found[repository_id]
        found: set[str] = set()
        for root, marker in (
            (self.paths.sbom_dir, 'sbom.json'),
            (self.paths.content_dir, None),
        ):
            for directory in self._listed[root].get(repository_id, ()):
                for scan in _children(directory):
                    if not is_sha(scan.name):
                        continue
                    if marker is not None and not (scan / marker).is_file():
                        continue
                    found.add(scan.name)
        self._commits_found[repository_id] = found
        return found

    def _graphs_of(self, repository_id: int) -> list[tuple[str, Path]]:
        """`(input key, document)` of each whole graph kept of the
        repository, oldest first: the legacy one, then every fetch."""
        if repository_id in self._graphs_found:
            return self._graphs_found[repository_id]
        found: list[tuple[str, Path]] = []
        for directory in self._listed[self.paths.depgraph_dir].get(
            repository_id, (),
        ):
            kept: list[tuple[str, Path]] = []
            legacy = directory / LEGACY_KEY / depgraph_store.DOCUMENT
            if looks_like_whole_json_object(legacy):
                kept.append((LEGACY_KEY, legacy))
            kept += [
                (fetch.directory.name, fetch.document)
                for fetch in depgraph_store.fetches(
                    self.paths.depgraph_dir, int(repository_id),
                )
            ]
            if kept:
                found = kept
        self._graphs_found[repository_id] = found
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
        unit: Unit,
        repository_id: int,
        row: Mapping[str, Any],
        kept: list[tuple[str, Path]],
    ) -> Iterator[Scan]:
        """Each graph kept of the repository. The newest fetch, or the
        legacy graph where there is none, is the one whose edges are the
        unit's, as `db edges` counted it."""
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
                _count_edges(unit, document)
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

    def _unreadable(self, path: Path, error: Exception) -> None:
        logger.warning('Unreadable document', path=str(path), error=str(error))
        self.unreadable += 1


def _count_edges(unit: Unit, document: Document) -> None:
    """The graph `document` is the one of `unit` that is counted."""
    unit.edges = frozenset(edges_in(document.body))
    unit.graph_seen = utc(document.observed_at)


def _plain(value: object) -> TypeGuard[int]:
    """A number as an id is: not a flag, which Python counts as one."""
    return isinstance(value, int) and not isinstance(value, bool)


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
        for entry in files_under(root):
            try:
                seconds = entry.stat().st_mtime
            except OSError:
                continue
            earliest = min(
                earliest,
                utc(datetime.fromtimestamp(seconds, tz=timezone.utc)),
            )
    return utc(earliest)


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
