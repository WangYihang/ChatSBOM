"""The collector's stages, each for one repository and one key (#161).

Each writes to the store what today's stage writes, and follows today's
rules, moved into functions both call:

- **release**, for the push P: the releases from the API
  (`/repositories/<id>/releases`, every page), the tags from `git
  ls-remote`, the bare tags dated (`collector/releases.py`), and the
  latest stable release of them kept as the release decision for P, with
  the list it was chosen from (#147).
- **commit**, for K: the release's tag, or the default branch's head,
  resolved on the same listing (`gitremote.resolve_commit`) and kept as
  the commit decision for K.
- **tree**, for S: every path at the commit, from git's blobless clone,
  as `05-github-tree/<id>/<S>/tree.txt`.
- **content**, for S: the manifests discovery selects from the tree,
  from raw content with no token, stored at their paths under
  `06-github-content/<id>/<S>/`, and `manifests.json` stamped with the
  stage's version (#100 Q4). A root without that stamp is fetched again
  whole.
- **sbom**, for S: Syft's document of the content root, with the
  lockfiles `sbom lock` generated merged in, as `07-sbom/<id>/<S>/
  sbom.json`, from the Syft cache where the same files were scanned by
  the same Syft, and otherwise from the pool.

A stage that finds nothing for its key raises `Nothing`: a repository
with no commit, a commit with no files. One that cannot finish raises:
`StageFailed`, `SyftFailed`, or a `GitHubError` of the client's. What
each did, and when it is due again, is the runner's to keep
(`collector/runner.py`); what is due at all is the store's to say
(`collector/due.py`).

One listing of the refs serves a collection: made after the push was
observed, it is as good for the commit stage as for the release stage
(#100 Q6), and it is not kept past the collection.
"""
from __future__ import annotations

import asyncio
import contextlib
import json
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timezone
from typing import Any

import structlog

from chatsbom.collector.client import Answer
from chatsbom.collector.client import GitHubClient
from chatsbom.collector.content import CONTENT_VERSION
from chatsbom.collector.content import Got
from chatsbom.collector.content import outcomes_of
from chatsbom.collector.content import read_document
from chatsbom.collector.content import settle
from chatsbom.collector.content import stamp_of
from chatsbom.collector.content import stored_discovery
from chatsbom.collector.content import walk
from chatsbom.collector.content import Walked
from chatsbom.collector.errors import Failed
from chatsbom.collector.errors import Gone
from chatsbom.collector.errors import NotFound
from chatsbom.collector.errors import Unauthorized
from chatsbom.collector.gitremote import GitRemote
from chatsbom.collector.gitremote import resolve_commit
from chatsbom.collector.raw import RawClient
from chatsbom.collector.releases import carried_dates
from chatsbom.collector.releases import git_dated
from chatsbom.collector.releases import release_history
from chatsbom.collector.releases import to_ask
from chatsbom.collector.state import CollectorState
from chatsbom.collector.syftpool import SyftPool
from chatsbom.core import decisions
from chatsbom.core.config import PathConfig
from chatsbom.core.decisions import Outcome
from chatsbom.core.decisions import ReleaseDecision
from chatsbom.core.fs import atomic_write_bytes
from chatsbom.core.fs import atomic_write_text
from chatsbom.core.git import RemoteRefs
from chatsbom.core.git import tags_of
from chatsbom.core.layout import push_text
from chatsbom.services.sbom_service import _cached_sbom
from chatsbom.services.sbom_service import content_fingerprint
from chatsbom.services.sbom_service import scan_directory

logger = structlog.get_logger('collector.stages')

#: Releases asked for a page.
PER_PAGE = 100

#: Pages of releases read at most: ten thousand releases.
MAX_PAGES = 100


class Nothing(Exception):
    """The stage ran, and there is nothing for its key."""


class StageFailed(Exception):
    """The stage could not finish, for a reason its text says."""


@dataclass(frozen=True)
class Target:
    """A repository to collect: its id, and its name now."""

    repository_id: int
    #: `owner/name`, as git and raw content are asked for it.
    full_name: str


@dataclass
class Tools:
    """What the stages run on, shared by every repository."""

    paths: PathConfig
    state: CollectorState
    github: GitHubClient
    raw: RawClient
    git: GitRemote
    syft: SyftPool
    #: Now, in UTC epoch seconds, as the budget counts.
    clock: Callable[[], float]
    #: How long an API request waits for a token with room, at most;
    #: None waits for as long as it takes.
    wait: float | None = None
    #: What was asked for: API requests (and GraphQL points) by the
    #: bucket billed, `raw` files and `syft` scans.
    spent: Counter[str] = field(default_factory=Counter)

    def now(self) -> datetime:
        return datetime.fromtimestamp(self.clock(), timezone.utc)

    def count(self, answer: Answer) -> None:
        """An API answer, by its bucket: a 304 is free."""
        if not answer.not_modified:
            self.spent[answer.bucket] += 1


@dataclass(frozen=True)
class Done:
    """What a stage wrote."""

    #: For a person, in a line.
    summary: str
    #: What it produced: T ('' for none), S, the content's digest.
    output: str = ''


def _plural(count: int, one: str, many: str | None = None) -> str:
    return f'{count:,} {one if count == 1 else (many or one + "s")}'


class RepositoryStages:
    """The stages of one repository, for one collection."""

    def __init__(self, tools: Tools, target: Target) -> None:
        self.tools = tools
        self.target = target
        self._listing: RemoteRefs | None = None

    @property
    def _id(self) -> int:
        return self.target.repository_id

    async def listing(self) -> RemoteRefs:
        """The repository's refs, listed once a collection."""
        if self._listing is None:
            listing = await self.tools.git.list_remote(self.target.full_name)
            if listing.error:
                raise StageFailed(
                    f'git ls-remote of {self.target.full_name} failed: '
                    f'{listing.error}',
                )
            self._listing = listing
        return self._listing

    # -- release ----------------------------------------------------------

    async def release(self, push: datetime) -> Done:
        """Decide the latest stable release for `push`, and keep it."""
        releases = await self._releases()
        listing = await self.listing()
        released = {entry.get('tag_name') for entry in releases}
        bare = {
            name: sha for name, sha in tags_of(listing.refs).items()
            if name not in released
        }
        dates = carried_dates(self._previous_list(), bare)
        missing = {
            name: sha for name,
            sha in bare.items() if name not in dates
        }
        if missing:
            dates.update(
                git_dated(
                    missing,
                    await self.tools.git.tag_dates(self.target.full_name),
                ),
            )
            for name in to_ask(name for name in missing if name not in dates):
                date = await self._commit_date(missing[name])
                if date:
                    dates[name] = date
        entries, latest = release_history(releases, bare, dates)
        kept = decisions.keep_release(
            self.tools.paths, {
                'id': self._id,
                'pushed_at': push_text(push),
                'all_releases': entries,
                'latest_stable_release': latest,
            },
        )
        if kept.decision is Outcome.CONFLICT:
            raise StageFailed(
                'another release decision is kept for this push, and stands',
            )
        if kept.decision is Outcome.UNKEYED:
            raise StageFailed(f'no push to key a release decision by: {push}')
        tag = latest.tag_name if latest is not None else None
        return Done(
            f'decided {tag} of {_plural(len(entries), "release")}'
            if tag is not None else
            f'decided no release of {_plural(len(entries), "release")}',
            tag or '',
        )

    async def _releases(self) -> list[dict[str, Any]]:
        """Every release, newest first, every page of them."""
        found: list[dict[str, Any]] = []
        where = f'/repositories/{self._id}/releases'
        params: dict[str, str | int] | None = {'per_page': PER_PAGE}
        for _ in range(MAX_PAGES):
            answer = await self.tools.github.get(
                where, params=params, conditional=False,
                wait=self.tools.wait,
            )
            self.tools.count(answer)
            page = answer.json()
            if not isinstance(page, list):
                raise StageFailed(
                    f'the releases of {self.target.full_name} are not a list',
                )
            found.extend(entry for entry in page if isinstance(entry, dict))
            following = answer.links.get('next')
            if not following or not page:
                return found
            where, params = following, None
        logger.warning(
            'Releases read to the most pages kept',
            repo=self.target.full_name, pages=MAX_PAGES,
        )
        return found

    def _previous_list(self) -> list[dict[str, Any]] | None:
        """The release list of the newest decision before this one."""
        paths = self.tools.paths
        for decision in decisions.release_decisions(paths, self._id):
            return decisions.release_list(paths, self._id, decision.releases)
        return None

    async def _commit_date(self, sha: str) -> str | None:
        """A commit's committer date, from the API, or None."""
        try:
            answer = await self.tools.github.get(
                f'/repositories/{self._id}/commits/{sha}', conditional=False,
                wait=self.tools.wait,
            )
        except Unauthorized:
            raise
        except (NotFound, Gone, Failed):
            return None
        self.tools.count(answer)
        body = answer.json()
        commit = body.get('commit') if isinstance(body, dict) else None
        committer = commit.get('committer') if isinstance(
            commit, dict,
        ) else None
        date = committer.get('date') if isinstance(committer, dict) else None
        return date if isinstance(date, str) else None

    # -- commit -----------------------------------------------------------

    async def commit(self, release: ReleaseDecision) -> Done:
        """Resolve the release decision's key to a commit, and keep it."""
        found = resolve_commit(await self.listing(), release.tag)
        if found is None:
            raise Nothing(
                f'no commit to collect: {self.target.full_name} has no HEAD',
            )
        kept = decisions.keep_commit(
            self.tools.paths, {
                'id': self._id,
                'pushed_at': push_text(release.push),
                'has_releases': release.tag is not None,
                'latest_stable_release': (
                    None if release.tag is None else {'tag_name': release.tag}
                ),
                'download_target': found.model_dump(mode='json'),
            },
        )
        if kept is Outcome.CONFLICT:
            raise StageFailed(
                f'another resolution of {release.key} is kept, and stands',
            )
        if kept is Outcome.UNKEYED:
            raise StageFailed(
                f'no key to keep a commit decision by: {release}',
            )
        return Done(
            f'resolved {release.key} to {found.commit_sha_short} '
            f'({found.ref_type} {found.ref})',
            found.commit_sha,
        )

    # -- tree -------------------------------------------------------------

    async def tree(self, sha: str) -> Done:
        """List every path at `sha`, and keep the list."""
        files = await self.tools.git.tree(self.target.full_name, sha)
        if files is None:
            raise StageFailed(
                f'git could not list the tree of {sha[:7]} in '
                f'{self.target.full_name}',
            )
        if not files:
            raise Nothing(f'{sha[:7]} has no files')
        atomic_write_text(
            self.tools.paths.tree_file(self._id, sha),
            ''.join(f'{path}\n' for path in files),
        )
        return Done(f'listed {_plural(len(files), "path")} at {sha[:7]}', sha)

    # -- content ----------------------------------------------------------

    async def content(self, sha: str) -> Done:
        """Fetch the manifests the tree has, at `sha`, and stamp them."""
        paths = self.tools.paths
        discovery = stored_discovery(paths, self._id, sha)
        if discovery is None:
            raise StageFailed(f'no whole tree of {sha[:7]} to read')
        root = paths.content_root(self._id, sha)
        root.mkdir(parents=True, exist_ok=True)
        index = paths.discovery_file(self._id, sha)
        # A root the stage in force stamped is taken up where it was left;
        # any other is fetched again whole (#100 Q4, strict).
        document = read_document(index)
        force = not (
            document is not None and document.get('commit_sha') == sha
            and (stamp_of(document) or 0) >= CONTENT_VERSION
        )
        known = {} if force else outcomes_of(document, sha)
        walking = walk(
            discovery, root, known=known, force=force,
            repository=self.target.full_name,
        )
        done: Walked
        fetched = 0
        try:
            wanted = next(walking)
            while True:
                answer = await self.tools.raw.fetch(
                    self.target.full_name, sha, wanted.path, wanted.room,
                )
                self.tools.spent['raw'] += 1
                fetched += isinstance(answer, Got) and answer.status == 200
                wanted = walking.send(answer)
        except StopIteration as stop:
            done = stop.value
        digest, _ = settle(
            discovery, done, repository_id=self._id, sha=sha, index=index,
            stamp=CONTENT_VERSION,
        )
        if done.transient:
            raise StageFailed(
                f'{len(done.transient)} of {len(discovery.selected)} files '
                f'could not be fetched, first {done.transient[0]}',
            )
        stored = sum(
            1 for entry in done.fetched.values() if entry.get('status') == 'ok'
        )
        again = ', the root fetched again whole' if force and document else ''
        return Done(
            f'{stored:,} of {_plural(len(discovery.selected), "manifest")} '
            f'stored, {_plural(done.total, "byte")} ({fetched:,} fetched'
            f'{again})',
            digest,
        )

    # -- sbom -------------------------------------------------------------

    async def sbom(
        self, sha: str, syft_version: str | None, *, priority: int = 0,
    ) -> Done:
        """Syft's document of the content root at `sha`, kept."""
        paths = self.tools.paths
        project = paths.content_root(self._id, sha)
        output = paths.sbom_file(self._id, sha)
        # The directory Syft scans, a copy with lockfiles merged in where
        # `sbom lock` made any: made and removed off the event loop.
        stack = contextlib.ExitStack()
        try:
            scanned = await asyncio.to_thread(
                stack.enter_context,
                scan_directory(
                    project, paths.generated_lock_path(self._id, sha),
                    repo=self.target.full_name,
                ),
            )
            fingerprint = await asyncio.to_thread(content_fingerprint, scanned)
            cache = paths.get_sbom_cache_path(
                self._id, fingerprint, syft_version,
            )
            document = await asyncio.to_thread(_cached_sbom, cache)
            cached = document is not None
            if document is None:
                document = await self.tools.syft.scan(
                    scanned, priority=priority,
                )
                self.tools.spent['syft'] += 1
        finally:
            await asyncio.to_thread(stack.close)
        atomic_write_bytes(output, document)
        if not cached:
            try:
                atomic_write_bytes(cache, document)
            except OSError as error:
                logger.warning(
                    'Could not keep a scan in the Syft cache',
                    path=str(cache), error=str(error),
                )
        packages = await asyncio.to_thread(_packages, document)
        how = 'from the cache of' if cached else 'scanned by'
        return Done(
            f'{how} Syft {syft_version or "unknown"}: '
            f'{_plural(packages, "package")}',
            sha,
        )


def _packages(document: bytes) -> int:
    """How many artifacts a Syft document lists."""
    try:
        loaded = json.loads(document)
    except ValueError:
        return 0
    artifacts = loaded.get('artifacts') if isinstance(loaded, dict) else None
    return len(artifacts) if isinstance(artifacts, list) else 0
