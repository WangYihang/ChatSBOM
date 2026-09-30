"""The repositories' git remotes, as the collector's stages read them
(#161; #128 section 2.1).

What today's stages ask of git, they ask here, with today's code
(`services/git_service.py`): `git ls-remote --symref`, every ref and
the branch HEAD points at, for the release stage's tags and the commit
stage's refs; the tag fetch that dates the tags with no release; and the
blobless clone the tree is listed from. None of it spends REST quota.
Each runs in a thread, since `GitService` waits on its subprocess, and
at most `in_flight` at once: the process runs about four network tasks
a token at a time (#128), and git's are among them.

The token, where there is one, goes to github.com in git's environment
as `GitService` sends it, never on a command line; `base` stands in for
github.com, and a test's repositories on disk are reached as `file://`.

`resolve_commit` is the commit stage's rule, `CommitService`'s, on one
listing: the release's tag, else the default branch's head.
"""
from __future__ import annotations

import asyncio

import structlog

from chatsbom.collector.tokens import Token
from chatsbom.models.download_target import DownloadTarget
from chatsbom.services.git_service import GitService
from chatsbom.services.git_service import head_commit
from chatsbom.services.git_service import ref_commit
from chatsbom.services.git_service import RemoteRefs
from chatsbom.services.git_service import TagDate

logger = structlog.get_logger('collector.git')

#: Where the repositories are.
GITHUB = 'https://github.com'

#: git's network tasks at once, for one token.
IN_FLIGHT = 4


def _split(full_name: str) -> tuple[str, str]:
    owner, _, name = full_name.partition('/')
    return owner, name


class GitRemote:
    """git, on the repositories at `base`."""

    def __init__(
        self,
        *,
        token: Token | None = None,
        base: str = GITHUB,
        in_flight: int = IN_FLIGHT,
    ) -> None:
        self._token = token
        self._base = base.rstrip('/')
        self._slots = asyncio.Semaphore(max(1, in_flight))

    def url(self, full_name: str) -> str:
        """A repository's remote."""
        return f'{self._base}/{full_name}.git'

    def _service(self) -> GitService:
        return GitService(self._token.secret if self._token else None)

    async def list_remote(self, full_name: str) -> RemoteRefs:
        """Every ref of the repository, and the branch HEAD points at: one
        `git ls-remote --symref`. A listing that failed says why
        (`RemoteRefs.error`)."""
        owner, name = _split(full_name)
        async with self._slots:
            return await asyncio.to_thread(
                self._service().list_remote, owner, name,
                url=self.url(full_name),
            )

    async def tag_dates(self, full_name: str) -> dict[str, TagDate] | None:
        """Every tag's commit and date, by fetching the tags' commits
        alone; None if git failed."""
        owner, name = _split(full_name)
        async with self._slots:
            return await asyncio.to_thread(
                self._service().get_tag_dates, owner, name,
                url=self.url(full_name),
            )

    async def tree(self, full_name: str, sha: str) -> list[str] | None:
        """Every path at the commit; None if git failed."""
        owner, name = _split(full_name)
        async with self._slots:
            return await asyncio.to_thread(
                self._service().get_repository_tree, owner, name, sha,
                url=self.url(full_name),
            )


def resolve_commit(
    listing: RemoteRefs, tag: str | None,
) -> DownloadTarget | None:
    """The commit a repository is collected at, from one listing: the
    release's `tag`, exactly or as a tag or a branch; or, with no release
    or a tag that is gone, the head of the branch HEAD points at, never a
    guessed `main`. None when there is no HEAD either: an empty
    repository."""
    sha: str | None = None
    ref, ref_type = '', 'branch'
    if tag is not None:
        ref, ref_type = tag, 'release'
        sha = ref_commit(listing.refs, tag)
        if not sha:
            logger.warning(
                'Tag not found, falling back to default branch', tag=tag,
            )
    if not sha:
        ref_type = 'branch'
        branch, sha = head_commit(listing)
        # A server that names no branch still has a HEAD.
        ref = branch or 'HEAD'
    if not sha:
        return None
    return DownloadTarget(
        ref=ref, ref_type=ref_type, commit_sha=sha, commit_sha_short=sha[:7],
    )
