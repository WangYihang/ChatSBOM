from __future__ import annotations

import json
import os
import shutil
import subprocess
import tempfile
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import cast

import git
import structlog

from chatsbom.core.config import get_config
from chatsbom.core.fs import atomic_write_text
from chatsbom.core.git import error_text as _error_text
from chatsbom.core.git import git_auth_env
from chatsbom.core.git import GIT_QUIET_ENV
from chatsbom.core.git import head_commit
from chatsbom.core.git import LS_REMOTE_TIMEOUT
from chatsbom.core.git import parse_ls_remote
from chatsbom.core.git import parse_tag_listing
from chatsbom.core.git import ref_commit
from chatsbom.core.git import RemoteRefs
from chatsbom.core.git import run_git as _git
from chatsbom.core.git import short_name
from chatsbom.core.git import TAG_FETCH_FILTERS
from chatsbom.core.git import TAG_FETCH_TIMEOUT
from chatsbom.core.git import TAG_FORMAT as _TAG_FORMAT
from chatsbom.core.git import TagDate
from chatsbom.core.git import tags_of
from chatsbom.core.git import TREE_FETCH_TIMEOUT

logger = structlog.get_logger('git_service')

#: What the pipeline's callers and tests import from here, which is
#: `core/git.py`'s now (#171).
__all__ = [
    'GIT_QUIET_ENV', 'GitService', 'RemoteRefs', 'TagDate', '_error_text',
    '_git', 'git_auth_env', 'head_commit', 'parse_ls_remote',
    'parse_symref_head', 'parse_tag_listing', 'ref_commit', 'short_name',
    'tags_of',
]


class GitService:
    """Service for interacting with Git using GitPython, bypassing GitHub REST API limits."""

    def __init__(self, token: str | None = None):
        self.token = token
        self.g = git.cmd.Git()
        self.config = get_config()

    def get_repo_refs(self, owner: str, repo: str, cache_path: Path | None = None) -> tuple[dict[str, str], bool]:
        """
        Fetch all tags and branches for a repository, for resolving a ref.
        Returns (refs_dict, is_cached).

        Keyed by full ref name and by short name alike, so `main` is here
        without saying whether it is a branch or a tag. For the tags alone,
        use `get_repo_tags`.
        """
        listing = self.list_remote(owner, repo, cache_path=cache_path)
        return listing.refs, listing.cached

    def list_remote(
        self,
        owner: str,
        repo: str,
        cache_path: Path | None = None,
        *,
        need_head: bool = False,
        url: str | None = None,
    ) -> RemoteRefs:
        """Every ref of the repository, and the branch HEAD points at.

        One `git ls-remote --symref <url>`: the listing every ref is
        resolved from also names the default branch, in its first line
        (`ref: refs/heads/master\tHEAD`), at no cost in API quota. That
        is where the default branch comes from: the ledger's copy is the
        search snapshot's, possibly stale, and for a repository tracked
        without a snapshot it is empty (#55 pilot).

        `need_head`: a cache written before the listing kept HEAD's
        branch has none to give, and is listed again rather than trusted.
        `url` stands in for github.com, as for `get_tag_dates`. A listing
        that failed is empty, and says why (`RemoteRefs.error`).
        """
        if cache_path and cache_path.exists():
            try:
                with open(cache_path, encoding='utf-8') as f:
                    cache_data = json.load(f)

                updated_at = cache_data.get('updated_at')
                if updated_at and not (need_head and 'head' not in cache_data):
                    updated_dt = datetime.fromisoformat(updated_at)
                    now = datetime.now(timezone.utc)
                    if (now - updated_dt).total_seconds() < self.config.github.cache_ttl:
                        return RemoteRefs(
                            refs=cache_data.get('data', {}),
                            head=cache_data.get('head') or '',
                            cached=True,
                        )
            except Exception as e:
                logger.debug(
                    'Failed to load refs cache',
                    path=str(cache_path), error=str(e),
                )

        url = url or f"https://github.com/{owner}/{repo}.git"

        try:
            # The token as a header in git's environment: in the URL, it
            # was on the command line, for any user of the machine to
            # read in `ps` (#47).
            output = self.g.ls_remote(
                url, symref=True, kill_after_timeout=LS_REMOTE_TIMEOUT,
                env={**git_auth_env(self.token), **GIT_QUIET_ENV},
            )
            # GitPython types `ls_remote` (its own method from 3.1.51) as
            # anything `execute` may answer; asked for nothing else, it
            # answers the output, as text.
            refs, head = parse_ls_remote(cast(str, output), short_name)

            if cache_path and refs:
                try:
                    # Create structured cache data
                    cache_to_save = {
                        'url': url,
                        'updated_at': datetime.now(timezone.utc).isoformat(),
                        'head': head,
                        'data': refs,
                    }
                    # Atomic, under a temporary name of its own: a fixed
                    # `index.tmp` was shared by concurrent writers, and a
                    # failed write left it behind for good.
                    atomic_write_text(
                        cache_path, json.dumps(cache_to_save, indent=2),
                    )
                except Exception as e:
                    logger.warning(
                        'Failed to save refs cache',
                        path=str(cache_path), error=str(e),
                    )

            return RemoteRefs(refs=refs, head=head, cached=False)

        except git.GitCommandError as e:
            logger.error(
                'Git ls-remote failed',
                url=self._mask_url(url),
                error=self._mask_url(str(e)),
            )
            return RemoteRefs(error=self._mask_url(_error_text(e))[:300])
        except Exception as e:
            logger.error(
                'Unexpected error in git ls-remote',
                url=self._mask_url(url),
                error=self._mask_url(str(e)),
            )
            return RemoteRefs(error=self._mask_url(str(e))[:300])

    def get_repo_tags(self, owner: str, repo: str, cache_path: Path | None = None) -> tuple[dict[str, str], bool]:
        """
        Fetch a repository's tags, by tag name, each at the commit it points to.
        Returns (tags_dict, is_cached).

        Read from the full `refs/tags/*` names only. The short names
        beside them are shared with branches, and `HEAD` has no prefix
        at all: taken as tags, they made releases of branches, dated by
        their head commit and so newer than any real release.

        An annotated tag's own object is not a commit; `get_repo_refs`
        has already replaced it with the commit it peels to (`^{}`).
        """
        refs, is_cached = self.get_repo_refs(
            owner, repo, cache_path=cache_path,
        )
        return tags_of(refs), is_cached

    def get_tag_dates(
        self, owner: str, repo: str, *, url: str | None = None,
    ) -> dict[str, TagDate] | None:
        """Every tag's commit and date, from git, or None if git failed.

        The release stage dates a tag that has no GitHub release by its
        commit. That was one `/commits/{sha}` REST call per tag, a mean
        of 47.4 per repository over the corpus (design #55, F19), about
        2.8 M calls for 60 k repositories. This fetches the same dates
        over the git protocol, which spends no REST quota:

            git init --bare <tmp>
            git fetch --depth=1 --filter=tree:0 <url> +refs/tags/*:refs/tags/*
            git for-each-ref refs/tags

        Commit and tag objects only: the tips, at depth 1, with no trees
        and no blobs. The date is the committer date of the commit a tag
        points to, as `/commits/{sha}` gave it (`commit.committer.date`),
        so the releases chosen do not move. Only a tag of a tree or a
        blob, or a tag of a tag, falls back to its own tagger date.

        `url` stands in for github.com in tests. The token, when there
        is one, is sent as a header through git's environment, never on
        a command line.
        """
        remote = url or f'https://github.com/{owner}/{repo}.git'
        env = {**os.environ, **git_auth_env(self.token), **GIT_QUIET_ENV}
        with tempfile.TemporaryDirectory(prefix='chatsbom-tags-') as tmp:
            try:
                _git(['init', '--bare', '--quiet', tmp], env=env)
                for i, object_filter in enumerate(TAG_FETCH_FILTERS):
                    try:
                        _git(
                            [
                                '-C', tmp, 'fetch', '--quiet', '--no-tags',
                                '--no-write-fetch-head', '--depth=1',
                                f'--filter={object_filter}', remote,
                                '+refs/tags/*:refs/tags/*',
                            ],
                            env=env, timeout=TAG_FETCH_TIMEOUT,
                        )
                        break
                    except subprocess.CalledProcessError:
                        if i == len(TAG_FETCH_FILTERS) - 1:
                            raise
                listing = _git(
                    [
                        '-C', tmp, 'for-each-ref', f'--format={_TAG_FORMAT}',
                        'refs/tags',
                    ],
                    env=env,
                )
            except (OSError, subprocess.SubprocessError) as e:
                logger.warning(
                    'Git tag fetch failed',
                    repo=f'{owner}/{repo}',
                    error=self._mask_url(_error_text(e))[:300],
                )
                return None
        return parse_tag_listing(listing)

    def default_branch_head(
        self, owner: str, repo: str,
    ) -> tuple[str, str] | None:
        """`(branch, sha)` of the default branch's HEAD, or None.

        `git ls-remote --symref <url> HEAD`: one round trip, and no API
        quota. What the dependency graph is stamped with, because GitHub
        builds the graph from the default branch when it is asked: asked
        immediately before the fetch, this is the commit it describes,
        give or take a push in between.

        Anonymous, whatever token this service holds: the repositories
        are public, and a token in a URL is one more place for it to be
        logged.
        """
        url = f'https://github.com/{owner}/{repo}.git'
        try:
            output = self.g.ls_remote(
                '--symref', url, 'HEAD',
                kill_after_timeout=LS_REMOTE_TIMEOUT, env=GIT_QUIET_ENV,
            )
        except Exception as e:  # noqa: BLE001 - reported, the fetch goes on
            logger.warning(
                'Git ls-remote for HEAD failed',
                repo=f'{owner}/{repo}', error=self._mask_url(str(e))[:300],
            )
            return None
        return parse_symref_head(cast(str, output))

    def _get_short_name(self, ref_full: str) -> str | None:
        return short_name(ref_full)

    def _mask_url(self, url: str) -> str:
        """Mask the token in a GitHub URL for safe logging."""
        if self.token and self.token in url:
            return url.replace(self.token, '*****')
        return url

    def resolve_ref(self, owner: str, repo: str, ref: str, cache_path: Path | None = None) -> tuple[str | None, bool, int]:
        """Resolve a specific ref to a SHA. Returns (sha, is_cached, num_refs)."""
        refs, is_cached = self.get_repo_refs(
            owner, repo, cache_path=cache_path,
        )
        return ref_commit(refs, ref), is_cached, len(refs)

    def resolve_head(
        self, owner: str, repo: str, cache_path: Path | None = None,
    ) -> tuple[str, str | None, bool, int]:
        """The default branch and its commit: `(branch, sha, is_cached,
        num_refs)`, with `sha` None when the remote has no HEAD.

        What the commit stage collects when there is no release to
        collect. From the same listing, and cache, as `resolve_ref`:
        the branch is the one HEAD points at, never a guess. `branch` is
        '' only when the server did not say (an empty repository).
        """
        listing = self.list_remote(
            owner, repo, cache_path=cache_path, need_head=True,
        )
        branch, sha = head_commit(listing)
        return branch, sha, listing.cached, len(listing.refs)

    def get_repository_tree(
        self, owner: str, repo: str, sha: str,
        cache_path: Path | None = None, *, url: str | None = None,
    ) -> list[str] | None:
        """
        Fetch the full file tree for a specific commit SHA using git ls-tree.
        Avoids GitHub API rate limits.

        `url` stands in for github.com, as for `get_tag_dates`. None when
        git failed; an empty list for a commit with no files.
        """
        if cache_path and cache_path.exists():
            try:
                with open(cache_path, encoding='utf-8') as f:
                    return [line.strip() for line in f if line.strip()]
            except Exception as e:
                logger.debug(
                    'Failed to load tree cache',
                    path=str(cache_path), error=str(e),
                )

        repo_url = url or f"https://github.com/{owner}/{repo}.git"
        # The token in the environment, never on a command line, and a
        # time limit on each git (#47).
        env = {**os.environ, **git_auth_env(self.token), **GIT_QUIET_ENV}

        temp_dir = Path(tempfile.mkdtemp(prefix='chatsbom-tree-'))
        try:
            # 1. Blobless clone (metadata only, no file content)
            _git(
                [
                    'clone', '--quiet', '--filter=blob:none', '--no-checkout',
                    '--depth', '1', '--end-of-options', repo_url,
                    str(temp_dir),
                ],
                env=env, timeout=TREE_FETCH_TIMEOUT,
            )

            # 2. The commit, which comes from data: after
            # `--end-of-options`, where one spelled `--upload-pack=<x>`
            # is a name, not an option that runs <x>.
            _git(
                [
                    '-C', str(temp_dir), 'fetch', '--quiet', '--depth=1',
                    '--end-of-options', 'origin', sha,
                ],
                env=env, timeout=TREE_FETCH_TIMEOUT,
            )

            # 3. List tree recursively
            # -r: recurse
            # --name-only: filenames only
            # --full-tree: path relative to root
            output = _git(
                [
                    '-C', str(temp_dir), 'ls-tree', '-r', '--name-only',
                    '--full-tree', '--end-of-options', sha,
                ],
                env=env,
            )

            files = [
                line.strip()
                for line in output.splitlines()
                if line.strip()
            ]

            if cache_path and files:
                try:
                    atomic_write_text(
                        cache_path,
                        ''.join(f"{file_path}\n" for file_path in files),
                    )
                except Exception as e:
                    logger.warning(
                        'Failed to save tree cache',
                        path=str(cache_path), error=str(e),
                    )

            return files

        except (OSError, subprocess.SubprocessError) as e:
            logger.error(
                'Git tree fetch failed',
                repo=f"{owner}/{repo}", sha=sha,
                error=self._mask_url(_error_text(e))[:300],
            )
            return None
        except Exception as e:
            logger.error(
                'Unexpected error in git tree fetch',
                repo=f"{owner}/{repo}", sha=sha,
                error=str(e),
            )
            return None
        finally:
            # Cleanup
            if temp_dir.exists():
                shutil.rmtree(temp_dir, ignore_errors=True)


def parse_symref_head(output: str) -> tuple[str, str] | None:
    """`(branch, sha)` from `git ls-remote --symref <url> HEAD`, or None.

    The answer names the branch HEAD points at, then its commit:

        ref: refs/heads/main\tHEAD
        4a5b...\tHEAD
    """
    branch = sha = ''
    for line in output.splitlines():
        head, _, name = line.partition('\t')
        if name.strip() != 'HEAD':
            continue
        if head.startswith('ref: refs/heads/'):
            branch = head.removeprefix('ref: refs/heads/').strip()
        elif len(head.strip()) == 40:
            sha = head.strip().lower()
    if not sha:
        return None
    return branch, sha
