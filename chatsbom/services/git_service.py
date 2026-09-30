from __future__ import annotations

import base64
import json
import os
import re
import shutil
import subprocess
import tempfile
from collections.abc import Callable
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import cast

import git
import structlog

from chatsbom.core.config import get_config
from chatsbom.core.fs import atomic_write_text

logger = structlog.get_logger('git_service')

TAG_REF_PREFIX = 'refs/tags/'


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


#: Wall-clock limit on one repository's tag fetch. The largest tag sets
#: in the corpus (several thousand tags) fetch in well under a minute;
#: past this the stage falls back to the capped API lookups.
TAG_FETCH_TIMEOUT = 300

#: Wall-clock limit on `git ls-remote`, one round trip.
LS_REMOTE_TIMEOUT = 30

#: Wall-clock limit on each of the tree stage's clone and fetch, of one
#: commit's trees and no file contents.
TREE_FETCH_TIMEOUT = 300

#: What the tag fetch leaves out, tried in order. `tree:0` fetches
#: commits and tags only. A tag of a tree (Linux has `v2.6.11-tree`)
#: needs that tree, which a server may refuse to send under `tree:0`
#: ("remote did not send all necessary objects"); `blob:none` sends
#: trees and still no file contents.
TAG_FETCH_FILTERS = ('tree:0', 'blob:none')

#: No prompt for credentials (a repository gone private would hang the
#: worker on one), and no user or system config: this is a scratch
#: repository, and a `url.<x>.insteadOf` there could send it elsewhere.
GIT_QUIET_ENV = {
    'GIT_TERMINAL_PROMPT': '0',
    'GIT_CONFIG_NOSYSTEM': '1',
    'GIT_CONFIG_GLOBAL': os.devnull,
}

#: One line per tag, NUL-separated: name, the object the ref names and
#: its type, the object that dereferences to (for an annotated tag) and
#: its type, then the committer date of each and the tagger date.
_TAG_FIELDS = (
    '%(refname:strip=2)',
    '%(objectname)', '%(objecttype)',
    '%(*objectname)', '%(*objecttype)',
    '%(committerdate:iso-strict)', '%(*committerdate:iso-strict)',
    '%(creatordate:iso-strict)',
)
_TAG_FORMAT = '%00'.join(_TAG_FIELDS)


@dataclass(frozen=True)
class RemoteRefs:
    """What `git ls-remote --symref` says of a repository."""
    #: Full and short ref names, and `HEAD`, each to its commit.
    refs: dict[str, str] = field(default_factory=dict)
    #: The branch HEAD points at; '' when not said.
    head: str = ''
    cached: bool = False
    #: Why git could not list the refs; '' when it listed them. A
    #: listing that failed is empty, as one of an empty repository is.
    error: str = ''


def parse_ls_remote(
    output: str, short_name: Callable[[str], str | None],
) -> tuple[dict[str, str], str]:
    """`(refs, head_branch)` from `git ls-remote --symref <url>`.

    Annotated tags (`refs/tags/v1^{}`) take precedence over the tag
    object itself, so a tag resolves to its commit. The symref line,
    `ref: refs/heads/<branch>\tHEAD`, names the default branch; it is
    not a ref.
    """
    refs: dict[str, str] = {}
    head = ''
    for line in output.splitlines():
        if not line.strip():
            continue
        if line.startswith('ref: '):
            target, _, name = line[len('ref: '):].partition('\t')
            if name.strip() == 'HEAD' and target.startswith('refs/heads/'):
                head = target.removeprefix('refs/heads/').strip()
            continue
        parts = line.split(None, 1)
        if len(parts) < 2:
            continue
        sha, ref_full = parts
        if ref_full.endswith('^{}'):
            base_ref = ref_full[:-3]
            refs[base_ref] = sha
            short = short_name(base_ref)
            if short:
                refs[short] = sha
        elif ref_full not in refs:
            refs[ref_full] = sha
            short = short_name(ref_full)
            if short:
                refs[short] = sha
    return refs, head


def short_name(ref_full: str) -> str | None:
    """A branch's or a tag's name without `refs/heads/` or `refs/tags/`,
    as a listing keys it beside its full name; None for any other ref.

    Short and full names share one dict, and git allows a branch called
    `refs/tags/v1`: its short name would pose as that tag, and, listed
    before it, take the real tag's place.
    """
    if ref_full.startswith('refs/tags/'):
        short = ref_full[10:]
    elif ref_full.startswith('refs/heads/'):
        short = ref_full[11:]
    else:
        return None
    if short.startswith('refs/'):
        return None
    return short


def tags_of(refs: Mapping[str, str]) -> dict[str, str]:
    """A listing's tags, by name, each at the commit it points to.

    Read from the full `refs/tags/*` names only. The short names beside
    them are shared with branches, and `HEAD` has no prefix at all:
    taken as tags, they made releases of branches, dated by their head
    commit and so newer than any real release.
    """
    return {
        ref.removeprefix(TAG_REF_PREFIX): sha
        for ref, sha in refs.items()
        if ref.startswith(TAG_REF_PREFIX)
    }


def ref_commit(refs: Mapping[str, str], ref: str) -> str | None:
    """The commit `ref` names in a listing: exactly, then as a tag, then
    as a branch."""
    if ref in refs:
        return refs[ref]
    for prefix in ['refs/tags/', 'refs/heads/']:
        if (prefix + ref) in refs:
            return refs[prefix + ref]
    return None


def head_commit(listing: RemoteRefs) -> tuple[str, str | None]:
    """The default branch, as HEAD names it ('' when not said), and its
    commit: None when the remote has no HEAD, an empty repository."""
    sha = listing.refs.get('HEAD')
    if listing.head:
        sha = listing.refs.get(f'refs/heads/{listing.head}', sha)
    return listing.head, sha


@dataclass(frozen=True)
class TagDate:
    """What git says about one tag: the commit it names, and its date."""
    #: The commit an annotated tag points to, or a lightweight tag's own
    #: object: what `ls-remote` lists as the tag's `^{}` or bare sha.
    sha: str
    #: ISO 8601, with its offset; '' when git has no date for it.
    date: str


#: `GIT_CONFIG_COUNT` as git reads it, with C's `strtoul`: a number, with
#: any space and a sign before it, and nothing after it.
_GIT_CONFIG_COUNT = re.compile(r'[ \t\n\v\f\r]*\+?([0-9]+)')

#: The most entries git reads from its environment: an `int`'s most.
_MOST_GIT_CONFIG_ENTRIES = 2**31 - 1


def _git_config_count(count: str | None) -> int:
    """How many entries of git config `count`, a `GIT_CONFIG_COUNT`,
    says the environment holds, read as git reads it: 0 for none, and
    for a count git refuses, a word or a negative number among them."""
    match = _GIT_CONFIG_COUNT.fullmatch(count or '')
    if match is None:
        return 0
    entries = int(match[1])
    return entries if entries < _MOST_GIT_CONFIG_ENTRIES else 0


def git_auth_env(
    token: str | None, environ: Mapping[str, str] = os.environ,
) -> dict[str, str]:
    """git config, as environment variables, that authenticates to GitHub.

    Environment rather than `-c` or a URL: both of those are on the
    command line, which any user of the machine can read in `ps`.

    To put over `environ`, the environment git is started with, and
    after the entries of git config it holds already: `GIT_CONFIG_COUNT`
    of them, each a `GIT_CONFIG_KEY_<n>` and a `GIT_CONFIG_VALUE_<n>`,
    which may set a proxy, a CA bundle or a URL rewrite for every git on
    the machine. The token was entry 0 of a count of 1, which replaced
    the first and dropped the rest (#113). A count git refuses, over
    which it runs no command at all, is replaced still: git runs, with
    the token's entry alone.
    """
    if not token:
        return {}
    index = _git_config_count(environ.get('GIT_CONFIG_COUNT'))
    basic = base64.b64encode(f'x-access-token:{token}'.encode()).decode()
    return {
        'GIT_CONFIG_COUNT': str(index + 1),
        f'GIT_CONFIG_KEY_{index}': 'http.https://github.com/.extraheader',
        f'GIT_CONFIG_VALUE_{index}': f'Authorization: Basic {basic}',
    }


def _git(
    args: list[str], *, env: dict[str, str], timeout: float = 60,
) -> str:
    """Run git; its stdout, or raise `CalledProcessError`/`TimeoutExpired`."""
    return subprocess.run(
        ['git', *args], env=env, timeout=timeout, check=True,
        capture_output=True, text=True, stdin=subprocess.DEVNULL,
    ).stdout


def _error_text(error: BaseException) -> str:
    stderr = getattr(error, 'stderr', None)
    return f'{stderr.strip()} ({error})' if stderr else str(error)


def parse_tag_listing(listing: str) -> dict[str, TagDate]:
    """`{tag: TagDate}` from `git for-each-ref --format=<_TAG_FORMAT>`."""
    tags: dict[str, TagDate] = {}
    for line in listing.splitlines():
        fields = line.split('\0')
        if len(fields) != len(_TAG_FIELDS):
            continue
        name, sha, kind, peeled, peeled_kind, date, peeled_date, created = fields
        if kind == 'commit':
            tags[name] = TagDate(sha=sha, date=date)
        elif kind == 'tag' and peeled_kind == 'commit':
            tags[name] = TagDate(sha=peeled, date=peeled_date)
        else:
            # A tag of a tag, a tree or a blob. `ls-remote` peels to the
            # end, so its sha will not match this one and the tag is
            # dated by the fallback; the tagger date is better than none
            # if it is ever used.
            tags[name] = TagDate(sha=peeled or sha, date=created)
    return tags


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
