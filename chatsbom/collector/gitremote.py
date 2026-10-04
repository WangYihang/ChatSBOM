"""The repositories' git remotes, as the collector's stages read them
(#161; #128 section 2.1).

What today's stages ask of git, they ask here: `git ls-remote --symref`,
every ref and the branch HEAD points at, for the release stage's tags
and the commit stage's refs; the tag fetch that dates the tags with no
release; and the blobless clone the tree is listed from. None of it
spends REST quota. At most `in_flight` run at once: the process runs
about four network tasks a token at a time (#128), and git's are among
them.

Each git is a child the collector can stop (#171): started in a process
group of its own, and killed with whatever it started, the helpers git
runs for HTTPS and for packs, when it outlives its time limit or when
the collection it serves is given up, as `chatsbom collect` gives up
what is in flight when it stops. Its scratch repository goes with it.
Run in a thread, as they were, a git given up on ran on to its time
limit, and held the process's exit for as long.

The token, where there is one, goes to github.com in git's environment,
never on a command line (`core/git.git_auth_env`); `base` stands in for
github.com, and a test's repositories on disk are reached as `file://`.
Nothing git says is kept or logged with the token in it.

`resolve_commit` is the commit stage's rule, `CommitService`'s, on one
listing: the release's tag, else the default branch's head.
"""
from __future__ import annotations

import asyncio
import os
import re
import shutil
import signal
import tempfile
import time
from collections.abc import Callable
from collections.abc import Mapping
from pathlib import Path

import structlog

from chatsbom.collector.tokens import scrub
from chatsbom.collector.tokens import Token
from chatsbom.core import git
from chatsbom.core.git import head_commit
from chatsbom.core.git import parse_ls_remote
from chatsbom.core.git import parse_tag_listing
from chatsbom.core.git import ref_commit
from chatsbom.core.git import RemoteRefs
from chatsbom.core.git import short_name
from chatsbom.core.git import TagDate
from chatsbom.models.download_target import DownloadTarget

logger = structlog.get_logger('collector.git')

#: Where the repositories are.
GITHUB = 'https://github.com'

#: git's network tasks at once, for one token.
IN_FLIGHT = 4

#: What of what git said an error keeps, at most.
QUOTED = 300

#: Each git that reaches the network is run this many times at most,
#: while it fails on the way (`transient`, #189).
ATTEMPTS = 3

#: Seconds before it is run again after its first such failure, and
#: twice as long after each later one: 5, then 10.
PAUSE = 5.0

#: Seconds that running gits again may add to one of `GitRemote`'s
#: operations, a listing, a tag fetch or a tree, at most: the pauses and
#: the gits run after the first failure, together. A git run again is
#: given what is left of it, or its own time limit if that is shorter.
PATIENCE = 60.0


class GitFailed(Exception):
    """A git that did not finish: it exited with an error, or ran out
    its time and was killed (`timed_out`)."""

    def __init__(self, message: str, *, timed_out: bool = False) -> None:
        super().__init__(message)
        self.timed_out = timed_out


#: What git, and curl under it, say of a failure no pause mends: a
#: repository gone or never there, a token not taken, a ref not there,
#: an HTTP 4xx. Looked for first: what says one of these is permanent,
#: whatever else it says.
PERMANENT = re.compile(
    '|'.join((
        r'not found',
        r'authentication failed',
        r'invalid username or password',
        r'could not read username',
        r'terminal prompts disabled',
        r"couldn't find remote ref",
        r'not our ref',
        r'does not appear to be a git repository',
        r'returned error: 4\d\d',
        r'\bhttp 4\d\d\b',
    )),
)

#: What they say of trouble on the way that passes (#189): a name that
#: did not resolve, a connection refused, reset, or timed out, a TLS
#: handshake that did not finish, a stream cut short, a server's 5xx.
TRANSIENT = re.compile(
    '|'.join((
        r'could not resolve host',
        r'resolving timed out',
        r'failed to connect to',
        r"couldn't connect to server",
        r'connection refused',
        r'connection reset',
        r'connection timed out',
        r'operation timed out',
        r'recv failure',
        r'send failure',
        r'empty reply from server',
        r'ssl connection timeout',
        r'gnutls',
        r'ssl_error_syscall',
        r'error in the http2 framing layer',
        r'was not closed cleanly',
        r'rpc failed',
        r'early eof',
        r'unexpected disconnect',
        r'the remote end hung up unexpectedly',
        r'returned error: 5\d\d',
        r'\bhttp 5\d\d\b',
    )),
)


def transient(error: GitFailed) -> bool:
    """Whether what a git failed with is trouble on the way that passes
    within minutes, worth asking again; not, for a failure no pause
    mends, nor for one not recognised. A git killed at its time limit
    is one whose answer did not come: transient."""
    if error.timed_out:
        return True
    said = str(error).lower()
    if PERMANENT.search(said):
        return False
    return bool(TRANSIENT.search(said))


class Patience:
    """What running gits again may yet add to one operation: `PATIENCE`
    seconds, from its first failure."""

    def __init__(
        self, clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._clock = clock
        self._until: float | None = None

    def left(self) -> float:
        """Seconds left of it; the first time it is asked, all of it."""
        now = self._clock()
        if self._until is None:
            self._until = now + PATIENCE
        return self._until - now


def _empty(directory: Path) -> None:
    """Everything in `directory` gone; the directory kept."""
    for entry in directory.iterdir():
        if entry.is_dir() and not entry.is_symlink():
            shutil.rmtree(entry, ignore_errors=True)
        else:
            entry.unlink(missing_ok=True)


def _split(full_name: str) -> tuple[str, str]:
    owner, _, name = full_name.partition('/')
    return owner, name


def _kill(process: asyncio.subprocess.Process) -> None:
    """Kill a git and everything it started: its process group."""
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


async def run(
    args: list[str],
    *,
    env: Mapping[str, str],
    timeout: float,
    cwd: Path | None = None,
) -> bytes:
    """git with `args`, to its end: what it wrote on stdout. Raises
    `GitFailed` for a git that exits with an error or outlives
    `timeout`; one given up on, cancelled, is killed first, with what
    it started."""
    process = await asyncio.create_subprocess_exec(
        'git', *args,
        stdin=asyncio.subprocess.DEVNULL,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        env=dict(env),
        cwd=cwd,
        start_new_session=True,
    )
    try:
        output, errors = await asyncio.wait_for(
            process.communicate(), timeout,
        )
    except TimeoutError:
        _kill(process)
        await process.communicate()
        raise GitFailed(
            f'git {_command(args)} was killed after {timeout:g} s',
            timed_out=True,
        ) from None
    except BaseException:
        # Given up on: it goes too, and is waited for.
        _kill(process)
        await process.communicate()
        raise
    if process.returncode != 0:
        said = errors.decode('utf-8', 'replace').strip()
        raise GitFailed(
            f'git {_command(args)} exited {process.returncode}'
            + (f': {said}' if said else ''),
        )
    return output


def _command(args: list[str]) -> str:
    """The git command `args` run: the first that is not an option, nor
    the directory `-C` names."""
    rest = iter(args)
    for arg in rest:
        if arg == '-C':
            next(rest, None)
        elif not arg.startswith('-'):
            return arg
    return ''


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

    def _env(self) -> dict[str, str]:
        """git's environment: this process's, quiet, with the token as a
        header."""
        return {
            **os.environ,
            **git.git_auth_env(self._token.secret if self._token else None),
            **git.GIT_QUIET_ENV,
        }

    def _said(self, error: BaseException) -> str:
        """What a git that failed said, short, and with no token."""
        text = scrub(str(error), (self._token,) if self._token else ())
        return text if len(text) <= QUOTED else f'{text[:QUOTED]}...'

    async def _network(
        self,
        args: list[str],
        *,
        env: Mapping[str, str],
        timeout: float,
        repo: str,
        patience: Patience,
        into: Path | None = None,
    ) -> bytes:
        """`run`, of a git that reaches the network: run again after a
        pause while it fails on the way (`transient`), `ATTEMPTS` times
        at most and within the operation's `patience`. A failure no
        pause mends is raised at once, and the last when there is no
        more asking, as a single run raises it. A clone's directory,
        `into`, is emptied of what one cut short wrote before it is run
        again."""
        limit = timeout
        for attempt in range(1, ATTEMPTS + 1):
            if into is not None and attempt > 1:
                _empty(into)
            try:
                return await run(args, env=env, timeout=limit)
            except GitFailed as error:
                pause = PAUSE * 2 ** (attempt - 1)
                left = patience.left() - pause
                if attempt == ATTEMPTS or left <= 0 or not transient(error):
                    raise
                logger.warning(
                    'Git failed: asking again after a pause', repo=repo,
                    attempt=attempt, error=self._said(error),
                )
            # Cancelled here, as the collection is given up, it stops.
            await asyncio.sleep(pause)
            limit = min(timeout, left)
        raise AssertionError('every attempt returns or raises')

    async def list_remote(self, full_name: str) -> RemoteRefs:
        """Every ref of the repository, and the branch HEAD points at: one
        `git ls-remote --symref`. A listing that failed says why
        (`RemoteRefs.error`)."""
        async with self._slots:
            try:
                output = await self._network(
                    ['ls-remote', '--symref', self.url(full_name)],
                    env=self._env(), timeout=git.LS_REMOTE_TIMEOUT,
                    repo=full_name, patience=Patience(),
                )
            except GitFailed as error:
                said = self._said(error)
                logger.warning(
                    'Git ls-remote failed', repo=full_name, error=said,
                )
                return RemoteRefs(error=said)
        # As GitPython read it: a ref git holds as bytes that are not
        # UTF-8 keeps them (`core/decisions.readable`).
        refs, head = parse_ls_remote(
            output.decode('utf-8', 'surrogateescape'), short_name,
        )
        return RemoteRefs(refs=refs, head=head)

    async def tag_dates(self, full_name: str) -> dict[str, TagDate] | None:
        """Every tag's commit and date, by fetching the tags' commits
        alone into a scratch repository; None if git failed.

            git init --bare <tmp>
            git fetch --depth=1 --filter=tree:0 <url> +refs/tags/*:refs/tags/*
            git for-each-ref refs/tags

        Commit and tag objects only: the tips, at depth 1, with no trees
        and no blobs; a server that refuses `tree:0` is asked for
        `blob:none` (`core/git.TAG_FETCH_FILTERS`)."""
        env = self._env()
        async with self._slots:
            scratch = Path(tempfile.mkdtemp(prefix='chatsbom-tags-'))
            try:
                await run(
                    ['init', '--bare', '--quiet', str(scratch)],
                    env=env, timeout=git.LOCAL_TIMEOUT,
                )
                patience = Patience()
                for index, object_filter in enumerate(git.TAG_FETCH_FILTERS):
                    try:
                        await self._network(
                            [
                                '-C', str(scratch), 'fetch', '--quiet',
                                '--no-tags', '--no-write-fetch-head',
                                '--depth=1', f'--filter={object_filter}',
                                self.url(full_name),
                                '+refs/tags/*:refs/tags/*',
                            ],
                            env=env, timeout=git.TAG_FETCH_TIMEOUT,
                            repo=full_name, patience=patience,
                        )
                        break
                    except GitFailed:
                        if index == len(git.TAG_FETCH_FILTERS) - 1:
                            raise
                listing = await run(
                    [
                        '-C', str(scratch), 'for-each-ref',
                        f'--format={git.TAG_FORMAT}', 'refs/tags',
                    ],
                    env=env, timeout=git.LOCAL_TIMEOUT,
                )
                text = listing.decode('utf-8')
            except (GitFailed, UnicodeDecodeError) as error:
                logger.warning(
                    'Git tag fetch failed', repo=full_name,
                    error=self._said(error),
                )
                return None
            finally:
                shutil.rmtree(scratch, ignore_errors=True)
        return parse_tag_listing(text)

    async def tree(self, full_name: str, sha: str) -> list[str] | None:
        """Every path at the commit, from a blobless clone of its trees
        alone; None if git failed, and an empty list for a commit with
        no files.

        The commit comes from data, so it follows `--end-of-options`,
        where one spelled `--upload-pack=<x>` is a name, not an option
        that runs <x>."""
        env = self._env()
        async with self._slots:
            scratch = Path(tempfile.mkdtemp(prefix='chatsbom-tree-'))
            try:
                patience = Patience()
                await self._network(
                    [
                        'clone', '--quiet', '--filter=blob:none',
                        '--no-checkout', '--depth', '1', '--end-of-options',
                        self.url(full_name), str(scratch),
                    ],
                    env=env, timeout=git.TREE_FETCH_TIMEOUT,
                    repo=full_name, patience=patience, into=scratch,
                )
                await self._network(
                    [
                        '-C', str(scratch), 'fetch', '--quiet', '--depth=1',
                        '--end-of-options', 'origin', sha,
                    ],
                    env=env, timeout=git.TREE_FETCH_TIMEOUT,
                    repo=full_name, patience=patience,
                )
                output = await run(
                    [
                        '-C', str(scratch), 'ls-tree', '-r', '--name-only',
                        '--full-tree', '--end-of-options', sha,
                    ],
                    env=env, timeout=git.LOCAL_TIMEOUT,
                )
                text = output.decode('utf-8')
            except (GitFailed, UnicodeDecodeError) as error:
                logger.warning(
                    'Git tree fetch failed', repo=full_name, sha=sha,
                    error=self._said(error),
                )
                return None
            finally:
                shutil.rmtree(scratch, ignore_errors=True)
        return [line.strip() for line in text.splitlines() if line.strip()]


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
