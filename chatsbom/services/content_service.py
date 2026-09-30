"""Download a repository's manifests, as the tree says it has them.

Which files is decided by `core/discovery.py`, from the stored tree:
every manifest and lockfile at any depth, of every ecosystem, within
the caps. Each is fetched from `raw.githubusercontent.com` at the
commit and stored at its own path under the content root:

    06-github-content/<repository_id>/<sha>/<path in the repository>

so `app/server/pom.xml` and `app/client/package.json` land side by
side, as Syft and the classifier expect to find them.

What was selected, fetched and left out is written beside the tree as
`manifests.json` (`PathConfig.discovery_file`).

Which files are asked for, what each answer means and what
`manifests.json` says are the collector's content stage's rules too,
kept once in `collector/content.py`: this drives them with `requests`.
"""
from __future__ import annotations

import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote

import requests
import structlog
from rich.console import Console

from chatsbom.collector.content import Answer
from chatsbom.collector.content import Got
from chatsbom.collector.content import Lost
from chatsbom.collector.content import MAX_FILE_BYTES
from chatsbom.collector.content import previous_outcomes
from chatsbom.collector.content import settle
from chatsbom.collector.content import stored_discovery
from chatsbom.collector.content import TooLarge
from chatsbom.collector.content import walk
from chatsbom.core.client import get_plain_client
from chatsbom.core.config import get_config
from chatsbom.core.discovery import Discovery
from chatsbom.core.discovery import MAX_BYTES
from chatsbom.core.discovery import MAX_FILES
from chatsbom.core.stats import BaseStats
from chatsbom.models.repository import Repository

logger = structlog.get_logger('content_service')
console = Console()

_CHUNK = 1 << 16


class ContentFetchError(RuntimeError):
    """Some files could not be fetched for a reason that may pass: a
    server error, a rate limit, a dropped connection. What was fetched
    stays on disk and is skipped next time; the stage is recorded as
    failed, so it backs off and is retried."""


@dataclass
class ContentStats(BaseStats):
    repo: str = ''
    downloaded_files: int = 0
    missing_files: int = 0
    status_message: str = ''
    local_path: str = ''

    def inc_downloaded(self):
        with self._lock:
            self.downloaded_files += 1

    def inc_missing(self):
        with self._lock:
            self.missing_files += 1


class _TooLarge(Exception):
    def __init__(self, size: int) -> None:
        super().__init__(size)
        self.size = size


def raw_url(owner: str, repo: str, sha: str, path: str) -> str:
    """The raw URL of one file at one commit, each segment quoted.

    Always the commit, for releases too: the files are stored under it,
    and a tag (`v1`, `latest`, `nightly`) can have moved on since the
    commit stage resolved it.
    """
    quoted = '/'.join(quote(segment, safe='') for segment in path.split('/'))
    return f'https://raw.githubusercontent.com/{owner}/{repo}/{sha}/{quoted}'


class ContentService:
    """Service for downloading raw content files from GitHub."""

    def __init__(
        self,
        token: str | None = None,
        timeout: int = 10,
        pool_size: int = 50,
        max_files: int = MAX_FILES,
        max_bytes: int = MAX_BYTES,
        max_file_bytes: int = MAX_FILE_BYTES,
    ):
        # No HTTP cache. The files are stored at their commit and never
        # asked for twice, so a cache only duplicated them into
        # `.requests-cache`; and it read every body whole, which a byte
        # cap cannot allow. A 429 is waited out as `Retry-After` asks.
        self.session = get_plain_client(
            pool_size=pool_size, respect_retry_after=True,
        )
        if token:
            self.session.headers.update({'Authorization': f"Bearer {token}"})
        self.config = get_config()
        self.timeout = timeout
        self.max_files = max_files
        self.max_bytes = max_bytes
        self.max_file_bytes = max_file_bytes

    def discovery_for(self, repository_id: int, sha: str) -> Discovery | None:
        """The discovery list of a stored tree, or None if there is none
        (or it was cut short: see `github tree`)."""
        return stored_discovery(
            self.config.paths, repository_id, sha, max_files=self.max_files,
        )

    def process_repo(
        self,
        repository: Repository,
        discovery: Discovery | None = None,
        force: bool = False,
    ) -> dict | None:
        """Fetch the manifests `discovery` selected for `repository`.

        `discovery` defaults to the one read from the stored tree.
        Returns the repository with `local_content_path` and
        `content_digest` (the stage's output key), or None when there is
        no commit or no tree to read.

        Raises `ContentFetchError` if a file could not be fetched for a
        reason that may pass, after trying all of them; and `OSError`
        if one could not be written.
        """
        owner = repository.owner
        repo = repository.repo
        repo_display = f"{owner}/{repo}"

        dt = repository.download_target
        if not dt or not dt.commit_sha:
            logger.warning(
                f"No download target for {repo_display}, skipping content download",
            )
            return None
        sha = dt.commit_sha

        if discovery is None:
            discovery = self.discovery_for(repository.id, sha)
            if discovery is None:
                logger.warning(
                    'No stored tree to discover manifests from',
                    repo=repo_display, sha=sha[:7],
                )
                return None

        target_dir = self.config.paths.content_root(repository.id, sha)
        target_dir.mkdir(parents=True, exist_ok=True)

        # What the last pass at this commit found, so a walk that
        # passes through this stage again does not ask again for what
        # was not there or was too large. `--force` asks again.
        index = self.config.paths.discovery_file(repository.id, sha)
        known = {} if force else previous_outcomes(index, sha)
        start_time = time.time()

        # The walk decides what to ask for and what each answer means
        # (`collector/content.py`); this asks.
        walking = walk(
            discovery, target_dir, known=known, force=force,
            max_bytes=self.max_bytes, max_file_bytes=self.max_file_bytes,
            repository=repo_display,
        )
        try:
            wanted = next(walking)
            while True:
                url = raw_url(owner, repo, sha, wanted.path)
                answer: Answer
                try:
                    status, body = self._get(url, wanted.room)
                    answer = Got(status, body)
                except _TooLarge as error:
                    answer = TooLarge(error.size)
                except requests.RequestException as error:
                    answer = Lost(str(error))
                wanted = walking.send(answer)
        except StopIteration as stop:
            done = stop.value

        digest, _ = settle(
            discovery, done,
            repository_id=repository.id,
            sha=sha,
            index=index,
            max_files=self.max_files,
            max_bytes=self.max_bytes,
            max_file_bytes=self.max_file_bytes,
        )

        logger.info(
            'Content discovered',
            repo=repo_display,
            sha=sha[:7],
            candidates=discovery.candidates,
            selected=len(discovery.selected),
            stored=len(done.written),
            bytes=done.total,
            capped=len(done.capped),
            ecosystems=','.join(discovery.ecosystems),
            elapsed=f'{time.time() - start_time:.3f}s',
        )

        if done.transient:
            raise ContentFetchError(
                f'{len(done.transient)} of {len(discovery.selected)} files '
                f'could not be fetched, first {done.transient[0]}',
            )

        repo_dict = repository.model_dump(mode='json')
        repo_dict['local_content_path'] = str(target_dir)
        repo_dict['content_digest'] = digest
        return repo_dict

    def _get(self, url: str, room: int) -> tuple[int, bytes | None]:
        """`(status, body)` of one file, the body read up to the caps.

        Raises `_TooLarge` with the size seen once the body passes
        `max_file_bytes` or `room`, without reading the rest.
        """
        limit = min(self.max_file_bytes, room)
        with self.session.get(url, timeout=self.timeout, stream=True) as response:
            if response.status_code != 200:
                return response.status_code, None
            declared = response.headers.get('Content-Length')
            if declared and declared.isdigit() and int(declared) > limit:
                raise _TooLarge(int(declared))
            chunks: list[bytes] = []
            size = 0
            for chunk in response.iter_content(_CHUNK):
                size += len(chunk)
                if size > limit:
                    raise _TooLarge(size)
                chunks.append(chunk)
            return 200, b''.join(chunks)


def stored_files(root: Path) -> list[str]:
    """The files under a content root, as repository paths, sorted."""
    if not root.is_dir():
        return []
    return sorted(
        path.relative_to(root).as_posix()
        for path in root.rglob('*') if path.is_file()
    )
