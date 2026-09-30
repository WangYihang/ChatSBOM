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
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from urllib.parse import quote

import requests
import structlog
from rich.console import Console

from chatsbom.core.client import get_plain_client
from chatsbom.core.config import get_config
from chatsbom.core.discovery import content_digest
from chatsbom.core.discovery import discover
from chatsbom.core.discovery import Discovery
from chatsbom.core.discovery import discovery_document
from chatsbom.core.discovery import dumps
from chatsbom.core.discovery import is_safe_path
from chatsbom.core.discovery import MAX_BYTES
from chatsbom.core.discovery import MAX_FILES
from chatsbom.core.discovery import OVER_BYTE_CAP
from chatsbom.core.discovery import read_tree
from chatsbom.core.fs import atomic_write_bytes
from chatsbom.core.fs import atomic_write_text
from chatsbom.core.fs import is_whole_tree
from chatsbom.core.stats import BaseStats
from chatsbom.models.repository import Repository

logger = structlog.get_logger('content_service')
console = Console()

#: No one manifest is bigger than this. A lockfile of a large monorepo
#: reaches a few MiB; anything past 16 MiB is generated data under a
#: manifest's name, and would hold up Syft for nothing.
MAX_FILE_BYTES = 16 * 2**20

#: Recorded against a file larger than `MAX_FILE_BYTES`.
TOO_LARGE = 'over-file-byte-cap'

_CHUNK = 1 << 16


def _settled(status: str) -> bool:
    """Whether a file's last outcome stands at this commit: not there,
    too large, or refused. A server error or a rate limit may pass, and
    is asked again."""
    if status in ('absent', TOO_LARGE, 'unsafe-path'):
        return True
    if status.startswith('http-'):
        code = status.removeprefix('http-')
        return code.isdigit() and 400 <= int(code) < 500 and code != '429'
    return False


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
        stored = self.config.paths.tree_file(repository_id, sha)
        if not is_whole_tree(stored):
            return None
        try:
            text = stored.read_text(encoding='utf-8', errors='replace')
        except OSError:
            return None
        return discover(read_tree(text), max_files=self.max_files)

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
        known = {} if force else self._previous_outcomes(repository.id, sha)

        written: list[tuple[str, int]] = []
        fetched: dict[str, dict[str, Any]] = {}
        capped: list[tuple[str, str]] = []
        transient: list[str] = []
        total = 0
        start_time = time.time()

        for position, item in enumerate(discovery.selected):
            path = item.path
            if not is_safe_path(path):
                # `discover` already refused it; a list built elsewhere
                # gets the same check.
                fetched[path] = {'status': 'unsafe-path'}
                continue
            destination = target_dir.joinpath(*path.split('/'))

            if not force and destination.is_file():
                size = destination.stat().st_size
                if total + size > self.max_bytes:
                    capped.extend(
                        (rest.path, OVER_BYTE_CAP)
                        for rest in discovery.selected[position:]
                    )
                    break
                total += size
                written.append((path, size))
                fetched[path] = {'status': 'ok', 'size': size}
                continue

            previous = known.get(path, {})
            if previous.get('status') == OVER_BYTE_CAP:
                capped.extend(
                    (rest.path, OVER_BYTE_CAP)
                    for rest in discovery.selected[position:]
                )
                break
            if _settled(str(previous.get('status') or '')):
                fetched[path] = dict(previous)
                continue

            url = raw_url(owner, repo, sha, path)
            try:
                status, body = self._get(url, self.max_bytes - total)
            except _TooLarge as error:
                if error.size > self.max_file_bytes:
                    fetched[path] = {'status': TOO_LARGE, 'size': error.size}
                    continue
                # Within the per-file cap, past what is left of the
                # repository's: this and every file after it.
                capped.extend(
                    (rest.path, OVER_BYTE_CAP)
                    for rest in discovery.selected[position:]
                )
                break
            except requests.RequestException as error:
                logger.warning(
                    'Content download error', repo=repo_display, file=path,
                    error=str(error),
                )
                transient.append(path)
                fetched[path] = {'status': 'error'}
                continue

            if status == 200 and body is not None:
                # Whole or not at all: a file here is skipped next time,
                # so a prefix left by a kill or a full disk would be what
                # Syft scanned from then on.
                atomic_write_bytes(destination, body)
                total += len(body)
                written.append((path, len(body)))
                fetched[path] = {'status': 'ok', 'size': len(body)}
            elif status == 404:
                # Not there at this commit after all: listed by a tree
                # that was, or a submodule path.
                fetched[path] = {'status': 'absent'}
            elif status == 429 or status >= 500:
                transient.append(path)
                fetched[path] = {'status': f'http-{status}'}
            else:
                logger.warning(
                    'Content download refused', repo=repo_display,
                    file=path, status_code=status,
                )
                fetched[path] = {'status': f'http-{status}'}

        digest = content_digest(written)
        document = discovery_document(
            discovery,
            repository_id=repository.id,
            commit_sha=sha,
            fetched=fetched,
            extra_skipped=capped,
            max_files=self.max_files,
            max_bytes=self.max_bytes,
            max_file_bytes=self.max_file_bytes,
        )
        document['bytes'] = total
        document['digest'] = digest
        index = self.config.paths.discovery_file(repository.id, sha)
        text = dumps(document)
        try:
            unchanged = index.read_text(encoding='utf-8') == text
        except OSError:
            unchanged = False
        if not unchanged:
            # Rewritten only when it says something new: a walk that
            # finds the same files leaves the file as it was.
            atomic_write_text(index, text)

        logger.info(
            'Content discovered',
            repo=repo_display,
            sha=sha[:7],
            candidates=discovery.candidates,
            selected=len(discovery.selected),
            stored=len(written),
            bytes=total,
            capped=len(capped),
            ecosystems=','.join(discovery.ecosystems),
            elapsed=f'{time.time() - start_time:.3f}s',
        )

        if transient:
            raise ContentFetchError(
                f'{len(transient)} of {len(discovery.selected)} files could '
                f'not be fetched, first {transient[0]}',
            )

        repo_dict = repository.model_dump(mode='json')
        repo_dict['local_content_path'] = str(target_dir)
        repo_dict['content_digest'] = digest
        return repo_dict

    def _previous_outcomes(
        self, repository_id: int, sha: str,
    ) -> dict[str, dict[str, Any]]:
        """`path -> {status, size}` from the stored `manifests.json` of
        this commit (status `over-byte-cap` for what the byte cap left
        out), or {} if there is none."""
        index = self.config.paths.discovery_file(repository_id, sha)
        try:
            document = json.loads(index.read_text(encoding='utf-8'))
        except (OSError, ValueError):
            return {}
        if not isinstance(document, dict) or document.get('commit_sha') != sha:
            return {}
        outcomes: dict[str, dict[str, Any]] = {}
        for entry in document.get('selected') or ():
            if isinstance(entry, dict) and entry.get('status'):
                outcomes[str(entry.get('path'))] = {
                    key: entry[key] for key in ('status', 'size')
                    if key in entry
                }
        for entry in document.get('skipped') or ():
            if isinstance(entry, dict) and entry.get('reason') == OVER_BYTE_CAP:
                outcomes[str(entry.get('path'))] = {'status': OVER_BYTE_CAP}
        return outcomes

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
