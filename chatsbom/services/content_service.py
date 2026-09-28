import time
from dataclasses import dataclass

import requests
import structlog
from rich.console import Console

from chatsbom.core.client import get_http_client
from chatsbom.core.config import get_config
from chatsbom.core.fs import atomic_write_bytes
from chatsbom.core.stats import BaseStats
from chatsbom.models.language import Language
from chatsbom.models.language import LanguageFactory
from chatsbom.models.repository import Repository

logger = structlog.get_logger('content_service')
console = Console()


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


class ContentService:
    """Service for downloading raw content files from GitHub."""

    def __init__(self, token: str | None = None, timeout: int = 10, pool_size: int = 50):
        self.session = get_http_client(pool_size=pool_size)
        if token:
            self.session.headers.update({'Authorization': f"Bearer {token}"})
        self.config = get_config()
        self.timeout = timeout

    def process_repo(self, repository: Repository, language: Language) -> dict | None:
        """
        Downloads all manifest files for a given repository.
        Returns repository dict with 'local_content_path' if successful.
        """
        owner = repository.owner
        repo = repository.repo
        repo_display = f"{owner}/{repo}"
        start_time = time.time()

        dt = repository.download_target
        if not dt:
            logger.warning(
                f"No download target for {repo_display}, skipping content download",
            )
            return None

        # Path: data/06-github-content/<repository_id>/<sha>/. The
        # language only picks which manifests to ask for.
        target_dir = self.config.paths.content_root(
            repository.id, dt.commit_sha,
        )
        target_dir.mkdir(parents=True, exist_ok=True)

        handler = LanguageFactory.get_handler(language)
        targets = handler.get_sbom_paths()

        has_content = False

        # Raw URL structure: https://raw.githubusercontent.com/{owner}/{repo}/{commit_sha}/{path}
        # Always the commit, for releases too: the files are stored under
        # it, and a tag (`v1`, `latest`, `nightly`) can have moved on
        # since the commit stage resolved it.
        base_raw_url = f"https://raw.githubusercontent.com/{owner}/{repo}/{dt.commit_sha}"

        for filename in targets:
            file_path = target_dir / filename
            url = f"{base_raw_url}/{filename}"

            # Skip if already exists (immutable content). Written whole
            # below, so a file here is complete. One that an in-place
            # write cut short before that cannot be told apart: a
            # manifest can be empty, or any text at all.
            if file_path.exists():
                has_content = True
                elapsed = time.time() - start_time
                logger.info(
                    'Content exists (Skipped)',
                    repo=repo_display,
                    file=filename,
                    elapsed=f"{elapsed:.3f}s",
                )
                continue

            try:
                file_start_time = time.time()
                response = self.session.get(url, timeout=self.timeout)
                file_elapsed = time.time() - file_start_time

                if response.status_code == 200:
                    # Whole or not at all: this file is skipped above once
                    # it exists, so a prefix left by a kill or a full disk
                    # was what Syft scanned from then on.
                    atomic_write_bytes(file_path, response.content)
                    has_content = True
                    logger.info(
                        'Content downloaded',
                        repo=repo_display,
                        file=filename,
                        status_code=response.status_code,
                        size=len(response.content),
                        elapsed=f"{file_elapsed:.3f}s",
                    )
                elif response.status_code == 404:
                    pass
                else:
                    logger.warning(
                        'Content download failed',
                        repo=repo_display,
                        file=filename,
                        status_code=response.status_code,
                        elapsed=f"{file_elapsed:.3f}s",
                    )

            except requests.RequestException as e:
                logger.error(f"Download error {repo_display}/{filename}: {e}")

        if has_content:
            repo_dict = repository.model_dump(mode='json')
            repo_dict['local_content_path'] = str(target_dir)
            return repo_dict

        return None
