import json
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import requests
import structlog
from rich.console import Console

from chatsbom.core.client import get_http_client
from chatsbom.models.language import Language
from chatsbom.models.language import LanguageFactory
from chatsbom.models.repository import Repository

logger = structlog.get_logger('downloader_service')
console = Console()


@dataclass
class DownloadResult:
    repo_full_name: str
    downloaded_files: int = 0
    missing_files: int = 0
    failed_files: int = 0
    skipped_files: int = 0
    cache_hits: int = 0
    status_message: str = ''


class DownloaderService:
    """Service for downloading SBOM-related files from GitHub."""

    def __init__(self, token: str | None = None, base_dir: str = 'data/sbom', timeout: int = 10, pool_size: int = 50):
        self.session = get_http_client(pool_size=pool_size)
        if token:
            self.session.headers.update({'Authorization': f"Bearer {token}"})
        self.base_dir = Path(base_dir)
        self.timeout = timeout

    def download_repository_assets(self, repo: Repository, language: Language) -> DownloadResult:
        """
        Downloads all SBOM manifest files for a given repository.

        Args:
            repo: Repository model instance
            language: Programming language of the repository

        Returns:
            DownloadResult containing statistics and status message
        """
        full_name = repo.full_name
        owner, name = full_name.split('/')

        # 1. Determine target directory
        dt = repo.download_target
        ref = dt.ref if dt else repo.default_branch
        commit_sha = dt.commit_sha if dt else 'unknown'
        commit_short = dt.commit_sha_short if dt else 'unknown'

        target_dir = self.base_dir / language.value / owner / name / ref / commit_sha
        target_dir.mkdir(parents=True, exist_ok=True)

        # 2. Save metadata.json
        self._save_metadata(repo, target_dir, ref, commit_sha, commit_short)

        # 3. Download manifest files
        handler = LanguageFactory.get_handler(language)
        targets = handler.get_sbom_paths()

        result = DownloadResult(repo_full_name=full_name)
        status_msgs = []

        base_raw_url = f"https://raw.githubusercontent.com/{owner}/{name}/{ref}"

        for filename in targets:
            file_path = target_dir / filename
            url = f"{base_raw_url}/{filename}"

            try:
                start_time = time.time()
                response = self.session.get(url, timeout=self.timeout)
                elapsed = time.time() - start_time

                is_cached = getattr(response, 'from_cache', False)
                if is_cached:
                    result.cache_hits += 1

                if response.status_code == 200:
                    file_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(file_path, 'wb') as f:
                        f.write(response.content)

                    result.downloaded_files += 1
                    status_msgs.append(f"[green]{filename}[/green]")

                    logger.debug(
                        'File Downloaded',
                        repo=full_name,
                        file=filename,
                        size=len(response.content),
                        elapsed=f"{elapsed:.2f}s",
                        cached=is_cached,
                    )
                elif response.status_code == 404:
                    result.missing_files += 1
                    status_msgs.append(
                        f"[dim yellow]no {filename}[/dim yellow]",
                    )
                else:
                    result.failed_files += 1
                    status_msgs.append(
                        f"[red]{filename} {response.status_code}[/red]",
                    )

            except requests.RequestException as e:
                logger.error(
                    'Download error', repo=full_name,
                    file=filename, error=str(e),
                )
                result.failed_files += 1
                status_msgs.append(f"[red]{filename} error[/red]")

        result.status_message = f"{full_name}: {', '.join(status_msgs)}" if status_msgs else f"{full_name}: skipped"
        return result

    def _save_metadata(self, repo: Repository, target_dir: Path, ref: str, commit_sha: str, commit_short: str):
        """Helper to save repository metadata snapshot."""
        metadata = {
            'repository': {
                'id': repo.id,
                'full_name': repo.full_name,
                'stars': repo.stars,
            },
            'snapshot': {
                'ref': ref,
                'ref_type': repo.download_target.ref_type if repo.download_target else 'branch',
                'commit_sha': commit_sha,
                'commit_sha_short': commit_short,
                'downloaded_at': datetime.now().isoformat(),
            },
        }

        # Add release info if applicable
        if repo.latest_stable_release:
            metadata['release'] = {
                'tag_name': repo.latest_stable_release.tag_name,
                'name': repo.latest_stable_release.name,
                'published_at': repo.latest_stable_release.published_at.isoformat() if repo.latest_stable_release.published_at else None,
            }

        metadata_path = target_dir / 'metadata.json'
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
