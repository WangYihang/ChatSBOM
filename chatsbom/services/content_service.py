from dataclasses import dataclass

import requests
import structlog
from rich.console import Console

from chatsbom.core.client import get_http_client
from chatsbom.core.config import get_config
from chatsbom.models.language import Language
from chatsbom.models.language import LanguageFactory
from chatsbom.models.repository import Repository

logger = structlog.get_logger('content_service')
console = Console()


@dataclass
class ContentStats:
    repo_full_name: str
    downloaded_files: int = 0
    missing_files: int = 0
    failed_files: int = 0
    skipped_files: int = 0
    cache_hits: int = 0
    status_message: str = ''
    local_path: str = ''


class ContentService:
    """Service for downloading raw content files from GitHub."""

    def __init__(self, token: str | None = None, timeout: int = 10, pool_size: int = 50):
        self.session = get_http_client(pool_size=pool_size)
        if token:
            self.session.headers.update({'Authorization': f"Bearer {token}"})
        self.config = get_config()
        self.timeout = timeout

    def process_repo(self, repo: Repository, language: Language) -> dict | None:
        """
        Downloads all manifest files for a given repository.
        Returns repository dict with 'local_content_path' if successful.
        """
        full_name = repo.full_name
        owner, name = full_name.split('/')

        dt = repo.download_target
        if not dt:
            logger.warning(
                f"No download target for {full_name}, skipping content download",
            )
            return None

        # Path: data/05-github-content/<lang>/<owner>/<repo>/<ref>/<sha>/
        target_dir = self.config.paths.content_dir / \
            language.value / owner / name / dt.ref / dt.commit_sha
        target_dir.mkdir(parents=True, exist_ok=True)

        handler = LanguageFactory.get_handler(language)
        targets = handler.get_sbom_paths()

        result = ContentStats(repo_full_name=full_name)
        status_msgs = []
        has_content = False

        # Raw URL structure: https://raw.githubusercontent.com/{owner}/{repo}/{commit_sha}/{path}
        # Using commit_sha is safer than ref for immutability
        base_raw_url = f"https://raw.githubusercontent.com/{owner}/{name}/{dt.commit_sha}"

        for filename in targets:
            file_path = target_dir / filename
            url = f"{base_raw_url}/{filename}"

            # Skip if already exists (immutable content)
            if file_path.exists():
                result.skipped_files += 1
                has_content = True
                continue

            try:
                response = self.session.get(url, timeout=self.timeout)

                is_cached = getattr(response, 'from_cache', False)
                if is_cached:
                    result.cache_hits += 1

                if response.status_code == 200:
                    file_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(file_path, 'wb') as f:
                        f.write(response.content)

                    result.downloaded_files += 1
                    has_content = True
                    status_msgs.append(f"[green]{filename}[/green]")
                elif response.status_code == 404:
                    result.missing_files += 1
                else:
                    result.failed_files += 1
                    status_msgs.append(
                        f"[red]{filename} {response.status_code}[/red]",
                    )

            except requests.RequestException as e:
                logger.error(f"Download error {full_name}/{filename}: {e}")
                result.failed_files += 1

        if has_content:
            repo_dict = repo.model_dump(mode='json')
            repo_dict['local_content_path'] = str(target_dir)
            result.local_path = str(target_dir)
            return repo_dict

        return None
