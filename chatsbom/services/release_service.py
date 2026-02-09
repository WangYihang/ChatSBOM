import json
import time
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path

import structlog

from chatsbom.core.config import get_config
from chatsbom.models.github_release import GitHubRelease
from chatsbom.models.repository import Repository
from chatsbom.services.github_service import GitHubService

logger = structlog.get_logger('release_service')


@dataclass
class ReleaseStats:
    total: int = 0
    enriched: int = 0
    skipped: int = 0
    failed: int = 0
    api_requests: int = 0
    cache_hits: int = 0
    start_time: float = field(default_factory=time.time)


class ReleaseService:
    """Service for enriching repository with release information."""

    def __init__(self, service: GitHubService):
        self.service = service
        self.config = get_config()

    def process_repo(self, repo: Repository, stats: ReleaseStats, language: str) -> dict | None:
        """Fetch releases and determine latest stable release."""
        owner, repo_name = repo.full_name.split('/')

        # Check cache
        cache_path = self.config.paths.get_release_cache_dir(
            language,
        ) / owner / repo_name / 'releases.json'

        releases_data = []
        if cache_path.exists():
            try:
                with open(cache_path) as f:
                    releases_data = json.load(f)
                    stats.cache_hits += 1
            except Exception:
                pass

        if not releases_data:
            logger.info('Fetching releases', repo=repo.full_name)
            try:
                releases_data = self.service.get_repository_releases(
                    owner, repo_name,
                )
                stats.api_requests += len(releases_data) // 100 + 1
                self._save_cache(releases_data, cache_path)
            except Exception as e:
                logger.error(
                    f"Failed to fetch releases for {repo.full_name}: {e}",
                )
                stats.failed += 1
                return None

        releases = [GitHubRelease.model_validate(r) for r in releases_data]
        repo.has_releases = len(releases) > 0
        repo.all_releases = releases[:10]  # Store top 10

        latest_stable = None
        for r in releases:
            if not r.is_prerelease and not r.is_draft:
                latest_stable = r
                break

        repo.latest_stable_release = latest_stable
        stats.enriched += 1
        return repo.model_dump(mode='json')

    def _save_cache(self, data: list, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, default=str)
