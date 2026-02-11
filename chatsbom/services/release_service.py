import json
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import structlog

from chatsbom.core.config import get_config
from chatsbom.core.stats import BaseStats
from chatsbom.models.github_release import GitHubRelease
from chatsbom.models.repository import Repository
from chatsbom.services.github_service import GitHubService

logger = structlog.get_logger('release_service')


@dataclass
class ReleaseStats(BaseStats):
    enriched: int = 0

    def inc_enriched(self):
        with self._lock:
            self.enriched += 1


class ReleaseService:
    """Service for enriching repository with release information."""

    def __init__(self, service: GitHubService):
        self.service = service
        self.config = get_config()

    def process_repo(self, repository: Repository, stats: ReleaseStats, language: str) -> dict | None:
        """Fetch releases and determine latest stable release."""
        owner = repository.owner
        repo = repository.repo
        start_time = time.time()

        # Check cache
        cache_path = self.config.paths.get_release_cache_path(owner, repo)

        releases_data = []
        if cache_path.exists():
            try:
                # Check TTL
                mtime = cache_path.stat().st_mtime
                if time.time() - mtime < self.config.github.cache_ttl:
                    with open(cache_path) as f:
                        releases_data = json.load(f)
                        stats.inc_cache_hits()
                        elapsed = time.time() - start_time
                        logger.info(
                            'Releases loaded (Cache)',
                            repo=f"{owner}/{repo}",
                            count=len(releases_data),
                            elapsed=f"{elapsed:.3f}s",
                        )
                else:
                    logger.debug(
                        'Releases cache expired',
                        repo=f"{owner}/{repo}",
                    )
            except Exception:
                pass

        if not releases_data:
            try:
                releases_data = self.service.get_repository_releases(
                    owner, repo,
                )
                stats.inc_api_requests(len(releases_data) // 100 + 1)
                self._save_cache(releases_data, cache_path)
                elapsed = time.time() - start_time
                logger.info(
                    'Releases loaded (API)',
                    repo=f"{owner}/{repo}",
                    count=len(releases_data),
                    elapsed=f"{elapsed:.3f}s",
                    status_code=200,
                )
            except Exception as e:
                elapsed = time.time() - start_time
                logger.error(
                    f"Failed to fetch releases for {owner}/{repo}: {e}",
                    elapsed=f"{elapsed:.3f}s",
                )
                stats.inc_failed()
                return None

        releases = [GitHubRelease.model_validate(r) for r in releases_data]

        # Sort releases by published_at (or created_at) descending to ensure correct order
        releases.sort(
            key=lambda x: x.published_at or x.created_at or datetime.min,
            reverse=True,
        )

        repository.has_releases = len(releases) > 0
        repository.total_releases = len(releases)
        repository.all_releases = releases  # Store all captured releases

        latest_stable = None
        for r in releases:
            if not r.is_prerelease and not r.is_draft:
                latest_stable = r
                break

        repository.latest_stable_release = latest_stable
        stats.inc_enriched()
        return repository.model_dump(mode='json')

    def _save_cache(self, data: list, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, default=str)
