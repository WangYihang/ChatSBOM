import json
import threading
import time
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
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
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def inc_enriched(self):
        with self._lock:
            self.enriched += 1

    def inc_failed(self):
        with self._lock:
            self.failed += 1

    def inc_skipped(self):
        with self._lock:
            self.skipped += 1

    def inc_api_requests(self, count: int = 1):
        with self._lock:
            self.api_requests += count

    def inc_cache_hits(self):
        with self._lock:
            self.cache_hits += 1


class ReleaseService:
    """Service for enriching repository with release information."""

    def __init__(self, service: GitHubService):
        self.service = service
        self.config = get_config()

    def process_repo(self, repository: Repository, stats: ReleaseStats, language: str) -> dict | None:
        """Fetch releases and determine latest stable release."""
        owner = repository.owner
        repo = repository.repo

        # Check cache
        cache_path = self.config.paths.get_release_cache_dir(
            language,
        ) / owner / repo / 'releases.json'

        releases_data = []
        if cache_path.exists():
            try:
                with open(cache_path) as f:
                    releases_data = json.load(f)
                    stats.inc_cache_hits()
                    logger.info(
                        'CACHE HIT', path=str(cache_path),
                        elapsed='0.000s', _style='dim',
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
            except Exception as e:
                logger.error(
                    f"Failed to fetch releases for {owner}/{repo}: {e}",
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
        repository.all_releases = releases[:10]  # Store top 10

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
