import json
import threading
import time
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path

import structlog

from chatsbom.core.config import get_config
from chatsbom.models.repository import Repository
from chatsbom.services.github_service import GitHubService

logger = structlog.get_logger('repo_service')


@dataclass
class RepoStats:
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

    def inc_api_requests(self):
        with self._lock:
            self.api_requests += 1

    def inc_cache_hits(self):
        with self._lock:
            self.cache_hits += 1


class RepoService:
    """Service for enriching repository metadata (Stars, License, Topics)."""

    def __init__(self, service: GitHubService):
        self.service = service
        self.config = get_config()

    def process_repo(self, repository: Repository, stats: RepoStats, language: str) -> dict | None:
        """Enrich a repository with metadata from GitHub API."""
        owner = repository.owner
        repo = repository.repo

        # Check cache first
        cache_path = self.config.paths.get_repo_cache_dir(
            language,
        ) / owner / f'{repo}.json'
        if cache_path.exists():
            try:
                with open(cache_path) as f:
                    cached_data = json.load(f)
                    stats.inc_cache_hits()
                    logger.info(
                        'CACHE HIT', path=str(cache_path),
                        elapsed='0.000s', _style='dim',
                    )
                    # Update repo with cached data but keep existing ID/url if needed
                    # For now just return the cached data as the enriched repo
                    # But we should probably merge.
                    # Simplified: Use cached data to update current repo object
                    cached_repo = Repository.model_validate(cached_data)
                    # Merge logic could go here if needed
                    return cached_repo.model_dump(mode='json')
            except Exception as e:
                logger.warning(
                    f"Failed to read cache for {f"{owner}/{repo}"}: {e}",
                )

        try:
            metadata = self.service.get_repository_metadata(owner, repo)
            stats.inc_api_requests()

            if metadata:
                repository.stars = metadata.get(
                    'stargazers_count', repository.stars,
                )
                repository.language = metadata.get('language')
                repository.description = metadata.get('description')
                repository.topics = metadata.get('topics', [])
                if metadata.get('license'):
                    repository.license_spdx_id = metadata.get(
                        'license', {},
                    ).get('spdx_id')
                    repository.license_name = metadata.get(
                        'license', {},
                    ).get('name')

                # Save to cache
                self._save_cache(repository, cache_path)
                stats.inc_enriched()
                return repository.model_dump(mode='json')
            else:
                stats.inc_failed()
                return None

        except Exception as e:
            logger.error(f"Failed to enrich {f"{owner}/{repo}"}: {e}")
            stats.inc_failed()
            return None

    def _save_cache(self, repository: Repository, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(repository.model_dump_json(indent=2))
