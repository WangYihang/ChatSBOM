import json
import time
from dataclasses import dataclass
from pathlib import Path

import structlog

from chatsbom.core.config import get_config
from chatsbom.core.stats import BaseStats
from chatsbom.models.repository import Repository
from chatsbom.services.github_service import GitHubService

logger = structlog.get_logger('repo_service')


@dataclass
class RepoStats(BaseStats):
    enriched: int = 0

    def inc_enriched(self):
        with self._lock:
            self.enriched += 1


class RepoService:
    """Service for enriching repository metadata (Stars, License, Topics)."""

    def __init__(self, service: GitHubService):
        self.service = service
        self.config = get_config()

    def process_repo(self, repository: Repository, stats: RepoStats, language: str) -> dict | None:
        """Enrich a repository with metadata from GitHub API."""
        owner = repository.owner
        repo = repository.repo
        start_time = time.time()

        # Check cache first
        cache_path = self.config.paths.get_repo_cache_path(owner, repo)

        if cache_path.exists():
            try:
                with open(cache_path) as f:
                    cached_data = json.load(f)
                    stats.inc_cache_hits()
                    elapsed = time.time() - start_time
                    logger.info(
                        'Repo enriched (Cache)',
                        repo=f"{owner}/{repo}",
                        elapsed=f"{elapsed:.3f}s",
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
                elapsed = time.time() - start_time
                logger.info(
                    'Repo enriched (API)',
                    repo=f"{owner}/{repo}",
                    elapsed=f"{elapsed:.3f}s",
                    stars=repository.stars,
                )
                return repository.model_dump(mode='json')
            else:
                elapsed = time.time() - start_time
                stats.inc_failed()
                logger.warning(
                    'Repo enrichment failed (Empty metadata)',
                    repo=f"{owner}/{repo}",
                    elapsed=f"{elapsed:.3f}s",
                )
                return None

        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(
                f"Failed to enrich {f"{owner}/{repo}"}: {e}",
                elapsed=f"{elapsed:.3f}s",
            )
            stats.inc_failed()
            return None

    def _save_cache(self, repository: Repository, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(repository.model_dump_json(indent=2))
