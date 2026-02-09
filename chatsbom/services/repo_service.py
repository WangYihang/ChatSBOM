import json
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


class RepoService:
    """Service for enriching repository metadata (Stars, License, Topics)."""

    def __init__(self, service: GitHubService):
        self.service = service
        self.config = get_config()

    def process_repo(self, repo: Repository, stats: RepoStats, language: str) -> dict | None:
        """Enrich a repository with metadata from GitHub API."""
        owner, repo_name = repo.full_name.split('/')

        # Check cache first
        cache_path = self.config.paths.get_repo_cache_dir(
            language,
        ) / owner / f'{repo_name}.json'
        if cache_path.exists():
            try:
                with open(cache_path) as f:
                    cached_data = json.load(f)
                    stats.cache_hits += 1
                    # Update repo with cached data but keep existing ID/url if needed
                    # For now just return the cached data as the enriched repo
                    # But we should probably merge.
                    # Simplified: Use cached data to update current repo object
                    cached_repo = Repository.model_validate(cached_data)
                    # Merge logic could go here if needed
                    return cached_repo.model_dump(mode='json')
            except Exception as e:
                logger.warning(
                    f"Failed to read cache for {repo.full_name}: {e}",
                )

        logger.info('Fetching metadata', repo=repo.full_name)

        try:
            metadata = self.service.get_repository_metadata(owner, repo_name)
            stats.api_requests += 1

            if metadata:
                repo.stars = metadata.get('stargazers_count', repo.stars)
                repo.language = metadata.get('language')
                repo.description = metadata.get('description')
                repo.topics = metadata.get('topics', [])
                if metadata.get('license'):
                    repo.license_spdx_id = metadata.get(
                        'license', {},
                    ).get('spdx_id')
                    repo.license_name = metadata.get('license', {}).get('name')

                # Save to cache
                self._save_cache(repo, cache_path)
                stats.enriched += 1
                return repo.model_dump(mode='json')
            else:
                stats.failed += 1
                return None

        except Exception as e:
            logger.error(f"Failed to enrich {repo.full_name}: {e}")
            stats.failed += 1
            return None

    def _save_cache(self, repo: Repository, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(repo.model_dump_json(indent=2))
