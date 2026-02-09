import json
import time
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path

import structlog

from chatsbom.core.config import get_config
from chatsbom.models.download_target import DownloadTarget
from chatsbom.models.repository import Repository
from chatsbom.services.github_service import GitHubService

logger = structlog.get_logger('commit_service')


@dataclass
class CommitStats:
    total: int = 0
    enriched: int = 0
    skipped: int = 0
    failed: int = 0
    api_requests: int = 0
    cache_hits: int = 0
    start_time: float = field(default_factory=time.time)


class CommitService:
    """Service for resolving tags/branches to specific commit SHAs."""

    def __init__(self, service: GitHubService):
        self.service = service
        self.config = get_config()

    def process_repo(self, repo: Repository, stats: CommitStats, language: str) -> dict | None:
        """Resolve download target to a commit SHA."""
        owner, repo_name = repo.full_name.split('/')

        # Determine target ref
        ref = repo.default_branch
        ref_type = 'branch'
        if repo.latest_stable_release:
            ref = repo.latest_stable_release.tag_name
            ref_type = 'release'

        # Check cache
        cache_path = self.config.paths.get_commit_cache_dir(
            language,
        ) / owner / repo_name / f'{ref}.json'

        commit_info = None
        if cache_path.exists():
            try:
                with open(cache_path) as f:
                    commit_info = json.load(f)
                    stats.cache_hits += 1
            except Exception:
                pass

        if not commit_info:
            logger.info('Resolving commit', repo=repo.full_name, ref=ref)
            try:
                commit_info = self.service.get_commit_metadata(
                    owner, repo_name, ref,
                )
                stats.api_requests += 1

                # Fallback to default branch if tag fails
                if not commit_info and ref_type == 'release':
                    logger.warning(
                        f"Tag {ref} not found, falling back to default branch", repo=repo.full_name,
                    )
                    ref = repo.default_branch
                    ref_type = 'branch'
                    commit_info = self.service.get_commit_metadata(
                        owner, repo_name, ref,
                    )
                    stats.api_requests += 1

                if commit_info:
                    self._save_cache(commit_info, cache_path)
            except Exception as e:
                logger.error(
                    f"Failed to resolve commit for {repo.full_name}: {e}",
                )
                stats.failed += 1
                return None

        if commit_info:
            repo.download_target = DownloadTarget(
                ref=ref,
                ref_type=ref_type,
                commit_sha=commit_info['sha'],
                commit_sha_short=commit_info['sha_short'],
            )
            stats.enriched += 1
            return repo.model_dump(mode='json')

        stats.failed += 1
        return None

    def _save_cache(self, data: dict, path: Path):
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
