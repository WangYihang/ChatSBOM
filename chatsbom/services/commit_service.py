import time
from dataclasses import dataclass
from dataclasses import field

import structlog

from chatsbom.core.config import get_config
from chatsbom.models.download_target import DownloadTarget
from chatsbom.models.repository import Repository
from chatsbom.services.git_service import GitService

logger = structlog.get_logger('commit_service')


@dataclass
class CommitStats:
    total: int = 0
    enriched: int = 0
    skipped: int = 0
    failed: int = 0
    api_requests: int = 0  # Now represents remote Git calls
    cache_hits: int = 0
    start_time: float = field(default_factory=time.time)


class CommitService:
    """Service for resolving tags/branches to specific commit SHAs using Git protocol."""

    def __init__(self, git_service: GitService):
        self.git = git_service
        self.config = get_config()

    def process_repo(self, repository: Repository, stats: CommitStats, language: str) -> dict | None:
        """Resolve download target to a commit SHA via GitService."""
        owner = repository.owner
        repo = repository.repo

        # Determine target ref
        ref = repository.default_branch
        ref_type = 'branch'
        if repository.latest_stable_release:
            ref = repository.latest_stable_release.tag_name
            ref_type = 'release'

        # Shared cache file for the entire repository
        cache_path = self.config.paths.get_commit_cache_dir(
            language,
        ) / owner / f'{repo}.json'

        try:
            # Resolve ref (handles caching internally)
            sha, is_cached = self.git.resolve_ref(
                owner, repo, ref, cache_path=cache_path,
            )

            # Fallback to default branch if release tag resolution fails
            if not sha and ref_type == 'release':
                logger.warning(
                    'Tag not found, falling back to default branch', repo=f"{owner}/{repo}", tag=ref,
                )
                ref = repository.default_branch
                ref_type = 'branch'
                sha, is_cached = self.git.resolve_ref(
                    owner, repo, ref, cache_path=cache_path,
                )

            # Update statistics
            if is_cached:
                stats.cache_hits += 1
            else:
                stats.api_requests += 1

            if sha:
                repository.download_target = DownloadTarget(
                    ref=ref,
                    ref_type=ref_type,
                    commit_sha=sha,
                    commit_sha_short=sha[:7],
                )
                stats.enriched += 1
                return repository.model_dump(mode='json')

        except Exception as e:
            logger.error(
                'Failed to process repository commit',
                repo=f"{owner}/{repo}", error=str(e),
            )

        stats.failed += 1
        return None
