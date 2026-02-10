import time
from dataclasses import dataclass

import structlog

from chatsbom.core.config import get_config
from chatsbom.core.stats import BaseStats
from chatsbom.models.download_target import DownloadTarget
from chatsbom.models.repository import Repository
from chatsbom.services.git_service import GitService

logger = structlog.get_logger('commit_service')


@dataclass
class CommitStats(BaseStats):
    enriched: int = 0

    def inc_enriched(self):
        with self._lock:
            self.enriched += 1


class CommitService:
    """Service for resolving tags/branches to specific commit SHAs using Git protocol."""

    def __init__(self, git_service: GitService):
        self.git = git_service
        self.config = get_config()

    def process_repo(self, repository: Repository, stats: CommitStats, language: str) -> dict | None:
        """Resolve download target to a commit SHA via GitService."""
        owner = repository.owner
        repo = repository.repo
        start_time = time.time()

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
            sha, is_cached, num_refs = self.git.resolve_ref(
                owner, repo, ref, cache_path=cache_path,
            )

            # Fallback to default branch if release tag resolution fails
            if not sha and ref_type == 'release':
                logger.warning(
                    'Tag not found, falling back to default branch', repo=f"{owner}/{repo}", tag=ref,
                )
                ref = repository.default_branch
                ref_type = 'branch'
                sha, is_cached, num_refs = self.git.resolve_ref(
                    owner, repo, ref, cache_path=cache_path,
                )

            # Update statistics
            if is_cached:
                stats.inc_cache_hits()
            else:
                stats.inc_api_requests()

            elapsed = time.time() - start_time
            if sha:
                repository.download_target = DownloadTarget(
                    ref=ref,
                    ref_type=ref_type,
                    commit_sha=sha,
                    commit_sha_short=sha[:7],
                )
                stats.inc_enriched()
                logger.info(
                    'Commit resolved',
                    repo=f"{owner}/{repo}",
                    ref=ref,
                    sha=sha[:7],
                    refs=num_refs,
                    cached=is_cached,
                    elapsed=f"{elapsed:.3f}s",
                )
                return repository.model_dump(mode='json')
            else:
                logger.warning(
                    'Failed to resolve commit',
                    repo=f"{owner}/{repo}",
                    ref=ref,
                    refs=num_refs,
                    elapsed=f"{elapsed:.3f}s",
                )

        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(
                'Error processing repository commit',
                repo=f"{owner}/{repo}", error=str(e),
                elapsed=f"{elapsed:.3f}s",
            )

        stats.inc_failed()
        return None
