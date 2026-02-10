import threading
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
                stats.inc_cache_hits()
            else:
                stats.inc_api_requests()

            if sha:
                repository.download_target = DownloadTarget(
                    ref=ref,
                    ref_type=ref_type,
                    commit_sha=sha,
                    commit_sha_short=sha[:7],
                )
                stats.inc_enriched()
                return repository.model_dump(mode='json')

        except Exception as e:
            logger.error(
                'Failed to process repository commit',
                repo=f"{owner}/{repo}", error=str(e),
            )

        stats.inc_failed()
        return None
