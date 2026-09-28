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

        # Shared cache file for the entire repository
        cache_path = self.config.paths.get_git_refs_cache_path(owner, repo)

        try:
            sha: str | None = None
            is_cached, num_refs = False, 0
            ref, ref_type = '', 'branch'
            release = repository.latest_stable_release
            if release:
                ref, ref_type = release.tag_name, 'release'
                sha, is_cached, num_refs = self.git.resolve_ref(
                    owner, repo, ref, cache_path=cache_path,
                )
                if not sha:
                    logger.warning(
                        'Tag not found, falling back to default branch',
                        repo=f"{owner}/{repo}", tag=ref,
                    )

            if not sha:
                # The default branch is the one HEAD points at, from the
                # same `ls-remote` listing. Never `repository.
                # default_branch`: a ledger row with none left it the
                # model's `'main'`, which is wrong for most of the corpus
                # (36,692 `master`), and the pilot's repositories with no
                # release stopped here with nothing collected (#55).
                ref_type = 'branch'
                branch, sha, is_cached, num_refs = self.git.resolve_head(
                    owner, repo, cache_path=cache_path,
                )
                # A server that names no branch still has a HEAD.
                ref = branch or 'HEAD'
                if branch:
                    repository.default_branch = branch

            # `ls-remote` is the git protocol, which no REST quota
            # meters: counted as an API request, it spent `run --quota`
            # on work that costs none.
            if is_cached:
                stats.inc_cache_hits()

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
