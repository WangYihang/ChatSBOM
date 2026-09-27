import json
import time
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
from pathlib import Path

import structlog

from chatsbom.core.config import get_config
from chatsbom.core.fs import atomic_write_text
from chatsbom.core.stats import BaseStats
from chatsbom.models.github_release import GitHubRelease
from chatsbom.models.github_release import RELEASE_CACHE_VERSION
from chatsbom.models.github_release import ReleaseCache
from chatsbom.models.repository import Repository
from chatsbom.services.git_service import GitService
from chatsbom.services.github_service import GitHubService

logger = structlog.get_logger('release_service')

# Sorts before every real date. Aware, like the dates it is compared
# with: a naive floor raised TypeError on the first undated tag, and
# the repository got no release record at all.
UNDATED = datetime.min.replace(tzinfo=timezone.utc)


@dataclass
class ReleaseStats(BaseStats):
    enriched: int = 0

    def inc_enriched(self):
        with self._lock:
            self.enriched += 1


class ReleaseService:
    """Service for enriching repository with release information."""

    def __init__(self, service: GitHubService, git_service: GitService):
        self.service = service
        self.git_service = git_service
        self.config = get_config()

    def process_repo(self, repository: Repository, stats: ReleaseStats, language: str) -> dict | None:
        """Fetch releases and git tags, merging them into a complete history."""
        owner = repository.owner
        repo = repository.repo
        start_time = time.time()

        cache_path = self.config.paths.get_release_cache_path(owner, repo)

        cache_data = ReleaseCache()
        if cache_path.exists():
            try:
                mtime = cache_path.stat().st_mtime
                if time.time() - mtime < self.config.github.cache_ttl:
                    with open(cache_path) as f:
                        cached = ReleaseCache.model_validate(json.load(f))
                        # Any other version is fetched afresh below:
                        # version 1 held branches and HEAD among its tags.
                        if cached.version == RELEASE_CACHE_VERSION:
                            cache_data = cached
                            stats.inc_cache_hits()
                            elapsed = time.time() - start_time
                            logger.info(
                                'Releases loaded (Cache)',
                                repo=f"{owner}/{repo}",
                                releases=len(cache_data.releases),
                                tags=len(cache_data.tags),
                                elapsed=f"{elapsed:.3f}s",
                            )
            except Exception:
                pass

        if not cache_data.releases and not cache_data.tags:
            try:
                # 1. Fetch official releases via GitHub API
                releases_json = self.service.get_repository_releases(
                    owner, repo,
                )

                # 2. Fetch all tags via Git Protocol (ls-remote), as
                # {tag_name: commit_sha}. Tags only: HEAD and branches
                # aren't releases, and each would cost a date lookup.
                tags, _ = self.git_service.get_repo_tags(owner, repo)

                cache_data = ReleaseCache(
                    version=RELEASE_CACHE_VERSION,
                    releases=releases_json,
                    tags=tags,
                )
                self._save_cache(cache_data, cache_path)
                stats.inc_api_requests(1)

                elapsed = time.time() - start_time
                logger.info(
                    'Releases loaded (API)',
                    repo=f"{owner}/{repo}",
                    releases=len(releases_json),
                    tags=len(tags),
                    elapsed=f"{elapsed:.3f}s",
                    status_code=200,
                )
            except Exception as e:
                logger.error(
                    f"Failed to fetch history for {owner}/{repo}: {e}",
                )
                stats.inc_failed()
                return None

        # Process and merge
        releases_map = {r['tag_name']: r for r in cache_data.releases}
        all_entries = []

        # Convert releases to models
        for r_json in cache_data.releases:
            entry = GitHubRelease.model_validate(r_json)
            entry.source = 'github_release'
            all_entries.append(entry)

        # Handle tags from GitService that don't have releases
        for tag_name, sha in cache_data.tags.items():
            if tag_name not in releases_map:
                # Use GitHub API only to supplement missing date information
                pub_date_str = self.service.get_commit_date(owner, repo, sha)
                pub_date = None
                if pub_date_str:
                    try:
                        pub_date = datetime.fromisoformat(
                            pub_date_str.replace('Z', '+00:00'),
                        )
                    except (ValueError, TypeError):
                        pass

                # Pre-release and draft are flags of a GitHub release;
                # a bare tag has neither, so both stay False.
                entry = GitHubRelease(
                    id=0,
                    tag_name=tag_name,
                    name=tag_name,
                    published_at=pub_date,
                    created_at=pub_date,
                    target_commitish=sha,
                    source='git_tag',
                )
                all_entries.append(entry)

        # Sort all by date, undated last
        all_entries.sort(
            key=lambda x: x.published_at or x.created_at or UNDATED,
            reverse=True,
        )

        repository.has_releases = len(all_entries) > 0
        repository.total_releases = len(all_entries)
        repository.all_releases = all_entries

        latest_stable = None
        for r in all_entries:
            if not r.is_prerelease and not r.is_draft:
                latest_stable = r
                break

        repository.latest_stable_release = latest_stable
        stats.inc_enriched()
        return repository.model_dump(mode='json')

    def _save_cache(self, data: ReleaseCache, path: Path):
        # Whole or not at all: written in place, a refresh cut short
        # replaced a good cache with a prefix of the next one.
        atomic_write_text(path, data.model_dump_json(indent=2))
