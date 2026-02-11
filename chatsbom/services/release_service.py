import json
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import structlog

from chatsbom.core.config import get_config
from chatsbom.core.stats import BaseStats
from chatsbom.models.github_release import GitHubRelease
from chatsbom.models.github_release import ReleaseCache
from chatsbom.models.repository import Repository
from chatsbom.services.git_service import GitService
from chatsbom.services.github_service import GitHubService

logger = structlog.get_logger('release_service')


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
                        cached = json.load(f)
                        if isinstance(cached, dict) and 'releases' in cached:
                            cache_data = ReleaseCache.model_validate(cached)
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

                # 2. Fetch all tags via Git Protocol (ls-remote)
                # refs is a dict of {ref_name: sha}
                refs, _ = self.git_service.get_repo_refs(owner, repo)
                tags_only = {
                    name: sha for name,
                    sha in refs.items() if not name.startswith('refs/')
                }

                cache_data = ReleaseCache(
                    releases=releases_json,
                    tags=tags_only,
                )
                self._save_cache(cache_data, cache_path)
                stats.inc_api_requests(1)

                elapsed = time.time() - start_time
                logger.info(
                    'Releases loaded (API)',
                    repo=f"{owner}/{repo}",
                    releases=len(releases_json),
                    tags=len(tags_only),
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

                entry = GitHubRelease(
                    id=0,
                    tag_name=tag_name,
                    name=tag_name,
                    published_at=pub_date,
                    created_at=pub_date,
                    target_commitish=sha,
                    is_prerelease=False,
                    is_draft=False,
                    source='git_tag',
                )
                all_entries.append(entry)

        # Sort all by date
        all_entries.sort(
            key=lambda x: x.published_at or x.created_at or datetime.min,
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
        path.parent.mkdir(parents=True, exist_ok=True)
        with open(path, 'w', encoding='utf-8') as f:
            f.write(data.model_dump_json(indent=2))
