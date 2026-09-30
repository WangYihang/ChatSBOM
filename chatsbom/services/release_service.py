import json
import time
from dataclasses import dataclass
from pathlib import Path

import structlog

from chatsbom.collector.releases import API_DATE_CAP
from chatsbom.collector.releases import git_dated
from chatsbom.collector.releases import release_history
from chatsbom.collector.releases import to_ask
from chatsbom.collector.releases import version_key
from chatsbom.core.config import get_config
from chatsbom.core.fs import atomic_write_text
from chatsbom.core.stats import BaseStats
from chatsbom.models.github_release import RELEASE_CACHE_VERSION
from chatsbom.models.github_release import ReleaseCache
from chatsbom.models.repository import Repository
from chatsbom.services.git_service import GitService
from chatsbom.services.github_service import GitHubService

logger = structlog.get_logger('release_service')

# Where the release stage's rules are kept since the collector's stage
# follows them too (#161), named here as they were.
__all__ = ['API_DATE_CAP', 'ReleaseService', 'ReleaseStats', 'version_key']


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
        # What this repository costs `--quota`: the requests that
        # reached GitHub, counted where they are sent (cache hits free).
        sent_before = self._sent()

        cache_data = ReleaseCache()
        stale = False
        # Whether the tags in `cache_data` were dated by this code before
        # (see `_date_tags`); a cache written before they were is not.
        dated_before = False
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
                            dated_before = cached.tag_dates is not None
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
                    tag_dates=self._carried_dates(cache_path, tags),
                )
                # Saved below, once its tags are dated.
                stale = True
                dated_before = False

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
                stats.inc_api_requests(self._sent() - sent_before)
                return None

        releases_map = {r['tag_name']: r for r in cache_data.releases}
        bare_tags = {
            name: sha for name, sha in cache_data.tags.items()
            if name not in releases_map
        }
        dates = dict(cache_data.tag_dates or {})
        if not dated_before:
            dates = self._date_tags(owner, repo, bare_tags, dates)
            cache_data.tag_dates = dates
            stale = True
        if stale:
            try:
                self._save_cache(cache_data, cache_path)
            except OSError as e:
                # The history is whole in memory; only the next run's
                # shortcut is lost, and the cache already there is kept.
                logger.warning(
                    'Release cache not written',
                    repo=f'{owner}/{repo}', error=str(e),
                )
        stats.inc_api_requests(self._sent() - sent_before)

        # Every release and bare tag, newest first, and the latest stable
        # one among them (`collector/releases.py`).
        all_entries, latest_stable = release_history(
            cache_data.releases, bare_tags, dates,
        )

        repository.has_releases = len(all_entries) > 0
        repository.total_releases = len(all_entries)
        repository.all_releases = all_entries
        repository.latest_stable_release = latest_stable
        stats.inc_enriched()
        return repository.model_dump(mode='json')

    def _sent(self) -> int:
        """Requests this thread has sent to the REST API so far."""
        counter = getattr(self.service, 'requests_sent', None)
        return int(counter()) if callable(counter) else 0

    def _carried_dates(
        self, cache_path: Path, tags: dict[str, str],
    ) -> dict[str, str] | None:
        """Dates from the cache being replaced, for tags that did not move.

        A tag's date is its commit's, so it holds while the tag names the
        same commit: a refresh dates only the tags that are new or moved.
        None when there is nothing to carry.
        """
        try:
            old = ReleaseCache.model_validate_json(
                cache_path.read_text(encoding='utf-8'),
            )
        except Exception:
            return None
        if old.version != RELEASE_CACHE_VERSION or not old.tag_dates:
            return None
        carried = {
            name: date for name, date in old.tag_dates.items()
            if name in tags and old.tags.get(name) == tags[name]
        }
        return carried or None

    def _date_tags(
        self,
        owner: str,
        repo: str,
        bare_tags: dict[str, str],
        dates: dict[str, str],
    ) -> dict[str, str]:
        """`dates`, with every tag in `bare_tags` it lacks dated if possible.

        Git first (`GitService.get_tag_dates`: no REST quota at all). A
        tag git could not date, because the fetch failed or the tag moved
        between `ls-remote` and the fetch, is looked up with
        `/commits/{sha}`, at most `API_DATE_CAP` of them, newest version
        first. The rest stay undated and sort last.

        Called once per cache: a tag still undated afterwards is one
        nothing could date, and it is not asked about again until the
        cache is refreshed.
        """
        missing = {n: s for n, s in bare_tags.items() if n not in dates}
        if not missing:
            return dates
        from_git = self.git_service.get_tag_dates(owner, repo)
        dates.update(git_dated(missing, from_git))
        undated = [n for n in missing if n not in dates]
        for name in to_ask(undated):
            date = self.service.get_commit_date(owner, repo, bare_tags[name])
            if date:
                dates[name] = date
        if undated:
            logger.info(
                'Tags dated by API fallback',
                repo=f'{owner}/{repo}',
                git_failed=from_git is None,
                undated=len(undated),
                asked=min(len(undated), API_DATE_CAP),
            )
        return dates

    def _save_cache(self, data: ReleaseCache, path: Path):
        # Whole or not at all: written in place, a refresh cut short
        # replaced a good cache with a prefix of the next one.
        atomic_write_text(path, data.model_dump_json(indent=2))
