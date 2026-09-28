import datetime
import time
from dataclasses import dataclass

import requests
import structlog
from rich.progress import Progress
from rich.progress import TaskID

from chatsbom.core.stats import BaseStats
from chatsbom.core.storage import Storage
from chatsbom.services.github_service import GitHubService

logger = structlog.get_logger('search_service')


@dataclass
class SearchStats(BaseStats):
    repos_found: int = 0
    repos_saved: int = 0


def search_query(lang: str | None, stars: str, created: str | None = None) -> str:
    """A search query: the language filter, if any, then the rest.

    Unfiltered, there is no `language:` qualifier at all. The time
    slices once spelled it `language:{lang}` whatever it was, and sent
    `language:None` — a language nothing is written in — for every
    dense star count of an unfiltered search (design #55, F20).
    """
    parts = [f'language:{lang}'] if lang else []
    parts.append(f'stars:{stars}')
    if created:
        parts.append(f'created:{created}')
    return ' '.join(parts)


class SearchService:
    """Orchestrates the repository search process using GitHubService."""

    def __init__(self, service: GitHubService, lang: str | None, min_stars: int, output: str, limit: int | None = None, force: bool = False):
        self.service = service
        self.storage = Storage(output)
        self.lang = lang
        self.min_stars = min_stars
        self.current_max_stars: int | None = None
        self.limit = limit
        self.force = force  # Store force parameter

    def run(self, progress: Progress, task: TaskID):
        stats = SearchStats()

        if not self.force and self.storage.min_stars_seen <= self.min_stars:  # Use force parameter
            logger.info(
                'Search already complete for this threshold.',
                min_stars_required=self.min_stars,
                min_stars_found=self.storage.min_stars_seen,
            )
            return stats

        # An interrupted run resumes where it stopped rather than from
        # the top: results come most stars first, so everything above
        # the fewest stars stored is stored. That count is searched
        # again, in case it was cut off part way through.
        if not self.force and self.storage.visited_ids:
            self.current_max_stars = int(self.storage.min_stars_seen)
            logger.info(
                'Resuming search', stored=len(self.storage.visited_ids),
                from_stars=self.current_max_stars,
            )

        while True:
            if self.limit and stats.repos_saved >= self.limit:
                logger.info('Limit reached.', limit=self.limit)
                break

            # At least `min_stars`, as the snapshot promises: `>` left
            # out the repositories with exactly that many.
            if self.current_max_stars is None:
                query = search_query(self.lang, f'>={self.min_stars}')
                desc = f">= {self.min_stars}"
            else:
                query = search_query(
                    self.lang, f'{self.min_stars}..{self.current_max_stars}',
                )
                desc = f"{self.min_stars}..{self.current_max_stars}"

            progress.update(task, stars=desc, status='Scanning')

            batch_items = []
            min_stars_in_batch: int = 999999999  # Large integer

            # GitHub Search API pagination
            for page in range(1, 11):
                try:
                    req_start = time.time()
                    data = self._search(query, page, stats)
                    req_elapsed = time.time() - req_start

                    items = data.get('items', [])

                    # Log API call details
                    logger.info(
                        'Search API Request',
                        query=query,
                        page=page,
                        count=len(items),
                        elapsed=f"{req_elapsed:.3f}s",
                        status_code=200,
                    )

                    if not items:
                        break

                    for item in items:
                        batch_items.append(item)
                        stars = int(item.get('stargazers_count', 0))
                        min_stars_in_batch = min(min_stars_in_batch, stars)

                        # Strict Language Check
                        if self.lang:
                            repo_lang = (item.get('language') or '').lower()
                            target_lang = str(self.lang).lower()

                            if repo_lang != target_lang:
                                # Skip if language doesn't match (e.g. searching for 'go' but getting 'html')
                                logger.debug(
                                    'Skipping Language Mismatch',
                                    repo=f"{item['owner']['login']}/{item['name']}",
                                    expected=target_lang,
                                    found=repo_lang,
                                )
                                continue

                        if self.storage.save(item):
                            progress.advance(task)
                            stats.repos_saved += 1
                            logger.info(
                                'Repo Saved',
                                repo=f"{item['owner']['login']}/{item['name']}",
                                stars=stars,
                            )
                    if len(items) < 100:
                        break

                except requests.HTTPError as e:
                    if e.response.status_code in [403, 429]:
                        self._handle_rate_limit(e.response, task, progress)
                        continue
                    else:
                        logger.error(f"API Error: {e}")
                        break

            count = len(batch_items)
            if count == 0:
                logger.info('No more results. Done!', _style='bold green')
                break

            if count < 1000:
                if self.current_max_stars is None or min_stars_in_batch <= self.min_stars:
                    break
                else:
                    self.current_max_stars = int(min_stars_in_batch) - 1
            else:
                if self.current_max_stars is not None and min_stars_in_batch == self.current_max_stars:
                    logger.warning(
                        f"Dense Star Wall at {min_stars_in_batch}★. Switching to Time Slicing...",
                    )
                    self._process_time_slice(
                        int(min_stars_in_batch), task, progress, stats,
                    )
                    self.current_max_stars = int(min_stars_in_batch) - 1
                else:
                    self.current_max_stars = int(min_stars_in_batch)

            if self.current_max_stars is not None and self.current_max_stars < self.min_stars:
                break

        return stats

    def _search(self, query: str, page: int, stats: SearchStats) -> dict:
        """One page of results, counted as sent or as a cache hit."""
        counter = getattr(self.service, 'requests_sent', None)
        if not callable(counter):
            data = self.service.search_repositories(query, page=page)
            stats.inc_api_requests()
            return data
        before = counter()
        data = self.service.search_repositories(query, page=page)
        if counter() > before:
            stats.inc_api_requests()
        else:
            stats.inc_cache_hits()
        return data

    def _handle_rate_limit(self, response, task_id, progress):
        reset_time = int(
            response.headers.get(
                'X-RateLimit-Reset', time.time() + 60,
            ),
        )
        wait_seconds = max(60, reset_time - int(time.time())) + 2
        logger.warning(f"Rate limit triggered. Waiting {wait_seconds}s...")
        for i in range(wait_seconds, 0, -1):
            progress.update(task_id, status=f"[bold red]Limit {i}s")
            time.sleep(1)

    def _process_time_slice(self, stars: int, task_id: TaskID, progress: Progress, stats: SearchStats):
        """Handles dense star counts by slicing via 'created' date."""
        start_dt = datetime.datetime(2008, 1, 1)
        end_dt = datetime.datetime.now()
        stack = [(start_dt, end_dt)]

        while stack:
            s, e = stack.pop()
            date_range = f"{s.strftime('%Y-%m-%d')}..{e.strftime('%Y-%m-%d')}"
            query = search_query(self.lang, str(stars), date_range)

            progress.update(
                task_id, status='Time Slice',
                stars=f"{stars}★ [{date_range}]",
            )

            items = []
            for page in range(1, 11):
                try:
                    data = self._search(query, page, stats)
                except requests.HTTPError as error:
                    # Rate limits are waited out inside the service; any
                    # other refusal ends this slice, not the search.
                    logger.error(
                        'Search API error', query=query, page=page,
                        error=str(error),
                    )
                    break
                batch = data.get('items', [])
                if not batch:
                    break
                items.extend(batch)
                if len(batch) < 100:
                    break

            if len(items) >= 1000 and e - s > datetime.timedelta(days=1):
                mid_ts = s.timestamp() + (e.timestamp() - s.timestamp()) / 2
                mid = datetime.datetime.fromtimestamp(mid_ts)
                stack.append((mid + datetime.timedelta(seconds=1), e))
                stack.append((s, mid))
            else:
                for item in items:
                    # Strict Language Check
                    if self.lang:
                        repo_lang = (item.get('language') or '').lower()
                        target_lang = str(self.lang).lower()

                        if repo_lang != target_lang:
                            continue

                    if self.storage.save(item):
                        progress.advance(task_id)
                        stats.repos_saved += 1
