import datetime
import time
from dataclasses import dataclass
from dataclasses import field

import requests
import structlog
from rich.progress import Progress
from rich.progress import TaskID

from chatsbom.core.storage import Storage
from chatsbom.services.github_service import GitHubService

logger = structlog.get_logger('search_service')


@dataclass
class SearchStats:
    api_requests: int = 0
    cache_hits: int = 0
    repos_found: int = 0
    repos_saved: int = 0
    start_time: float = field(default_factory=time.time)


class SearchService:
    """Orchestrates the repository search process using GitHubService."""

    def __init__(self, service: GitHubService, lang: str, min_stars: int, output: str):
        self.service = service
        self.storage = Storage(output)
        self.lang = lang
        self.min_stars = min_stars
        self.current_max_stars: int | None = None

    def run(self, progress: Progress, task: TaskID):
        stats = SearchStats()

        if self.storage.min_stars_seen <= self.min_stars:
            logger.info(
                'Search already complete for this threshold.',
                min_stars_required=self.min_stars,
                min_stars_found=self.storage.min_stars_seen,
            )
            return stats

        while True:
            if self.current_max_stars is None:
                query = f"language:{self.lang} stars:>{self.min_stars}"
                desc = f"> {self.min_stars}"
            else:
                query = f"language:{self.lang} stars:{self.min_stars}..{self.current_max_stars}"
                desc = f"{self.min_stars}..{self.current_max_stars}"

            progress.update(task, stars=desc, status='Scanning')

            batch_items = []
            min_stars_in_batch: int = 999999999  # Large integer

            # GitHub Search API pagination
            for page in range(1, 11):
                try:
                    data = self.service.search_repositories(
                        query, page=page,
                    )
                    items = data.get('items', [])
                    if not items:
                        break

                    stats.api_requests += 1
                    if getattr(data, 'from_cache', False):
                        stats.cache_hits += 1

                    for item in items:
                        batch_items.append(item)
                        stars = int(item.get('stargazers_count', 0))
                        min_stars_in_batch = min(min_stars_in_batch, stars)

                        if self.storage.save(item):
                            progress.advance(task)
                            stats.repos_saved += 1
                            progress.console.print(
                                f"  [green]★[/] [bold]{item['full_name']}[/] "
                                f"[dim]({stars:,} stars)[/]",
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
                logger.info('[bold green]No more results. Done!')
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
            query = f"language:{self.lang} stars:{stars} created:{date_range}"

            progress.update(
                task_id, status='Time Slice',
                stars=f"{stars}★ [{date_range}]",
            )

            items = []
            for page in range(1, 11):
                data = self.service.search_repositories(query, page=page)
                batch = data.get('items', [])
                if not batch:
                    break
                items.extend(batch)
                if len(batch) < 100:
                    break

            if len(items) >= 1000:
                mid_ts = s.timestamp() + (e.timestamp() - s.timestamp()) / 2
                mid = datetime.datetime.fromtimestamp(mid_ts)
                stack.append((mid + datetime.timedelta(seconds=1), e))
                stack.append((s, mid))
            else:
                for item in items:
                    if self.storage.save(item):
                        progress.advance(task_id)
                        stats.repos_saved += 1
