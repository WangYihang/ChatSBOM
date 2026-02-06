import datetime
import json
import os
import time
from collections.abc import Generator
from dataclasses import dataclass
from dataclasses import field

import dotenv
import requests
import structlog
import typer
from rich.console import Console
from rich.progress import Progress
from rich.progress import SpinnerColumn
from rich.progress import TaskID
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn
from rich.table import Table

from sbom_insight.client import get_http_client
from sbom_insight.models.language import Language

dotenv.load_dotenv()
logger = structlog.get_logger('Searcher')
console = Console()
console = Console()


@dataclass
class SearchStats:
    api_requests: int = 0
    cache_hits: int = 0
    repos_found: int = 0
    repos_saved: int = 0
    start_time: float = field(default_factory=time.time)


class GitHubClient:
    """Handles GitHub API interaction, authentication, and rate limiting."""

    def __init__(self, token: str, delay: float = 2.0):
        self.session = get_http_client()
        self.session.headers.update({
            'Authorization': f"Bearer {token}",
            'Accept': 'application/vnd.github.v3+json',
            'User-Agent': 'SBOM-Insight',
        })
        self.delay = delay
        self.last_req_time = 0.0

    def search_repositories(self, query: str, task_id: TaskID, progress: Progress, stats: SearchStats) -> Generator[dict, None, None]:
        """
        Iterates through pagination (pages 1-10) for a given query.
        Handles API rate limits automatically.
        """
        page = 1
        # GitHub API Search limit: 1000 results (10 pages * 100)
        max_pages = 10

        while page <= max_pages:
            self._wait_for_rate_limit()

            url = 'https://api.github.com/search/repositories'
            params = {
                'q': query,
                'sort': 'stars',
                'order': 'desc',
                'per_page': '100',
                'page': str(page),
            }

            try:
                progress.update(
                    task_id, status=f"Page {page}",
                )
                start_time = time.time()
                resp = self.session.get(url, params=params, timeout=20)
                elapsed = time.time() - start_time
                is_cached = getattr(resp, 'from_cache', False)

                if not is_cached:
                    self.last_req_time = time.time()
                    logger.info(
                        'API Request',
                        page=page,
                        status=resp.status_code,
                        elapsed=f"{elapsed:.2f}s",
                        url=resp.url,
                        query=query,
                    )
                else:
                    stats.cache_hits += 1
                    timestamp = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    log_msg = (
                        f"{timestamp} \\[info     ] API Request                    "
                        f"elapsed={elapsed:.2f}s page={page} "
                        f"query='{query}' status={resp.status_code} "
                        f"url='{resp.url}' [green](Cached)[/green]"
                    )
                    console.print(f"[dim]{log_msg}[/dim]")

                stats.api_requests += 1

                if resp.status_code == 200:
                    data = resp.json()
                    items = data.get('items', [])
                    if not items:
                        return
                    stats.repos_found += len(items)
                    yield from items
                    if len(items) < 100:  # End of results
                        return
                    page += 1

                elif resp.status_code in [403, 429]:
                    self._handle_api_limit(resp, task_id, progress)
                elif resp.status_code == 422:
                    logger.error(
                        'API 422 Error (Unprocessable Entity). Stopping current query.',
                    )
                    return
                else:
                    logger.error(f"API Error {resp.status_code}: {resp.text}")
                    return

            except requests.RequestException as e:
                logger.error(f"Network error: {e}. Retrying in 5s...")
                time.sleep(5)

    def _wait_for_rate_limit(self):
        """Token bucket style local rate limiting."""
        gap = time.time() - self.last_req_time
        if gap < self.delay:
            time.sleep(self.delay - gap)

    def _handle_api_limit(self, resp: requests.Response, task_id: TaskID, progress: Progress):
        """Handles 403/429 responses by waiting until reset."""
        reset_time = int(
            resp.headers.get(
                'X-RateLimit-Reset', time.time() + 60,
            ),
        )
        wait_seconds = max(60, reset_time - int(time.time())) + 2

        logger.warning(f"Rate limit triggered. Waiting {wait_seconds}s...")
        for i in range(wait_seconds, 0, -1):
            progress.update(
                task_id, status=f"[bold red]Limit {i}s",
            )
            time.sleep(1)


class Storage:
    """Manages file persistence and deduplication."""

    def __init__(self, filepath: str):
        self.filepath = filepath
        self.visited_ids: set[int] = set()
        self.min_stars_seen: float = float('inf')
        self._load_existing()

    def _load_existing(self):
        if not os.path.exists(self.filepath):
            return

        count = 0
        try:
            with open(self.filepath, encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        try:
                            data = json.loads(line)
                            self.visited_ids.add(data['id'])

                            # Track minimum stars seen
                            stars = data.get('stargazers_count', float('inf'))
                            self.min_stars_seen = min(
                                self.min_stars_seen, stars,
                            )

                            count += 1
                        except json.JSONDecodeError:
                            pass
            logger.info(
                f"Loaded {count} existing records. Min stars: {self.min_stars_seen}",
            )
        except Exception as e:
            logger.error(f"Failed to load existing data: {e}")

    def save(self, item: dict) -> bool:
        """Saves an item if it hasn't been seen before. Returns True if saved."""
        if item['id'] in self.visited_ids:
            return False

        self.visited_ids.add(item['id'])

        record = {
            'id': item['id'],
            'full_name': item['full_name'],
            'stars': item['stargazers_count'],
            'url': item['html_url'],
            'created_at': item['created_at'],
            'default_branch': item.get('default_branch', 'main'),
            'description': item.get('description', ''),
            'topics': item.get('topics', []),
        }

        with open(self.filepath, 'a', encoding='utf-8') as f:
            f.write(json.dumps(record, ensure_ascii=False) + '\n')
            f.flush()
        return True


class Searcher:
    """Main searcher logic controller."""

    def __init__(self, token: str, lang: str, min_stars: int, output: str):
        self.client = GitHubClient(token)
        self.storage = Storage(output)
        self.lang = lang
        self.min_stars = min_stars
        self.current_max_stars: int | None = None

    def run(self):
        # Freshness Check removed (handled by HTTP cache)
        stats = SearchStats()

        with Progress(
            SpinnerColumn(),
            TextColumn('[bold blue]{task.description}'),
            TextColumn('•'),
            TextColumn('[bold yellow]{task.fields[status]}'),
            TextColumn('•'),
            TextColumn('[bold green]{task.completed} repos'),
            TextColumn('•'),
            TextColumn('[cyan]Values: {task.fields[stars]}'),
            TextColumn('•'),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(
                '[green]Crawling...',
                total=None,
                status='Init',
                stars='N/A',
            )

            # 0. Completeness Check
            if self.storage.min_stars_seen <= self.min_stars:
                logger.info(
                    'Search already complete for this threshold.',
                    min_stars_required=self.min_stars,
                    min_stars_found=self.storage.min_stars_seen,
                )
                return

            while True:
                # 1. Determine Query Range
                if self.current_max_stars is None:
                    query = f"language:{self.lang} stars:>{self.min_stars}"
                    desc = f"> {self.min_stars}"
                else:
                    query = f"language:{self.lang} stars:{self.min_stars}..{self.current_max_stars}"
                    desc = f"{self.min_stars}..{self.current_max_stars}"

                progress.update(
                    task,
                    stars=desc,
                    status='Scanning',
                )

                # 2. Execute Batch
                batch_items = []
                min_stars_in_batch = float('inf')

                for item in self.client.search_repositories(query, task, progress, stats):
                    batch_items.append(item)
                    stars = item['stargazers_count']
                    min_stars_in_batch = min(min_stars_in_batch, stars)

                    if self.storage.save(item):
                        progress.advance(task)
                        stats.repos_saved += 1

                # 3. Analyze Batch for Next Cursor
                count = len(batch_items)
                if count == 0:
                    logger.info('[bold green]No more results. Done!')
                    break

                if count < 1000:
                    # If we haven't hit the bottom star limit, but returned <1000,
                    # it implies we exhausted this specific query range.
                    if self.current_max_stars is None or min_stars_in_batch <= self.min_stars:
                        logger.info(
                            f"Batch ({count}) < 1000 and hit floor. Done.",
                        )
                        break
                    else:
                        # Move cursor down safely
                        self.current_max_stars = min_stars_in_batch - 1
                else:
                    # We hit the 1000 limit. Check for "Star Wall"
                    if self.current_max_stars is not None and min_stars_in_batch == self.current_max_stars:
                        logger.warning(
                            f"Dense Star Wall at {min_stars_in_batch}★. Switching to Time Slicing...",
                        )
                        self._process_time_slice(
                            min_stars_in_batch, task, progress, stats,
                        )
                        self.current_max_stars = min_stars_in_batch - 1
                    else:
                        # Normal cursor movement
                        self.current_max_stars = min_stars_in_batch

                # Boundary Check
                if self.current_max_stars is not None and self.current_max_stars < self.min_stars:
                    logger.info(
                        '[bold green]Reached minimum star threshold. Done.',
                    )
                    break

        # Print Summary Table (End of Run)
        elapsed_time = time.time() - stats.start_time
        table = Table(title='Search Summary')
        table.add_column('Metric', style='cyan')
        table.add_column('Value', style='magenta')

        table.add_row('Total API Requests', str(stats.api_requests))
        table.add_row('API Cache Hits', str(stats.cache_hits))
        table.add_row('Repos Discovered', str(stats.repos_found))
        table.add_row('New Repos Saved', str(stats.repos_saved))
        table.add_row('Total Duration', f"{elapsed_time:.2f}s")

        console.print(table)

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
                task_id,
                status='Time Slice',
                stars=f"{stars}★ [{date_range}]",
            )

            items = list(
                self.client.search_repositories(
                    query, task_id, progress, stats,
                ),
            )

            if len(items) >= 1000:
                # Too many results, split time range
                mid_ts = s.timestamp() + (e.timestamp() - s.timestamp()) / 2
                mid = datetime.datetime.fromtimestamp(mid_ts)
                stack.append((mid + datetime.timedelta(seconds=1), e))
                stack.append((s, mid))
            else:
                # Process results
                for item in items:
                    if self.storage.save(item):
                        progress.advance(task_id)
                        stats.repos_saved += 1


def main(
    token: str = typer.Option(
        None, envvar='GITHUB_TOKEN', help='GitHub Token',
    ),
    language: Language | None = typer.Option(
        None, help='Target Programming Language (default: all)',
    ),
    min_stars: int = typer.Option(1000, help='Minimum Star Count'),
    output_path_arg: str | None = typer.Option(
        None, '--output', help='Output JSONL Path',
    ),
):
    """
    GitHub SBOM Searcher.
    Crawls repositories by Star count, using cursor slicing to bypass 1000-item limits.
    """
    if not token:
        console.print(
            '[bold red]Error:[/] GITHUB_TOKEN is not set.\n\n'
            'The GitHub Search API requires authentication. '
            'Please set the GITHUB_TOKEN environment variable:\n\n'
            '  [cyan]export GITHUB_TOKEN="your_github_token"[/]\n\n'
            'Or add it to your [cyan].env[/] file:\n\n'
            '  [cyan]GITHUB_TOKEN=your_github_token[/]\n\n'
            'You can create a token at: '
            '[link=https://github.com/settings/tokens]https://github.com/settings/tokens[/link]',
        )
        raise typer.Exit(1)

    if language is None:
        logger.warning('No language specified. Crawling ALL languages...')
        target_languages = list(Language)
    else:
        target_languages = [language]

    for lang in target_languages:

        # Determine output path for this language
        if output_path_arg is None:
            current_output = f"{lang}.jsonl"
        else:
            current_output = output_path_arg

        logger.info(
            'Starting Search',
            language=str(lang),
            min_stars=min_stars,
            output=current_output,
        )

        try:
            searcher = Searcher(token, lang, min_stars, current_output)
            searcher.run()
        except KeyboardInterrupt:
            logger.warning('Aborted by user.')
            raise typer.Exit(1)
        except Exception as e:
            logger.exception(f"Fatal Error processing {lang}: {e}")
            continue


if __name__ == '__main__':
    typer.run(main)
