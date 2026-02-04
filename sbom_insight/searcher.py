import datetime
import json
import os
import time
from collections.abc import Generator

import dotenv
import requests
import structlog
import typer
from rich.console import Console
from rich.progress import BarColumn
from rich.progress import Progress
from rich.progress import SpinnerColumn
from rich.progress import TaskID
from rich.progress import TextColumn

from sbom_insight.models.language import Language

dotenv.load_dotenv()
logger = structlog.get_logger('Searcher')
console = Console()
console = Console()


class GitHubClient:
    """Handles GitHub API interaction, authentication, and rate limiting."""

    def __init__(self, token: str, delay: float = 2.0):
        self.session = requests.Session()
        self.session.headers.update({
            'Authorization': f"Bearer {token}",
            'Accept': 'application/vnd.github.v3+json',
            'User-Agent': 'SBOM-Insight',
        })
        self.delay = delay
        self.last_req_time = 0.0

    def search_repositories(self, query: str, task_id: TaskID, progress: Progress) -> Generator[dict, None, None]:
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
                    task_id, description=f"[bold cyan]API Request (Page {page})...",
                )
                resp = self.session.get(url, params=params, timeout=20)
                self.last_req_time = time.time()

                if resp.status_code == 200:
                    data = resp.json()
                    items = data.get('items', [])
                    if not items:
                        return
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
                task_id, description=f"[bold yellow]Rate Limit Cooldown... {i}s",
            )
            time.sleep(1)


class Storage:
    """Manages file persistence and deduplication."""

    def __init__(self, filepath: str):
        self.filepath = filepath
        self.visited_ids: set[int] = set()
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
                            count += 1
                        except json.JSONDecodeError:
                            pass
            logger.info(f"Loaded {count} existing records.")
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
        with Progress(
            SpinnerColumn(),
            TextColumn('[progress.description]{task.description}'),
            BarColumn(),
            TextColumn('{task.completed} repos'),
            console=console,
        ) as progress:
            task = progress.add_task('[green]Starting...', total=None)

            while True:
                # 1. Determine Query Range
                if self.current_max_stars is None:
                    query = f"language:{self.lang} stars:>{self.min_stars}"
                    desc = f"> {self.min_stars}"
                else:
                    query = f"language:{self.lang} stars:{self.min_stars}..{self.current_max_stars}"
                    desc = f"{self.min_stars}..{self.current_max_stars}"

                progress.update(
                    task, description=f"[bold green]Scanning: Stars {desc}",
                )

                # 2. Execute Batch
                batch_items = []
                min_stars_in_batch = float('inf')

                for item in self.client.search_repositories(query, task, progress):
                    batch_items.append(item)
                    stars = item['stargazers_count']
                    min_stars_in_batch = min(min_stars_in_batch, stars)

                    if self.storage.save(item):
                        progress.advance(task)

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
                            min_stars_in_batch, task, progress,
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

    def _process_time_slice(self, stars: int, task_id: TaskID, progress: Progress):
        """Handles dense star counts by slicing via 'created' date."""
        start_dt = datetime.datetime(2008, 1, 1)
        end_dt = datetime.datetime.now()
        stack = [(start_dt, end_dt)]

        while stack:
            s, e = stack.pop()
            date_range = f"{s.strftime('%Y-%m-%d')}..{e.strftime('%Y-%m-%d')}"
            query = f"language:{self.lang} stars:{stars} created:{date_range}"

            progress.update(
                task_id, description=f"[bold magenta]Time Slice: {stars}★ [{date_range}]",
            )

            items = list(
                self.client.search_repositories(
                    query, task_id, progress,
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
    if language is None:
        console.print(
            '[bold yellow]No language specified. Crawling ALL languages...[/bold yellow]',
        )
        target_languages = list(Language)
    else:
        target_languages = [language]

    for lang in target_languages:
        console.rule(f'[bold cyan]GitHub SBOM Searcher: {lang}[/bold cyan]')

        # Determine output path for this language
        if output_path_arg is None:
            current_output = f"{lang}.jsonl"
        else:
            current_output = output_path_arg

        console.print(f"Language : [bold]{lang}[/bold]")
        console.print(f"Min Stars: [bold]{min_stars}[/bold]")
        console.print(f"Output   : [bold]{current_output}[/bold]")
        console.rule()

        try:
            searcher = Searcher(token, lang, min_stars, current_output)
            searcher.run()
        except KeyboardInterrupt:
            console.print('\n[bold yellow]Aborted by user.[/bold yellow]')
            raise typer.Exit(1)
        except Exception as e:
            console.print_exception()
            logger.critical(f"Fatal Error processing {lang}: {e}")
            continue


if __name__ == '__main__':
    typer.run(main)
