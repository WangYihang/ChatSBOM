import concurrent.futures
import json
import os
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import dotenv
import requests
import structlog
import typer
from rich.console import Console
from rich.progress import BarColumn
from rich.progress import Progress
from rich.progress import SpinnerColumn
from rich.progress import TextColumn
from rich.table import Table

from chatsbom.core.client import get_http_client
from chatsbom.models.language import Language
from chatsbom.models.language import LanguageFactory

dotenv.load_dotenv()
console = Console()
logger = structlog.get_logger('downloader')


@dataclass
class DownloadResult:
    repo: str
    status_msg: str
    downloaded_files: int = 0
    missing_files: int = 0
    failed_files: int = 0
    skipped_files: int = 0
    cache_hits: int = 0


class SBOMDownloader:
    """Handles concurrent downloading of SBOM files from GitHub."""

    def __init__(self, token: str | None, base_dir: str, timeout: int = 10, pool_size: int = 50):
        self.session = get_http_client(pool_size=pool_size)

        if token:
            self.session.headers.update({'Authorization': f"Bearer {token}"})

        self.base_dir = Path(base_dir)
        self.timeout = timeout

    def download_repo(self, repo: dict, lang: Language) -> DownloadResult:
        """Downloads SBOM files for a single repository."""
        full_name = repo['full_name']
        owner, name = full_name.split('/')
        branch = repo.get('default_branch', 'master')

        target_dir = self.base_dir / lang / owner / name / branch
        target_dir.mkdir(parents=True, exist_ok=True)

        base_url = f"https://raw.githubusercontent.com/{owner}/{name}/{branch}"
        language_handler = LanguageFactory.get_handler(lang)
        targets: list[str] = language_handler.get_sbom_paths()

        result_msgs = []
        stats = DownloadResult(repo=full_name, status_msg='')

        for filename in targets:
            file_path = target_dir / filename

            try:
                start_time = time.time()
                url = f"{base_url}/{filename}"
                resp = self.session.get(url, timeout=self.timeout)
                elapsed = time.time() - start_time

                # Check for cache hit (requests-cache adds 'from_cache' attribute)
                if getattr(resp, 'from_cache', False):
                    stats.cache_hits += 1

                if resp.status_code == 200:
                    file_path.parent.mkdir(parents=True, exist_ok=True)
                    with open(file_path, 'wb') as f:
                        f.write(resp.content)

                    # Visual Caching Indicator
                    is_cached = getattr(resp, 'from_cache', False)
                    if is_cached:
                        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                        log_msg = (
                            f"{timestamp} \\[info     ] Downloaded                     "
                            f"elapsed={elapsed:.2f}s file={filename} "
                            f"repo={full_name} size={len(resp.content)} "
                            f"url={resp.url} [green](Cached)[/green]"
                        )
                        console.print(f"[dim]{log_msg}[/dim]")
                    else:
                        logger.info(
                            'Downloaded',
                            repo=full_name,
                            file=filename,
                            size=len(resp.content),
                            elapsed=f"{elapsed:.2f}s",
                            url=resp.url,
                        )

                    stats.downloaded_files += 1
                    result_msgs.append(f"[green]{filename}[/green]")
                elif resp.status_code == 404:
                    stats.missing_files += 1
                    result_msgs.append(
                        f"[dim yellow]no {filename}[/dim yellow]",
                    )
                else:
                    stats.failed_files += 1
                    logger.warning(
                        'Download Failed',
                        repo=full_name,
                        file=filename,
                        status=resp.status_code,
                        elapsed=f"{elapsed:.2f}s",
                        url=url,
                    )

                    result_msgs.append(
                        f"[red]{filename} {resp.status_code}[/red]",
                    )

            except requests.RequestException as e:
                logger.error(
                    'Download Error',
                    repo=full_name,
                    file=filename,
                    error=str(e),
                )
                stats.failed_files += 1
                result_msgs.append(f"[red]{filename} Err[/red]")

        if not result_msgs:
            stats.status_msg = f"[dim]{full_name} skip[/dim]"
        else:
            stats.status_msg = f"{full_name}: {', '.join(result_msgs)}"

        return stats


def load_targets(jsonl_path: str) -> list[dict]:
    """Loads repository targets from a JSONL file."""
    targets: list[dict] = []
    path = Path(jsonl_path)
    if not path.exists():
        return targets

    with path.open(encoding='utf-8') as f:
        for line in f:
            if line.strip():
                try:
                    targets.append(json.loads(line))
                except json.JSONDecodeError:
                    pass
    return targets


def main(
    input_file: str | None = typer.Option(
        None, help='Input JSONL file path (default: {language}.jsonl)',
    ),
    output_dir: str = typer.Option(
        'data', help='Download destination directory',
    ),
    language: Language | None = typer.Option(
        None, help='Target Language (default: all)',
    ),
    token: str = typer.Option(
        None, envvar='GITHUB_TOKEN', help='GitHub Token',
    ),
    concurrency: int = typer.Option(32, help='Number of concurrent threads'),
    limit: int | None = typer.Option(
        None, help='Limit number of processed repos (for testing)',
    ),
):
    """
    Download SBOM files from repositories.
    """
    if language is None:
        if input_file:
            logger.error(
                'Cannot specify input_file when targeting ALL languages.',
            )
            raise typer.Exit(1)
        logger.warning('No language specified. Downloading ALL languages...')
        target_languages = list(Language)
    else:
        target_languages = [language]

    downloader = SBOMDownloader(token, output_dir, pool_size=concurrency)

    with Progress(
        SpinnerColumn(),
        TextColumn('[bold blue]{task.description}'),
        BarColumn(),
        TextColumn('[progress.percentage]{task.percentage:>3.0f}%'),
        TextColumn('•'),
        TextColumn('[green]{task.completed}/{task.total}'),
        TextColumn('•'),
        TextColumn('[dim]{task.fields[status]}', justify='left'),
        console=console,
    ) as progress:

        overall_stats = {
            'repos': 0,
            'downloaded': 0,
            'missing': 0,
            'failed': 0,
            'cache_hits': 0,
        }
        start_time_all = time.time()

        for lang in target_languages:
            if input_file:
                target_file = input_file
            else:
                target_file = f"{lang}.jsonl"

            # Check if file exists, if not, skip efficiently
            if not os.path.exists(target_file):
                logger.debug(
                    f"Target file {target_file} not found. Skipping {lang}.",
                )
                continue

            tasks = load_targets(target_file)
            if not tasks:
                logger.warning(f"Input file empty: {target_file}. Skipping.")
                continue

            if limit:
                tasks = tasks[:limit]

            logger.info(
                'Starting Processing',
                language=str(lang),
                target_file=target_file,
                total_tasks=len(tasks),
            )

            main_task = progress.add_task(
                f'Downloading {lang}...', total=len(tasks), status='Starting...',
            )

            with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
                future_to_repo = {
                    executor.submit(downloader.download_repo, repo, lang): repo
                    for repo in tasks
                }

                for future in concurrent.futures.as_completed(future_to_repo):
                    try:
                        result = future.result()
                        overall_stats['repos'] += 1
                        overall_stats['downloaded'] += result.downloaded_files
                        overall_stats['missing'] += result.missing_files
                        overall_stats['failed'] += result.failed_files
                        overall_stats['cache_hits'] += result.cache_hits
                        progress.update(
                            main_task, advance=1,
                            status=result.status_msg,
                        )
                    except Exception as e:
                        logger.error(f"Error processing repo: {e}")
                        progress.update(
                            main_task, advance=1,
                            status='[red]Error[/red]',
                        )

    # Print Summary Table
    total_time = time.time() - start_time_all
    table = Table(title='Download Summary')
    table.add_column('Metric', style='cyan')
    table.add_column('Value', style='magenta')

    table.add_row('Total Repos Processed', str(overall_stats['repos']))
    table.add_row('Files Downloaded', str(overall_stats['downloaded']))
    table.add_row('Files Missing (404)', str(overall_stats['missing']))
    table.add_row('Failed Downloads', str(overall_stats['failed']))
    table.add_row('Cache Hits', str(overall_stats['cache_hits']))
    table.add_row('Total Duration', f"{total_time:.2f}s")

    console.print(table)


if __name__ == '__main__':
    typer.run(main)
