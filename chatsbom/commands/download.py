import concurrent.futures
import os
import time

import dotenv
import structlog
import typer
from rich.console import Console
from rich.progress import BarColumn
from rich.progress import Progress
from rich.progress import SpinnerColumn
from rich.progress import TextColumn
from rich.table import Table

from chatsbom.core.config import get_config
from chatsbom.core.storage import load_jsonl
from chatsbom.models.language import Language
from chatsbom.services.downloader_service import DownloaderService

dotenv.load_dotenv()
console = Console()
logger = structlog.get_logger('download_command')


def main(
    input_file: str | None = typer.Option(None, help='Input JSONL file path'),
    output_dir: str = typer.Option(
        'data/sbom', help='Download destination directory',
    ),
    language: Language | None = typer.Option(
        None, help='Target Language (default: all)',
    ),
    token: str = typer.Option(
        None, envvar='GITHUB_TOKEN', help='GitHub Token',
    ),
    concurrency: int = typer.Option(32, help='Number of concurrent threads'),
    limit: int | None = typer.Option(
        None, help='Limit number of processed repos',
    ),
):
    """
    Download SBOM manifest files from GitHub repositories.
    """
    config = get_config()
    target_languages = [language] if language else list(Language)
    service = DownloaderService(
        token=token or config.github.token, base_dir=output_dir, pool_size=concurrency,
    )

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
            'repos': 0, 'downloaded': 0,
            'missing': 0, 'failed': 0, 'cache_hits': 0,
        }
        start_time = time.time()

        for lang in target_languages:
            target_file = input_file or str(
                config.paths.get_repo_list_path(str(lang), operation='enrich'),
            )
            if not os.path.exists(target_file):
                continue

            repos = load_jsonl(target_file)
            if not repos:
                continue
            if limit:
                repos = repos[:limit]

            task_id = progress.add_task(
                f'Downloading {lang}...', total=len(repos), status='Starting...',
            )

            with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
                futures = {
                    executor.submit(
                        service.download_repository_assets, repo, lang,
                    ): repo for repo in repos
                }
                for future in concurrent.futures.as_completed(futures):
                    try:
                        res = future.result()
                        overall_stats['repos'] += 1
                        overall_stats['downloaded'] += res.downloaded_files
                        overall_stats['missing'] += res.missing_files
                        overall_stats['failed'] += res.failed_files
                        overall_stats['cache_hits'] += res.cache_hits
                        progress.update(
                            task_id, advance=1,
                            status=res.status_message,
                        )
                    except Exception as e:
                        logger.error(f"Error processing repository: {e}")
                        progress.update(
                            task_id, advance=1,
                            status='[red]Error[/red]',
                        )

    # Print Summary
    table = Table(title='Download Summary')
    table.add_column('Metric', style='cyan')
    table.add_column('Value', style='magenta')
    table.add_row('Total Repositories', str(overall_stats['repos']))
    table.add_row('Files Downloaded', str(overall_stats['downloaded']))
    table.add_row('Missing (404)', str(overall_stats['missing']))
    table.add_row('Failed', str(overall_stats['failed']))
    table.add_row('Cache Hits', str(overall_stats['cache_hits']))
    table.add_row('Total Duration', f"{time.time() - start_time:.2f}s")
    console.print(table)


if __name__ == '__main__':
    typer.run(main)
