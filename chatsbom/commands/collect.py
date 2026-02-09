import structlog
import typer
from rich.console import Console
from rich.progress import Progress
from rich.progress import SpinnerColumn
from rich.progress import Table
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn

from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.models.language import Language
from chatsbom.services.collector_service import SearchStats

logger = structlog.get_logger('collect_command')
console = Console()


def print_summary(stats: SearchStats):
    import time
    elapsed_time = time.time() - stats.start_time
    table = Table(title='Search Summary')
    table.add_column('Metric', style='cyan')
    table.add_column('Value', style='magenta')
    table.add_row('Total API Requests', str(stats.api_requests))
    table.add_row('API Cache Hits', str(stats.cache_hits))
    table.add_row('New Repos Saved', str(stats.repos_saved))
    table.add_row('Total Duration', f"{elapsed_time:.2f}s")
    console.print(table)


@handle_errors
def main(
    token: str = typer.Option(
        None, envvar='GITHUB_TOKEN', help='GitHub Token',
    ),
    language: Language | None = typer.Option(
        None, help='Target Programming Language (default: all)',
    ),
    min_stars: int = typer.Option(None, help='Minimum Star Count'),
    output_path_arg: str | None = typer.Option(
        None, '--output', help='Output JSONL Path',
    ),
):
    """
    Collect repository links from GitHub.
    """
    container = get_container()
    config = container.config

    # Defaults
    if min_stars is None:
        min_stars = config.github.default_min_stars

    if language is None:
        logger.warning('No language specified. Crawling ALL languages...')
        target_languages = list(Language)
    else:
        target_languages = [language]

    for lang in target_languages:
        # Determine output path
        if output_path_arg:
            current_output = output_path_arg
        else:
            current_output = str(
                config.paths.get_repo_list_path(
                    str(lang), operation='collect',
                ),
            )

        logger.info(
            'Starting Search', language=str(lang),
            min_stars=min_stars, output=current_output,
        )

        # Factory create service via container
        collector = container.create_collector_service(
            str(lang), min_stars, current_output, token,
        )

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
                '[green]Crawling...', total=None, status='Init', stars='N/A',
            )
            stats = collector.run(progress, task)
            print_summary(stats)


if __name__ == '__main__':
    typer.run(main)
