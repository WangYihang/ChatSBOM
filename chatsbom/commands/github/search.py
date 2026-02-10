import structlog
import typer
from rich.console import Console  # Import Console here
from rich.progress import BarColumn
from rich.progress import MofNCompleteColumn
from rich.progress import Progress
from rich.progress import SpinnerColumn
from rich.progress import Table
from rich.progress import TaskProgressColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn
from rich.progress import TimeRemainingColumn

from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.core.github import check_github_token
from chatsbom.models.language import Language
from chatsbom.services.search_service import SearchStats

# Re-declare logger as it's used globally below
logger = structlog.get_logger('search_command')
app = typer.Typer()


def print_summary(stats: SearchStats):
    console = Console()  # Define console locally
    table = Table(title='Search Summary')
    table.add_column('Metric', style='cyan')
    table.add_column('Value', style='magenta')
    table.add_row('Total API Requests', str(stats.api_requests))
    table.add_row('API Cache Hits', str(stats.cache_hits))
    table.add_row('New Repos Saved', str(stats.repos_saved))
    table.add_row('Total Duration', f"{stats.elapsed_time:.2f}s")
    console.print(table)


@app.callback(invoke_without_command=True)
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
    limit: int | None = typer.Option(None, help='Limit number of items'),
    force: bool = typer.Option(
        False, help='Force refresh, ignoring cache (where applicable)',
    ),
):
    """
    Search for repositories on GitHub.
    """
    check_github_token(token)
    container = get_container()
    config = container.config

    # Defaults
    if min_stars is None:
        min_stars = config.github.default_min_stars

    if language is None:
        logger.warning('No language specified. Searching ALL languages...')
        target_languages = list(Language)
    else:
        target_languages = [language]

    for lang in target_languages:
        # Determine output path
        if output_path_arg:
            current_output = output_path_arg
        else:
            current_output = str(
                config.paths.get_search_list_path(str(lang)),
            )

        logger.info(
            'Starting Search', language=str(lang),
            min_stars=min_stars, output=current_output,
        )

        # Factory create service via container
        searcher = container.create_search_service(
            str(lang), min_stars, current_output, token, limit, force,
        )

        with Progress(
            SpinnerColumn(),
            TextColumn('[bold blue]{task.description}'),
            BarColumn(),
            TaskProgressColumn(),
            MofNCompleteColumn(),
            TextColumn('•'),
            TextColumn('[bold yellow]{task.fields[status]}'),
            TextColumn('•'),
            TextColumn('[cyan]Values: {task.fields[stars]}'),
            TextColumn('•'),
            TimeElapsedColumn(),
            TextColumn('•'),
            TimeRemainingColumn(),
            console=Console(),  # Use a local Console instance for Progress
        ) as progress:
            task = progress.add_task(
                '[green]Searching...', total=None, status='Init', stars='N/A',
            )
            stats = searcher.run(progress, task)
            print_summary(stats)
