import structlog
import typer
from rich.progress import BarColumn
from rich.progress import MofNCompleteColumn
from rich.progress import Progress
from rich.progress import SpinnerColumn
from rich.progress import TaskProgressColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn
from rich.progress import TimeRemainingColumn

from chatsbom.core.clickhouse import check_clickhouse_connection
from chatsbom.core.container import get_container
from chatsbom.core.logging import console
from chatsbom.models.language import Language
from chatsbom.services.db_service import DbStats

logger = structlog.get_logger('db_command')
app = typer.Typer(help='Database operations')


@app.command()
def index(
    language: Language | None = typer.Option(None, help='Target Language'),
    limit: int | None = typer.Option(None, help='Limit number of items'),
):
    """
    Ingest SBOM and repository data into ClickHouse.
    Reads from: data/06-sbom
    """

    container = get_container()
    config = container.config

    # Check Connection (Admin)
    db_config = config.get_db_config('admin')
    check_clickhouse_connection(
        host=db_config.host,
        port=db_config.port,
        user=db_config.user,
        password=db_config.password,
        database=db_config.database,
        console=console,
        require_database=False,
    )

    service = container.get_db_service()

    # Initialize Repo (ensures tables exist)
    repo_db = container.get_ingestion_repository()
    repo_db.ensure_schema()

    target_languages = [language] if language else list(Language)

    total_stats = DbStats()

    for lang in target_languages:
        lang_str = str(lang)
        input_path = config.paths.get_sbom_list_path(lang_str)

        if not input_path.exists():
            logger.warning(
                f"No SBOM data found for {lang_str}", path=str(input_path),
            )
            continue

        # Count total lines for progress bar
        with open(input_path, encoding='utf-8') as f:
            total_repos = sum(1 for line in f if line.strip())

        with Progress(
            SpinnerColumn(),
            TextColumn('[progress.description]{task.description}'),
            BarColumn(),
            TaskProgressColumn(),
            MofNCompleteColumn(),
            TextColumn('•'),
            TimeElapsedColumn(),
            TextColumn('•'),
            TimeRemainingColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(
                f"Indexing {lang_str}...", total=total_repos,
            )

            stats = service.ingest_from_list(
                input_path,
                repo_db,
                progress_callback=lambda: progress.advance(task),
            )

            total_stats.repos += stats.repos
            total_stats.artifacts += stats.artifacts
            total_stats.releases += stats.releases
            total_stats.failed += stats.failed
            total_stats.skipped += stats.skipped

    logger.info(
        'Indexing Complete',
        repos=total_stats.repos,
        artifacts=total_stats.artifacts,
        releases=total_stats.releases,
        failed=total_stats.failed,
        skipped=total_stats.skipped,
    )


@app.command()
def status():
    """Show database statistics."""

    container = get_container()
    config = container.config

    # Check Connection (Guest)
    db_config = config.get_db_config('guest')
    check_clickhouse_connection(
        host=db_config.host,
        port=db_config.port,
        user=db_config.user,
        password=db_config.password,
        database=db_config.database,
        console=console,
        require_database=True,
    )

    query_repo = container.get_query_repository()

    try:
        stats = query_repo.get_stats()
        from rich.table import Table
        table = Table(title='Database Statistics')
        table.add_column('Metric', style='cyan')
        table.add_column('Value', style='magenta')

        for k, v in stats.items():
            table.add_row(k.replace('_', ' ').title(), str(v))

        console.print(table)
    except Exception as e:
        console.print(f"[red]Error fetching status: {e}[/red]")


@app.command()
def query(
    component: str = typer.Argument(..., help='Component name to search for'),
    limit: int = typer.Option(10, help='Max results'),
    language: str = typer.Option(None, help='Filter by repository language'),
):
    """Query dependencies across repositories."""

    container = get_container()
    config = container.config

    # Check Connection (Guest)
    db_config = config.get_db_config('guest')
    check_clickhouse_connection(
        host=db_config.host,
        port=db_config.port,
        user=db_config.user,
        password=db_config.password,
        database=db_config.database,
        console=console,
        require_database=True,
    )

    query_repo = container.get_query_repository()

    try:
        # Step 1: Search for library candidates
        candidate_limit = max(limit, 20)  # At least 20 candidates
        candidates = query_repo.search_library_candidates(
            component, language=language, limit=candidate_limit,
        )
        if not candidates:
            console.print(
                f"[yellow]No libraries found matching '{component}'[/yellow]",
            )
            return

        # Display candidates
        from rich.table import Table
        cand_table = Table(title=f"Library Candidates: {component}")
        cand_table.add_column('#', style='dim')
        cand_table.add_column('Library Name', style='cyan')
        cand_table.add_column('Repository Count', style='magenta')

        for idx, (name, count) in enumerate(candidates, start=1):
            cand_table.add_row(str(idx), name, str(count))

        console.print(cand_table)

        # Prompt for selection
        console.print()
        choice = typer.prompt(
            'Select a library number (or 0 to cancel)',
            default='0',
            show_default=False,
        )

        try:
            choice_idx = int(choice)
        except ValueError:
            console.print('[red]Invalid input, exiting.[/red]')
            return

        if choice_idx < 1 or choice_idx > len(candidates):
            console.print('[yellow]No selection made, exiting.[/yellow]')
            return

        selected_name = candidates[choice_idx - 1][0]

        # Step 2: Get detailed dependents for selected library
        results = query_repo.get_dependents(
            selected_name, language=language, limit=limit,
        )
        if not results:
            console.print(
                f"[yellow]No dependents found for '{selected_name}'[/yellow]",
            )
            return

        result_table = Table(title=f"Dependents of {selected_name}")

        result_table.add_column('Owner', style='green')
        result_table.add_column('Repo', style='green')
        result_table.add_column('Stars', style='yellow')
        result_table.add_column('Version', style='cyan')
        result_table.add_column('URL', style='dim')

        for owner, repo, stars, version, url in results:
            result_table.add_row(owner, repo, str(stars), version, url)

        console.print(result_table)

    except Exception as e:
        console.print(f"[red]Error querying: {e}[/red]")


if __name__ == '__main__':
    app()
