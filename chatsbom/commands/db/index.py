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
from chatsbom.core.schema import ARTIFACTS
from chatsbom.models.language import Language
from chatsbom.services.db_service import DbStats

logger = structlog.get_logger('db_index')
app = typer.Typer()


@app.callback(invoke_without_command=True)
def main(
    language: Language | None = typer.Option(None, help='Target Language'),
    limit: int | None = typer.Option(
        None, help='Only ingest the first N repositories per language',
    ),
    rebuild: bool = typer.Option(
        False,
        '--rebuild',
        help='Drop and recreate the artifacts table before ingesting',
    ),
):
    """
    Ingest SBOM and repository data into ClickHouse.

    Reads from data/07-sbom, preferring data/09-github-depgraph when it
    exists: that ledger carries the same repositories plus a
    `depgraph_path`, so both SBOM sources land in one pass.
    """

    if rebuild and language is not None:
        # --rebuild drops the whole table; --language narrows what is
        # re-ingested. Together they discard eight languages and refill
        # one, so the combination reads as narrow and acts as total.
        console.print(
            '[bold red]Error:[/] --rebuild cannot be combined with '
            '--language.\n\n'
            '--rebuild discards the artifacts table for [bold]every '
            'language[/bold], and --language would then re-ingest only '
            'one — leaving the rest empty.\n\n'
            '[green]To rebuild everything:[/] [cyan]chatsbom db index '
            '--rebuild[/]\n'
            '[green]To refresh one language:[/] [cyan]chatsbom db index '
            '--language java[/]',
        )
        raise typer.Exit(1)

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

    if rebuild:
        # Discarded as part of ensure_schema, not after it: the engine
        # check would otherwise abort before the rebuild could run.
        console.print(
            '[yellow]Rebuilding the artifacts table[/] '
            '(discarding rows from older schemas)',
        )
    repo_db.ensure_schema(rebuild={ARTIFACTS.name} if rebuild else None)

    target_languages = [language] if language else list(Language)

    total_stats = DbStats()

    for lang in target_languages:
        lang_str = str(lang)
        # The SBOM ledger is the complete list of repositories; the
        # depgraph ledger only says which of them have a stored graph.
        # Treating the latter as the input list once cut Java from 1,215
        # repositories to 87, because `--limit` had truncated it.
        input_path = config.paths.get_sbom_list_path(lang_str)
        depgraph_index = config.paths.get_depgraph_list_path(lang_str)

        if not input_path.exists():
            logger.warning(
                f"No SBOM data found for {lang_str}", path=str(input_path),
            )
            continue

        logger.info(
            'Indexing from',
            language=lang_str,
            ledger=str(input_path),
            depgraphs=str(depgraph_index) if depgraph_index.exists() else None,
        )

        # Count total lines for progress bar
        with open(input_path, encoding='utf-8') as f:
            total_repos = sum(1 for line in f if line.strip())
        if limit is not None:
            total_repos = min(total_repos, limit)

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
                limit=limit,
                depgraph_index=depgraph_index,
            )

            total_stats.repos += stats.repos
            total_stats.artifacts += stats.artifacts
            total_stats.releases += stats.releases
            total_stats.failed += stats.failed
            total_stats.skipped += stats.skipped

    # Collapse superseded ReplacingMergeTree rows so reads need no FINAL
    # on the large tables.
    console.print('[dim]Optimizing tables...[/dim]')
    repo_db.optimize()

    logger.info(
        'Indexing Complete',
        repos=total_stats.repos,
        artifacts=total_stats.artifacts,
        releases=total_stats.releases,
        failed=total_stats.failed,
        skipped=total_stats.skipped,
    )
