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

logger = structlog.get_logger('db_index')
app = typer.Typer()


@app.callback(invoke_without_command=True)
def main(
    language: Language | None = typer.Option(None, help='Target Language'),
    limit: int | None = typer.Option(None, help='Limit number of items'),
):
    """
    Ingest SBOM and repository data into ClickHouse.
    Reads from: data/07-sbom
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
