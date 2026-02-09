import time
from pathlib import Path

import typer
from rich.console import Console
from rich.progress import BarColumn
from rich.progress import MofNCompleteColumn
from rich.progress import Progress
from rich.progress import SpinnerColumn
from rich.progress import TaskProgressColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn
from rich.table import Table

from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.models.language import Language

console = Console()


@handle_errors
def main(
    host: str = typer.Option(None, help='ClickHouse host'),
    port: int = typer.Option(None, help='ClickHouse http port'),
    user: str = typer.Option(None, help='ClickHouse user'),
    password: str = typer.Option(None, help='ClickHouse password'),
    database: str = typer.Option(None, help='ClickHouse database'),
    clean: bool = typer.Option(False, help='Drop tables before importing'),
    language: list[Language] | None = typer.Option(
        None, help='Specific languages to import',
    ),
    input_file: Path | None = typer.Option(
        None, help='Specific file to import',
    ),
):
    """Index SBOM data into the database (Admin)."""
    container = get_container()

    # 1. Update Config (SSOT via container config reference if needed, but Container handles creation)
    # Since we need to override the *Global* config for the container to produce the right repository:
    if host:
        container.config._db_base.host = host
    if port:
        container.config._db_base.port = port
    if user:
        # This might override base, but Admin role usually overrides this
        container.config._db_base.user = user
    if password:
        container.config._db_base.password = password
    if database:
        container.config._db_base.database = database

    # 2. Get Services via DI
    service = container.get_indexer_service()

    # 3. Get Write Repository
    # Note: CLI args override environment variables for connection, but role enforcement happens in Container/Config
    repo = container.get_ingestion_repository()

    # Pre-check connection using the repo's internal client
    # We don't need check_clickhouse_connection helper anymore if we trust the Repo to fail fast or we wrap it
    # But for UX, let's keep a quick check or just try/except the ensure_schema

    with repo:
        if clean:
            repo.reset_schema()
        else:
            repo.ensure_schema()

        # 4. File Selection
        files_to_process = []
        if input_file:
            files_to_process = [input_file] if input_file.exists() else []
        else:
            target_langs = language if language else list(Language)
            for lang in target_langs:
                f = container.config.paths.get_repo_list_path(
                    lang.value, operation='enrich',
                )
                if f.exists():
                    files_to_process.append(f)

        # 5. Processing
        overall_stats = {'repos': 0, 'artifacts': 0, 'start_time': time.time()}

        with Progress(
            SpinnerColumn(), TextColumn(
                '[bold blue]{task.description}',
            ), BarColumn(),
            TaskProgressColumn(), MofNCompleteColumn(), TimeElapsedColumn(),
            console=console,
        ) as progress:

            for f in files_to_process:
                total_lines = sum(1 for _ in open(f))
                task_id = progress.add_task(
                    f"Importing {f.stem}", total=total_lines,
                )

                def progress_callback():
                    progress.advance(task_id)

                # Passing the repository *instance* to the service?
                # Ideally the service *has* the repository, but IndexerService is currently file-centric logic.
                # Let's keep passing it for now or refactor IndexerService to own the repo.
                # Given IndexerService is file-parser, passing repo is fine for dependency injection method injection.
                file_stats = service.process_file(f, repo, progress_callback)
                overall_stats['repos'] += file_stats['repos']
                overall_stats['artifacts'] += file_stats['artifacts']

    # 6. Summary
    elapsed = time.time() - overall_stats['start_time']
    table = Table(title='Import Summary')
    table.add_column('Metric', style='cyan')
    table.add_column('Value', style='magenta')
    table.add_row('Files Processed', str(len(files_to_process)))
    table.add_row('Repositories', f"{overall_stats['repos']:,}")
    table.add_row('Artifacts', f"{overall_stats['artifacts']:,}")
    table.add_row('Total Duration', f"{elapsed:.2f}s")
    console.print(table)


if __name__ == '__main__':
    typer.run(main)
