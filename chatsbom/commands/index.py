import json
import time
from dataclasses import dataclass
from dataclasses import field
from datetime import datetime
from pathlib import Path
from typing import Any

import clickhouse_connect
import typer
from clickhouse_connect.driver.client import Client
from rich.console import Console
from rich.progress import BarColumn
from rich.progress import MofNCompleteColumn
from rich.progress import Progress
from rich.progress import SpinnerColumn
from rich.progress import TaskProgressColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn
from rich.table import Table

from chatsbom.models.language import Language

console = Console()

REPO_COLUMNS = [
    'id', 'owner', 'repo', 'full_name', 'url',
    'stars', 'description', 'created_at', 'language', 'topics',
]
ARTIFACT_COLUMNS = [
    'repository_id', 'artifact_id', 'name',
    'version', 'type', 'purl', 'found_by', 'licenses',
]
BATCH_SIZE = 1000


@dataclass
class ImportStats:
    files_processed: int = 0
    repos_processed: int = 0
    artifacts_imported: int = 0
    start_time: float = field(default_factory=time.time)


def get_client(host, port, user, password, database) -> Client:
    return clickhouse_connect.get_client(
        host=host, port=port, username=user, password=password, database=database,
    )


def init_db(client: Client):
    from sbom_insight.core.schema import ARTIFACTS_DDL
    from sbom_insight.core.schema import REPOSITORIES_DDL
    client.command(REPOSITORIES_DDL)
    client.command(ARTIFACTS_DDL)


def parse_iso_time(time_str: str | None) -> datetime:
    if not time_str:
        return datetime(1970, 1, 1)
    try:
        # Handle "2014-06-03T23:37:33Z" format
        return datetime.fromisoformat(time_str.replace('Z', '+00:00'))
    except Exception:
        return datetime(1970, 1, 1)


def parse_repo_line(line: str, language: str) -> tuple[list[Any] | None, dict[str, Any] | None]:
    """Parse a single line from jsonl and return a tuple of (repo_row, metadata_dict)."""
    try:
        repo_meta = json.loads(line)
    except json.JSONDecodeError:
        return None, None

    full_name = repo_meta.get('full_name', '')
    if not full_name:
        return None, None

    parts = full_name.split('/')
    if len(parts) == 2:
        owner, repo = parts
    else:
        owner, repo = '', full_name

    repo_id = int(repo_meta.get('id', 0))
    created_at = parse_iso_time(repo_meta.get('created_at'))

    repo_row = [
        repo_id,
        owner,
        repo,
        full_name,
        repo_meta.get('url', ''),
        int(repo_meta.get('stars', 0)),
        repo_meta.get('description', '') or '',
        created_at,
        language,
        repo_meta.get('topics', []) or [],
    ]

    # Pass essential metadata for artifact scanning
    meta_context = {
        'id': repo_id,
        'owner': owner,
        'repo': repo,
        'language': language,
    }

    return repo_row, meta_context


def scan_artifacts(meta_context: dict[str, Any]) -> list[list[Any]]:
    """Scan for SBOM files based on repository metadata and return a list of artifact rows."""
    if not meta_context:
        return []

    language = meta_context['language']
    owner = meta_context['owner']
    repo = meta_context['repo']
    repo_id = meta_context['id']

    # Expected path: data/{language}/{owner}/{repo}/**/sbom.json
    base_dir = Path('data') / language / owner / repo
    if not base_dir.exists():
        return []

    artifacts_rows = []
    # Recursively find sbom.json
    sbom_files = list(base_dir.rglob('sbom.json'))

    for sbom_file in sbom_files:
        try:
            with open(sbom_file) as sf:
                sbom_data = json.load(sf)
                artifacts = sbom_data.get('artifacts', [])
                for art in artifacts:
                    artifact_row = [
                        repo_id,
                        art.get('id', ''),
                        art.get('name', ''),
                        art.get('version', ''),
                        art.get('type', ''),
                        art.get('purl', ''),
                        art.get('foundBy', ''),
                        [
                            lic.get('value', '') or lic.get(
                                'spdxExpression', '',
                            )
                            for lic in art.get('licenses', [])
                        ],
                    ]
                    artifacts_rows.append(artifact_row)
        except Exception:
            # Optionally log error
            pass

    return artifacts_rows


def import_file(client: Client, file_path: str, progress: Progress, stats: ImportStats):
    """Import a single jsonl file into ClickHouse."""
    language = Path(file_path).stem

    # Count lines first for progress bar
    try:
        with open(file_path) as f:
            total_lines = sum(1 for _ in f)
    except FileNotFoundError:
        console.print(f"[red]File {file_path} not found.[/red]")
        return

    task_id = progress.add_task(
        f"[cyan]Importing {language}[/cyan]", total=total_lines,
    )
    stats.files_processed += 1

    with open(file_path) as f:
        # Don't read all lines into memory if possible, but for JSONL iterating file object is fine
        # However, to be safe with progress tracking we iterate file object
        # Re-open or seek 0? Seek 0.
        f.seek(0)

        repo_batch = []
        artifact_batch = []

        for line in f:
            repo_row, meta_context = parse_repo_line(line, language)
            if not repo_row or not meta_context:
                progress.advance(task_id)
                continue

            repo_batch.append(repo_row)
            stats.repos_processed += 1

            # Scan artifacts
            artifacts = scan_artifacts(meta_context)
            if artifacts:
                artifact_batch.extend(artifacts)
                stats.artifacts_imported += len(artifacts)

            # Flush batches
            if len(repo_batch) >= BATCH_SIZE:
                client.insert(
                    'repositories', repo_batch,
                    column_names=REPO_COLUMNS,
                )
                repo_batch = []

            if len(artifact_batch) >= BATCH_SIZE:
                client.insert(
                    'artifacts', artifact_batch,
                    column_names=ARTIFACT_COLUMNS,
                )
                artifact_batch = []

            progress.advance(task_id)

        # Flush remaining
        if repo_batch:
            client.insert(
                'repositories', repo_batch,
                column_names=REPO_COLUMNS,
            )
        if artifact_batch:
            client.insert(
                'artifacts', artifact_batch,
                column_names=ARTIFACT_COLUMNS,
            )


def main(
    host: str = typer.Option('localhost', help='ClickHouse host'),
    port: int = typer.Option(8123, help='ClickHouse http port'),
    user: str = typer.Option('admin', help='ClickHouse user'),
    password: str = typer.Option('admin', help='ClickHouse password'),
    database: str = typer.Option('sbom', help='ClickHouse database'),
    clean: bool = typer.Option(False, help='Drop tables before importing'),
    language: list[Language] | None = typer.Option(
        None, help='Specific languages to import',
    ),
    input_file: Path | None = typer.Option(
        None, help='Specific file to import (ignoring language argument)',
    ),
):
    """Index SBOM data into the database."""

    # Ensure database exists
    try:
        # Connect to default database first
        tmp_client = clickhouse_connect.get_client(
            host=host, port=port, username=user, password=password, database='default',
        )
        tmp_client.command(f"CREATE DATABASE IF NOT EXISTS {database}")
    except Exception as e:
        console.print(
            f'[bold red]Error:[/] Failed to connect to ClickHouse at '
            f'[cyan]{host}:{port}[/]\n\n'
            f'Details: {e}\n\n'
            'Please ensure:\n'
            '  1. ClickHouse is running: [cyan]docker compose up -d[/]\n'
            '  2. Host and port are correct\n'
            '  3. User credentials are valid',
        )
        raise typer.Exit(1)

    try:
        client = get_client(host, port, user, password, database)
    except Exception as e:
        console.print(f"[red]Failed to connect to ClickHouse: {e}[/red]")
        raise typer.Exit(code=1)

    if clean:
        console.print('[yellow]Dropping existing tables...[/yellow]')
        client.command('DROP TABLE IF EXISTS repositories')
        client.command('DROP TABLE IF EXISTS artifacts')

    init_db(client)
    console.print('[green]Database initialized.[/green]')

    files_to_process = []
    if input_file:
        if input_file.exists():
            files_to_process.append(input_file)
        else:
            console.print(
                f"[red]Input file {input_file} does not exist.[/red]",
            )
            raise typer.Exit(code=1)
    else:
        langs_to_process = language if language else list(Language)
        for lang in langs_to_process:
            f = Path(f"{lang.value}.jsonl")
            if f.exists():
                files_to_process.append(f)
            else:
                if language:
                    console.print(
                        f"[yellow]File {f} for language {lang.value} not found.[/yellow]",
                    )

    stats = ImportStats()

    with Progress(
        SpinnerColumn(),
        TextColumn('[bold blue]{task.description}'),
        BarColumn(),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TimeElapsedColumn(),
        console=console,
        transient=False,
    ) as progress:
        for f in files_to_process:
            import_file(client, str(f), progress, stats)

    # Summary Table
    elapsed_time = time.time() - stats.start_time
    table = Table(title='Import Summary')
    table.add_column('Metric', style='cyan')
    table.add_column('Value', style='magenta')

    table.add_row('Files Processed', str(stats.files_processed))
    table.add_row('Repositories Processed', f"{stats.repos_processed:,}")
    table.add_row('Artifacts Imported', f"{stats.artifacts_imported:,}")
    table.add_row('Total Duration', f"{elapsed_time:.2f}s")

    console.print(table)
