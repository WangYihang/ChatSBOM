import csv
from collections import defaultdict
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor

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

from chatsbom.core.container import get_container
from chatsbom.core.logging import console
from chatsbom.services.openapi_service import OpenApiService

logger = structlog.get_logger('openapi_clone')
app = typer.Typer()


@app.callback(invoke_without_command=True)
def main(
    input_csv: str = typer.Option(
        'openapi_candidates.csv', '--input', help='Input CSV file from candidates command',
    ),
    force: bool = typer.Option(
        False, help='Re-clone even if directory exists',
    ),
    workers: int = typer.Option(4, help='Number of concurrent clone workers'),
    top: int = typer.Option(
        0, help='Limit to top N projects per framework (by stars). 0 means no limit.',
    ),
):
    """
    Clone repositories listed in the candidates CSV.
    """
    container = get_container()
    config = container.config
    dest = config.paths.framework_repos_dir
    service = OpenApiService()

    try:
        with open(input_csv, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except FileNotFoundError:
        console.print(f'[bold red]CSV file not found: {input_csv}[/bold red]')
        raise typer.Exit(1)

    if top > 0:
        framework_groups = defaultdict(list)
        for row in rows:
            framework_groups[row.get('framework', '')].append(row)
        filtered_rows = []
        for framework, group in framework_groups.items():
            group.sort(key=lambda r: int(r.get('stars', 0) or 0), reverse=True)
            filtered_rows.extend(group[:top])
        rows = filtered_rows

    seen = set()
    repos_to_clone = []
    for row in rows:
        key = (row['owner'], row['repo'])
        if key in seen:
            continue
        seen.add(key)
        ver = service.get_version_path(
            row.get('latest_release'), row.get('commit_sha'),
        )
        if not force and (dest / row['owner'] / row['repo'] / ver).exists():
            continue
        repos_to_clone.append(row)

    if not repos_to_clone:
        console.print(
            '[yellow]All repositories already cloned. Use --force to re-clone.[/yellow]',
        )
        return

    console.print(
        f'Cloning [cyan]{len(repos_to_clone)}[/cyan] repositories into [bold]{dest}[/bold]...',
    )

    cloned, failed = 0, 0
    with Progress(SpinnerColumn(), TextColumn('[progress.description]{task.description}'), BarColumn(), TaskProgressColumn(), MofNCompleteColumn(), TextColumn('•'), TimeElapsedColumn(), TextColumn('•'), TimeRemainingColumn(), console=console) as progress:
        task = progress.add_task('Cloning repos...', total=len(repos_to_clone))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    service.clone_repo, row['owner'], row['repo'], dest, row.get(
                        'latest_release',
                    ), row.get('commit_sha'),
                ): row for row in repos_to_clone
            }
            for future in as_completed(futures):
                row = futures[future]
                owner, repo, success, message, size = future.result()
                if success:
                    cloned += 1
                    logger.info(
                        'Cloned', owner=owner, repo=repo,
                        status=message, size=size,
                    )
                else:
                    failed += 1
                    logger.error(
                        'Clone failed', owner=owner,
                        repo=repo, error=message,
                    )
                progress.advance(task)

    console.print(
        f'[bold green]Done![/bold green] Cloned: [cyan]{cloned}[/cyan], Failed: [red]{failed}[/red]',
    )
