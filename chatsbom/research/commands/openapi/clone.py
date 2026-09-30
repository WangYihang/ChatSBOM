import csv
from collections import defaultdict
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor

import structlog
import typer
from rich.markup import escape
from rich.progress import BarColumn
from rich.progress import MofNCompleteColumn
from rich.progress import SpinnerColumn
from rich.progress import TaskProgressColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn
from rich.progress import TimeRemainingColumn

from chatsbom.core.config import get_config
from chatsbom.core.diagnostics import fail
from chatsbom.core.logging import console
from chatsbom.core.logging import progress_bar
from chatsbom.research.services.openapi_service import OpenApiService

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
    config = get_config()
    dest = config.paths.framework_repos_dir
    service = OpenApiService()

    try:
        with open(input_csv, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except FileNotFoundError:
        # On stderr, where the logs go: stdout is for what the command
        # reports, and this was printed there (#124).
        fail(
            f'[bold red]CSV file not found: {escape(str(input_csv))}[/bold red]',
            'CSV not found', logger, path=str(input_csv),
        )

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
        f'Cloning [cyan]{len(repos_to_clone)}[/cyan] repositories into [bold]{escape(str(dest))}[/bold]...',
    )

    cloned, failed = 0, 0
    with progress_bar(
        SpinnerColumn(),
        TextColumn('[bold blue]{task.fields[current]}'),
        BarColumn(bar_width=None),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TextColumn('•'),
        TimeElapsedColumn(),
        TextColumn('•'),
        TimeRemainingColumn(),
        expand=True,
    ) as progress:
        task = progress.add_task(
            'Cloning...', total=len(
                repos_to_clone,
            ), current='Initializing...',
        )
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    service.clone_repo, row['owner'], row['repo'], dest,
                    row.get('latest_release'), row.get('commit_sha'),
                ): row for row in repos_to_clone
            }
            for future in as_completed(futures):
                row = futures[future]
                owner, repo, success, message, stats = future.result()
                tag = row.get('latest_release') or 'HEAD'

                if success:
                    cloned += 1
                    status_text = f"[green]✔ {owner}/{repo}[/green] [dim]({stats['shadow_size']})[/dim]"
                    logger.info(
                        'Cloned', owner=owner, repo=repo, tag=tag,
                        status=message,
                        duration=f"{stats['duration']}s",
                        shadow=stats['shadow_size'],
                        global_cache=stats['global_size'],
                        saved=stats['saved'],
                    )
                else:
                    failed += 1
                    status_text = f"[red]✘ {owner}/{repo}[/red]"
                    logger.error(
                        'Clone failed', owner=owner,
                        repo=repo, tag=tag, error=message,
                    )

                progress.update(task, advance=1, current=status_text)

    console.print(
        f'[bold green]Done![/bold green] Cloned: [cyan]{cloned}[/cyan], Failed: [red]{failed}[/red]',
    )
