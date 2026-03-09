import json
from pathlib import Path

import structlog
import typer
from rich.progress import BarColumn
from rich.progress import Progress
from rich.progress import SpinnerColumn
from rich.progress import TaskProgressColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn
from rich.progress import TimeRemainingColumn

from chatsbom.core.config import get_config
from chatsbom.core.logging import console
from chatsbom.models.repository import Repository
from chatsbom.services.github_service import GitHubService

logger = structlog.get_logger('readme_command')
app = typer.Typer(
    help='Batch download README files for GitHub repositories.',
)


def run_download(
    repos: list[Repository],
    github_service: GitHubService,
):
    """Run batch download with real-time progress."""
    downloaded_count = 0
    skipped_count = 0
    error_count = 0

    with Progress(
        SpinnerColumn(),
        TextColumn('[progress.description]{task.description}'),
        BarColumn(bar_width=40),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task(
            '[green]Downloading READMEs...', total=len(repos),
        )

        for repo in repos:
            owner = repo.owner
            name = repo.repo

            # Check if already cached locally
            cache_path = github_service.config.paths.get_readme_cache_path(
                owner, name,
            )
            if cache_path.exists():
                skipped_count += 1
                progress.update(
                    task, advance=1,
                    description=f"[yellow]Skipped {owner}/{name}",
                )
                continue

            readme = github_service.get_readme(owner, name)
            if readme:
                downloaded_count += 1
                progress.update(
                    task, advance=1, description=f"[green]Downloaded {owner}/{name}",
                )
            else:
                error_count += 1
                progress.update(
                    task, advance=1,
                    description=f"[red]Failed {owner}/{name}",
                )

    console.print('\n[bold green]✓ Download Complete![/bold green]')
    console.print(f"  - Newly downloaded: {downloaded_count}")
    console.print(f"  - Already cached: {skipped_count}")
    console.print(f"  - Failed/Missing: {error_count}\n")


@app.callback(invoke_without_command=True)
def main(
    input_path: Path | None = typer.Option(
        None, '--input', '-i', help='Input JSONL file of repositories',
    ),
    limit: int = typer.Option(
        100, help='Limit number of repositories to process',
    ),
    github_token: str = typer.Option(
        None, envvar='GITHUB_TOKEN', help='GitHub Token',
    ),
):
    """
    Batch download READMEs for repositories listed in a JSONL file.
    """
    config = get_config()

    # 1. Path Resolution
    if not input_path:
        # Default to all.jsonl in the search directory
        input_path = config.paths.search_dir / 'all.jsonl'

    if not input_path.exists():
        console.print(f"[red]Error: Input file {input_path} not found.[/red]")
        raise typer.Exit(1)

    # 2. Data Loading
    repos = []
    with open(input_path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)
                # Adapt common field variations
                if 'repo_name' in data and 'name' not in data:
                    data['name'] = data['repo_name']
                if 'repo' in data and 'name' not in data:
                    data['name'] = data['repo']
                if '/' in data.get('name', '') and 'owner' not in data:
                    data['owner'], data['name'] = data['name'].split('/', 1)
                if 'owner' not in data:
                    data['owner'] = 'unknown'

                repos.append(Repository.model_validate(data))
                if len(repos) >= limit:
                    break
            except Exception as e:
                logger.warning(
                    'Failed to parse input line',
                    error=str(e), line_snippet=line[:50],
                )

    if not repos:
        console.print('[yellow]No repositories found to process.[/yellow]')
        return

    logger.info(
        'Starting batch README download', count=len(repos),
    )

    # 3. Service Initialization
    github_service = GitHubService(
        token=github_token or config.github.token or '',
    )

    # 4. Sequential Execution
    run_download(repos, github_service)


if __name__ == '__main__':
    app()
