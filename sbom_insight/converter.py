import concurrent.futures
import subprocess
import time
from pathlib import Path

import structlog
import typer
from rich.console import Console
from rich.progress import BarColumn
from rich.progress import Progress
from rich.progress import SpinnerColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn

logger = structlog.get_logger('converter')
console = Console()


def find_project_dirs(base_dir: Path) -> list[Path]:
    """
    Finds leaf project directories.
    Structure: base_dir / language / owner / repo / branch / [files]
    We assume the 'branch' directory is the project root.
    """
    projects = []
    # base_dir / lang / owner / repo / branch
    if not base_dir.exists():
        return []

    # Iterate languages
    for lang_dir in base_dir.iterdir():
        if not lang_dir.is_dir():
            continue

        # Iterate owners
        for owner_dir in lang_dir.iterdir():
            if not owner_dir.is_dir():
                continue

            # Iterate repos
            for repo_dir in owner_dir.iterdir():
                if not repo_dir.is_dir():
                    continue

                # Iterate branches (project roots)
                for branch_dir in repo_dir.iterdir():
                    if branch_dir.is_dir():
                        projects.append(branch_dir)
    return projects


def convert_project(project_dir: Path, output_format: str, overwrite: bool) -> str:
    """Runs syft on a project directory."""
    output_file = project_dir / 'sbom.json'

    if output_file.exists() and not overwrite:
        return '[dim]Skip[/dim]'

    # syft dir:. -o json
    cmd = [
        'syft',
        f"dir:{project_dir.absolute()}",
        '-o', output_format,
    ]

    try:
        # Capture output to avoid polluting CLI
        start_time = time.time()
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            check=True,
        )
        elapsed = time.time() - start_time

        # Write output to file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(result.stdout)

        logger.info(
            'SBOM Generated',
            project=str(project_dir),
            output=str(output_file),
            size=len(result.stdout),
            elapsed=f"{elapsed:.2f}s",
        )
        return '[green]Done[/green]'
    except subprocess.CalledProcessError as e:
        logger.error(f"Syft failed for {project_dir}: {e.stderr}")
        return '[red]Fail[/red]'
    except Exception as e:
        logger.error(f"Error {project_dir}: {e}")
        return '[red]Err[/red]'


def main(
    input_dir: str = typer.Option(
        'data', help='Root data directory',
    ),
    concurrency: int = typer.Option(
        4, help='Number of concurrent syft processes',
    ),
    output_format: str = typer.Option(
        'json', '--format', help='Syft output format (json, spdx-json, cyclonedx-json)',
    ),
    overwrite: bool = typer.Option(
        False, help='Overwrite existing SBOM files',
    ),
    limit: int | None = typer.Option(
        None, help='Limit number of projects to convert (for testing)',
    ),
):
    """
    Convert downloaded project manifests to SBOMs using Syft.
    """
    base_path = Path(input_dir)
    if not base_path.exists():
        logger.error(f"Input directory not found: {base_path}")
        raise typer.Exit(1)

    if limit:
        logger.warning(f"Test Mode: Limiting to top {limit} projects")

    projects = find_project_dirs(base_path)
    if limit:
        projects = projects[:limit]

    logger.info(
        'Starting SBOM Conversion',
        input_dir=input_dir,
        concurrency=concurrency,
        format=output_format,
        overwrite=overwrite,
        found_projects=len(projects),
    )

    with Progress(
        SpinnerColumn(),
        TextColumn('[bold blue]{task.description}'),
        BarColumn(),
        TextColumn('[progress.percentage]{task.percentage:>3.0f}%'),
        TextColumn('•'),
        TextColumn('[green]{task.completed}/{task.total}'),
        TextColumn('•'),
        TextColumn('[dim]{task.fields[status]}'),
        TextColumn('•'),
        TimeElapsedColumn(),
        console=console,
    ) as progress:

        task = progress.add_task(
            'Converting...',
            total=len(projects),
            status='Starting',
        )

        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            future_to_project = {
                executor.submit(convert_project, p, output_format, overwrite): p
                for p in projects
            }

            for future in concurrent.futures.as_completed(future_to_project):
                result = future.result()
                progress.update(task, advance=1, status=result)

    logger.info('Conversion complete.')


if __name__ == '__main__':
    typer.run(main)
