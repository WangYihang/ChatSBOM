import concurrent.futures
import subprocess
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path

import structlog
import typer
from rich.console import Console
from rich.progress import BarColumn
from rich.progress import Progress
from rich.progress import SpinnerColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn
from rich.table import Table

from chatsbom.models.language import Language

logger = structlog.get_logger('converter')
console = Console()


@dataclass
class ConvertResult:
    project_path: str
    status_msg: str
    converted: int = 0
    skipped: int = 0
    failed: int = 0


def find_project_dirs(base_dir: Path, language: Language | None = None) -> list[Path]:
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

        # Filter by language if specified
        if language and lang_dir.name != language.value:
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


def convert_project(project_dir: Path, output_format: str, overwrite: bool) -> ConvertResult:
    """Runs syft on a project directory."""
    output_file = project_dir / 'sbom.json'
    stats = ConvertResult(project_path=str(project_dir), status_msg='')

    if output_file.exists() and not overwrite:
        stats.skipped += 1
        stats.status_msg = '[dim]Skip[/dim]'

        # Log skipped/cached
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        log_msg = (
            f"{timestamp} \\[info     ] SBOM Generated                 "
            f"elapsed=0.00s output={output_file} "
            f"project={project_dir} size={output_file.stat().st_size} "
            f"[green](Cached)[/green]"
        )
        console.print(f"[dim]{log_msg}[/dim]")

        return stats

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
        stats.converted += 1
        stats.status_msg = '[green]Done[/green]'
        return stats
    except subprocess.CalledProcessError as e:
        logger.error(f"Syft failed for {project_dir}: {e.stderr}")
        stats.failed += 1
        stats.status_msg = '[red]Fail[/red]'
        return stats
    except Exception as e:
        logger.error(f"Error {project_dir}: {e}")
        stats.failed += 1
        stats.status_msg = '[red]Err[/red]'
        return stats


def main(
    input_dir: str = typer.Option(
        'data/sbom', help='Root data directory',
    ),
    concurrency: int = typer.Option(
        16, help='Number of concurrent syft processes',
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
    language: Language | None = typer.Option(
        None, help='Filter by Language (default: all)',
    ),
):
    """
    Convert downloaded project manifests to SBOMs using Syft.
    """
    # Check if syft is installed
    try:
        subprocess.run(
            ['syft', 'version'],
            capture_output=True,
            check=True,
        )
    except FileNotFoundError:
        console.print(
            '[bold red]Error:[/] syft is not installed.\n\n'
            'The convert-sbom command requires Syft to generate SBOMs. '
            'Please install Syft:\n\n'
            '  [cyan]# macOS / Linux (Homebrew)[/]\n'
            '  [cyan]brew tap anchore/syft[/]\n'
            '  [cyan]brew install syft[/]\n\n'
            '  [cyan]# Or via install script[/]\n'
            '  [cyan]curl -sSfL https://get.anchore.io/syft | sudo sh -s -- -b /usr/local/bin[/]\n\n'
            'For more options, visit: '
            '[link=https://github.com/anchore/syft?tab=readme-ov-file#installation]'
            'https://github.com/anchore/syft?tab=readme-ov-file#installation[/link]',
        )
        raise typer.Exit(1)
    except subprocess.CalledProcessError:
        console.print(
            '[bold yellow]Warning:[/] Could not verify syft version, proceeding anyway.',
        )

    base_path = Path(input_dir)
    if not base_path.exists():
        logger.error(f"Input directory not found: {base_path}")
        raise typer.Exit(1)

    if limit:
        logger.warning(f"Test Mode: Limiting to top {limit} projects")

    projects = find_project_dirs(base_path, language)
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

        overall_stats = {
            'converted': 0,
            'skipped': 0,
            'failed': 0,
            'total': len(projects),
        }
        start_time = time.time()

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
                overall_stats['converted'] += result.converted
                overall_stats['skipped'] += result.skipped
                overall_stats['failed'] += result.failed
                progress.update(task, advance=1, status=result.status_msg)

    # Print Summary Table
    total_time = time.time() - start_time
    table = Table(title='Conversion Summary')
    table.add_column('Metric', style='cyan')
    table.add_column('Value', style='magenta')

    table.add_row('Total Projects', str(overall_stats['total']))
    table.add_row('Converted (Success)', str(overall_stats['converted']))
    table.add_row('Skipped (Exists)', str(overall_stats['skipped']))
    table.add_row('Failed', str(overall_stats['failed']))
    table.add_row('Total Duration', f"{total_time:.2f}s")

    console.print(table)


if __name__ == '__main__':
    typer.run(main)
