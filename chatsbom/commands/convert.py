import concurrent.futures
import subprocess
import time

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
from chatsbom.services.converter_service import ConverterService

logger = structlog.get_logger('convert_command')
console = Console()


def check_syft_installed():
    """Verify that Syft is installed and available in the system path."""
    try:
        subprocess.run(['syft', 'version'], capture_output=True, check=True)
        return True
    except (FileNotFoundError, subprocess.CalledProcessError):
        return False


def main(
    input_dir: str = typer.Option('data/sbom', help='Root data directory'),
    concurrency: int = typer.Option(16, help='Number of concurrent processes'),
    output_format: str = typer.Option(
        'json', '--format', help='Syft output format',
    ),
    overwrite: bool = typer.Option(
        False, help='Overwrite existing SBOM files',
    ),
    limit: int | None = typer.Option(None, help='Limit number of projects'),
    language: Language | None = typer.Option(None, help='Filter by Language'),
):
    """
    Convert downloaded project manifests to SBOMs using Syft.
    """
    if not check_syft_installed():
        console.print(
            '[bold red]Error:[/] syft is not installed. Please install it to continue.',
        )
        raise typer.Exit(1)

    service = ConverterService(base_dir=input_dir)
    projects = service.find_projects(language)
    if limit:
        projects = projects[:limit]

    logger.info('Starting SBOM Conversion', projects_found=len(projects))

    with Progress(
        SpinnerColumn(),
        TextColumn('[bold blue]{task.description}'),
        BarColumn(),
        TextColumn('[progress.percentage]{task.percentage:>3.0f}%'),
        TextColumn('•'),
        TextColumn('[green]{task.completed}/{task.total}'),
        TextColumn('•'),
        TimeElapsedColumn(),
        console=console,
    ) as progress:

        stats = {'converted': 0, 'skipped': 0, 'failed': 0}
        start_time = time.time()
        task_id = progress.add_task('Converting...', total=len(projects))

        with concurrent.futures.ThreadPoolExecutor(max_workers=concurrency) as executor:
            futures = {
                executor.submit(
                    service.convert_to_sbom, p, output_format, overwrite,
                ): p for p in projects
            }
            for future in concurrent.futures.as_completed(futures):
                res = future.result()
                if res.is_converted:
                    stats['converted'] += 1
                elif res.is_skipped:
                    stats['skipped'] += 1
                else:
                    stats['failed'] += 1
                progress.advance(task_id)

    # Print Summary
    table = Table(title='Conversion Summary')
    table.add_column('Metric', style='cyan')
    table.add_column('Value', style='magenta')
    table.add_row('Total Projects', str(len(projects)))
    table.add_row('Converted', str(stats['converted']))
    table.add_row('Skipped', str(stats['skipped']))
    table.add_row('Failed', str(stats['failed']))
    table.add_row('Total Duration', f"{time.time() - start_time:.2f}s")
    console.print(table)


if __name__ == '__main__':
    typer.run(main)
