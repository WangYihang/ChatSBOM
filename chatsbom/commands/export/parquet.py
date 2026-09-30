from pathlib import Path

import humanize
import structlog
import typer
from rich.markup import escape
from rich.progress import Progress
from rich.progress import SpinnerColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn
from rich.table import Table

from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.core.diagnostics import fail
from chatsbom.core.extras import require_extra
from chatsbom.core.logging import console
from chatsbom.core.logging import progress_bar
from chatsbom.export.parquet import export_warehouse
from chatsbom.export.parquet import ExportResult

logger = structlog.get_logger('export_parquet')
app = typer.Typer()


def table_of(filename: str) -> str:
    """The table a Parquet filename belongs to.

    Exported filenames carry a content hash for cache busting —
    `artifacts-5d2cc120.parquet` — while `row_counts` is keyed by
    table. The summary stripped only the extension, so every lookup
    missed and every row printed `0`, including for a 49 MB file, while
    the log lines beside it reported the true counts.

    `row_counts`' keying has a test. The one place that reads it for a
    human did not, which is the same gap that let a footer ship with a
    duplicated prefix.
    """
    return filename.removesuffix('.parquet').rsplit('-', 1)[0]


@app.callback(invoke_without_command=True)
@handle_errors
def main(
    output: Path = typer.Option(
        Path('dist/data'), '--output', '-o',
        help='Directory to write the Parquet files and manifest into',
    ),
    warehouse: Path | None = typer.Option(
        None, '--warehouse', '-w',
        help='The warehouse to export; data/warehouse.duckdb by default',
    ),
) -> None:
    """Export the warehouse as Parquet, with a manifest describing it.

    A self-describing copy for DuckDB, pandas or a release: a file per
    table, named after its content, and manifest.json naming them. Read
    from the warehouse `warehouse build` makes, which is only read: no
    server is reached. The site does not read them: it serves a snapshot
    (`snapshot build`).
    """
    # First: the writer is an extra, and without it the warehouse is
    # opened for nothing.
    require_extra('export', 'pyarrow')

    result = _from_warehouse(warehouse, output)

    summary = Table(title='Export Complete')
    summary.add_column('File', style='cyan')
    summary.add_column('Rows', style='magenta', justify='right')
    summary.add_column('Size', style='green', justify='right')

    for name in sorted(result.sizes):
        count = result.row_counts.get(table_of(name))
        summary.add_row(
            name,
            f'{count:,}' if count is not None else '[red]?[/red]',
            humanize.naturalsize(result.sizes[name], binary=False),
        )
    summary.add_row(
        '[bold]total[/bold]', '',
        f'[bold]{humanize.naturalsize(result.total_bytes, binary=False)}[/bold]',
    )
    console.print(summary)
    manifest = escape(str(output / 'manifest.json'))
    console.print(f'[dim]Manifest: {manifest}[/dim]')


def _from_warehouse(warehouse: Path | None, output: Path) -> ExportResult:
    source = (
        warehouse if warehouse is not None
        else get_container().config.paths.warehouse_path
    )
    if not source.is_file():
        fail(
            f'[bold red]Error:[/] no warehouse at {escape(str(source))}: '
            'the export is read from it. Run [cyan]chatsbom warehouse '
            'build[/] first.',
            'No warehouse to export', logger, warehouse=str(source),
        )

    with exporting(
        f'Exporting {escape(str(source))} to {escape(str(output))}...',
    ):
        return export_warehouse(source, output)


def exporting(description: str) -> Progress:
    """A spinner while the export runs, on stderr with the logs: stdout
    is for what it wrote (#114)."""
    progress = progress_bar(
        SpinnerColumn(),
        TextColumn('[progress.description]{task.description}'),
        TimeElapsedColumn(),
    )
    progress.add_task(description, total=None)
    return progress
