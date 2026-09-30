from enum import Enum
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

from chatsbom.core.clickhouse import check_clickhouse_connection
from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.core.diagnostics import fail
from chatsbom.core.extras import require_extra
from chatsbom.core.logging import console
from chatsbom.core.logging import progress_bar
from chatsbom.export.parquet import export_dataset
from chatsbom.export.parquet import export_warehouse
from chatsbom.export.parquet import ExportResult

logger = structlog.get_logger('export_parquet')
app = typer.Typer()


class Source(str, Enum):
    """Where the export reads the dataset (#148)."""

    #: The ClickHouse server `db index` fills, which the dashboard reads
    #: until the cutover (#128).
    CLICKHOUSE = 'clickhouse'
    #: The warehouse `warehouse build` makes from the store, which is
    #: the only one from the cutover on.
    WAREHOUSE = 'warehouse'


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
    source: Source = typer.Option(
        Source.CLICKHOUSE, '--from',
        help='Read the dataset from ClickHouse, or from the warehouse '
        '`warehouse build` makes',
    ),
    warehouse: Path | None = typer.Option(
        None, '--warehouse', '-w',
        help='The warehouse `--from warehouse` reads; '
        'data/warehouse.duckdb by default',
    ),
) -> None:
    """Export the dataset as Parquet, with a manifest describing it.

    A self-describing copy for DuckDB, pandas or a release: a file per
    table, named after its content, and manifest.json naming them. The
    site does not read them: it serves a snapshot (`snapshot build`).

    `--from warehouse` reads the warehouse instead of ClickHouse, and
    writes the same tables, schema and manifest: no server is reached.
    """
    # First: the writer is an extra, and without it a connection is
    # made for nothing.
    require_extra('export', 'pyarrow')

    if source is Source.WAREHOUSE:
        result = _from_warehouse(warehouse, output)
    else:
        if warehouse is not None:
            # Read from ClickHouse, the files would not be of the
            # warehouse named, and nothing would say so.
            fail(
                '[bold red]Error:[/] [cyan]--warehouse[/] names the '
                'warehouse [cyan]--from warehouse[/] reads, and this '
                'export reads ClickHouse. Add [cyan]--from warehouse[/].',
                'A warehouse named for an export of ClickHouse', logger,
                warehouse=str(warehouse),
            )
        result = _from_clickhouse(output)

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


def _from_clickhouse(output: Path) -> ExportResult:
    container = get_container()
    # Admin, not guest: the guest profile caps result rows to bound the
    # cost of interactive queries, and a bulk export is neither
    # interactive nor something that may be silently truncated.
    db_config = container.config.get_db_config('admin')
    check_clickhouse_connection(
        host=db_config.host,
        port=db_config.port,
        user=db_config.user,
        password=db_config.password,
        database=db_config.database,
        require_database=True,
    )

    with container.get_export_repository() as query_repo, exporting(
        f'Exporting to {escape(str(output))}...',
    ):
        return export_dataset(query_repo, output)


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
