from pathlib import Path

import humanize
import structlog
import typer
from rich.table import Table

from chatsbom.core.clickhouse import check_clickhouse_connection
from chatsbom.core.container import get_container
from chatsbom.core.logging import console
from chatsbom.export.parquet import export_dataset

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
def main(
    output: Path = typer.Option(
        Path('dist/data'), '--output', '-o',
        help='Directory to write the Parquet files and manifest into',
    ),
) -> None:
    """Export the dataset as Parquet for the web dashboard.

    The result is a handful of static files: no query backend is needed to
    serve them, and the browser can query them directly.
    """
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
        console=console,
        require_database=True,
    )

    console.print(f'[bold green]Exporting to {output}...[/bold green]')

    with container.get_export_repository() as query_repo:
        result = export_dataset(query_repo, output)

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
    console.print(f'[dim]Manifest: {output / "manifest.json"}[/dim]')
