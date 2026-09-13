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
    db_config = container.config.get_db_config('guest')
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

    with container.get_query_repository() as query_repo:
        result = export_dataset(query_repo, output)

    summary = Table(title='Export Complete')
    summary.add_column('File', style='cyan')
    summary.add_column('Rows', style='magenta', justify='right')
    summary.add_column('Size', style='green', justify='right')

    for name in sorted(result.sizes):
        table_name = name.removesuffix('.parquet')
        summary.add_row(
            name,
            f'{result.row_counts.get(table_name, 0):,}',
            humanize.naturalsize(result.sizes[name], binary=False),
        )
    summary.add_row(
        '[bold]total[/bold]', '',
        f'[bold]{humanize.naturalsize(result.total_bytes, binary=False)}[/bold]',
    )
    console.print(summary)
    console.print(f'[dim]Manifest: {output / "manifest.json"}[/dim]')
