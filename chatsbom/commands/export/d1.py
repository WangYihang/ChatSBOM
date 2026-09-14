from pathlib import Path

import humanize
import structlog
import typer
from rich.table import Table

from chatsbom.core.clickhouse import check_clickhouse_connection
from chatsbom.core.container import get_container
from chatsbom.core.logging import console
from chatsbom.export.d1 import export_d1

logger = structlog.get_logger('export_d1')
app = typer.Typer()


@app.callback(invoke_without_command=True)
def main(
    output: Path = typer.Option(
        Path('dist/d1'), '--output', '-o',
        help='Directory to write the D1 SQL scripts into',
    ),
) -> None:
    """Export the dataset as SQL for Cloudflare D1.

    Four scripts, applied in order with `wrangler d1 execute --file`:
    schema, data, aggregates, then indexes.

    Aggregates are precomputed because the overview's panels read every
    artifact row by definition — measured at 3,122 ms for the source
    comparison and 1,082 ms for the relationship split, which on D1 is
    the bill as well as the latency since it charges for rows read.

    Indexes come last because inserting into an indexed table updates
    every index per row; building them once over finished data is
    markedly faster.

    Artifact rows are normalised on the way out. A direct translation of
    the Parquet schema measures 762.6 MB in SQLite with the indexes the
    queries need, which is over D1's 500 MB free tier; interning the
    repeated strings brings it to 291.5 MB with no rows lost.
    """
    container = get_container()
    # Admin, not guest: the guest profile caps result rows to bound the
    # cost of interactive queries, and a bulk export must not be
    # silently truncated.
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

    console.print(f'[bold green]Exporting D1 SQL to {output}...[/bold green]')

    with container.get_export_repository() as query_repo:
        result = export_d1(query_repo, output)

    summary = Table(title='D1 Export Complete')
    summary.add_column('File', style='cyan')
    summary.add_column('Size', style='green', justify='right')
    for name in sorted(result.files):
        summary.add_row(
            name, humanize.naturalsize(
                result.files[name], binary=False,
            ),
        )
    summary.add_row(
        '[bold]total[/bold]',
        f'[bold]{humanize.naturalsize(result.total_bytes, binary=False)}[/bold]',
    )
    console.print(summary)

    rows = Table(title='Rows')
    rows.add_column('Table', style='cyan')
    rows.add_column('Rows', style='magenta', justify='right')
    for name in sorted(result.row_counts):
        rows.add_row(name, f'{result.row_counts[name]:,}')
    console.print(rows)

    console.print(
        '[dim]Apply in order:\n'
        f'  npx wrangler d1 execute chatsbom --remote --file {output}/01-schema.sql\n'
        f'  npx wrangler d1 execute chatsbom --remote --file {output}/02-data.sql\n'
        f'  npx wrangler d1 execute chatsbom --remote --file {output}/03-aggregates.sql\n'
        f'  npx wrangler d1 execute chatsbom --remote --file {output}/04-indexes.sql'
        '[/dim]',
    )
