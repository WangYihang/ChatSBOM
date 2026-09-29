import shlex
from pathlib import Path

import humanize
import structlog
import typer
from rich.markup import escape
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

    Scripts applied in the order of their names, one `wrangler d1
    execute --file` each: the schema, the data in numbered parts of at
    most 50 MB (`02-<table>-0001.sql` onwards), the aggregates, then the
    indexes. Each can be applied again without changing the result, so
    a failed or timed-out one is run again and the import goes on from
    there.

    Aggregates are precomputed because the overview's panels read every
    artifact row by definition — measured at 3,122 ms for the source
    comparison and 1,082 ms for the relationship split, which on D1 is
    the bill as well as the latency since it charges for rows read.

    Indexes come last because inserting into an indexed table updates
    every index per row; building them once over finished data is
    markedly faster.

    Artifact rows are normalised on the way out. A direct translation of
    the Parquet schema measured 762.6 MB in SQLite with the indexes the
    queries need, against 291.5 MB normalised, for 6.1 million rows.

    The package-to-package edges are the ones `db edges` stored in
    ClickHouse; run it first, or the export stops and says so.
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
        require_database=True,
    )

    console.print(
        f'[bold green]Exporting D1 SQL to {escape(str(output))}...[/bold green]',
    )

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
        '[dim]Apply every file, in name order; a file that fails can be '
        'run again, and the rest after it:\n'
        f'  {escape(apply_loop(output))}[/dim]',
    )


def apply_loop(output: Path) -> str:
    """The shell loop that applies an export in `output` to D1.

    By name, which is the order the files are applied in, and each part
    of the data is its own file. Quoted, so a directory with a space in
    its name is one argument.
    """
    where = shlex.quote(str(output))
    return (
        f'for f in {where}/[0-9][0-9]-*.sql; do '
        'npx wrangler d1 execute chatsbom --remote --file "$f" || break; '
        'done'
    )
