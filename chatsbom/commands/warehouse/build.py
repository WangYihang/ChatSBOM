"""Build the DuckDB warehouse from the store (#131).

ClickHouse is what the dashboard reads until the cutover (#128,
Appendix B). The collector's loop runs this in each index pass, after
`db index` (#150), so the two are built from the same store and can be
compared rollup by rollup (`chatsbom/warehouse/parity.py`).
"""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING

import structlog
import typer
from rich.markup import escape
from rich.progress import SpinnerColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn
from rich.table import Table

from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.core.diagnostics import fail
from chatsbom.core.logging import console
from chatsbom.core.logging import progress_bar

if TYPE_CHECKING:
    from chatsbom.warehouse.build import BuildReport

logger = structlog.get_logger('warehouse_build')
app = typer.Typer()


@app.callback(invoke_without_command=True)
@handle_errors
def main(
    output: Path | None = typer.Option(
        None,
        '--output',
        '-o',
        help='Where to write it; data/warehouse.duckdb by default',
    ),
) -> None:
    """
    Build the DuckDB warehouse from the store: every scan, the current
    facts and the rollups.

    Reads `data/` alone, with the parsers `db index` uses: every commit's
    Syft document and manifests, every fetch of the dependency graph, the
    records and the ledger, and the search snapshots. The corpus is the
    newest complete snapshot. Writes a new file and renames it over the
    last one, so a reader never sees half a pass; a second pass while one
    runs is refused.
    """
    # Here, not at the top: the CLI imports every command at start-up,
    # and only this one needs the warehouse, or DuckDB.
    from chatsbom.warehouse.build import build
    from chatsbom.warehouse.build import WarehouseBusy

    paths = get_container().config.paths
    target = output if output is not None else paths.warehouse_path
    if not paths.base_data_dir.is_dir():
        fail(
            f'[bold red]Error:[/] no store at '
            f'{escape(str(paths.base_data_dir))}: the warehouse is built '
            'from the files the collectors write there. Run it where '
            '[cyan]data/[/] is.',
            'No store to build the warehouse from', logger,
            store=str(paths.base_data_dir),
        )

    report: BuildReport
    try:
        with progress_bar(
            SpinnerColumn(),
            TextColumn('[progress.description]{task.description}'),
            TextColumn('{task.completed:,} repositories'),
            TimeElapsedColumn(),
        ) as progress:
            task = progress.add_task('Reading the store...', total=None)
            report = build(
                paths, target,
                progress=lambda count: progress.update(task, completed=count),
            )
    except WarehouseBusy as busy:
        fail(
            '[bold red]Error:[/] another pass is building the warehouse: '
            f'{escape(str(busy))} is held. One pass at a time reads the '
            'store; this one has stopped.',
            'Another pass is building the warehouse', logger,
            lock=str(busy),
        )

    show(report)


def show(report: BuildReport) -> None:
    """What the pass built, on stdout: it is the command's output."""
    megabytes = report.size / 1_000_000
    seconds = report.read_seconds + sum(report.derived_seconds.values())
    console.print(
        f'[green]Built[/] {escape(str(report.output))} '
        f'({megabytes:,.1f} MB) in {seconds:,.1f} s',
    )
    corpus = report.corpus or 'every repository: the store has no snapshot'
    console.print(
        f'Corpus: {escape(corpus)}, '
        f'{report.corpus_size:,} repositories',
    )
    scans = ' · '.join(
        f'{source} {count:,}' for source, count in sorted(report.scans.items())
    ) or 'none'
    console.print(
        f'Read {report.repositories:,} repositories in '
        f'{report.read_seconds:,.1f} s: scans {scans}; '
        f'{report.observations:,} observations',
    )
    if report.unreadable or report.unnamed:
        console.print(
            f'Left out: {report.unreadable:,} documents that could not be '
            f'read, {report.unnamed:,} repositories with outputs and no '
            'metadata',
        )
    table = Table(title='Derived', title_justify='left')
    table.add_column('Table')
    table.add_column('Rows', justify='right')
    table.add_column('Seconds', justify='right')
    for name, took in report.derived_seconds.items():
        table.add_row(
            name, f'{report.derived_rows.get(name, 0):,}', f'{took:.2f}',
        )
    console.print(table)
