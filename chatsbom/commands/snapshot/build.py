"""Publish a snapshot of the warehouse (#132).

The web service, `web`, serves it (#128, phase 3). The collector's
loop runs this in each index pass, once `warehouse build` has built the
warehouse (#150).
"""
from __future__ import annotations

from contextlib import closing
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
    from chatsbom.snapshot.build import Report

logger = structlog.get_logger('snapshot_build')
app = typer.Typer()


@app.callback(invoke_without_command=True)
@handle_errors
def main(
    warehouse: Path | None = typer.Option(
        None,
        '--warehouse',
        '-w',
        help='The warehouse to read; data/warehouse.duckdb by default',
    ),
    output: Path | None = typer.Option(
        None,
        '--output',
        '-o',
        help='Where snapshots are published; data/snapshots by default',
    ),
) -> None:
    """
    Publish a snapshot of the warehouse: one read-only SQLite file, for
    the web service to serve.

    Reads the warehouse `warehouse build` makes, and writes
    `<id>.sqlite` beside the published snapshots, its id the hash of
    what it serves. When `CURRENT` names that id already, the data has
    not changed and nothing is published. Otherwise the file is renamed
    into place, `CURRENT` names it, and the snapshots older than the
    last three are removed. A second pass while one runs is refused.
    """
    # Here, not at the top: the CLI imports every command at start-up,
    # and only this one writes a snapshot, with DuckDB.
    from chatsbom.snapshot.build import build
    from chatsbom.snapshot.publish import SnapshotBusy

    paths = get_container().config.paths
    source = warehouse if warehouse is not None else paths.warehouse_path
    target = output if output is not None else paths.snapshots_dir
    if not source.is_file():
        fail(
            f'[bold red]Error:[/] no warehouse at {escape(str(source))}: a '
            'snapshot is written from it. Run [cyan]chatsbom warehouse '
            'build[/] first.',
            'No warehouse to write a snapshot from', logger,
            warehouse=str(source),
        )

    report: Report
    try:
        with progress_bar(
            SpinnerColumn(),
            TextColumn('[progress.description]{task.description}'),
            TimeElapsedColumn(),
        ) as progress:
            progress.add_task('Writing the snapshot...', total=None)
            report = build(source, target)
    except SnapshotBusy as busy:
        fail(
            '[bold red]Error:[/] another pass is publishing a snapshot: '
            f'{escape(str(busy))} is held. One pass at a time writes the '
            'snapshots; this one has stopped.',
            'Another pass is publishing a snapshot', logger, lock=str(busy),
        )

    show(report)


def show(report: Report) -> None:
    """What the pass published, on stdout: it is the command's output."""
    from chatsbom.dataset.open import connect

    written, published = report.written, report.published
    seconds = sum(written.seconds.values())
    if published.changed:
        megabytes = published.path.stat().st_size / 1_000_000
        console.print(
            f'[green]Published[/] {written.id} ({megabytes:,.1f} MB) in '
            f'{seconds:,.1f} s',
        )
        console.print(escape(str(published.path)), soft_wrap=True)
    else:
        console.print(
            f'Unchanged: {written.id} is current, and nothing was '
            f'published ({seconds:,.1f} s)',
        )
    with closing(connect(published.path)) as snapshot:
        [(corpus, observed_from, observed_to)] = snapshot.execute(
            'SELECT corpus, observed_from, observed_to FROM meta',
        ).fetchall()
    corpus = corpus or 'every repository: the store has no search snapshot'
    span = f'{observed_from} to {observed_to}' if observed_from else 'none'
    console.print(f'Corpus: {escape(str(corpus))}; observed {span}')
    if published.removed:
        console.print(
            f'Removed {len(published.removed)}, older than the last '
            'three: '
            + ', '.join(escape(path.name) for path in published.removed),
        )
    if report.cleared:
        console.print(
            'Cleared what a pass that stopped had left: '
            + ', '.join(escape(path.name) for path in report.cleared),
        )
    table = Table(title='Tables', title_justify='left')
    table.add_column('Table')
    table.add_column('Rows', justify='right')
    table.add_column('Seconds', justify='right')
    for name, rows in written.rows.items():
        took = written.seconds.get(name)
        table.add_row(
            name, f'{rows:,}', '' if took is None else f'{took:.2f}',
        )
    for step in ('prepare', 'index', 'analyze'):
        table.add_row(f'({step})', '', f'{written.seconds[step]:.2f}')
    console.print(table)
