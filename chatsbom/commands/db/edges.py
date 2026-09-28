"""Ingest package-to-package dependency edges into ClickHouse.

Separate from `db index` on purpose. Both read the same stored SPDX
documents, but they fill different tables and cost different amounts:
rebuilding `artifacts` is seven minutes over 28,069 repositories, and
re-counting edges is a walk of the 24,936 dependency-graph documents.
Neither should force the other.

The edges are not derivable from `artifacts`. That table records what a
repository depends on; an edge records what one package pulls another
in by, which only the raw documents say.
"""
import structlog
import typer
from rich.progress import BarColumn
from rich.progress import SpinnerColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn

from chatsbom.core.clickhouse import check_clickhouse_connection
from chatsbom.core.container import get_container
from chatsbom.core.edges import collect_edges
from chatsbom.core.edges import DEPGRAPH_ROOT
from chatsbom.core.logging import console
from chatsbom.core.logging import progress_bar
from chatsbom.core.schema import EDGES

logger = structlog.get_logger('db_edges')
app = typer.Typer()

#: Rows per INSERT. The pairs are two short strings and two integers, so
#: a larger batch than the artifact path uses is still a small payload.
BATCH = 50_000


@app.callback(invoke_without_command=True)
def main(
    rebuild: bool = typer.Option(
        False,
        '--rebuild',
        help='Accepted for scripts that pass it: every run replaces the table',
    ),
) -> None:
    """Count package-to-package edges and store them.

    Reads `data/09-github-depgraph`. Nothing is re-fetched: the
    documents the collector already stored are the input.

    Every run counts every document, so its answer is the whole table,
    and it replaces the table: built aside and swapped in, so the edge
    panels read the previous count until the new one is complete. The
    table is a SummingMergeTree, and appending to it — what a run
    without `--rebuild` did — added every count to itself.
    """
    container = get_container()
    config = container.config

    db_config = config.get_db_config('admin')
    check_clickhouse_connection(
        host=db_config.host,
        port=db_config.port,
        user=db_config.user,
        password=db_config.password,
        database=db_config.database,
        console=console,
        require_database=False,
    )

    repo_db = container.get_ingestion_repository()
    # Always named: the table is rebuilt either way, so a drifted one is
    # repaired rather than refused.
    repo_db.ensure_schema(rebuild={EDGES.name})

    with progress_bar(
        SpinnerColumn(),
        TextColumn('[progress.description]{task.description}'),
        BarColumn(),
        TextColumn('•'),
        TimeElapsedColumn(),
    ) as progress:
        task = progress.add_task('Reading dependency graphs...', total=None)
        counts = collect_edges(DEPGRAPH_ROOT)
        progress.update(task, total=1, completed=1)

    if not counts:
        console.print(
            '[yellow]No edges found.[/] '
            f'Expected documents under {DEPGRAPH_ROOT}; '
            'run [cyan]chatsbom github depgraph[/] first.',
        )
        return

    # One timestamp for the whole run, taken from the newest document
    # behind the count rather than from the clock: the row describes an
    # observation, and re-counting the same documents must not make it
    # look fresher. `collect_edges` reports it alongside the pairs.
    observed = counts.observed_at

    written = 0
    pending: list[dict[str, object]] = []
    with repo_db.rebuilding(EDGES.name, carry=False):
        for (parent, child), repositories in counts.items():
            pending.append({
                'parent': parent,
                'child': child,
                'repositories': repositories,
                'observed_at': observed,
            })
            if len(pending) >= BATCH:
                _flush(repo_db, pending)
                written += len(pending)
                pending = []
        if pending:
            _flush(repo_db, pending)
            written += len(pending)

    # The two rollups over `edges`, now rather than at the daily
    # refresh: on a fresh install the forward-edge panels were empty
    # until then.
    repo_db.refresh_rollups(reading={EDGES.name})

    logger.info('Edges stored', pairs=written, observed_at=str(observed))
    console.print(
        f'[green]Stored[/] {written:,} package pairs, '
        f'observed {observed:%Y-%m-%d}.',
    )


def _flush(repo_db, rows: list[dict[str, object]]) -> None:
    repo_db.insert_batch(
        EDGES.name, EDGES.rows(rows), EDGES.column_names,
    )
