from contextlib import nullcontext
from pathlib import Path

import structlog
import typer
from rich.progress import BarColumn
from rich.progress import MofNCompleteColumn
from rich.progress import SpinnerColumn
from rich.progress import TaskProgressColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn
from rich.progress import TimeRemainingColumn

from chatsbom.core.clickhouse import check_clickhouse_connection
from chatsbom.core.container import get_container
from chatsbom.core.documents import FILE_MANIFESTS
from chatsbom.core.documents import FILES
from chatsbom.core.documents import LedgerRecords
from chatsbom.core.documents import RawDocuments
from chatsbom.core.documents import RawManifests
from chatsbom.core.documents import RawRecords
from chatsbom.core.documents import RecordSource
from chatsbom.core.documents import TrackedRecords
from chatsbom.core.ledger import resolve_names
from chatsbom.core.ledger import tracked_repositories
from chatsbom.core.logging import console
from chatsbom.core.logging import progress_bar
from chatsbom.core.schema import ARTIFACTS
from chatsbom.services.db_service import DbStats

logger = structlog.get_logger('db_index')
app = typer.Typer()


@app.callback(invoke_without_command=True)
def main(
    limit: int | None = typer.Option(
        None, help='Only ingest the first N repositories',
    ),
    repos_file: Path | None = typer.Option(
        None,
        '--repos-file',
        help=(
            'Only these repositories: one owner/repo (or id) per line, '
            'as the ledger tracks them'
        ),
    ),
    rebuild: bool = typer.Option(
        False,
        '--rebuild',
        help=(
            'Build the artifacts table again from its declaration, '
            'swapped in once the ingest has finished; older scans are '
            'kept'
        ),
    ),
    from_files: bool = typer.Option(
        False,
        '--from-files',
        help='Read from the data/ ledgers instead of raw_documents',
    ),
):
    """
    Ingest SBOM and repository data into ClickHouse.

    Every repository the ledger tracks is indexed (#55 §4.11), whatever
    its language and whether or not it has a scan: its newest record
    where the pipeline filed one, else what the repository resource and
    the ledger say of it. Each gets a `repositories` row, and whatever
    artifacts it has, from up to three sources: Syft's scan, GitHub's
    dependency graph, and the dependencies its Gradle build files
    declare (`source = 'manifest'`).

    Reads from `raw_documents` — the records, the SBOMs, the dependency
    graphs and the manifests — which is where `chatsbom db raw` lands
    everything the collectors produce.

    `--from-files` reads the `data/` ledgers instead. That was the
    default until the ledgers were slimmed, and it is a fallback now
    rather than an equal path: `data slim` strips `all_releases`, so a
    file-based pass writes no releases and whatever metadata the ledger
    last held. It is kept for a machine that has the files but nothing
    landed.
    """

    # --rebuild rebuilds the whole table, so anything that narrows what
    # is then re-ingested turns a total operation into a partial one
    # while reading as the narrow thing. Every narrowing option is
    # refused.
    #
    # `--limit` was added to this check after `--rebuild --limit 3`,
    # meant as a smoke test, discarded 19,384,196 rows and refilled 24
    # repositories. `--language` was already guarded, and `--repos-file`
    # took its place; the rebuild keeps the rows it does not re-read now
    # (#23), but not from a table on another engine, and that is the one
    # it exists for.
    narrowed = (
        ('--repos-file', repos_file is not None),
        ('--limit', limit is not None),
    )
    offending = [flag for flag, given in narrowed if given]
    if rebuild and offending:
        flags = ' and '.join(f"[cyan]{flag}[/]" for flag in offending)
        console.print(
            f"[bold red]Error:[/] --rebuild cannot be combined with "
            f"{flags}.\n\n"
            '--rebuild builds the artifacts table again for [bold]every '
            'repository[/bold]; from a table on another engine nothing is '
            'kept, and anything that narrows what is re-ingested then '
            'leaves the rest empty.\n\n'
            '[green]To rebuild everything:[/] [cyan]chatsbom db index '
            '--rebuild[/]\n'
            '[green]To refresh some repositories:[/] [cyan]chatsbom db '
            'index --repos-file repos.txt[/]\n'
            '[green]To try a few repositories:[/] [cyan]chatsbom db '
            'index --limit 3[/] [dim](no --rebuild)[/dim]',
        )
        raise typer.Exit(1)

    container = get_container()
    config = container.config
    paths = config.paths

    # The master list, read without writing to the ledger.
    tracked = tracked_repositories(paths.ledger_path)
    only: set[int] | None = None
    if repos_file is not None:
        if tracked is None:
            console.print(
                '[bold red]Error:[/] --repos-file names repositories the '
                f'ledger tracks, and there is no ledger at '
                f'{paths.ledger_path}.',
            )
            raise typer.Exit(1)
        only, missing = resolve_names(
            tracked, repos_file.read_text(encoding='utf-8').splitlines(),
        )
        if missing:
            console.print(
                f"[yellow]Not tracked, skipped:[/] {', '.join(missing)}",
            )

    # Check Connection (Admin)
    db_config = config.get_db_config('admin')
    check_clickhouse_connection(
        host=db_config.host,
        port=db_config.port,
        user=db_config.user,
        password=db_config.password,
        database=db_config.database,
        require_database=False,
    )

    service = container.get_db_service()

    # A repository, not yet a connection: its client connects on first
    # use, to CLICKHOUSE_DB. The database and its tables are made by
    # ensure_schema, below.
    repo_db = container.get_ingestion_repository()

    # The landing zone is the source now, and `--from-files` the
    # fallback. Flipped when `data slim` stripped `all_releases` from
    # the ledgers: a file-based pass is no longer an equal path, it is
    # a degraded one, and a default that quietly produces no releases
    # is the kind of silent wrong answer this project keeps finding.
    from_raw = not from_files
    if from_files:
        console.print(
            '[yellow]Reading the data/ ledgers.[/] They are slimmed — '
            '`all_releases` is not in them — so this pass writes no '
            'releases and whatever metadata the ledger last held.\n'
            '[dim]Drop --from-files to read the landed documents.[/dim]',
        )
    else:
        console.print(
            '[dim]Reading from[/] [cyan]raw_documents[/] '
            '[dim]— records, SBOMs, graphs and manifests[/dim]',
        )

    if rebuild:
        # Named to ensure_schema, not rebuilt after it: the engine
        # check would otherwise abort before the rebuild could run.
        console.print(
            '[yellow]Rebuilding the artifacts table[/] '
            '[dim]beside the one the dashboard reads, which is swapped '
            'for it once the ingest has finished[/dim]',
        )
    repo_db.ensure_schema(rebuild={ARTIFACTS.name} if rebuild else None)

    # After ensure_schema, which is what creates the database. These,
    # and the landed records below, read the landing zone through the
    # repository's client, which is bound to CLICKHOUSE_DB: made first,
    # they failed with
    # UNKNOWN_DATABASE on a database that did not exist yet, which is
    # the first run README describes and the one the hint for a
    # missing database recommends (#64).
    documents = RawDocuments(repo_db.client) if from_raw else FILES
    manifests = (
        RawManifests(repo_db.client, paths.content_dir)
        if from_raw else FILE_MANIFESTS
    )

    if from_raw:
        raw = RawRecords(repo_db.client)
        found: RecordSource = raw
        metadata = raw.metadata
    else:
        # Every list the pipeline wrote, `index.jsonl` included: the
        # records of repositories tracked with no language are filed
        # there (`chatsbom run`), and no language pass ever read them.
        found = LedgerRecords(
            sorted(paths.sbom_dir.glob('*.jsonl')),
            sorted(paths.repo_dir.glob('*.jsonl')),
        )
        metadata = None
    if tracked is None:
        logger.warning(
            'No ledger: indexing the repositories with records only',
            ledger=str(paths.ledger_path),
        )
    records = TrackedRecords(found, tracked, metadata=metadata, only=only)

    logger.info(
        'Indexing from',
        source='raw_documents' if from_raw else str(paths.sbom_dir),
        tracked=len(tracked) if tracked is not None else None,
        repositories=len(only) if only is not None else None,
    )

    # Counted by reading the source, because a progress bar with no
    # total reads as "hung" on a pass that takes minutes.
    total_repos = sum(1 for _ in records.records(limit))
    total_stats = DbStats()

    # Under --rebuild every write below goes to the table being built,
    # and the table readers see is swapped for it only once the loop
    # has finished: an exception leaves them the old one.
    with repo_db.rebuilding(ARTIFACTS.name) if rebuild else nullcontext():
        if not total_repos:
            logger.warning('Nothing to index')
        else:
            # Without this, re-ingesting appends rather than refreshes:
            # `artifacts` is append-only by design, so the same scan read
            # twice is the same observation stored twice. Measured, once:
            # `db index --language python` added 687,000 duplicate rows.
            #
            # The same for each dependency graph, which is its own
            # document rather than part of the Syft scan it is indexed
            # beside (#22). A scan's manifest rows go with the scan.
            #
            # When rebuilding too: the rows already stored were carried
            # into the new table, these among them.
            forgotten = repo_db.forget_scans(
                service.scans_in(records, limit),
            )
            graphs = repo_db.forget_graphs(
                service.graphs_in(
                    records, documents, limit,
                    depgraph_root=paths.depgraph_dir,
                ),
            )
            if forgotten or graphs:
                console.print(
                    f"[dim]Replacing[/] {forgotten:,} [dim]stored scans "
                    f"and[/] {graphs:,} [dim]graphs[/dim]",
                )

            with progress_bar(
                SpinnerColumn(),
                TextColumn('[progress.description]{task.description}'),
                BarColumn(),
                TaskProgressColumn(),
                MofNCompleteColumn(),
                TextColumn('•'),
                TimeElapsedColumn(),
                TextColumn('•'),
                TimeRemainingColumn(),
            ) as progress:
                task = progress.add_task('Indexing...', total=total_repos)
                total_stats = service.ingest_from_list(
                    records,
                    repo_db,
                    progress_callback=lambda: progress.advance(task),
                    limit=limit,
                    documents=documents,
                    manifests=manifests,
                    depgraph_root=paths.depgraph_dir,
                )

    # What `optimize` merges, and what it no longer does, is said there.
    console.print('[dim]Optimizing tables...[/dim]')
    repo_db.optimize()

    # The rollups summarise what was just written, so they are stale the
    # moment an ingest finishes; after a rebuild they read the table
    # swapped in, by name, and hold what they computed from the old one
    # until this. A changed definition needs nothing more: ensure_schema
    # replaced it already.
    console.print('[dim]Refreshing rollups...[/dim]')
    # The dictionary first: a rollup could read it, and the dependants
    # panel would otherwise show the previous run's stars beside this
    # run's dependencies until LIFETIME caught up.
    repo_db.reload_dictionaries()
    repo_db.refresh_rollups()

    logger.info(
        'Indexing Complete',
        repos=total_stats.repos,
        artifacts=total_stats.artifacts,
        releases=total_stats.releases,
        failed=total_stats.failed,
        unscanned=total_stats.unscanned,
        without_artifacts=total_stats.without_artifacts,
    )
    if total_stats.unscanned or total_stats.without_artifacts:
        # Every one of these was indexed: the counts say what it was
        # indexed with, not that it was left out.
        console.print(
            f'[dim]Indexed with no Syft scan:[/] {total_stats.unscanned:,}'
            f' [dim]· with no artifact from any source:[/] '
            f'{total_stats.without_artifacts:,}',
        )
