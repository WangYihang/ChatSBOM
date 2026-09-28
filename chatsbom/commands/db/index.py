from contextlib import nullcontext

import structlog
import typer
from rich.progress import BarColumn
from rich.progress import MofNCompleteColumn
from rich.progress import Progress
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
from chatsbom.core.logging import console
from chatsbom.core.schema import ARTIFACTS
from chatsbom.models.language import Language
from chatsbom.services.db_service import DbStats

logger = structlog.get_logger('db_index')
app = typer.Typer()


@app.callback(invoke_without_command=True)
def main(
    language: Language | None = typer.Option(None, help='Target Language'),
    limit: int | None = typer.Option(
        None, help='Only ingest the first N repositories per language',
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

    Reads from data/07-sbom, preferring data/09-github-depgraph when it
    exists: that ledger carries the same repositories plus a
    `depgraph_path`, so both SBOM sources land in one pass.

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
    # while reading as the narrow thing. Both narrowing options are
    # refused.
    #
    # `--limit` was added to this check after `--rebuild --limit 3`,
    # meant as a smoke test, discarded 19,384,196 rows and refilled 24
    # repositories. `--language` was already guarded; `--limit` has the
    # same shape and had no guard, which is the whole argument for
    # naming the class of mistake rather than the instance. The rebuild
    # keeps the rows it does not re-read now (#23), but not from a
    # table on another engine, and that is the one it exists for.
    narrowed = (
        ('--language', language is not None),
        ('--limit', limit is not None),
    )
    offending = [flag for flag, given in narrowed if given]
    if rebuild and offending:
        flags = ' and '.join(f"[cyan]{flag}[/]" for flag in offending)
        console.print(
            f"[bold red]Error:[/] --rebuild cannot be combined with "
            f"{flags}.\n\n"
            '--rebuild builds the artifacts table again for [bold]every '
            'language[/bold]; from a table on another engine nothing is '
            'kept, and anything that narrows what is re-ingested then '
            'leaves the rest empty.\n\n'
            '[green]To rebuild everything:[/] [cyan]chatsbom db index '
            '--rebuild[/]\n'
            '[green]To refresh one language:[/] [cyan]chatsbom db index '
            '--language java[/]\n'
            '[green]To try a few repositories:[/] [cyan]chatsbom db '
            'index --limit 3[/] [dim](no --rebuild)[/dim]',
        )
        raise typer.Exit(1)

    container = get_container()
    config = container.config

    # Check Connection (Admin)
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

    service = container.get_db_service()

    # Initialize Repo (ensures tables exist)
    repo_db = container.get_ingestion_repository()

    # The landing zone is the source now, and `--from-files` the
    # fallback. Flipped when `data slim` stripped `all_releases` from
    # the ledgers: a file-based pass is no longer an equal path, it is
    # a degraded one, and a default that quietly produces no releases
    # is the kind of silent wrong answer this project keeps finding.
    from_raw = not from_files
    documents = RawDocuments(repo_db.client) if from_raw else FILES
    manifests = (
        RawManifests(repo_db.client, config.paths.content_dir)
        if from_raw else FILE_MANIFESTS
    )
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

    target_languages = [language] if language else list(Language)

    total_stats = DbStats()

    # Under --rebuild every write below goes to the table being built,
    # and the table readers see is swapped for it only once the loop
    # has finished: an exception leaves them the old one.
    with repo_db.rebuilding(ARTIFACTS.name) if rebuild else nullcontext():
        for lang in target_languages:
            lang_str = str(lang)
            # The SBOM ledger is the complete list of repositories; the
            # depgraph ledger only says which of them have a stored graph.
            # Treating the latter as the input list once cut Java from 1,215
            # repositories to 87, because `--limit` had truncated it.
            depgraph_index = config.paths.get_depgraph_list_path(lang_str)
            input_path = config.paths.get_sbom_list_path(lang_str)
            # Repository metadata as `github repo` last refreshed it. The
            # record carries a snapshot from when the SBOM was generated, so
            # without this a metadata refresh never reaches the database:
            # measured, the ledger knew 722 repositories had been pushed in
            # September while `repositories.pushed_at` still topped out at
            # 2026-02-09.
            metadata_index = config.paths.repo_dir / f"{lang_str}.jsonl"

            if from_raw:
                records: RecordSource = RawRecords(repo_db.client)
            else:
                if not input_path.exists():
                    logger.warning(
                        f"No SBOM data found for {lang_str}",
                        path=str(input_path),
                    )
                    continue
                records = LedgerRecords(
                    input_path,
                    metadata_index if metadata_index.exists() else None,
                )

            logger.info(
                'Indexing from',
                language=lang_str,
                source='raw_documents' if from_raw else str(input_path),
                depgraphs=(
                    str(depgraph_index) if depgraph_index.exists() else None
                ),
            )

            # Counted by reading the source, because a progress bar with no
            # total reads as "hung" on a language that takes three minutes.
            total_repos = sum(1 for _ in records.records(lang_str, limit))
            if not total_repos:
                logger.warning(f"Nothing to index for {lang_str}")
                continue

            # Without this, re-ingesting appends rather than refreshes:
            # `artifacts` is append-only by design, so the same scan read
            # twice is the same observation stored twice. Measured, once:
            # `db index --language python` added 687,000 duplicate rows.
            #
            # The same for each dependency graph, which is its own document
            # rather than part of the Syft scan it is indexed beside (#22).
            #
            # When rebuilding too: the rows already stored were carried
            # into the new table, these among them.
            forgotten = repo_db.forget_scans(
                service.scans_in(records, lang_str, limit),
            )
            graphs = repo_db.forget_graphs(
                service.graphs_in(
                    records, documents, lang_str, limit, depgraph_index,
                    depgraph_root=config.paths.depgraph_dir,
                ),
            )
            if forgotten or graphs:
                console.print(
                    f"[dim]Replacing[/] {forgotten:,} [dim]stored scans "
                    f"and[/] {graphs:,} [dim]graphs for {lang_str}[/dim]",
                )

            with Progress(
                SpinnerColumn(),
                TextColumn('[progress.description]{task.description}'),
                BarColumn(),
                TaskProgressColumn(),
                MofNCompleteColumn(),
                TextColumn('•'),
                TimeElapsedColumn(),
                TextColumn('•'),
                TimeRemainingColumn(),
                console=console,
            ) as progress:
                task = progress.add_task(
                    f"Indexing {lang_str}...", total=total_repos,
                )

                stats = service.ingest_from_list(
                    records,
                    repo_db,
                    lang_str,
                    progress_callback=lambda: progress.advance(task),
                    limit=limit,
                    depgraph_index=depgraph_index,
                    documents=documents,
                    manifests=manifests,
                    depgraph_root=config.paths.depgraph_dir,
                )

                total_stats.repos += stats.repos
                total_stats.artifacts += stats.artifacts
                total_stats.releases += stats.releases
                total_stats.failed += stats.failed
                total_stats.skipped += stats.skipped

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
        skipped=total_stats.skipped,
    )
