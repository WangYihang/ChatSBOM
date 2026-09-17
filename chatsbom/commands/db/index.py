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
        help='Drop and recreate the artifacts table before ingesting',
    ),
    from_raw: bool = typer.Option(
        False,
        '--from-raw',
        help='Read the SBOMs and manifests from raw_documents, not data/',
    ),
):
    """
    Ingest SBOM and repository data into ClickHouse.

    Reads from data/07-sbom, preferring data/09-github-depgraph when it
    exists: that ledger carries the same repositories plus a
    `depgraph_path`, so both SBOM sources land in one pass.

    With --from-raw the SBOMs *and the manifests* come from the
    `raw_documents` table that `chatsbom db raw` filled, and the ledgers
    are read only for the repository list and metadata. Same rows either way — `observed_at`
    included, because `db raw` copied each file's mtime into
    `fetched_at` — which is what makes the 31 GB under data/
    reproducible from 1.92 GiB in the database rather than load-bearing.
    """

    # --rebuild drops the whole table, so anything that narrows what is
    # then re-ingested turns a total operation into a partial one while
    # reading as the narrow thing. Both narrowing options are refused.
    #
    # `--limit` was added to this check after `--rebuild --limit 3`,
    # meant as a smoke test, discarded 19,384,196 rows and refilled 24
    # repositories. `--language` was already guarded; `--limit` has the
    # same shape and had no guard, which is the whole argument for
    # naming the class of mistake rather than the instance.
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
            '--rebuild discards the artifacts table for [bold]every '
            'language[/bold]; anything that narrows what is re-ingested '
            'then leaves the rest empty.\n\n'
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

    documents = RawDocuments(repo_db.client) if from_raw else FILES
    manifests = (
        RawManifests(repo_db.client, config.paths.content_dir)
        if from_raw else FILE_MANIFESTS
    )
    if from_raw:
        console.print(
            '[dim]Reading everything from[/] [cyan]raw_documents[/] '
            '[dim]— records, SBOMs and manifests[/dim]',
        )

    if rebuild:
        # Discarded as part of ensure_schema, not after it: the engine
        # check would otherwise abort before the rebuild could run.
        console.print(
            '[yellow]Rebuilding the artifacts table[/] '
            '(discarding rows from older schemas)',
        )
    repo_db.ensure_schema(rebuild={ARTIFACTS.name} if rebuild else None)

    target_languages = [language] if language else list(Language)

    total_stats = DbStats()

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
            depgraphs=str(depgraph_index) if depgraph_index.exists() else None,
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
        # Skipped when rebuilding, where the table was just dropped.
        if not rebuild:
            forgotten = repo_db.forget_scans(
                service.scans_in(records, lang_str, limit),
            )
            if forgotten:
                console.print(
                    f"[dim]Replacing[/] {forgotten:,} [dim]stored scans "
                    f"for {lang_str}[/dim]",
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
            )

            total_stats.repos += stats.repos
            total_stats.artifacts += stats.artifacts
            total_stats.releases += stats.releases
            total_stats.failed += stats.failed
            total_stats.skipped += stats.skipped

    # Collapse superseded ReplacingMergeTree rows so reads need no FINAL
    # on the large tables.
    console.print('[dim]Optimizing tables...[/dim]')
    repo_db.optimize()

    # The rollups summarise what was just written, so they are stale the
    # moment a rebuild finishes. `recreate` when rebuilding, because a
    # dropped-and-refilled base table can leave a rollup describing rows
    # that no longer exist.
    console.print('[dim]Refreshing rollups...[/dim]')
    # The dictionary first: a rollup could read it, and the dependants
    # panel would otherwise show the previous run's stars beside this
    # run's dependencies until LIFETIME caught up.
    repo_db.reload_dictionaries()
    repo_db.refresh_rollups(recreate=rebuild)

    logger.info(
        'Indexing Complete',
        repos=total_stats.repos,
        artifacts=total_stats.artifacts,
        releases=total_stats.releases,
        failed=total_stats.failed,
        skipped=total_stats.skipped,
    )
