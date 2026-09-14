import humanize
import structlog
import typer
from rich.table import Table

from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.core.logging import console
from chatsbom.core.prune import prune_scan_dirs
from chatsbom.core.prune import PruneReport

logger = structlog.get_logger('data_prune')
app = typer.Typer()


@app.callback(invoke_without_command=True)
@handle_errors
def main(
    keep: int = typer.Option(
        2, help='Scans to retain per repository, newest first',
    ),
    apply: bool = typer.Option(
        False,
        '--apply',
        help='Actually delete. Without this the command only reports.',
    ),
) -> None:
    """
    Keep the newest N scans per repository; discard older ones.

    A single snapshot already occupies 46 GB under data/. Continuous
    collection produces another content tree and another SBOM for every
    new commit, so without retention the disk fills and collection stops
    silently — the worst failure mode available.

    What is removed are *inputs*: recomputable from GitHub, keyed by
    commit. The history that matters has already been appended to
    ClickHouse, so nothing analytical is lost.

    Reports by default; pass --apply to delete.
    """
    if keep < 1:
        console.print('[bold red]Error:[/] --keep must be at least 1.')
        raise typer.Exit(1)

    paths = get_container().config.paths
    # Only the stages that store one directory per scan. 03-github-release
    # and 04-github-commit hold a single JSONL ledger per language, so
    # scan retention does not apply to them — see the note printed below.
    stages = {
        '05-github-tree': paths.tree_dir,
        '06-github-content': paths.content_dir,
        '07-sbom': paths.sbom_dir,
        '09-github-depgraph': paths.depgraph_dir,
        '10-generated-lock': paths.generated_lock_dir,
    }

    if not apply:
        console.print(
            '[yellow]Dry run[/] — nothing will be deleted. '
            'Pass [cyan]--apply[/cyan] to act.',
        )

    table = Table(title=f'Retention (keep {keep} per repository)')
    table.add_column('Stage', style='cyan')
    table.add_column('Scans kept', style='green', justify='right')
    table.add_column('Scans removed', style='yellow', justify='right')
    table.add_column('Freed', style='magenta', justify='right')

    total = PruneReport(dry_run=not apply)
    for name, directory in stages.items():
        report = prune_scan_dirs(directory, keep=keep, dry_run=not apply)
        total += report
        table.add_row(
            name,
            f'{report.kept:,}',
            f'{report.removed:,}',
            humanize.naturalsize(report.bytes_freed, binary=False),
        )

    table.add_row(
        '[bold]total[/bold]',
        f'[bold]{total.kept:,}[/bold]',
        f'[bold]{total.removed:,}[/bold]',
        f'[bold]{humanize.naturalsize(total.bytes_freed, binary=False)}[/bold]',
    )
    console.print(table)

    console.print(
        '[dim]03-github-release and 04-github-commit hold one JSONL ledger '
        'per language rather than per-scan directories, so scan retention '
        'does not reach them (5.7 GB each). They are deduplicated by '
        'repository id, which also means a re-collected repository\'s new '
        'releases are not appended — a separate fix.[/dim]',
    )

    logger.info(
        'Retention pass complete',
        removed=total.removed,
        kept=total.kept,
        bytes_freed=total.bytes_freed,
        applied=apply,
    )
