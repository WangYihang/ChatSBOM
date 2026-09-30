import humanize
import structlog
import typer
from rich.table import Table

from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.core.logging import console
from chatsbom.core.prune import current_scans
from chatsbom.core.prune import prune_decisions
from chatsbom.core.prune import prune_scan_dirs
from chatsbom.core.prune import PruneReport

logger = structlog.get_logger('data_prune')
app = typer.Typer()


@app.callback(invoke_without_command=True)
@handle_errors
def main(
    # 1 or more, a range Typer checks, as each `--limit`'s is (#114).
    # Checked here by hand, a `--keep 0` was said on stdout with status
    # 1, where a usage error is said on stderr with status 2 (#124).
    keep: int = typer.Option(
        2, min=1,
        help=(
            'Scans, and release decisions, to retain per repository, '
            'newest first, 1 or more'
        ),
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

    The scan the current commit decision points to is never removed,
    nor the decisions it descends from (#100 Q13); it is kept beside
    the N newest scans, not in place of one. Of the release and
    commit decisions, each repository keeps the N newest release
    decisions and what they name (`core/prune.py` says why).

    Reports by default; pass --apply to delete.
    """
    paths = get_container().config.paths
    # The stages that store one directory per scan. 03-github-release
    # and 04-github-commit hold a directory per decision, below.
    stages = {
        '05-github-tree': paths.tree_dir,
        '06-github-content': paths.content_dir,
        '07-sbom': paths.sbom_dir,
        # Not 09-github-depgraph: a dependency graph is not recomputable
        # once GitHub's endpoint has closed, so every fetch is kept for
        # good (`core/depgraph_store`).
        '10-generated-lock': paths.generated_lock_dir,
    }

    if not apply:
        console.print(
            '[yellow]Dry run[/] — nothing will be deleted. '
            'Pass [cyan]--apply[/cyan] to act.',
        )

    # Read before anything goes: what the decisions say is current.
    current = current_scans(paths)
    retained: dict[int, set[str]] = {}

    table = Table(title=f'Retention (keep {keep} per repository)')
    table.add_column('Stage', style='cyan')
    table.add_column('Scans kept', style='green', justify='right')
    table.add_column('Scans removed', style='yellow', justify='right')
    table.add_column('Freed', style='magenta', justify='right')

    total = PruneReport(dry_run=not apply)
    for name, directory in stages.items():
        report = prune_scan_dirs(
            directory, keep=keep, dry_run=not apply, current=current,
            retained=retained,
        )
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

    decided = prune_decisions(
        paths, keep=keep, scans=retained, dry_run=not apply,
    )
    decisions_table = Table(title=f'Decisions (keep {keep} per repository)')
    decisions_table.add_column('Kind', style='cyan')
    decisions_table.add_column('Kept', style='green', justify='right')
    decisions_table.add_column('Removed', style='yellow', justify='right')
    decisions_table.add_row(
        'release decisions (03)',
        f'{decided.releases_kept:,}', f'{decided.releases_removed:,}',
    )
    decisions_table.add_row(
        'release lists (03)',
        f'{decided.lists_kept:,}', f'{decided.lists_removed:,}',
    )
    decisions_table.add_row(
        'commit decisions (04)',
        f'{decided.commits_kept:,}', f'{decided.commits_removed:,}',
    )
    console.print(decisions_table)
    console.print(
        f'[dim]Decisions freed '
        f'{humanize.naturalsize(decided.bytes_freed, binary=False)}. '
        'What the current scan descends from is kept, whatever its age, '
        'beside the newest. A commit decision is kept while a kept '
        'release decision stands on it or its scan is kept; a list, '
        'while a kept release decision names it, or for a day after it '
        'was written.'
        + (
            f' {decided.unreadable:,} could not be read, and were left.'
            if decided.unreadable else ''
        )
        + '[/dim]',
    )

    logger.info(
        'Retention pass complete',
        removed=total.removed,
        kept=total.kept,
        bytes_freed=total.bytes_freed,
        decisions_removed=(
            decided.releases_removed + decided.commits_removed
            + decided.lists_removed
        ),
        decisions_kept=(
            decided.releases_kept + decided.commits_kept + decided.lists_kept
        ),
        applied=apply,
    )
