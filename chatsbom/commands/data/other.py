from collections import Counter

import structlog
import typer
from rich.table import Table

from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.core.logging import console
from chatsbom.core.others import full_name
from chatsbom.core.others import select_others
from chatsbom.core.others import UNLABELLED
from chatsbom.core.others import write_jsonl
from chatsbom.models.language import Language

logger = structlog.get_logger('data_other')
app = typer.Typer()


@app.callback(invoke_without_command=True)
@handle_errors
def main(
    include: list[str] = typer.Option(
        [], '--include', help='owner/repo to place first (repeatable)',
    ),
    include_orphans: bool = typer.Option(
        False,
        '--include-orphans',
        help='Also take repositories a language list claimed but that '
        'never reached its SBOM ledger (e.g. WebGoat)',
    ),
    spread: bool = typer.Option(
        False,
        '--spread',
        help='Round-robin across GitHub languages instead of by stars',
    ),
    limit: int | None = typer.Option(None, help='Keep only the first N'),
    force: bool = typer.Option(
        False, help='Replace an existing other.jsonl',
    ),
) -> None:
    """
    Build the `other` lane from the unfiltered sweep, without API calls.

    Takes every repository in data/01-github-search/all.jsonl that no
    language list holds — a Django app GitHub calls "Svelte", say — and
    writes data/01-github-search/other.jsonl. Every later stage then
    runs with `--language other`, fetching every ecosystem's manifests;
    `repositories.language` keeps GitHub's own label.

    Reads from: data/01-github-search/all.jsonl, the language lists, data/07-sbom
    Writes to:  data/01-github-search/other.jsonl
    """
    paths = get_container().config.paths
    languages = [str(lang) for lang in Language if lang is not Language.OTHER]

    sweep = paths.get_search_list_path('all')
    output = paths.get_search_list_path(str(Language.OTHER))

    if not sweep.exists():
        console.print(
            f'[bold red]Error:[/] no unfiltered sweep at {sweep}.\n\n'
            '[green]Collect one with:[/] [cyan]chatsbom github search[/] '
            '[dim](no --language)[/dim]',
        )
        raise typer.Exit(1)
    if output.exists() and not force:
        console.print(
            f'[bold red]Error:[/] {output} exists. Pass --force to replace '
            'it; later stages skip repositories they have already done.',
        )
        raise typer.Exit(1)

    try:
        selection = select_others(
            sweep=sweep,
            claimed=[
                path
                for lang in languages
                for path in (
                    paths.get_search_list_path(lang),
                    paths.get_repo_list_path(lang),
                )
            ],
            reached=[paths.get_sbom_list_path(lang) for lang in languages],
            include=include,
            include_orphans=include_orphans,
            spread=spread,
            limit=limit,
        )
    except ValueError as e:
        console.print(f'[bold red]Error:[/] {e}')
        raise typer.Exit(1)

    written = write_jsonl(output, selection.records)

    labels = Counter(
        record.get('language') or UNLABELLED for record in selection.records
    )
    table = Table(title=f'other lane: {written:,} repositories')
    table.add_column('GitHub language')
    table.add_column('Repositories', justify='right')
    for label, count in labels.most_common(15):
        table.add_row(label, f'{count:,}')
    if len(labels) > 15:
        rest = sum(count for _, count in labels.most_common()[15:])
        table.add_row(f'({len(labels) - 15} more)', f'{rest:,}')
    console.print(table)
    console.print(
        f'[dim]In no language list:[/] {selection.unclaimed:,}  '
        f'[dim]Claimed but never indexed:[/] {selection.orphans:,}'
        + ('' if include_orphans else ' [dim](not taken; --include-orphans)[/dim]'),
    )
    if selection.included:
        console.print(f'[dim]Placed first:[/] {", ".join(selection.included)}')
    console.print(f'[green]Wrote[/] {output}')
    logger.info(
        'Other lane written',
        path=str(output), repositories=written,
        first=[full_name(r) for r in selection.records[:3]],
    )
