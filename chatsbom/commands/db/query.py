import contextlib
import sys
from typing import NoReturn

import structlog
import typer
from rich.markup import escape
from rich.table import Table

from chatsbom.core.clickhouse import check_clickhouse_connection
from chatsbom.core.container import get_container
from chatsbom.core.diagnostics import fail
from chatsbom.core.diagnostics import say
from chatsbom.core.logging import console
from chatsbom.core.logging import stderr_console
from chatsbom.services.db_service import DbService

logger = structlog.get_logger('db_query')
app = typer.Typer()


@app.callback(invoke_without_command=True)
def main(
    component: str = typer.Argument(..., help='Component name to search for'),
    # 1 or more: `--limit 0` asked for no dependents and said there
    # were none, and the server refused `--limit -1` (#114).
    limit: int = typer.Option(10, min=1, help='Max results, 1 or more'),
    language: str = typer.Option(
        None, help="Filter by the repository's GitHub language, e.g. java",
    ),
    ecosystem: str = typer.Option(
        None,
        help=(
            "Filter by the package's ecosystem, e.g. maven, npm, pypi, "
            'composer'
        ),
    ),
    direct_only: bool = typer.Option(
        False,
        '--direct-only',
        help='Only repositories that declare the package in their manifest',
    ),
):
    """Query dependencies across repositories.

    What it prints on stdout is the answer, the dependents. The rest is
    on stderr: the candidates and the question they are for, and what
    it says when there is nothing to show or the query fails.
    """

    container = get_container()
    config = container.config

    # Check Connection (Guest)
    db_config = config.get_db_config('guest')
    check_clickhouse_connection(
        host=db_config.host,
        port=db_config.port,
        user=db_config.user,
        password=db_config.password,
        database=db_config.database,
        require_database=True,
    )

    query_repo = container.get_query_repository()
    service = DbService()

    # Only the queries are caught. Around the prompt too, this took an
    # answer that never came, the prompt's abort, for a failed query,
    # and printed "Error querying: " with nothing after it.
    try:
        # Step 1: Search for library candidates
        candidates = service.search_library(
            query_repo, component, language=language, limit=limit,
            ecosystem=ecosystem,
        )
    except Exception as e:
        _failed(e)

    # What was typed, and what the database holds — names from the
    # SBOMs, as their tools wrote them — are escaped: titles and cells
    # are read as markup too.
    if not candidates:
        # Nothing found is an answer, not a failure: said on stderr,
        # and the command exits 0.
        say(
            f"[yellow]No libraries found matching '{escape(component)}'"
            '[/yellow]',
            'No libraries found', logger, component=component,
        )
        return

    # The candidates are the question, and go where it goes: on stderr,
    # as a shell's `select` puts its menu. On stdout, with stdout
    # redirected, the person answering could not see what they were
    # choosing from, and a script reading it got the menu with the
    # answer.
    cand_table = Table(title=f"Library Candidates: {escape(component)}")
    cand_table.add_column('#', style='dim')
    cand_table.add_column('Library Name', style='cyan')
    cand_table.add_column('Repository Count', style='magenta')

    for idx, candidate in enumerate(candidates, start=1):
        cand_table.add_row(
            str(idx), escape(candidate.name),
            f'{candidate.repository_count:,}',
        )

    stderr_console.print(cand_table)
    stderr_console.print()

    # With `err=True` typer still writes the prompt's last character
    # through `input()`, to stdout, for readline's sake: with stdout
    # sent to stderr meanwhile, none of the question is on stdout.
    with contextlib.redirect_stdout(sys.stderr):
        choice = typer.prompt(
            'Select a library number (or 0 to cancel)', default='0',
            show_default=False, err=True,
        )

    choice_idx: int | None
    try:
        choice_idx = int(choice)
    except ValueError:
        choice_idx = None

    if choice_idx == 0:
        # The person chose to stop: no failure, and no output.
        say(
            '[yellow]No selection made, exiting.[/yellow]',
            'No selection made', logger, 'info',
        )
        return

    if choice_idx is None or not 1 <= choice_idx <= len(candidates):
        # A failure: exiting 0, a typo in a script's answer read as a
        # query that found nothing.
        fail(
            f'[red]Invalid input {escape(repr(choice))}: choose a number '
            f'from 1 to {len(candidates)}, or 0 to cancel.[/red]',
            'Invalid selection', logger, choice=choice,
            candidates=len(candidates),
        )

    selected_name = candidates[choice_idx - 1].name

    try:
        # Step 2: Get detailed dependents for selected library
        results = service.get_library_dependents(
            query_repo, selected_name, language=language, limit=limit,
            direct_only=direct_only, ecosystem=ecosystem,
        )
    except Exception as e:
        _failed(e)

    if not results:
        say(
            f"[yellow]No dependents found for '{escape(selected_name)}'"
            '[/yellow]',
            'No dependents found', logger, library=selected_name,
        )
        return

    title = f"Dependents of {escape(selected_name)}"
    if direct_only:
        title += ' (direct only)'
    result_table = Table(title=title)
    result_table.add_column('Repository', style='green')
    result_table.add_column('Stars', style='yellow', justify='right')
    result_table.add_column('Version', style='cyan')
    result_table.add_column('Depends', style='magenta')
    result_table.add_column('URL', style='dim')

    for dep in results:
        result_table.add_row(
            escape(dep.full_name), f'{dep.stars:,}', escape(dep.version),
            dep.relationship, escape(dep.url),
        )

    console.print(result_table)


def _failed(error: Exception) -> NoReturn:
    """A query that failed: said on stderr, exiting 1, where it was
    printed on stdout and exited 0."""
    fail(
        f'[red]Error querying: {escape(str(error))}[/red]',
        'Error querying', logger, error=str(error),
    )
