import typer
from rich.console import Console
from rich.prompt import Prompt
from rich.table import Table

from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors

console = Console()


@handle_errors
def main(
    library: str = typer.Argument(
        ...,
        help='Library name to search for (e.g. requests)',
    ),
    host: str = typer.Option(None, help='ClickHouse host'),
    port: int = typer.Option(None, help='ClickHouse http port'),
    database: str = typer.Option(None, help='ClickHouse database'),
    limit: int = typer.Option(50, help='Max results to display'),
    language: str = typer.Option(
        None, help='Filter by programming language (e.g. python, go)',
    ),
):
    """
    Search for repositories that depend on a specific library (Guest).
    """
    container = get_container()

    # CLI Overrides
    if host:
        container.config._db_base.host = host
    if port:
        container.config._db_base.port = port
    if database:
        container.config._db_base.database = database

    with container.get_query_repository() as repo:
        # Step 1: Find candidates using fuzzy search
        console.print(
            f"[dim]Searching for libraries match '{library}'...[/dim]",
        )

        candidates = repo.search_library_candidates(library, language=language)

        selected_library = library
        if not candidates:
            console.print(
                f"[yellow]No exact or partial matches found for '{library}'{' in ' + language if language else ''}. Using input as-is.[/yellow]",
            )
        else:
            ctable = Table(
                title=f"Multiple libraries match '{library}'. Please select one:",
            )
            ctable.add_column('No.', style='cyan', justify='right')
            ctable.add_column('Library Name', style='green')
            ctable.add_column('Projects Using', style='magenta')

            for idx, (name, cnt) in enumerate(candidates, 1):
                ctable.add_row(str(idx), name, str(cnt))

            console.print(ctable)

            # Interactive prompts handled here, logic in repo
            # If strictly separating View from Logic, prompts are View.
            choices = [str(i) for i in range(1, len(candidates) + 1)]
            choice = Prompt.ask('Select Library', choices=choices, default='1')
            selected_library = candidates[int(choice) - 1][0]
            console.print(f"[bold]Selected:[/bold] {selected_library}")

        # Count total results first
        total_count = repo.get_dependent_count(
            selected_library, language=language,
        )

        if total_count == 0:
            console.print(
                f"[yellow]No repositories found depending on '{selected_library}'.[/yellow]",
            )
            return

        # Get detailed results
        rows = repo.get_dependents(
            selected_library, language=language, limit=limit,
        )

        table = Table(
            title=f"Dependents of '{selected_library}' (Top {limit} of {total_count})",
        )
        table.add_column('Owner', style='cyan')
        table.add_column('Repo', style='green')
        table.add_column('Stars', style='magenta', justify='right')
        table.add_column('Version', style='yellow')
        table.add_column('URL', style='blue')

        for row in rows:
            owner, repo_name, stars, version, url = row
            table.add_row(owner, repo_name, str(stars), version, url)

        console.print(table)
        console.print(
            f"[dim]Note: Shown top {len(rows)} results of {total_count} total, sorted by stars.[/dim]",
        )


if __name__ == '__main__':
    typer.run(main)
