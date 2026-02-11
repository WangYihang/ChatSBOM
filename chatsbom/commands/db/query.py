import structlog
import typer
from rich.table import Table

from chatsbom.core.clickhouse import check_clickhouse_connection
from chatsbom.core.container import get_container
from chatsbom.core.logging import console
from chatsbom.services.db_service import DbService

logger = structlog.get_logger('db_query')
app = typer.Typer()


@app.callback(invoke_without_command=True)
def main(
    component: str = typer.Argument(..., help='Component name to search for'),
    limit: int = typer.Option(10, help='Max results'),
    language: str = typer.Option(None, help='Filter by repository language'),
):
    """Query dependencies across repositories."""

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
        console=console,
        require_database=True,
    )

    query_repo = container.get_query_repository()
    service = DbService()

    try:
        # Step 1: Search for library candidates
        candidates = service.search_library(
            query_repo, component, language=language, limit=limit,
        )

        if not candidates:
            console.print(
                f"[yellow]No libraries found matching '{component}'[/yellow]",
            )
            return

        # Display candidates
        cand_table = Table(title=f"Library Candidates: {component}")
        cand_table.add_column('#', style='dim')
        cand_table.add_column('Library Name', style='cyan')
        cand_table.add_column('Repository Count', style='magenta')

        for idx, (name, count) in enumerate(candidates, start=1):
            cand_table.add_row(str(idx), name, str(count))

        console.print(cand_table)

        # Prompt for selection
        console.print()
        choice = typer.prompt(
            'Select a library number (or 0 to cancel)', default='0', show_default=False,
        )

        try:
            choice_idx = int(choice)
        except ValueError:
            console.print('[red]Invalid input, exiting.[/red]')
            return

        if choice_idx < 1 or choice_idx > len(candidates):
            console.print('[yellow]No selection made, exiting.[/yellow]')
            return

        selected_name = candidates[choice_idx - 1][0]

        # Step 2: Get detailed dependents for selected library
        results = service.get_library_dependents(
            query_repo, selected_name, language=language, limit=limit,
        )

        if not results:
            console.print(
                f"[yellow]No dependents found for '{selected_name}'[/yellow]",
            )
            return

        result_table = Table(title=f"Dependents of {selected_name}")
        result_table.add_column('Owner', style='green')
        result_table.add_column('Repo', style='green')
        result_table.add_column('Stars', style='yellow')
        result_table.add_column('Version', style='cyan')
        result_table.add_column('URL', style='dim')

        for owner, repo, stars, version, url in results:
            result_table.add_row(owner, repo, str(stars), version, url)

        console.print(result_table)

    except Exception as e:
        console.print(f"[red]Error querying: {e}[/red]")
