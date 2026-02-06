from typing import Any

import clickhouse_connect
import typer
from rich.console import Console
from rich.table import Table

console = Console()


def main(
    library: str = typer.Argument(
        ...,
        help='Library name to search for (e.g. requests)',
    ),
    host: str = typer.Option('localhost', help='ClickHouse host'),
    port: int = typer.Option(8123, help='ClickHouse http port'),
    user: str = typer.Option('guest', help='ClickHouse user (default: guest)'),
    password: str = typer.Option(
        'guest', help='ClickHouse password (default: guest)',
    ),
    database: str = typer.Option('sbom', help='ClickHouse database'),
    limit: int = typer.Option(50, help='Max results to display'),
    language: str = typer.Option(
        None, help='Filter by programming language (e.g. python, go)',
    ),
):
    """
    Search for repositories that depend on a specific library.
    Query is performed using the read-only 'guest' user.
    """
    from sbom_insight.core.clickhouse import check_clickhouse_connection

    check_clickhouse_connection(
        host=host, port=port, user=user, password=password,
        database=database, console=console, require_database=True,
    )

    client = clickhouse_connect.get_client(
        host=host, port=port, username=user, password=password, database=database,
    )

    # Step 1: Find candidates using fuzzy search
    console.print(f"[dim]Searching for libraries match '{library}'...[/dim]")

    lang_filter = ''
    params: dict[str, Any] = {'pattern': f"%{library}%"}

    if language:
        lang_filter = 'AND r.language = {language:String}'
        params['language'] = language

    candidate_query = f"""
    SELECT a.name, count() as cnt
    FROM artifacts a
    JOIN repositories r ON a.repository_id = r.id
    WHERE a.name ILIKE {{pattern:String}} {lang_filter}
    GROUP BY a.name
    ORDER BY cnt DESC
    LIMIT 20
    """
    try:
        candidates_res = client.query(candidate_query, parameters=params)
        candidates = candidates_res.result_rows
    except Exception as e:
        console.print(f"[red]Candidate search failed: {e}[/red]")
        raise typer.Exit(code=1)

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

        from rich.prompt import Prompt

        choices = [str(i) for i in range(1, len(candidates) + 1)]
        choice = Prompt.ask('Select Library', choices=choices, default='1')

        selected_library = candidates[int(choice) - 1][0]
        console.print(f"[bold]Selected:[/bold] {selected_library}")

    # Count total results first
    count_query = f"""
    SELECT count()
    FROM artifacts a
    JOIN repositories r ON a.repository_id = r.id
    WHERE a.name = {{library:String}} {lang_filter}
    """

    params = {'library': selected_library}
    if language:
        params['language'] = language

    try:
        total_count = client.query(
            count_query, parameters=params,
        ).result_rows[0][0]
    except Exception as e:
        console.print(f"[red]Count query failed: {e}[/red]")
        raise typer.Exit(code=1)

    if total_count == 0:
        console.print(
            f"[yellow]No repositories found depending on '{selected_library}'.[/yellow]",
        )
        return

    query = f"""
    SELECT
        r.owner,
        r.repo,
        r.stars,
        a.version,
        r.url
    FROM artifacts AS a
    JOIN repositories AS r ON a.repository_id = r.id
    WHERE a.name = {{library:String}} {lang_filter}
    ORDER BY r.stars DESC
    LIMIT {{limit:UInt32}}
    """

    try:
        # Re-use params but update limit
        params['limit'] = limit
        result = client.query(query, parameters=params)
    except Exception as e:
        console.print(f"[red]Query failed: {e}[/red]")
        raise typer.Exit(code=1)

    rows = result.result_rows
    table = Table(
        title=f"Dependents of '{selected_library}' (Top {limit} of {total_count})",
    )
    table.add_column('Owner', style='cyan')
    table.add_column('Repo', style='green')
    table.add_column('Stars', style='magenta', justify='right')
    table.add_column('Version', style='yellow')
    table.add_column('URL', style='blue')

    for row in rows:
        owner, repo, stars, version, url = row
        table.add_row(owner, repo, str(stars), version, url)

    console.print(table)
    console.print(
        f"[dim]Note: Shown top {len(rows)} results of {total_count} total, sorted by stars.[/dim]",
    )
