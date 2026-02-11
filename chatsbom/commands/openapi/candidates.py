import csv

import typer

from chatsbom.core.container import get_container
from chatsbom.core.logging import console
from chatsbom.services.openapi_service import OpenApiService

app = typer.Typer()


@app.callback(invoke_without_command=True)
def main(
    output: str = typer.Option(
        'openapi_candidates.csv', help='Output CSV file path',
    ),
):
    """
    Find framework-using projects that contain OpenAPI spec files.
    """
    container = get_container()
    query_repo = container.get_query_repository()
    service = OpenApiService()

    console.print('[bold green]Querying usage for frameworks...[/bold green]')
    results = service.find_candidates(query_repo.client)

    if not results:
        console.print('[yellow]No OpenAPI specs found.[/yellow]')
        return

    try:
        with open(output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'language', 'framework', 'framework_version', 'owner',
                'repo', 'stars', 'default_branch', 'latest_release',
                'commit_sha', 'url', 'openapi_file', 'openapi_url',
            ])
            writer.writerows(results)

        console.print(
            f'[bold green]Found {len(results)} OpenAPI specs across {len({(r[3], r[4]) for r in results})} projects → {output}[/bold green]',
        )
    except Exception as e:
        console.print(f'[bold red]Failed to write CSV: {e}[/bold red]')
