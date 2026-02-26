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
    result = service.find_candidates(query_repo.client)

    if not result.candidates:
        console.print('[yellow]No OpenAPI specs found.[/yellow]')
        return

    try:
        from rich.table import Table

        with open(output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'language', 'framework', 'framework_version', 'owner',
                'repo', 'stars', 'default_branch', 'latest_release',
                'commit_sha', 'url', 'openapi_file', 'openapi_url',
                'matched_dependencies', 'has_openapi_file', 'has_openapi_deps', 'generation_command',
            ])
            # Sort candidates by language (asc), framework (asc), and stars (desc)
            sorted_candidates = sorted(
                result.candidates,
                key=lambda c: (c.language, c.framework, -c.stars),
            )

            for candidate in sorted_candidates:
                writer.writerow(candidate.to_csv_row())

        table = Table(title='OpenAPI Candidate Statistics')
        table.add_column('Language', style='cyan')
        table.add_column('Framework', style='magenta')
        table.add_column('Matched', justify='right', style='green')
        table.add_column('File only', justify='right', style='blue')
        table.add_column('Deps only', justify='right', style='blue')
        table.add_column('Both', justify='right', style='blue')
        table.add_column('Total', justify='right', style='blue')
        table.add_column('Percentage', justify='right', style='yellow')

        total_matched = len({(c.owner, c.repo) for c in result.candidates})

        # Sort by language then framework
        sorted_stats = sorted(
            result.stats, key=lambda s: (s.language, s.framework),
        )

        for stat in sorted_stats:
            table.add_row(
                stat.language or '-',
                stat.framework,
                str(stat.matched_projects),
                str(stat.count_file_only),
                str(stat.count_deps_only),
                str(stat.count_both),
                str(stat.total_projects),
                f'{stat.percentage:.1f}%',
            )

        # Calculate global totals
        g_matched = sum(s.matched_projects for s in result.stats)
        g_file_only = sum(s.count_file_only for s in result.stats)
        g_deps_only = sum(s.count_deps_only for s in result.stats)
        g_both = sum(s.count_both for s in result.stats)
        g_total = sum(s.total_projects for s in result.stats)
        g_percentage = (g_matched / g_total * 100) if g_total > 0 else 0

        table.add_section()
        table.add_row(
            'Total',
            '',
            str(g_matched),
            str(g_file_only),
            str(g_deps_only),
            str(g_both),
            str(g_total),
            f'{g_percentage:.1f}%',
            style='bold yellow',
        )

        console.print(table)
        console.print(
            f'[bold green]Total: Found {len(result.candidates)} OpenAPI specs across {total_matched} unique projects → {output}[/bold green]',
        )
    except Exception as e:
        console.print(f'[bold red]Failed to process results: {e}[/bold red]')
