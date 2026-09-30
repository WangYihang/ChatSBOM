import csv
from pathlib import Path

import structlog
import typer
from rich.markup import escape

from chatsbom.core.container import get_container
from chatsbom.core.diagnostics import fail
from chatsbom.core.diagnostics import say
from chatsbom.core.logging import console
from chatsbom.services.openapi_service import OpenApiService
from chatsbom.warehouse import connect

logger = structlog.get_logger('openapi_candidates')
app = typer.Typer()


@app.callback(invoke_without_command=True)
def main(
    output: str = typer.Option(
        'openapi_candidates.csv', help='Output CSV file path',
    ),
    warehouse: Path | None = typer.Option(
        None,
        '--warehouse',
        '-w',
        help='The warehouse to read; data/warehouse.duckdb by default',
    ),
):
    """
    Find framework-using projects that contain OpenAPI spec files.

    Which projects use a framework, and at what version, is read from
    the warehouse `warehouse build` makes: each project's current scan,
    of the corpus. Whether a project has a spec is read from its tree.
    """
    source = (
        warehouse if warehouse is not None
        else get_container().config.paths.warehouse_path
    )
    if not source.is_file():
        fail(
            f'[bold red]Error:[/] no warehouse at {escape(str(source))}: '
            'the candidates are read from it. Run [cyan]chatsbom '
            'warehouse build[/] first.',
            'No warehouse to read the candidates from', logger,
            warehouse=str(source),
        )
    service = OpenApiService()

    console.print('[bold green]Querying usage for frameworks...[/bold green]')
    with connect(source, read_only=True) as con:
        result = service.find_candidates(con)

    if not result.candidates:
        # Nothing found is no failure, and the status stays 0. Nor is it
        # output: said where the logs go, as `db edges` said it (#114).
        say(
            '[yellow]No OpenAPI specs found.[/yellow]',
            'No OpenAPI specs found', logger,
        )
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
            f'[bold green]Total: Found {len(result.candidates)} OpenAPI specs across {total_matched} unique projects → {escape(str(output))}[/bold green]',
        )
    except Exception as e:
        # A failure, on stderr: printed on stdout, with status 0, a script
        # took the CSV it was never given for a success (#124).
        fail(
            f'[bold red]Failed to process results: {escape(str(e))}[/bold red]',
            'Failed to process results', logger,
            output=output, error=str(e),
        )
