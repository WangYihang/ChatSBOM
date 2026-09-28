import csv
from pathlib import Path

import typer
from rich.markup import escape

from chatsbom.core.container import get_container
from chatsbom.core.extras import require_extra
from chatsbom.core.logging import console
from chatsbom.services.openapi_service import OpenApiService

app = typer.Typer()


@app.callback(invoke_without_command=True)
def main(
    input_csv: str = typer.Option(
        'openapi_candidates.csv', '--input', help='Input CSV from candidates command',
    ),
    code_endpoints_dir: str = typer.Option(
        'data/08-code-endpoints', help='Directory containing external code-derived endpoints',
    ),
    output_data: str = typer.Option(
        'openapi_drift_data.csv', help='Output analysis data CSV (the data contract)',
    ),
):
    """
    Analyze the drift between OpenAPI specs and actual code endpoints.

    One row per candidate, at the release or commit it was cloned at:
    the precision, recall and F1 of the spec's (method, path) pairs
    against those the code implements.
    """
    # Imported here rather than at the top, where every command paid for
    # it at start-up; and first, since it comes with an extra.
    require_extra('openapi', 'pandas')
    import pandas as pd

    # Files alone, read and written: the database connection opened here
    # was never used, and failed the command wherever there was none.
    config = get_container().config
    service = OpenApiService()

    code_dir = Path(code_endpoints_dir)
    repo_base = config.paths.framework_repos_dir

    try:
        with open(input_csv, encoding='utf-8') as f:
            candidates = list(csv.DictReader(f))
    except FileNotFoundError:
        console.print(
            f'[bold red]CSV not found: {escape(str(input_csv))}[/bold red]',
        )
        raise typer.Exit(1)

    drift_results = service.analyze_drift(candidates, code_dir, repo_base)

    if drift_results:
        pd.DataFrame(drift_results).to_csv(output_data, index=False)
        console.print(
            f"[bold green]Analysis data saved to {escape(str(output_data))}[/bold green]",
        )
    else:
        console.print('[yellow]No drift data collected.[/yellow]')
