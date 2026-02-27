import csv

import pandas as pd
import typer

from chatsbom.core.container import get_container
from chatsbom.core.logging import console
from chatsbom.services.openapi_service import OpenApiService

app = typer.Typer()


@app.callback(invoke_without_command=True)
def main(
    input_csv: str = typer.Option(
        'openapi_candidates.csv', '--input', help='Input CSV from candidates command',
    ),
    output_csv: str = typer.Option(
        'openapi_paths.csv', '--output', help='Output CSV containing paths for each OpenAPI file',
    ),
):
    """
    Export the list of paths (endpoints) for each OpenAPI file found in the cloned repositories.
    """
    container = get_container()
    config = container.config
    service = OpenApiService()

    repo_base = config.paths.framework_repos_dir

    try:
        with open(input_csv, encoding='utf-8') as f:
            candidates = list(csv.DictReader(f))
    except FileNotFoundError:
        console.print(f'[bold red]CSV not found: {input_csv}[/bold red]')
        raise typer.Exit(1)

    path_results = []

    from rich.progress import (
        Progress,
        TextColumn,
        BarColumn,
        TaskProgressColumn,
        TimeRemainingColumn,
        SpinnerColumn,
    )

    with Progress(
        SpinnerColumn(),
        TextColumn('[progress.description]{task.description}'),
        BarColumn(),
        TaskProgressColumn(),
        TimeRemainingColumn(),
        TextColumn('[blue]{task.fields[repo]}'),
        console=console,
    ) as progress:
        task = progress.add_task(
            'Extracting paths...',
            total=len(candidates),
            repo='',
        )

        for c in candidates:
            owner = c['owner']
            repo_name = c['repo']
            progress.update(task, repo=f"{owner}/{repo_name}")

            latest_release = c.get('latest_release', '').strip()
            commit_sha = c.get('commit_sha', '').strip()

            # Identify the version tag or commit for the candidate
            tag = latest_release if latest_release else commit_sha
            if not tag:
                tag = c.get('default_branch', 'HEAD').strip()

            # Determine snapshot directory
            snapshot_dir = repo_base / owner / repo_name / \
                service.get_version_path(latest_release, commit_sha)

            if not snapshot_dir.exists():
                progress.advance(task)
                continue

            project_path_count = 0
            parsed_files_count = 0
            seen_in_project = set()  # Track (method, path) for the current project

            try:
                # Find all files within snapshot_dir
                files = []
                for p in snapshot_dir.rglob('*'):
                    if p.is_file():
                        try:
                            rel_path = p.relative_to(snapshot_dir).as_posix()
                            files.append(rel_path)
                        except ValueError:
                            pass

                openapi_files = service.find_openapi_files(files)
                for f_path in openapi_files:
                    try:
                        full_path = snapshot_dir / f_path
                        with open(full_path, encoding='utf-8') as f:
                            content = f.read()

                        is_yaml = f_path.lower().endswith(('.yaml', '.yml'))
                        endpoints = service.parse_openapi_spec(
                            content, is_yaml,
                        )

                        if endpoints:
                            parsed_files_count += 1
                            for method, path in endpoints:
                                # Deduplicate (method, path) per project
                                if (method, path) not in seen_in_project:
                                    seen_in_project.add((method, path))
                                    project_path_count += 1
                                    path_results.append({
                                        'language': c.get('language', ''),
                                        'framework': c.get('framework', ''),
                                        'owner': owner,
                                        'repo': repo_name,
                                        'stars': c.get('stars', 0),
                                        'tag': tag,
                                        'method': method,
                                        'path': path,
                                    })
                    except Exception:
                        continue

                if project_path_count > 0:
                    lang = c.get('language', 'unknown')
                    fw = c.get('framework', 'unknown')
                    stars = c.get('stars', 0)
                    progress.console.print(
                        f"[dim]  - {owner}/{repo_name} [[cyan]{lang}[/cyan]/[magenta]{fw}[/magenta]] ({stars}⭐): Found {project_path_count} paths in {parsed_files_count} files[/dim]",
                    )
            except Exception:
                pass

            progress.advance(task)

    if path_results:
        df = pd.DataFrame(path_results)
        # Sort by path then method
        df = df.sort_values(by=['path', 'method'])
        df.to_csv(output_csv, index=False)
        console.print(
            f"[bold green]Path list saved to {output_csv} ({len(path_results)} entries)[/bold green]",
        )
    else:
        console.print('[yellow]No OpenAPI paths found.[/yellow]')
