import structlog
import typer
from rich.markup import escape

from chatsbom.core.container import get_container
from chatsbom.core.extras import require_extra
from chatsbom.core.logging import console
from chatsbom.core.logging import progress_bar
from chatsbom.services.openapi_service import OpenApiService

logger = structlog.get_logger('openapi_list_paths')
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
    # Imported here rather than at the top, where every command paid for
    # it at start-up; and first, since it comes with an extra.
    require_extra('openapi', 'pandas')
    import pandas as pd

    container = get_container()
    config = container.config
    service = OpenApiService()

    repo_base = config.paths.framework_repos_dir

    try:
        # Every column as the text the file holds, as `clone`, `drift`
        # and `stats` read it with `csv.DictReader`. Left to guess types,
        # pandas read a column of tags like `1.10` as the number 1.1
        # (#47), `07` as 7, and `null` or `NA` as no value, and the
        # snapshot looked for was one `clone` never made.
        df_candidates = pd.read_csv(
            input_csv, dtype=str, keep_default_na=False,
        )
    except FileNotFoundError:
        console.print(
            f'[bold red]CSV not found: {escape(input_csv)}[/bold red]',
        )
        raise typer.Exit(1)

    path_results = []

    from rich.progress import BarColumn
    from rich.progress import SpinnerColumn
    from rich.progress import TaskProgressColumn
    from rich.progress import TextColumn
    from rich.progress import TimeRemainingColumn

    with progress_bar(
        SpinnerColumn(),
        TextColumn('[progress.description]{task.description}'),
        BarColumn(),
        TaskProgressColumn(),
        TimeRemainingColumn(),
        TextColumn('[blue]{task.fields[repo]}'),
    ) as progress:
        task = progress.add_task(
            'Extracting paths...',
            total=len(df_candidates),
            repo='',
        )

        for c in df_candidates.itertuples(index=False):
            # Safe access to columns
            owner = str(c.owner)
            repo_name = str(c.repo)
            progress.update(task, repo=f"{owner}/{repo_name}")

            latest_release = str(c.latest_release).strip(
            ) if pd.notna(c.latest_release) else ''
            commit_sha = str(c.commit_sha).strip(
            ) if pd.notna(c.commit_sha) else ''
            default_branch = str(c.default_branch).strip(
            ) if pd.notna(c.default_branch) else ''
            # An empty cell is '' now, where it was NaN: 'HEAD' for it
            # still.
            tag = latest_release or commit_sha or default_branch or 'HEAD'

            # Determine snapshot directory
            snapshot_dir = repo_base / owner / repo_name / \
                service.get_version_path(latest_release, commit_sha)

            if not snapshot_dir.exists():
                progress.advance(task)
                continue

            project_path_count = 0
            parsed_files_count = 0
            seen_in_project = set()

            try:
                # Find all files within snapshot_dir
                files = [
                    p.relative_to(snapshot_dir).as_posix()
                    for p in snapshot_dir.rglob('*') if p.is_file()
                ]
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
                                if (method, path) not in seen_in_project:
                                    seen_in_project.add((method, path))
                                    project_path_count += 1
                                    path_results.append({
                                        'language': getattr(c, 'language', ''),
                                        'framework': getattr(c, 'framework', ''),
                                        'owner': owner,
                                        'repo': repo_name,
                                        'stars': int(getattr(c, 'stars', 0)),
                                        'tag': tag,
                                        'method': method,
                                        'path': path,
                                    })
                    except Exception:
                        continue

                if project_path_count > 0:
                    # Through the logger, which prints above the bar, and
                    # as JSON when a machine reads stderr.
                    logger.info(
                        'OpenAPI paths found',
                        repo=f'{owner}/{repo_name}',
                        language=getattr(c, 'language', 'unknown'),
                        framework=getattr(c, 'framework', 'unknown'),
                        stars=int(getattr(c, 'stars', 0)),
                        paths=project_path_count,
                        files=parsed_files_count,
                        _style='dim',
                    )
            except Exception:
                pass

            progress.advance(task)

    if path_results:
        df_results = pd.DataFrame(path_results)
        # Sorting priority: language, framework, stars (desc), owner, repo, tag, path, method
        sort_cols = [
            'language', 'framework', 'stars',
            'owner', 'repo', 'tag', 'path', 'method',
        ]
        ascending = [True, True, False, True, True, True, True, True]

        df_results = df_results.sort_values(by=sort_cols, ascending=ascending)
        df_results.to_csv(output_csv, index=False)
        console.print(
            f"[bold green]Path list saved to {escape(output_csv)} ({len(df_results)} unique entries)[/bold green]",
        )
    else:
        console.print('[yellow]No OpenAPI paths found.[/yellow]')
