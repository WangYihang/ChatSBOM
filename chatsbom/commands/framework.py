import csv
import json
import os
import re
import subprocess
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import structlog
import typer
from rich.progress import BarColumn
from rich.progress import MofNCompleteColumn
from rich.progress import Progress
from rich.progress import SpinnerColumn
from rich.progress import TaskProgressColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn
from rich.progress import TimeRemainingColumn

from chatsbom.core.container import get_container
from chatsbom.core.logging import console
from chatsbom.models.framework import Framework
from chatsbom.models.framework import FrameworkFactory

logger = structlog.get_logger('framework_command')
app = typer.Typer(help='Framework analysis commands')

# Common OpenAPI filenames (case-insensitive matching)
OPENAPI_FILENAMES = {
    'openapi.yaml', 'openapi.yml', 'openapi.json',
    'swagger.yaml', 'swagger.yml', 'swagger.json',
}

# File extensions to scan for content-based matching
SCANNABLE_EXTENSIONS = {'.yaml', '.yml', '.json'}

# Regex to quickly check if a file might be an OpenAPI spec (top-level key)
_OPENAPI_VERSION_RE = re.compile(
    r'^\s*["\']?(?:openapi|swagger)["\']?\s*[:=]\s*["\']?[\d.]+',
    re.MULTILINE,
)


@app.command()
def export(
    output: str = typer.Option(
        'framework_usage.csv', help='Output CSV file path',
    ),
):
    """
    Export framework usage data to a CSV file.
    """
    container = get_container()
    query_repo = container.get_query_repository()
    client = query_repo.client

    results = []

    console.print('[bold green]Querying usage for frameworks...[/bold green]')

    for framework_enum in Framework:
        try:
            framework = FrameworkFactory.create(framework_enum)
            package_names = framework.get_package_names()

            if not package_names:
                continue

            packages_str = "', '".join(package_names)

            query = f"""
            SELECT
                r.language,
                '{framework_enum.value}' as framework,
                a.version as framework_version,
                r.owner,
                r.repo,
                r.stars,
                r.default_branch,
                r.latest_release_tag,
                r.sbom_commit_sha
            FROM artifacts a
            JOIN repositories r ON a.repository_id = r.id
            WHERE a.name IN ('{packages_str}')
            """

            data = client.query(query).result_rows

            processed_data = []
            for row in data:
                language, framework_name, framework_version, owner, repo, stars, default_branch, latest_release, commit_sha = row

                language = str(language).lower() if language else ''
                framework_name = str(framework_name).lower()
                framework_version = str(framework_version).lower()
                owner = str(owner).lower()
                repo = str(repo).lower()
                stars = str(stars)
                default_branch = str(default_branch).lower()
                latest_release = str(
                    latest_release,
                ).lower() if latest_release else ''
                commit_sha = str(commit_sha).lower() if commit_sha else ''

                if latest_release:
                    url = f'https://github.com/{owner}/{repo}/releases/tag/{latest_release}'
                elif commit_sha:
                    url = f'https://github.com/{owner}/{repo}/tree/{commit_sha}'
                else:
                    url = f'https://github.com/{owner}/{repo}'

                processed_data.append([
                    language, framework_name, framework_version, owner, repo,
                    stars, default_branch, latest_release, commit_sha, url,
                ])

            results.extend(processed_data)
            console.print(
                f'Found [cyan]{len(data)}[/cyan] projects for [bold]{framework_enum.value}[/bold]',
            )

        except Exception as e:
            console.print(
                f'[bold red]Error querying for {framework_enum.value}: {e}[/bold red]',
            )

    try:
        with open(output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'language', 'framework', 'framework_version', 'owner',
                'repo', 'stars', 'default_branch', 'latest_release',
                'commit_sha', 'url',
            ])
            writer.writerows(results)

        console.print(
            f'[bold green]Successfully exported {len(results)} rows to {output}[/bold green]',
        )
    except Exception as e:
        console.print(f'[bold red]Failed to write CSV: {e}[/bold red]')


def _clone_repo(owner: str, repo: str, dest: Path, commit_sha: str | None = None) -> tuple[str, str, bool, str]:
    """Clone a single repo. Returns (owner, repo, success, message)."""
    repo_dir = dest / owner / repo
    if repo_dir.exists():
        return (owner, repo, True, 'already exists')

    repo_dir.parent.mkdir(parents=True, exist_ok=True)
    clone_url = f'https://github.com/{owner}/{repo}.git'

    try:
        result = subprocess.run(
            ['git', 'clone', '--depth=1', clone_url, str(repo_dir)],
            capture_output=True,
            text=True,
            timeout=300,
        )
        if result.returncode != 0:
            # Clean up partial clone
            if repo_dir.exists():
                subprocess.run(['rm', '-rf', str(repo_dir)], check=False)
            return (owner, repo, False, result.stderr.strip())
        return (owner, repo, True, 'cloned')
    except subprocess.TimeoutExpired:
        if repo_dir.exists():
            subprocess.run(['rm', '-rf', str(repo_dir)], check=False)
        return (owner, repo, False, 'timeout')
    except Exception as e:
        return (owner, repo, False, str(e))


@app.command()
def clone(
    input_csv: str = typer.Option(
        'framework_usage.csv', '--input', help='Input CSV file from export command',
    ),
    force: bool = typer.Option(
        False, help='Re-clone even if directory exists',
    ),
    workers: int = typer.Option(4, help='Number of concurrent clone workers'),
):
    """
    Shallow-clone repositories listed in the framework usage CSV.
    Repos are cloned into data/07-framework-repos/<owner>/<repo>/.
    """
    container = get_container()
    config = container.config
    dest = config.paths.framework_repos_dir
    dest.mkdir(parents=True, exist_ok=True)

    # Read CSV
    try:
        with open(input_csv, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except FileNotFoundError:
        console.print(f'[bold red]CSV file not found: {input_csv}[/bold red]')
        console.print('Run [cyan]chatsbom framework export[/cyan] first.')
        raise typer.Exit(1)

    # De-duplicate by (owner, repo)
    seen = set()
    repos_to_clone = []
    for row in rows:
        key = (row['owner'], row['repo'])
        if key in seen:
            continue
        seen.add(key)

        repo_dir = dest / row['owner'] / row['repo']
        if not force and repo_dir.exists():
            continue
        elif force and repo_dir.exists():
            subprocess.run(['rm', '-rf', str(repo_dir)], check=False)

        repos_to_clone.append(row)

    if not repos_to_clone:
        console.print(
            '[yellow]All repositories already cloned. Use --force to re-clone.[/yellow]',
        )
        return

    console.print(
        f'Cloning [cyan]{len(repos_to_clone)}[/cyan] repositories (depth=1) into [bold]{dest}[/bold]...',
    )

    cloned = 0
    failed = 0

    with Progress(
        SpinnerColumn(),
        TextColumn('[progress.description]{task.description}'),
        BarColumn(),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TextColumn('•'),
        TimeElapsedColumn(),
        TextColumn('•'),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task('Cloning repos...', total=len(repos_to_clone))

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {
                executor.submit(
                    _clone_repo,
                    row['owner'],
                    row['repo'],
                    dest,
                    row.get('commit_sha'),
                ): row
                for row in repos_to_clone
            }

            for future in as_completed(futures):
                owner, repo, success, message = future.result()
                if success:
                    cloned += 1
                    logger.info(
                        'Cloned', owner=owner,
                        repo=repo, status=message,
                    )
                else:
                    failed += 1
                    logger.error(
                        'Clone failed', owner=owner,
                        repo=repo, error=message,
                    )
                progress.advance(task)

    console.print(
        f'[bold green]Done![/bold green] Cloned: [cyan]{cloned}[/cyan], Failed: [red]{failed}[/red]',
    )


def _is_openapi_spec(filepath: Path) -> bool:
    """Check if a file is a valid OpenAPI/Swagger spec by parsing its structure."""
    try:
        text = filepath.read_text(encoding='utf-8', errors='ignore')[:16384]

        # Quick regex pre-check: must have a top-level openapi/swagger version
        if not _OPENAPI_VERSION_RE.search(text):
            return False

        # Try to parse and validate structure
        ext = filepath.suffix.lower()
        if ext == '.json':
            doc = json.loads(text)
        elif ext in {'.yaml', '.yml'}:
            # Lightweight YAML parsing: only check top-level keys
            # Avoid importing PyYAML (not in deps). Use regex instead.
            has_info = re.search(
                r'^\s*["\']?info["\']?\s*:', text, re.MULTILINE,
            )
            has_paths = re.search(
                r'^\s*["\']?(?:paths|webhooks|channels)["\']?\s*:',
                text, re.MULTILINE,
            )
            return bool(has_info and has_paths)
        else:
            return False

        # For JSON: check top-level structure
        if not isinstance(doc, dict):
            return False
        has_version_key = 'openapi' in doc or 'swagger' in doc
        has_info_key = 'info' in doc
        has_paths_key = any(
            k in doc for k in ('paths', 'webhooks', 'channels')
        )
        return has_version_key and has_info_key and has_paths_key

    except (OSError, PermissionError, json.JSONDecodeError, UnicodeDecodeError):
        return False


def _search_repo_for_openapi(repo_dir: Path, owner: str, repo: str) -> list[dict]:
    """Search a single cloned repo for OpenAPI spec files."""
    results = []

    for root, _dirs, files in os.walk(repo_dir):
        # Skip .git directory
        rel_root = Path(root).relative_to(repo_dir)
        if '.git' in rel_root.parts:
            continue

        for filename in files:
            filepath = Path(root) / filename
            rel_path = filepath.relative_to(repo_dir)
            match_type = None

            # Strategy 1: Filename match (still validate structure)
            if filename.lower() in OPENAPI_FILENAMES:
                match_type = 'filename'

            # Strategy 2: Content match (only for scannable extensions)
            elif filepath.suffix.lower() in SCANNABLE_EXTENSIONS:
                if _is_openapi_spec(filepath):
                    match_type = 'content'

            if match_type:
                results.append({
                    'owner': owner,
                    'repo': repo,
                    'file_path': str(rel_path),
                    'match_type': match_type,
                })

    return results


@app.command('search-openapi')
def search_openapi(
    output: str = typer.Option(
        'openapi_files.csv', help='Output CSV file path',
    ),
    input_csv: str = typer.Option(
        'framework_usage.csv', '--input',
        help='Input CSV file from export command',
    ),
):
    """
    Search cloned repositories for OpenAPI/Swagger spec files.

    Searches by both filename (openapi.yaml, swagger.json, etc.)
    and file content (looking for 'openapi:' or 'swagger:' keys).
    """
    from collections import defaultdict

    from rich.table import Table

    container = get_container()
    config = container.config
    repos_dir = config.paths.framework_repos_dir

    if not repos_dir.exists():
        console.print(
            f'[bold red]Repos directory not found: {repos_dir}[/bold red]',
        )
        console.print('Run [cyan]chatsbom framework clone[/cyan] first.')
        raise typer.Exit(1)

    # Enumerate all cloned repos: repos_dir/<owner>/<repo>/
    repo_dirs = []
    for owner_dir in sorted(repos_dir.iterdir()):
        if not owner_dir.is_dir():
            continue
        for repo_name_dir in sorted(owner_dir.iterdir()):
            if not repo_name_dir.is_dir():
                continue
            repo_dirs.append(
                (owner_dir.name, repo_name_dir.name, repo_name_dir),
            )

    if not repo_dirs:
        console.print('[yellow]No cloned repositories found.[/yellow]')
        return

    console.print(
        f'Searching [cyan]{len(repo_dirs)}[/cyan] repositories for OpenAPI specs...',
    )

    all_results: list[dict] = []

    with Progress(
        SpinnerColumn(),
        TextColumn('[progress.description]{task.description}'),
        BarColumn(),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TextColumn('•'),
        TimeElapsedColumn(),
        TextColumn('•'),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task('Searching...', total=len(repo_dirs))

        for owner, repo, repo_path in repo_dirs:
            try:
                results = _search_repo_for_openapi(repo_path, owner, repo)
                all_results.extend(results)
            except Exception as e:
                logger.error(
                    'Search failed', owner=owner,
                    repo=repo, error=str(e),
                )
            progress.advance(task)

    # Write output CSV
    try:
        with open(output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(
                f, fieldnames=['owner', 'repo', 'file_path', 'match_type'],
            )
            writer.writeheader()
            writer.writerows(all_results)

        console.print(
            f'[bold green]Found {len(all_results)} OpenAPI files '
            f'across {len(repo_dirs)} repos.[/bold green]',
        )
        console.print(f'Results written to [cyan]{output}[/cyan]')
    except Exception as e:
        console.print(f'[bold red]Failed to write CSV: {e}[/bold red]')

    # Build openapi files grouped by (owner, repo)
    openapi_by_repo: dict[tuple[str, str], list[str]] = defaultdict(list)
    for item in all_results:
        key = (item['owner'], item['repo'])
        openapi_by_repo[key].append(item['file_path'])

    if not openapi_by_repo:
        return

    # Read framework_usage.csv for metadata
    repo_metadata: dict[tuple[str, str], dict] = {}
    try:
        with open(input_csv, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                key = (row['owner'], row['repo'])
                if key not in repo_metadata:
                    repo_metadata[key] = row
    except FileNotFoundError:
        console.print(
            f'[yellow]Warning: {input_csv} not found, '
            'table will have limited metadata.[/yellow]',
        )

    # Display rich table
    table = Table(
        title='OpenAPI Spec Files by Project',
        show_lines=True,
    )
    table.add_column('Language', style='cyan')
    table.add_column('Framework', style='magenta')
    table.add_column('Version', style='dim')
    table.add_column('Owner', style='green')
    table.add_column('Repo', style='green')
    table.add_column('Stars', style='yellow', justify='right')
    table.add_column('Branch', style='dim')
    table.add_column('Tag', style='dim')
    table.add_column('OpenAPI Files', style='cyan')

    def _sort_key(item: tuple[tuple[str, str], list[str]]) -> tuple:
        (owner, repo), _files = item
        meta = repo_metadata.get((owner, repo), {})
        return (
            meta.get('language', ''),
            meta.get('framework', ''),
            -(int(meta.get('stars', 0) or 0)),
        )

    for (owner, repo), files in sorted(
        openapi_by_repo.items(), key=_sort_key,
    ):
        meta = repo_metadata.get((owner, repo), {})
        table.add_row(
            meta.get('language', ''),
            meta.get('framework', ''),
            meta.get('framework_version', ''),
            owner,
            repo,
            meta.get('stars', ''),
            meta.get('default_branch', ''),
            meta.get('latest_release', ''),
            ', '.join(files),
        )

    console.print()
    console.print(table)

    # Summary: ratio of projects with OpenAPI per (language, framework)
    total_by_group: dict[
        tuple[str, str],
        set[tuple[str, str]],
    ] = defaultdict(set)
    openapi_by_group: dict[
        tuple[str, str],
        set[tuple[str, str]],
    ] = defaultdict(set)

    for (owner, repo), meta in repo_metadata.items():
        group_key = (meta.get('language', ''), meta.get('framework', ''))
        total_by_group[group_key].add((owner, repo))
        if (owner, repo) in openapi_by_repo:
            openapi_by_group[group_key].add((owner, repo))

    summary = Table(title='OpenAPI Coverage by Language / Framework')
    summary.add_column('Language', style='cyan')
    summary.add_column('Framework', style='magenta')
    summary.add_column('Total', justify='right')
    summary.add_column('With OpenAPI', justify='right', style='green')
    summary.add_column('Ratio', justify='right', style='yellow')

    for group_key in sorted(total_by_group.keys()):
        total = len(total_by_group[group_key])
        with_openapi = len(openapi_by_group.get(group_key, set()))
        ratio = f'{with_openapi / total * 100:.1f}%' if total else '0.0%'
        summary.add_row(
            group_key[0], group_key[1],
            str(total), str(with_openapi), ratio,
        )

    console.print()
    console.print(summary)
