import csv
import json
import subprocess
from collections import defaultdict
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

logger = structlog.get_logger('openapi_command')
app = typer.Typer(help='OpenAPI discovery and analysis')

# Common OpenAPI filenames (case-insensitive matching)
OPENAPI_FILENAMES = {
    'openapi.yaml', 'openapi.yml', 'openapi.json',
    'swagger.yaml', 'swagger.yml', 'swagger.json',
}


def _find_openapi_files(files: list[str]) -> list[str]:
    """
    Check if any files in the tree match OpenAPI filename patterns.
    Returns list of matching file paths.
    """
    matches = []
    for filepath in files:
        basename = filepath.rsplit('/', 1)[-1].lower()
        if basename in OPENAPI_FILENAMES:
            matches.append(filepath)
    return matches


@app.command('candidates')
def candidates(
    output: str = typer.Option(
        'openapi_candidates.csv', help='Output CSV file path',
    ),
):
    """
    Find framework-using projects that contain OpenAPI spec files.

    Combines framework usage data (from DB) with pre-fetched file trees
    (from `chatsbom github tree`) to identify candidate projects.
    """
    container = get_container()
    config = container.config
    query_repo = container.get_query_repository()
    client = query_repo.client

    # Step 1: Check if tree dir exists (sanity check)
    if not config.paths.tree_dir.exists():
        console.print(
            '[bold red]No file tree data found.[/bold red]\n'
            'Run [cyan]chatsbom github tree[/cyan] first.',
        )
        raise typer.Exit(1)

    # Step 2: Query framework usage from DB
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

            framework_total = 0
            framework_matched = 0

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

                framework_total += 1

                # Step 3: Check file tree for OpenAPI files
                # Load tree from sharded file: data/05-github-tree/{language}/{owner}/{repo}/{ref}/{sha}/tree.json
                ref = latest_release if latest_release else default_branch
                tree_file = config.paths.get_tree_file_path(
                    language, owner, repo, ref, commit_sha,
                )

                if not tree_file.exists():
                    # Fallback for missing language folder or different commit?
                    # Try to find any tree file for this repo?
                    # For now, strict match on SHA is best as trees change.
                    continue

                try:
                    with open(tree_file, encoding='utf-8') as f:
                        tree_files = json.load(f)
                except (OSError, json.JSONDecodeError):
                    continue

                openapi_files = _find_openapi_files(tree_files)
                if not openapi_files:
                    continue

                framework_matched += 1

                if latest_release:
                    url = f'https://github.com/{owner}/{repo}/releases/tag/{latest_release}'
                elif commit_sha:
                    url = f'https://github.com/{owner}/{repo}/tree/{commit_sha}'
                else:
                    url = f'https://github.com/{owner}/{repo}'

                results.append([
                    language, framework_name, framework_version, owner, repo,
                    stars, default_branch, latest_release, commit_sha, url,
                    '; '.join(openapi_files),
                ])

            console.print(
                f'[bold]{framework_enum.value}[/bold]: '
                f'[cyan]{framework_matched}[/cyan]/{framework_total} projects have OpenAPI specs',
            )

        except Exception as e:
            console.print(
                f'[bold red]Error querying for {framework_enum.value}: {e}[/bold red]',
            )

    # Step 4: Write output CSV
    try:
        with open(output, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                'language', 'framework', 'framework_version', 'owner',
                'repo', 'stars', 'default_branch', 'latest_release',
                'commit_sha', 'url', 'openapi_files',
            ])
            writer.writerows(results)

        console.print(
            f'[bold green]Found {len(results)} candidates with OpenAPI specs → {output}[/bold green]',
        )
    except Exception as e:
        console.print(f'[bold red]Failed to write CSV: {e}[/bold red]')


def _dir_size(path: Path) -> int:
    """Return total size in bytes of a directory tree."""
    total = 0
    try:
        for entry in path.rglob('*'):
            if entry.is_file():
                total += entry.stat().st_size
    except OSError:
        pass
    return total


def _format_size(size_bytes: int) -> str:
    """Format bytes into a human-readable string."""
    if size_bytes < 1024:
        return f'{size_bytes}B'
    elif size_bytes < 1024 * 1024:
        return f'{size_bytes / 1024:.1f}KB'
    elif size_bytes < 1024 * 1024 * 1024:
        return f'{size_bytes / (1024 * 1024):.1f}MB'
    else:
        return f'{size_bytes / (1024 * 1024 * 1024):.2f}GB'


def _version_path(tag: str | None, commit_sha: str | None) -> Path:
    """Build a two-level version path (<ref>/<sha>) matching content_service pattern."""
    ref = tag.strip() if tag else 'HEAD'
    sha = commit_sha.strip() if commit_sha else 'HEAD'
    return Path(ref) / sha


def _clone_repo(
    owner: str, repo: str, dest: Path,
    tag: str | None = None, commit_sha: str | None = None,
) -> tuple[str, str, bool, str, str]:
    """Clone a single repo. Returns (owner, repo, success, message, size)."""
    ver = _version_path(tag, commit_sha)
    repo_dir = dest / owner / repo / ver
    if repo_dir.exists():
        size = _format_size(_dir_size(repo_dir))
        return (owner, repo, True, 'already exists', size)

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
            return (owner, repo, False, result.stderr.strip(), '')
        size = _format_size(_dir_size(repo_dir))
        return (owner, repo, True, 'cloned', size)
    except subprocess.TimeoutExpired:
        if repo_dir.exists():
            subprocess.run(['rm', '-rf', str(repo_dir)], check=False)
        return (owner, repo, False, 'timeout', '')
    except Exception as e:
        return (owner, repo, False, str(e), '')


@app.command()
def clone(
    input_csv: str = typer.Option(
        'openapi_candidates.csv', '--input', help='Input CSV file from candidates command',
    ),
    force: bool = typer.Option(
        False, help='Re-clone even if directory exists',
    ),
    workers: int = typer.Option(4, help='Number of concurrent clone workers'),
    top: int = typer.Option(
        0, help='Limit to top N projects per framework (by stars). 0 means no limit.',
    ),
):
    """
    Shallow-clone repositories listed in the candidates CSV.
    Repos are cloned into .repositories/<owner>/<repo>/<version>/.
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
        console.print('Run [cyan]chatsbom openapi candidates[/cyan] first.')
        raise typer.Exit(1)

    # Filter to top N per framework if --top is specified
    if top > 0:
        framework_groups: dict[str, list[dict]] = defaultdict(list)
        for row in rows:
            framework_groups[row.get('framework', '')].append(row)

        filtered_rows = []
        for framework, group in framework_groups.items():
            group.sort(key=lambda r: int(r.get('stars', 0) or 0), reverse=True)
            filtered_rows.extend(group[:top])
            console.print(
                f'Framework [bold]{framework}[/bold]: keeping top {min(top, len(group))}/{len(group)} projects',
            )
        rows = filtered_rows

    # De-duplicate by (owner, repo)
    seen = set()
    repos_to_clone = []
    for row in rows:
        key = (row['owner'], row['repo'])
        if key in seen:
            continue
        seen.add(key)

        ver = _version_path(row.get('latest_release'), row.get('commit_sha'))
        repo_dir = dest / row['owner'] / row['repo'] / ver
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
                    row.get('latest_release'),
                    row.get('commit_sha'),
                ): row
                for row in repos_to_clone
            }

            for future in as_completed(futures):
                row = futures[future]
                owner, repo, success, message, size = future.result()
                ref = row.get('latest_release') or 'HEAD'
                sha = row.get('commit_sha') or ''
                if success:
                    cloned += 1
                    logger.info(
                        'Cloned', owner=owner,
                        repo=repo, ref=ref, sha=sha,
                        status=message, size=size,
                    )
                else:
                    failed += 1
                    logger.error(
                        'Clone failed', owner=owner,
                        repo=repo, ref=ref, sha=sha,
                        error=message,
                    )
                progress.advance(task)

    console.print(
        f'[bold green]Done![/bold green] Cloned: [cyan]{cloned}[/cyan], Failed: [red]{failed}[/red]',
    )
