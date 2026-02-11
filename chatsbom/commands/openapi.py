import csv
import json
import re
import shutil
from collections import defaultdict
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import matplotlib.pyplot as plt
import pandas as pd
import structlog
import typer
import yaml
from git import Repo
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


def normalize_path(path: str) -> str:
    """
    Standardize API paths for matching.
    - Lowercase
    - Remove trailing slash
    - Replace parameters (:id, {id}, <id>) with {}
    """
    if not path:
        return '/'

    # Remove query string if any
    path = path.split('?')[0]

    path = path.lower().strip()
    if path.endswith('/') and path != '/':
        path = path[:-1]

    # Standardize parameters: {id}, :id, <id>, <int:id> -> {}
    path = re.sub(r'\{[^}]+\}', '{}', path)
    path = re.sub(r':[a-zA-Z0-9_]+', '{}', path)
    path = re.sub(r'<[^>]+>', '{}', path)

    if not path.startswith('/'):
        path = '/' + path

    return path


def parse_openapi_spec(content: str, is_yaml: bool = True) -> set[tuple[str, str]]:
    """
    Parse OpenAPI/Swagger spec and extract normalized (method, path) pairs.
    """
    try:
        if is_yaml:
            spec = yaml.safe_load(content)
        else:
            spec = json.loads(content)

        if not spec or not isinstance(spec, dict):
            return set()

        endpoints = set()
        paths = spec.get('paths', {})
        if not isinstance(paths, dict):
            return set()

        for path, methods in paths.items():
            if not isinstance(methods, dict):
                continue

            norm_path = normalize_path(path)
            for method in methods.keys():
                # Standard HTTP methods only
                if method.lower() in {'get', 'post', 'put', 'delete', 'patch', 'options', 'head'}:
                    endpoints.add((method.upper(), norm_path))

        return endpoints
    except Exception as e:
        logger.debug('Failed to parse OpenAPI spec', error=str(e))
        return set()


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
    total_matched_projects = 0

    console.print('[bold green]Querying usage for frameworks...[/bold green]')

    for framework_enum in Framework:
        try:
            framework = FrameworkFactory.create(framework_enum)
            package_names = framework.get_package_names()

            if not package_names:
                continue

            packages_str = "', '".join(package_names)

            query = f"""
            SELECT DISTINCT
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
                # Load tree from sharded file: data/05-github-tree/{language}/{owner}/{repo}/{ref}/{sha}/tree.txt
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
                        tree_files = [
                            line.strip()
                            for line in f if line.strip()
                        ]
                except OSError:
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

                # Use commit_sha if available, fallback to ref used for tree
                sha_or_ref = commit_sha if commit_sha else ref

                for openapi_file in openapi_files:
                    openapi_url = f'https://github.com/{owner}/{repo}/blob/{sha_or_ref}/{openapi_file}'
                    results.append([
                        language, framework_name, framework_version, owner, repo,
                        stars, default_branch, latest_release, commit_sha, url,
                        openapi_file, openapi_url,
                    ])

            console.print(
                f'[bold]{framework_enum.value}[/bold]: '
                f'[cyan]{framework_matched}[/cyan]/{framework_total} projects have OpenAPI specs',
            )
            total_matched_projects += framework_matched

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
                'commit_sha', 'url', 'openapi_file', 'openapi_url',
            ])
            writer.writerows(results)

        console.print(
            f'[bold green]Found {len(results)} OpenAPI specs across {total_matched_projects} projects → {output}[/bold green]',
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
    full: bool = False,
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
        kwargs = {}
        if not full:
            kwargs['depth'] = 1

        Repo.clone_from(clone_url, str(repo_dir), **kwargs)

        size = _format_size(_dir_size(repo_dir))
        return (owner, repo, True, 'cloned', size)
    except Exception as e:
        # Clean up partial clone
        if repo_dir.exists():
            shutil.rmtree(str(repo_dir), ignore_errors=True)
        return (owner, repo, False, str(e), '')


@app.command()
def clone(
    input_csv: str = typer.Option(
        'openapi_candidates.csv', '--input', help='Input CSV file from candidates command',
    ),
    force: bool = typer.Option(
        False, help='Re-clone even if directory exists',
    ),
    full: bool = typer.Option(
        False, help='Perform a full clone (required for historical drift analysis)',
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
            shutil.rmtree(str(repo_dir), ignore_errors=True)

        repos_to_clone.append(row)

    if not repos_to_clone:
        console.print(
            '[yellow]All repositories already cloned. Use --force to re-clone.[/yellow]',
        )
        return

    console.print(
        f'Cloning [cyan]{len(repos_to_clone)}[/cyan] repositories (full={full}) into [bold]{dest}[/bold]...',
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
                    full,
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


@app.command()
def drift(
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
    Analyze the drift between OpenAPI specs and actual code endpoints across releases.
    Generates a raw data CSV for later plotting.
    """
    container = get_container()
    config = container.config
    client = container.get_query_repository().client

    code_dir = Path(code_endpoints_dir)
    repo_base = config.paths.framework_repos_dir

    # Read Candidates
    try:
        with open(input_csv, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            candidates = list(reader)
    except FileNotFoundError:
        console.print(f'[bold red]CSV not found: {input_csv}[/bold red]')
        raise typer.Exit(1)

    # Group by project
    projects = defaultdict(list)
    for c in candidates:
        projects[(c['owner'], c['repo'])].append(c)

    drift_results = []

    for (owner, repo_name), group in projects.items():
        console.print(
            f"Analyzing data for [bold cyan]{owner}/{repo_name}[/bold cyan]...",
        )

        query = f"""
        SELECT
            tag_name,
            published_at,
            target_commitish
        FROM releases r
        JOIN repositories repo ON r.repository_id = repo.id
        WHERE repo.owner = '{owner}' AND repo.repo = '{repo_name}'
        ORDER BY published_at ASC
        """
        releases = client.query(query).result_rows
        if not releases:
            continue

        repo_search_dir = repo_base / owner / repo_name
        git_repo_path = None
        for p in repo_search_dir.glob('*/*'):
            if (p / '.git').exists():
                git_repo_path = p
                break

        if not git_repo_path:
            console.print(
                f"  [red]Local git repository not found for {owner}/{repo_name}.[/red]",
            )
            continue

        try:
            git_repo = Repo(git_repo_path)
        except Exception as e:
            console.print(f"  [red]Failed to open git repo: {e}[/red]")
            continue

        prev_tag = None
        for tag, published_at, target in releases:
            # 1. Load Code Endpoints
            code_json = code_dir / owner / repo_name / f"{tag}.json"
            if not code_json.exists():
                prev_tag = tag
                continue

            try:
                with open(code_json, encoding='utf-8') as f:
                    code_raw = json.load(f)
                code_endpoints = {
                    (
                        item['method'].upper(), normalize_path(
                            item['path'],
                        ),
                    ) for item in code_raw
                }
            except Exception:
                prev_tag = tag
                continue

            # 2. Load OpenAPI Endpoints from Git at this tag
            spec_endpoints = set()
            openapi_files = []
            try:
                files = git_repo.git.ls_tree(
                    '-r', '--name-only', tag,
                ).split('\n')
                openapi_files = _find_openapi_files(files)
                for f_path in openapi_files:
                    content = git_repo.git.show(f"{tag}:{f_path}")
                    is_yaml = f_path.lower().endswith(('.yaml', '.yml'))
                    spec_endpoints.update(parse_openapi_spec(content, is_yaml))
            except Exception:
                pass

            # 3. Calculate Activity (Commits between tags)
            code_commits = 0
            spec_commits = 0
            if prev_tag:
                try:
                    # Total commits in interval
                    diff_range = f"{prev_tag}..{tag}"
                    code_commits = int(
                        git_repo.git.rev_list('--count', diff_range),
                    )
                    # Commits touching OpenAPI files
                    if openapi_files:
                        spec_commits = int(
                            git_repo.git.rev_list(
                                '--count', diff_range, '--', *openapi_files,
                            ),
                        )
                except Exception:
                    pass

            # 4. Calculate Metrics
            common = code_endpoints.intersection(spec_endpoints)
            spec_only = spec_endpoints - code_endpoints

            overlap_pct = (
                len(common) / len(code_endpoints) * 100
            ) if code_endpoints else 0
            stale_pct = (
                len(spec_only) / len(spec_endpoints) * 100
            ) if spec_endpoints else 0

            drift_results.append({
                'owner': owner,
                'repo': repo_name,
                'tag': tag,
                'date': published_at,
                'code_count': len(code_endpoints),
                'spec_count': len(spec_endpoints),
                'overlap_pct': overlap_pct,
                'stale_pct': stale_pct,
                'code_commits': code_commits,
                'spec_commits': spec_commits,
            })
            prev_tag = tag

    if drift_results:
        import pandas as pd
        df = pd.DataFrame(drift_results)
        df.to_csv(output_data, index=False)
        console.print(
            f"[bold green]Analysis data saved to {output_data}[/bold green]",
        )
    else:
        console.print('[yellow]No drift data collected.[/yellow]')


@app.command('plot-drift')
def plot_drift(
    input_data: str = typer.Option(
        'openapi_drift_data.csv', help='Input analysis data CSV',
    ),
    output_dir: str = typer.Option(
        'figures/drift', help='Directory to save plots',
    ),
):
    """
    Generate drift evolution charts from the analysis data.
    Separated from the main analysis for styling flexibility.
    """
    try:
        df_all = pd.read_csv(input_data)
        df_all['date'] = pd.to_datetime(df_all['date'])
    except Exception as e:
        console.print(f"[bold red]Failed to read input data: {e}[/bold red]")
        raise typer.Exit(1)

    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    for (owner, repo), df in df_all.groupby(['owner', 'repo']):
        console.print(f"Plotting [bold cyan]{owner}/{repo}[/bold cyan]...")
        df = df.sort_values('date')

        if len(df) < 2:
            continue

        fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), sharex=True)

        # Plot 1: Endpoint Counts & Activity
        ax1.plot(
            df['date'], df['code_count'], marker='o',
            label='Code Endpoints', color='#1f77b4', linewidth=2,
        )
        ax1.plot(
            df['date'], df['spec_count'], marker='s',
            label='OpenAPI Endpoints', color='#d62728', linewidth=2,
        )

        # Add bar chart for commit activity on the same axis (optional secondary Y)
        ax1_twin = ax1.twinx()
        ax1_twin.bar(
            df['date'], df['code_commits'], alpha=0.1,
            color='gray', label='Commit Activity', width=5,
        )
        ax1_twin.set_ylabel('Commits in Interval', color='gray')

        ax1.set_ylabel('Count')
        ax1.set_title(
            f'Drift Evolution: {owner}/{repo}\n(Agreement between Code and Docs)', fontsize=14,
        )
        ax1.legend(loc='upper left')
        ax1.grid(True, linestyle='--', alpha=0.6)

        # Plot 2: Percentages (The "Drift")
        ax2.plot(
            df['date'], df['overlap_pct'], marker='v',
            label='Consistency (Overlap %)', color='#2ca02c', linewidth=2,
        )
        ax2.fill_between(
            df['date'], df['overlap_pct'],
            100, color='#2ca02c', alpha=0.1,
        )

        ax2.plot(
            df['date'], df['stale_pct'], marker='x',
            label='Staleness (Zombie Specs %)', color='#ff7f0e', linestyle='--',
        )

        ax2.set_ylabel('Percentage (%)')
        ax2.set_xlabel('Release Date')
        ax2.set_ylim(0, 110)
        ax2.legend(loc='lower left')
        ax2.grid(True, linestyle='--', alpha=0.6)

        plt.tight_layout()
        save_path = out_path / f"{owner}_{repo}.png"
        plt.savefig(save_path, dpi=150)
        plt.close()
        console.print(f"  [green]Saved → {save_path}[/green]")
