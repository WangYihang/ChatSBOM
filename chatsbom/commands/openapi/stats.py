import csv
from collections import defaultdict
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import litellm
import structlog
import tiktoken
import typer
from rich.progress import BarColumn
from rich.progress import MofNCompleteColumn
from rich.progress import Progress
from rich.progress import SpinnerColumn
from rich.progress import TaskProgressColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn
from rich.progress import TimeRemainingColumn
from rich.table import Table

from chatsbom.core.container import get_container
from chatsbom.core.logging import console
from chatsbom.models.language import Language
from chatsbom.models.language import LanguageFactory
from chatsbom.services.openapi_service import IGNORED_DIR_NAMES
from chatsbom.services.openapi_service import OpenApiService

logger = structlog.get_logger('openapi_stats')
app = typer.Typer()

# LLM Context Window Limits (dynamically fetched from litellm)
# Updated to 2026 latest models
MODEL_MAPPING = {
    'GPT-5': 'gpt-5-chat',
    'Claude-4.6': 'claude-opus-4-6-20260205',
    'Gemini-2.5-Pro': 'gemini-2.5-pro-preview-06-05',
}


def get_context_windows():
    """Fetch context window limits from litellm."""
    windows = {}
    for label, model_id in MODEL_MAPPING.items():
        try:
            info = litellm.get_model_info(model_id)
            # Use max_input_tokens if available, otherwise fallback to max_tokens
            limit = info.get('max_input_tokens') or info.get('max_tokens')
            if limit:
                # Add 10% safety margin (user often wants to include some prompt/output space)
                safe_limit = int(limit * 0.9)
                windows[label] = {
                    'limit': safe_limit,
                    'full_limit': limit,
                    'display': f"{label} ({limit // 1000}k)",
                }
        except Exception as e:
            logger.warning(
                f"Failed to fetch info for {model_id}", error=str(e),
            )
    return windows


def count_file_stats(path: Path, enc):
    """Count lines and tokens in a file."""
    try:
        # Limit file size to avoid memory issues (e.g. 1MB)
        if path.stat().st_size > 1 * 1024 * 1024:
            return 0, 0

        content = path.read_text(encoding='utf-8', errors='ignore')
        lines = len(content.splitlines())
        tokens = len(enc.encode(content))
        return lines, tokens
    except Exception:
        return 0, 0


def analyze_repo(repo_dir: Path, enc, target_extensions: list[str]):
    """Analyze a single repository directory."""
    total_lines = 0
    total_tokens = 0

    for p in repo_dir.rglob('*'):
        if p.is_file():
            # Skip ignored directories
            if any(part.lower() in IGNORED_DIR_NAMES for part in p.parts):
                continue

            # Filter by language-specific extensions
            if p.suffix.lower() in target_extensions:
                lines, tokens = count_file_stats(p, enc)
                total_lines += lines
                total_tokens += tokens

    return total_lines, total_tokens


@app.callback(invoke_without_command=True)
def main(
    input_csv: str = typer.Option(
        'openapi_candidates.csv', '--input', help='Input CSV file from candidates command',
    ),
    workers: int = typer.Option(8, help='Number of concurrent workers'),
    top: int = typer.Option(
        0, help='Limit to top N projects per framework (by stars). 0 means no limit.',
    ),
):
    """
    Analyze cloned repositories for LOC and token count.
    Only considers relevant source files for each project's language.
    Also evaluates if the project fits within various LLM context windows (fetched via LiteLLM).
    """
    container = get_container()
    config = container.config
    workspaces_dir = config.paths.framework_repos_dir
    service = OpenApiService()

    # Get dynamic context windows
    context_windows = get_context_windows()

    try:
        enc = tiktoken.get_encoding('cl100k_base')
    except Exception as e:
        logger.error('Failed to load tokenizer', error=str(e))
        raise typer.Exit(1)

    try:
        with open(input_csv, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except FileNotFoundError:
        console.print(f'[bold red]CSV file not found: {input_csv}[/bold red]')
        raise typer.Exit(1)

    if top > 0:
        framework_groups = defaultdict(list)
        for row in rows:
            framework_groups[row.get('framework', '')].append(row)
        filtered_rows = []
        for framework, group in framework_groups.items():
            group.sort(key=lambda r: int(r.get('stars', 0) or 0), reverse=True)
            filtered_rows.extend(group[:top])
        rows = filtered_rows

    results = []

    # Filter only those that are cloned
    repos_to_analyze = []
    for row in rows:
        ver = service.get_version_path(
            row.get('latest_release'), row.get('commit_sha'),
        )
        repo_dir = workspaces_dir / row['owner'] / row['repo'] / ver
        if repo_dir.exists():
            repos_to_analyze.append((row, repo_dir))

    if not repos_to_analyze:
        console.print(
            '[yellow]No cloned repositories found to analyze.[/yellow]',
        )
        return

    console.print(
        f'Analyzing [cyan]{len(repos_to_analyze)}[/cyan] repositories...',
    )

    with Progress(
        SpinnerColumn(),
        TextColumn('[bold blue]{task.fields[current]}'),
        BarColumn(bar_width=None),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TextColumn('•'),
        TimeElapsedColumn(),
        TextColumn('•'),
        TimeRemainingColumn(),
        console=console,
        expand=True,
    ) as progress:
        task = progress.add_task(
            'Analyzing...', total=len(repos_to_analyze), current='Initializing...',
        )

        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = {}
            for row, repo_dir in repos_to_analyze:
                lang_str = row.get('language', '').lower()
                try:
                    lang_enum = Language(lang_str)
                    handler = LanguageFactory.get_handler(lang_enum)
                    target_exts = handler.get_source_extensions()
                except (ValueError, KeyError):
                    # Fallback to a sensible default if language is unknown
                    target_exts = [
                        '.py', '.go', '.java',
                        '.rs', '.rb', '.js', '.ts', '.php',
                    ]

                futures[executor.submit(analyze_repo, repo_dir, enc, target_exts)] = (
                    row, repo_dir, target_exts,
                )

            for future in as_completed(futures):
                row, repo_dir, target_exts = futures[future]
                owner, repo = row['owner'], row['repo']

                try:
                    lines, tokens = future.result()

                    # Calculate compatibility
                    compatibility = {
                        label: tokens <= info['limit']
                        for label, info in context_windows.items()
                    }

                    results.append({
                        'owner': owner,
                        'repo': repo,
                        'framework': row.get('framework', 'unknown'),
                        'language': row.get('language', 'unknown'),
                        'lines': lines,
                        'tokens': tokens,
                        'stars': row.get('stars', 0),
                        'extensions': ','.join(target_exts),
                        **compatibility,
                    })
                    status_text = f"[green]✔ {owner}/{repo}[/green] [dim]({lines} lines, {tokens} tokens)[/dim]"
                except Exception as e:
                    logger.error(
                        'Analysis failed', owner=owner,
                        repo=repo, error=str(e),
                    )
                    status_text = f"[red]✘ {owner}/{repo}[/red]"

                progress.update(task, advance=1, current=status_text)

    # Output results in a table
    table = Table(
        title='Repository Statistics & LLM Context Compatibility (with 10% safety margin)',
    )
    table.add_column('Owner/Repo', style='cyan')
    table.add_column('Framework', style='green')
    table.add_column('Language', style='blue')
    table.add_column('LOC', justify='right')
    table.add_column('Tokens', justify='right')

    # Add columns for each model
    for label, info in context_windows.items():
        table.add_column(info['display'], justify='center')

    # Sort results by framework and then by stars
    results.sort(key=lambda x: (x['framework'], -int(x['stars'] or 0)))

    for res in results:
        row_data = [
            f"{res['owner']}/{res['repo']}",
            res['framework'],
            res['language'],
            f"{res['lines']:,}",
            f"{res['tokens']:,}",
        ]

        # Add checkmarks/crosses for model compatibility
        for label in context_windows.keys():
            if res.get(label):
                row_data.append('[bold green]✔[/bold green]')
            else:
                row_data.append('[bold red]✘[/bold red]')

        table.add_row(*row_data)

    console.print(table)

    # Also save to CSV
    output_path = Path('repository_stats.csv')
    fieldnames = [
        'owner', 'repo', 'framework', 'language', 'lines',
        'tokens', 'stars', 'extensions',
    ] + list(context_windows.keys())
    with open(output_path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    console.print(f"\n[bold green]Stats saved to {output_path}[/bold green]")


if __name__ == '__main__':
    app()
