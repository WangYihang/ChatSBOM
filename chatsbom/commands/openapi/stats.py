import csv
import os
from collections import defaultdict
from collections.abc import Iterator
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any
from typing import TextIO

import structlog
import typer
from rich.markup import escape
from rich.progress import BarColumn
from rich.progress import MofNCompleteColumn
from rich.progress import SpinnerColumn
from rich.progress import TaskProgressColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn
from rich.progress import TimeRemainingColumn
from rich.table import Table

from chatsbom.core.container import get_container
from chatsbom.core.extras import require_extra
from chatsbom.core.logging import console
from chatsbom.core.logging import progress_bar
from chatsbom.models.language import Language
from chatsbom.models.language import LanguageFactory
from chatsbom.services.openapi_service import IGNORED_DIR_NAMES
from chatsbom.services.openapi_service import OpenApiService

logger = structlog.get_logger('openapi_stats')
app = typer.Typer()

# LLM Context Window Limits: each label, and the model id its window is for
# Updated to 2026 latest models
MODEL_MAPPING = {
    'GPT-5': 'gpt-5-chat',
    'Opus-4.6': 'claude-opus-4-6-20260205',
    'Opus-4.5': 'claude-opus-4-5',
    'Gemini-3.1-Pro': 'gemini/gemini-3.1-pro-preview',
    'Gemini-3.0-Pro': 'gemini/gemini-3-pro-preview',
    'DeepSeek-V3.2': 'deepseek/deepseek-v3.2',
    'DeepSeek-R1': 'deepseek/deepseek-r1',
    'Llama-4-Scout': 'groq/meta-llama/llama-4-scout-17b-16e-instruct',
    'Qwen-3': 'cerebras/qwen-3-32b',
    'GLM-4.7': 'zai/glm-4.7',
    'Kimi-k2.5': 'moonshot/kimi-k2.5',
}

#: Each model's context window, by its id above: the most input tokens it
#: takes, `max_input_tokens` in the model map litellm 1.81.16 ships
#: (model_prices_and_context_window_backup.json), copied 2026-09-28.
#: litellm was asked on every run, and imported for these numbers alone:
#: importing it took two seconds, fetched its price list over the
#: network unless told not to, and loaded a `.env` of its own, found by
#: walking up from where it is installed (#26).
CONTEXT_WINDOWS = {
    'gpt-5-chat': 128_000,
    'claude-opus-4-6-20260205': 1_000_000,
    'claude-opus-4-5': 200_000,
    'gemini/gemini-3.1-pro-preview': 1_048_576,
    'gemini/gemini-3-pro-preview': 1_048_576,
    'deepseek/deepseek-v3.2': 163_840,
    'deepseek/deepseek-r1': 65_536,
    'groq/meta-llama/llama-4-scout-17b-16e-instruct': 131_072,
    'cerebras/qwen-3-32b': 128_000,
    'zai/glm-4.7': 200_000,
    'moonshot/kimi-k2.5': 262_144,
}


def get_context_windows():
    """Context window limits, each with a safety margin."""
    windows = {}
    for label, model_id in MODEL_MAPPING.items():
        limit = CONTEXT_WINDOWS[model_id]
        # Add 10% safety margin (user often wants to include some prompt/output space)
        safe_limit = int(limit * 0.9)
        windows[label] = {
            'limit': safe_limit,
            'full_limit': limit,
            'display': f"{label} ({limit // 1000}k)",
        }
    return windows


#: The encoding tokens are counted in: GPT-4's.
ENCODING = 'cl100k_base'

#: Characters handed to the tokenizer at a time, cut after a newline
#: where there is one. Its work on one piece of text grows with the
#: square of the piece's length: a megabyte of letters with no break in
#: it had not finished after five minutes, and in pieces this size it
#: takes seconds. Memory stays bounded too, whatever a file's size.
TOKENIZE_CHARS = 8192


def load_tokenizer(cache_dir: Path) -> Any:
    """The tokenizer, downloaded once, into `cache_dir`.

    tiktoken downloads an encoding the first time it is asked for it,
    1.7 MB from openaipublic.blob.core.windows.net, and keeps it in the
    system's temporary directory, which a reboot may empty: a download
    nobody was told of, made again after every reboot (#47). It is kept
    with what else chatsbom fetches instead, and the download is said
    before it happens. TIKTOKEN_CACHE_DIR, tiktoken's own setting, wins
    where it is set.
    """
    import tiktoken

    where = Path(os.environ.get('TIKTOKEN_CACHE_DIR') or cache_dir)
    if not any(where.glob('*')):
        logger.info(
            'Downloading the tokenizer, once', encoding=ENCODING,
            source='openaipublic.blob.core.windows.net', cache=str(where),
        )
    # Set for the load alone: tiktoken reads it then, and it is not ours
    # to leave behind in the environment.
    before = os.environ.get('TIKTOKEN_CACHE_DIR')
    os.environ['TIKTOKEN_CACHE_DIR'] = str(where)
    try:
        return tiktoken.get_encoding(ENCODING)
    finally:
        if before is None:
            del os.environ['TIKTOKEN_CACHE_DIR']
        else:
            os.environ['TIKTOKEN_CACHE_DIR'] = before


def count_file_stats(path: Path, enc: Any) -> tuple[int, int]:
    """Count lines and tokens in a file.

    Every file, whatever its size. Those over a megabyte counted as
    empty, and a generated file too large for any context window left
    its repository shown as fitting them all (#47). Text that spells a
    special token, `<|endoftext|>`, is text here: `encode` refused it,
    and its file counted nothing.
    """
    lines = tokens = 0
    last = ''
    try:
        with path.open(encoding='utf-8', errors='ignore') as f:
            for piece in _pieces(f, TOKENIZE_CHARS):
                lines += piece.count('\n')
                tokens += len(enc.encode_ordinary(piece))
                last = piece[-1]
    except OSError:
        return 0, 0
    # A last line with no newline after it is a line all the same.
    if last and last != '\n':
        lines += 1
    return lines, tokens


def _pieces(f: TextIO, size: int) -> Iterator[str]:
    """The text of `f`, about `size` characters at a time, each piece cut
    after its last newline where it has one: at most twice `size`."""
    carry = ''
    while block := f.read(size):
        text = carry + block
        cut = text.rfind('\n') + 1 or len(text)
        carry = text[cut:]
        yield text[:cut]
    if carry:
        yield carry


def analyze_repo(repo_dir: Path, enc, target_extensions: list[str]):
    """Analyze a single repository directory."""
    total_lines = 0
    total_tokens = 0

    for p in repo_dir.rglob('*'):
        if p.is_file():
            # Skip ignored directories: of the path in the repository.
            # In the whole path, a repository under /tmp, or one named
            # `examples`, counted nothing at all (#47).
            directories = p.relative_to(repo_dir).parts[:-1]
            if any(part.lower() in IGNORED_DIR_NAMES for part in directories):
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
    Also evaluates if the project fits within various LLM context windows.
    """
    # Imported where it is used rather than at the top: this command is
    # the only one that counts tokens, and at module level every command
    # paid for it. Asked for first, since it comes with an extra.
    require_extra('openapi', 'tiktoken')

    container = get_container()
    config = container.config
    workspaces_dir = config.paths.framework_repos_dir
    service = OpenApiService()

    context_windows = get_context_windows()

    try:
        enc = load_tokenizer(config.paths.cache_dir / 'tiktoken')
    except Exception as e:
        logger.error('Failed to load tokenizer', error=str(e))
        raise typer.Exit(1)

    try:
        with open(input_csv, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
    except FileNotFoundError:
        console.print(
            f'[bold red]CSV file not found: {escape(str(input_csv))}[/bold red]',
        )
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

    with progress_bar(
        SpinnerColumn(),
        TextColumn('[bold blue]{task.fields[current]}'),
        BarColumn(bar_width=None),
        TaskProgressColumn(),
        MofNCompleteColumn(),
        TextColumn('•'),
        TimeElapsedColumn(),
        TextColumn('•'),
        TimeRemainingColumn(),
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

    console.print(
        f"\n[bold green]Stats saved to {escape(str(output_path))}[/bold green]",
    )


if __name__ == '__main__':
    app()
