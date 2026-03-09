import asyncio
import json
from pathlib import Path

import structlog
import typer
from rich.progress import BarColumn
from rich.progress import Progress
from rich.progress import SpinnerColumn
from rich.progress import TaskProgressColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn
from rich.progress import TimeRemainingColumn

from chatsbom.core.config import get_config
from chatsbom.core.logging import console
from chatsbom.models.language import Language
from chatsbom.models.repository import Repository
from chatsbom.services.github_analysis_service import GitHubAnalysisService
from chatsbom.services.github_service import GitHubService

logger = structlog.get_logger('classify_command')
app = typer.Typer(
    help='Batch classify GitHub repositories and extract metadata using LLM.',
)


async def run_classification(
    repos: list[Repository],
    analyzer: GitHubAnalysisService,
    github_service: GitHubService,
    output_path: Path,
):
    """Run batch classification with real-time progress and safe writing."""
    processed_count = 0
    error_count = 0

    # Task creation
    tasks = [analyzer.analyze_repo(repo, github_service) for repo in repos]

    with Progress(
        SpinnerColumn(),
        TextColumn('[progress.description]{task.description}'),
        BarColumn(bar_width=40),
        TaskProgressColumn(),
        TimeElapsedColumn(),
        TimeRemainingColumn(),
        console=console,
    ) as progress:
        task = progress.add_task(
            '[green]Processing repos...', total=len(tasks),
        )

        # Use as_completed for dynamic progress updates
        for coro in asyncio.as_completed(tasks):
            res = await coro
            if res:
                # Append result to JSONL
                with open(output_path, 'a', encoding='utf-8') as f:
                    f.write(res.model_dump_json() + '\n')
                processed_count += 1
            else:
                error_count += 1
            progress.update(task, advance=1)

    console.print('\n[bold green]✓ Processing Complete![/bold green]')
    console.print(f"  - Total processed: {processed_count}")
    console.print(f"  - Errors/Skipped: {error_count}")
    console.print(f"  - Results saved to: [cyan]{output_path}[/cyan]\n")


@app.callback(invoke_without_command=True)
def main(
    language: Language = typer.Option(
        Language.PYTHON, help='Target Programming Language (used for default paths)',
    ),
    input_path: Path | None = typer.Option(
        None, '--input', '-i', help='Input JSONL file of repositories',
    ),
    output_path: Path | None = typer.Option(
        None, '--output', '-o', help='Output JSONL file for classification results',
    ),
    limit: int = typer.Option(
        100, help='Limit number of repositories to process',
    ),
    concurrency: int = typer.Option(
        20, help='Concurrency limit for LLM requests (async semaphore)',
    ),
    model: str = typer.Option(
        'deepseek-chat', help='LLM model to use (compatible with OpenAI API)',
    ),
    api_key: str = typer.Option(
        None, envvar='OPENAI_API_KEY', help='OpenAI API Key',
    ),
    base_url: str = typer.Option(
        None, envvar='OPENAI_BASE_URL', help='OpenAI Base URL',
    ),
    github_token: str = typer.Option(
        None, envvar='GITHUB_TOKEN', help='GitHub Token (for fetching README if missing)',
    ),
):
    """
    Classify repositories using LLM and extract structured info.

    This command reads a list of repositories (JSONL), processes them in parallel
    using instructor + pydantic, and saves the results.
    """
    if not api_key:
        console.print('[red]Error: OPENAI_API_KEY is required.[/red]')
        raise typer.Exit(1)

    config = get_config()

    # 1. Path Resolution
    if not input_path:
        input_path = config.paths.get_repo_list_path(str(language))

    if not input_path.exists():
        console.print(f"[red]Error: Input file {input_path} not found.[/red]")
        raise typer.Exit(1)

    if not output_path:
        output_path = config.paths.base_data_dir / \
            '08-github-analysis' / f"{language.value}.jsonl"

    output_path.parent.mkdir(parents=True, exist_ok=True)

    # 2. Data Loading (with simple mock-friendly logic)
    repos = []
    with open(input_path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                data = json.loads(line)

                # Adapt common field variations
                if 'repo_name' in data and 'name' not in data:
                    data['name'] = data['repo_name']
                if 'repo' in data and 'name' not in data:
                    data['name'] = data['repo']
                if '/' in data.get('name', '') and 'owner' not in data:
                    data['owner'], data['name'] = data['name'].split('/', 1)

                # Mock default owner if missing (requirement: repo_name usually implies owner/name)
                if 'owner' not in data:
                    data['owner'] = 'unknown'

                repos.append(Repository.model_validate(data))
                if len(repos) >= limit:
                    break
            except Exception as e:
                logger.warning(
                    'Failed to parse input line',
                    error=str(e), line_snippet=line[:50],
                )

    if not repos:
        console.print('[yellow]No repositories found to process.[/yellow]')
        return

    logger.info(
        'Starting batch classification', count=len(repos),
        language=language.value, model=model, concurrency=concurrency,
    )

    # 3. Service Initialization
    analyzer = GitHubAnalysisService(
        api_key=api_key,
        base_url=base_url,
        model=model,
        concurrency=concurrency,
    )
    # Use provided token or fallback to config
    github_service = GitHubService(
        token=github_token or config.github.token or '',
    )

    # 4. Async Execution Loop
    asyncio.run(
        run_classification(
            repos, analyzer, github_service, output_path,
        ),
    )


if __name__ == '__main__':
    app()
