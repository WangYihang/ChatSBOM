import structlog
import typer
from rich.console import Console
from rich.progress import BarColumn
from rich.progress import Progress
from rich.progress import SpinnerColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn

from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.core.storage import load_jsonl
from chatsbom.core.storage import Storage
from chatsbom.models.language import Language
from chatsbom.services.content_service import ContentStats

logger = structlog.get_logger('content_command')
console = Console()
app = typer.Typer()


@app.callback(invoke_without_command=True)
@handle_errors
def main(
    token: str = typer.Option(
        None, envvar='GITHUB_TOKEN', help='GitHub Token',
    ),
    language: Language | None = typer.Option(None, help='Target Language'),
    force: bool = typer.Option(
        False, help='Force re-download even if content exists',
    ),
    limit: int | None = typer.Option(None, help='Limit number of items'),
):
    """
    Download raw content (manifest files) from GitHub.
    Reads from: data/04-github-commit
    Writes to: data/05-github-content
    """
    container = get_container()
    config = container.config
    service = container.get_content_service(token)

    target_languages = [language] if language else list(Language)

    for lang in target_languages:
        lang_str = str(lang)
        input_path = config.paths.get_commit_list_path(lang_str)
        output_path = config.paths.get_content_list_path(lang_str)

        if not input_path.exists():
            logger.warning(
                f"No commit data found for {lang_str}", path=str(input_path),
            )
            continue

        repos = load_jsonl(input_path)
        if not repos:
            logger.warning('Empty repo list', language=lang_str)
            continue

        storage = Storage(output_path)
        stats = ContentStats(repo_full_name='Global')
        total_repos = len(repos)

        with Progress(
            SpinnerColumn(),
            TextColumn('[progress.description]{task.description}'),
            BarColumn(),
            TextColumn('{task.percentage:>3.0f}%'),
            TextColumn('•'),
            TimeElapsedColumn(),
            console=console,
        ) as progress:
            task = progress.add_task(
                f"Downloading Content {lang_str}...", total=total_repos,
            )

            count = 0
            for repo in repos:
                if limit and count >= limit:
                    break

                # Check if already processed
                if not force and repo.id in storage.visited_ids:
                    progress.advance(task)
                    stats.skipped_files += 1
                    continue

                repo_with_path = service.process_repo(repo, lang)
                if repo_with_path:
                    # Save the repo dict which now has 'local_content_path'
                    storage.save(repo_with_path)
                    stats.downloaded_files += 1
                else:
                    stats.failed_files += 1

                progress.advance(task)
                count += 1

        logger.info(
            'Content Download Complete',
            language=lang_str,
            downloaded=stats.downloaded_files,
            skipped=stats.skipped_files,
            failed=stats.failed_files,
        )
