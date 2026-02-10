import concurrent.futures

import structlog
import typer
from rich.console import Console
from rich.progress import BarColumn
from rich.progress import MofNCompleteColumn
from rich.progress import Progress
from rich.progress import SpinnerColumn
from rich.progress import TaskProgressColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn
from rich.progress import TimeRemainingColumn

from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.core.github import check_github_token
from chatsbom.core.storage import load_jsonl
from chatsbom.core.storage import Storage
from chatsbom.models.language import Language
from chatsbom.services.commit_service import CommitStats

logger = structlog.get_logger('commit_command')
app = typer.Typer()


@app.callback(invoke_without_command=True)
@handle_errors
def main(
    token: str = typer.Option(
        None, envvar='GITHUB_TOKEN', help='GitHub Token',
    ),
    language: Language | None = typer.Option(None, help='Target Language'),
    force: bool = typer.Option(
        False, help='Force refresh even if valid data exists',
    ),
    limit: int | None = typer.Option(None, help='Limit number of items'),
    workers: int = typer.Option(10, help='Number of concurrent workers'),
):
    """
    Resolve specific commit SHA for download targets.
    Reads from: data/03-github-release
    Writes to: data/04-github-commit
    """
    check_github_token(token)
    container = get_container()
    config = container.config
    service = container.get_commit_service(token)

    target_languages = [language] if language else list(Language)

    for lang in target_languages:
        lang_str = str(lang)
        input_path = config.paths.get_release_list_path(lang_str)
        output_path = config.paths.get_commit_list_path(lang_str)

        if not input_path.exists():
            logger.warning(
                f"No release data found for {lang_str}", path=str(input_path),
            )
            continue

        repos = load_jsonl(input_path)
        if not repos:
            logger.warning('Empty repo list', language=lang_str)
            continue

        if limit:
            repos = repos[:limit]

        storage = Storage(output_path)
        stats = CommitStats(total=len(repos))

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
            console=Console(),
        ) as progress:
            task = progress.add_task(
                f"Resolving Commits {lang_str}...", total=len(repos),
            )

            def process_single_repo(repo):
                try:
                    # Check if already processed
                    if not force and repo.id in storage.visited_ids:
                        stats.inc_skipped()
                        progress.advance(task)
                        return

                    logger.info(
                        'Processing repository',
                        repo=f"{repo.owner}/{repo.repo}",
                    )

                    enriched_data = service.process_repo(repo, stats, lang_str)
                    if enriched_data:
                        storage.save(enriched_data)

                    progress.advance(task)
                except Exception as e:
                    logger.error(
                        'Unexpected error in worker thread',
                        repo=f"{repo.owner}/{repo.repo}", error=str(e),
                    )
                    stats.inc_failed()
                    progress.advance(task)

            with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as executor:
                executor.map(process_single_repo, repos)

        logger.info(
            'Commit Resolution Complete',
            language=lang_str,
            enriched=stats.enriched,
            skipped=stats.skipped,
            failed=stats.failed,
            api_requests=stats.api_requests,
        )
