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
from chatsbom.services.repo_service import RepoStats

logger = structlog.get_logger('repo_command')
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
        False, help='Force refresh even if valid data exists',
    ),
    limit: int | None = typer.Option(None, help='Limit number of items'),
):
    """
    Enrich repository metadata (Stars, License, etc.).
    Reads from: data/01-github-search
    Writes to: data/02-github-repo
    """
    container = get_container()
    config = container.config
    service = container.get_repo_service(token)

    target_languages = [language] if language else list(Language)

    for lang in target_languages:
        lang_str = str(lang)
        input_path = config.paths.get_search_list_path(lang_str)
        output_path = config.paths.get_repo_list_path(lang_str)

        if not input_path.exists():
            logger.warning(
                f"No search data found for {lang_str}", path=str(input_path),
            )
            continue

        repos = load_jsonl(input_path)
        if not repos:
            logger.warning('Empty search list', language=lang_str)
            continue

        storage = Storage(output_path)
        stats = RepoStats(total=len(repos))

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
                f"Enriching {lang_str}...", total=len(repos),
            )

            count = 0
            for repo in repos:
                if limit and count >= limit:
                    break

                # Check if already processed in output (simplified check)
                if not force and repo.id in storage.visited_ids:
                    progress.advance(task)
                    stats.skipped += 1
                    continue

                enriched_data = service.process_repo(repo, stats, lang_str)
                if enriched_data:
                    storage.save(enriched_data)

                progress.advance(task)
                count += 1

        logger.info(
            'Repo Enrichment Complete',
            language=lang_str,
            enriched=stats.enriched,
            skipped=stats.skipped,
            failed=stats.failed,
            api_requests=stats.api_requests,
        )
