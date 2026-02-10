from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor

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
from chatsbom.core.storage import load_jsonl
from chatsbom.core.storage import Storage
from chatsbom.models.language import Language
from chatsbom.services.sbom_service import SbomStats

logger = structlog.get_logger('sbom_command')
app = typer.Typer()


@app.command()
def generate(
    language: Language | None = typer.Option(None, help='Target Language'),
    force: bool = typer.Option(
        False, help='Force regenerate even if SBOM exists',
    ),
    limit: int | None = typer.Option(None, help='Limit number of items'),
    workers: int = typer.Option(5, help='Number of concurrent workers'),
):
    """
    Generate SBOMs from downloaded content.
    Reads from: data/05-github-content
    Writes to: data/06-sbom
    """
    container = get_container()
    config = container.config
    service = container.get_sbom_service()

    target_languages = [language] if language else list(Language)

    for lang in target_languages:
        lang_str = str(lang)
        input_path = config.paths.get_content_list_path(lang_str)
        output_path = config.paths.get_sbom_list_path(lang_str)

        if not input_path.exists():
            logger.warning(
                f"No content data found for {lang_str}", path=str(input_path),
            )
            continue

        repos = load_jsonl(input_path)
        if not repos:
            logger.warning('Empty repo list', language=lang_str)
            continue

        if limit:
            repos = repos[:limit]

        storage = Storage(output_path)
        stats = SbomStats(total=len(repos))

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
                f"Generating SBOMs {lang_str}...", total=len(repos),
            )

            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = []
                for repo in repos:
                    # Check if already processed
                    if not force and repo.id in storage.visited_ids:
                        progress.advance(task)
                        stats.inc_skipped()
                        continue

                    # Convert model to dict to access extra fields easily
                    repo_dict = repo.model_dump(mode='json')

                    futures.append(
                        executor.submit(
                            service.process_repo, repo_dict, stats, lang_str, force,
                        ),
                    )

                for future in as_completed(futures):
                    try:
                        enriched_data = future.result()
                        if enriched_data:
                            storage.save(enriched_data)
                    except Exception as e:
                        logger.error(
                            'Error in worker thread during SBOM generation', error=str(e),
                        )
                        stats.inc_failed()

                    progress.advance(task)

        logger.info(
            'SBOM Generation Complete',
            language=lang_str,
            generated=stats.generated,
            cache_hits=stats.cache_hits,
            skipped=stats.skipped,
            failed=stats.failed,
            elapsed=f"{stats.elapsed_time:.2f}s",
        )


if __name__ == '__main__':
    app()
