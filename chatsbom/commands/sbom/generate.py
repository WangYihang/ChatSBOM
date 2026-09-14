import json
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
from chatsbom.core.storage import load_jsonl
from chatsbom.core.storage import Storage
from chatsbom.models.language import Language
from chatsbom.services.sbom_service import _is_usable_sbom
from chatsbom.services.sbom_service import SbomStats

logger = structlog.get_logger('sbom_generate')
app = typer.Typer()


def _unusable_ids(ledger: Path) -> set[int]:
    """Repository ids whose recorded SBOM cannot be read.

    The ledger says a repository is done; this asks whether the file it
    points at is worth anything. A truncated Syft write leaves a
    zero-byte JSON that every later run skipped and every `db index`
    failed, and the two in this corpus — `btmills/geopattern` and
    `layerJS/layerJS` — were the standing `failed=2`.

    Reads the ledger once per language and only stats the paths, so it
    costs nothing next to the scan it is guarding.
    """
    ids: set[int] = set()
    if not ledger.exists():
        return ids
    try:
        with ledger.open(encoding='utf-8') as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    continue
                repository_id = record.get('id')
                stored = record.get('sbom_path')
                if not isinstance(repository_id, int) or not stored:
                    continue
                if not _is_usable_sbom(Path(stored)):
                    ids.add(repository_id)
    except OSError as error:
        logger.warning(
            'Unreadable SBOM ledger', path=str(ledger),
            error=str(error),
        )
    return ids


@app.callback(invoke_without_command=True)
def main(
    language: Language | None = typer.Option(None, help='Target Language'),
    force: bool = typer.Option(
        False, help='Force regenerate even if SBOM exists',
    ),
    limit: int | None = typer.Option(None, help='Limit number of items'),
    workers: int = typer.Option(5, help='Number of concurrent workers'),
    use_generated_locks: bool = typer.Option(
        True,
        '--use-generated-locks/--no-generated-locks',
        help='Include lockfiles resolved by `sbom lock` in the scan',
    ),
):
    """
    Generate SBOMs from downloaded content.
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
        # Repositories the ledger calls done but whose stored SBOM is
        # unusable. Without this the outer skip below fires first and
        # `process_repo`'s own check is never reached, so a zero-byte
        # SBOM stayed unfixable without `--force` over the whole
        # language — 5,834 repositories to recover two.
        unusable = _unusable_ids(output_path)
        if unusable:
            console.print(
                f'[yellow]{len(unusable)}[/] stored SBOM(s) unreadable '
                f'— regenerating those.',
            )
        stats = SbomStats(total=len(repos))

        with Progress(SpinnerColumn(), TextColumn('[progress.description]{task.description}'), BarColumn(), TaskProgressColumn(), MofNCompleteColumn(), TextColumn('•'), TimeElapsedColumn(), TextColumn('•'), TimeRemainingColumn(), console=console) as progress:
            task = progress.add_task(
                f"Generating SBOMs {lang_str}...", total=len(repos),
            )

            with ThreadPoolExecutor(max_workers=workers) as executor:
                futures = []
                for repo in repos:
                    if (
                        not force
                        and repo.id in storage.visited_ids
                        and repo.id not in unusable
                    ):
                        progress.advance(task)
                        stats.inc_skipped()
                        continue

                    repo_dict = repo.model_dump(mode='json')
                    lock_dir = None
                    if use_generated_locks and repo.download_target:
                        lock_dir = config.paths.get_generated_lock_dir(
                            lang_str, repo.owner, repo.repo,
                            repo.download_target.commit_sha,
                        )
                    futures.append(
                        executor.submit(
                            service.process_repo, repo_dict, stats, lang_str,
                            force, lock_dir,
                        ),
                    )

                for future in as_completed(futures):
                    try:
                        enriched_data = future.result()
                        if enriched_data:
                            storage.save(enriched_data, replace=True)
                    except Exception as e:
                        logger.error(
                            'Error in worker thread during SBOM generation', error=str(e),
                        )
                        stats.inc_failed()
                    progress.advance(task)

        logger.info(
            'SBOM Generation Complete', language=lang_str, generated=stats.generated,
            cache_hits=stats.cache_hits, skipped=stats.skipped, failed=stats.failed, elapsed=f"{stats.elapsed_time:.2f}s",
        )
