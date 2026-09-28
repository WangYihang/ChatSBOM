"""`chatsbom sbom generate`: Syft over every stored content root.

A content root holds every manifest the content stage discovered in
the repository's tree, at its own path and of every ecosystem, so one
`syft dir:` scan of it covers a Maven backend under `app/server/` as
well as the `package.json` at the root. Lockfiles `sbom lock` resolved
are merged in at the directory they were resolved for.

The content roots are found by walking `06-github-content/<id>/<sha>/`,
not a per-language list: the directory is the list, and a repository
needs no language to be scanned. A root is skipped while its SBOM is
whole and newer than every file it was generated from
(`is_current_sbom`), so a root the content stage has since added
manifests to is scanned again.

The repository record is written by `chatsbom run`, which has it;
this command only scans. It needs no token and no database.
"""
from __future__ import annotations

from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import structlog
import typer
from rich.progress import BarColumn
from rich.progress import MofNCompleteColumn
from rich.progress import SpinnerColumn
from rich.progress import TaskProgressColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn
from rich.progress import TimeRemainingColumn

from chatsbom.core.container import get_container
from chatsbom.core.layout import scan_dirs
from chatsbom.core.ledger import Ledger
from chatsbom.core.logging import console
from chatsbom.core.logging import progress_bar
from chatsbom.services.sbom_service import DEFAULT_SYFT_TIMEOUT
from chatsbom.services.sbom_service import is_current_sbom
from chatsbom.services.sbom_service import SbomStats

logger = structlog.get_logger('sbom_generate')
app = typer.Typer()


def repositories_named(ledger_path: Path, repos_file: Path | None) -> set[int] | None:
    """The ids a `--repos-file` names, through the ledger, or None."""
    if repos_file is None:
        return None
    with Ledger(ledger_path) as ledger:
        repos, missing = ledger.resolve_repositories(
            repos_file.read_text(encoding='utf-8').splitlines(),
        )
    if missing:
        logger.warning(
            'Not tracked, left out', count=len(missing), first=missing[:10],
        )
    return repos


@app.callback(invoke_without_command=True)
def main(
    force: bool = typer.Option(
        False, help='Force regenerate even if SBOM exists',
    ),
    limit: int | None = typer.Option(
        None, help='Scan at most this many content roots',
    ),
    workers: int = typer.Option(5, help='Number of concurrent workers'),
    use_generated_locks: bool = typer.Option(
        True,
        '--use-generated-locks/--no-generated-locks',
        help='Include lockfiles resolved by `sbom lock` in the scan',
    ),
    syft_timeout: int = typer.Option(
        DEFAULT_SYFT_TIMEOUT,
        min=1,
        help=(
            'Seconds a Syft scan may run before it is killed and its '
            'repository counted as failed'
        ),
    ),
    repos_file: Path | None = typer.Option(
        None,
        '--repos-file',
        help='Only these repositories: one owner/repo (or id) per line',
        exists=True, dir_okay=False, readable=True,
    ),
):
    """
    Generate SBOMs from downloaded content, every ecosystem at once.

    Reads from: data/06-github-content/{repository_id}/{sha}/,
                data/10-generated-lock
    Writes to:  data/07-sbom/{repository_id}/{sha}/sbom.json
    """
    container = get_container()
    paths = container.config.paths
    repos = repositories_named(paths.ledger_path, repos_file)

    pending: list[tuple[int, str, Path]] = []
    current = 0
    for repository_id, sha, root in scan_dirs(paths.content_dir, repos):
        lock_dir = (
            paths.generated_lock_path(repository_id, sha)
            if use_generated_locks else None
        )
        if not force and is_current_sbom(
            paths.sbom_file(repository_id, sha), root, lock_dir,
        ):
            current += 1
            continue
        pending.append((repository_id, sha, root))
        if limit is not None and len(pending) >= limit:
            break

    if not pending:
        console.print(
            f'[green]Nothing to scan.[/] {current:,} SBOM(s) are current.',
        )
        return

    service = container.get_sbom_service()
    stats = SbomStats(total=len(pending))
    with progress_bar(
        SpinnerColumn(), TextColumn(
            '[progress.description]{task.description}',
        ),
        BarColumn(), TaskProgressColumn(), MofNCompleteColumn(),
        TextColumn('•'), TimeElapsedColumn(), TextColumn('•'),
        TimeRemainingColumn(),
    ) as progress:
        task = progress.add_task('Generating SBOMs...', total=len(pending))
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = []
            for repository_id, sha, root in pending:
                lock_dir = (
                    paths.generated_lock_path(repository_id, sha)
                    if use_generated_locks else None
                )
                record = {
                    'id': repository_id,
                    'local_content_path': str(root),
                }
                futures.append(
                    executor.submit(
                        service.process_repo, record, stats, force,
                        lock_dir, syft_timeout,
                    ),
                )
            for future in as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(
                        'Error in worker thread during SBOM generation',
                        error=str(e),
                    )
                    stats.inc_failed()
                progress.advance(task)

    logger.info(
        'SBOM Generation Complete',
        generated=stats.generated,
        cache_hits=stats.cache_hits,
        skipped=stats.skipped + current,
        failed=stats.failed,
        elapsed=f"{stats.elapsed_time:.2f}s",
    )
