"""`chatsbom sbom lock`: resolve lockfiles, per directory, in a container.

A recipe is chosen per *directory* from the manifests present there,
not per repository from its language (`sandbox.recipes_for`): a
directory holding `composer.json` and no `composer.lock` is resolved by
Composer, one holding a `Gemfile` and no `Gemfile.lock` by Bundler,
wherever it is in the repository and whatever the repository is
labelled. Each result is written under the directory it was resolved
for:

    data/10-generated-lock/<repository_id>/<sha>/<directory>/<lockfile>

and `sbom generate` merges it back at that same directory.

`--workers` resolves that many directories at once, each in a container
of its own, on a network where none can reach another
(`sandbox.lock_network`).
"""
from __future__ import annotations

import contextlib
import threading
from concurrent.futures import as_completed
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from pathlib import Path

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

from chatsbom.commands.sbom.generate import repositories_named
from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.core.layout import scan_dirs
from chatsbom.core.logging import console
from chatsbom.core.logging import progress_bar
from chatsbom.core.sandbox import DISABLED_RECIPES
from chatsbom.core.sandbox import docker_available
from chatsbom.core.sandbox import generate_lockfile
from chatsbom.core.sandbox import lock_network
from chatsbom.core.sandbox import LOCK_RECIPES
from chatsbom.core.sandbox import LockResult
from chatsbom.core.sandbox import LockTarget
from chatsbom.core.sandbox import recipes_for
from chatsbom.core.sandbox import SandboxError
from chatsbom.core.sandbox import SandboxLimits
from chatsbom.services.content_service import stored_files

logger = structlog.get_logger('sbom_lock')
app = typer.Typer()


@dataclass(frozen=True, slots=True)
class LockJob:
    """One directory of one content root, for one recipe to resolve."""

    repository_id: int
    target: LockTarget
    #: The directory holding the manifest, within the content root.
    project: Path
    #: Where its lockfile goes, within the generated-lock root.
    output: Path


def _resolve(
    job: LockJob,
    limits: SandboxLimits,
    force: bool,
    cancel: threading.Event,
) -> LockResult:
    """Resolve one job, in a worker."""
    if force:
        # Only this recipe's lockfiles: another may share the directory,
        # and be resolving into it at this moment.
        for lock in job.target.recipe.generated_in(job.output):
            lock.unlink()
    return generate_lockfile(
        job.target.ecosystem, job.project, job.output, limits, cancel=cancel,
    )


def _resolve_all(
    jobs: list[LockJob],
    limits: SandboxLimits,
    force: bool,
    workers: int,
) -> tuple[int, int]:
    """Resolve every job, `workers` at a time: how many resolved, and how
    many failed.

    Each result is the job's own, and logged with its repository and
    directory. Ctrl-C reaches this thread alone, so the resolutions in
    flight in the others are told (`cancel`), and each removes its
    container before this raises: left alone they would run to their
    deadline, and their containers with them.
    """
    resolved = failed = 0
    failures: list[Path] = []
    cancel = threading.Event()
    pool = ThreadPoolExecutor(max_workers=workers, thread_name_prefix='lock')
    try:
        with progress_bar(
            SpinnerColumn(),
            TextColumn('[progress.description]{task.description}'),
            BarColumn(),
            TaskProgressColumn(),
            MofNCompleteColumn(),
            TextColumn('•'),
            TimeElapsedColumn(),
            TextColumn('•'),
            TimeRemainingColumn(),
        ) as progress:
            task = progress.add_task('Locking...', total=len(jobs))
            futures = {
                pool.submit(_resolve, job, limits, force, cancel): job
                for job in jobs
            }
            for future in as_completed(futures):
                job = futures[future]
                progress.advance(task)
                try:
                    result: LockResult | None = future.result()
                except Exception:
                    logger.exception(
                        'Lockfile generation failed',
                        repository_id=job.repository_id,
                        directory=job.target.directory or '.',
                    )
                    result = None
                if result is not None and result.ok:
                    resolved += 1
                    logger.info(
                        'Lockfile resolved',
                        repository_id=job.repository_id,
                        directory=job.target.directory or '.',
                        files=[p.name for p in result.produced],
                    )
                else:
                    failed += 1
                    failures.append(job.output)
    except BaseException:
        cancel.set()
        raise
    finally:
        pool.shutdown(wait=True, cancel_futures=True)

    # Nothing half-made is left for `sbom generate`: the directory of a
    # failed resolution goes, if it holds nothing. Only once every worker
    # is done, since two recipes can share a directory, and the other
    # may be writing to it.
    for output in failures:
        with contextlib.suppress(OSError):
            output.rmdir()
    return resolved, failed


@app.callback(invoke_without_command=True)
@handle_errors
def main(
    ecosystem: str | None = typer.Option(
        None,
        help=(
            'Only this ecosystem\'s recipe: '
            + ', '.join(sorted(LOCK_RECIPES))
        ),
    ),
    # 1 or more, as `sbom generate`'s: `--limit 0` resolved nothing and
    # reported a run like any other, and `--limit -1`, a slice, every
    # root but the last (#114).
    limit: int | None = typer.Option(
        None,
        min=1,
        help=(
            'Resolve at most this many content roots, 1 or more; leave '
            'it out to resolve every one'
        ),
    ),
    force: bool = typer.Option(
        False, help='Re-resolve even if a lockfile was already generated',
    ),
    timeout: int = typer.Option(
        300, help='Seconds before a resolution is killed',
    ),
    memory: str = typer.Option('2g', help='Container memory limit'),
    cpus: str = typer.Option('2', help='Container CPU limit'),
    workers: int = typer.Option(
        1, min=1,
        help=(
            'Directories to resolve at once, each in a container of up '
            'to --memory and --cpus'
        ),
    ),
    repos_file: Path | None = typer.Option(
        None,
        '--repos-file',
        help='Only these repositories: one owner/repo (or id) per line',
        exists=True, dir_okay=False, readable=True,
    ),
) -> None:
    """
    Resolve lockfiles for projects that ship none, inside a container.

    Syft reports a dependency closure only when a lockfile exists, which
    is why Composer (22% coverage) came back thin. A directory that
    ships its own lockfile is left alone: that is what it pins, and what
    Syft reads. Resolving one means running the ecosystem's own resolver
    — and a Gemfile is Ruby — so every resolution runs with the project
    directory read-only and no other host path, no privileges, a
    read-only root filesystem, bounded resources, time and output, and a
    network of its own. Composer and Bundler only, at most 10
    directories a repository; see chatsbom/core/sandbox.py for why Maven
    and PyPI have no recipe.

    Reads from: data/06-github-content
    Writes to:  data/10-generated-lock
    """
    if ecosystem is not None and ecosystem not in LOCK_RECIPES:
        reason = DISABLED_RECIPES.get(ecosystem)
        console.print(
            f'[yellow]Nothing to resolve:[/] no lockfile recipe for '
            f'{escape(ecosystem)}'
            + (f': {escape(reason)}' if reason else '')
            + f'. Supported: {", ".join(sorted(LOCK_RECIPES))}.',
        )
        return

    if not docker_available():
        console.print(
            '[bold red]Error:[/] Docker is required to resolve lockfiles '
            'in isolation.\n\n'
            '[green]Solution:[/] install Docker and ensure the daemon is '
            'running, then re-run this command.',
        )
        raise typer.Exit(1)

    container = get_container()
    paths = container.config.paths
    limits = SandboxLimits(memory=memory, cpus=cpus, timeout=timeout)
    repos = repositories_named(paths.ledger_path, repos_file)

    scans = list(scan_dirs(paths.content_dir, repos))
    if limit is not None:
        scans = scans[:limit]

    cached = directories = 0
    jobs: list[LockJob] = []
    for repository_id, sha, project in scans:
        output_root = paths.generated_lock_path(repository_id, sha)

        # A directory that ships the lockfile is never a target: that is
        # what the project pins, and what Syft should read. Not even
        # `--force` resolves over it: that re-resolves what we wrote,
        # never what the project committed.
        for target in recipes_for(stored_files(project)):
            if ecosystem is not None and target.ecosystem != ecosystem:
                continue
            directories += 1
            output = target.within(output_root)
            if target.recipe.generated_in(output) and not force:
                cached += 1
                continue
            jobs.append(
                LockJob(repository_id, target, target.within(project), output),
            )

    if jobs:
        try:
            lock_network()
        except SandboxError as e:
            console.print(f'[bold red]Error:[/] {escape(str(e))}')
            raise typer.Exit(1)
    resolved, failed = _resolve_all(jobs, limits, force, workers)

    logger.info(
        'Lockfile generation complete',
        roots=len(scans),
        directories=directories,
        resolved=resolved,
        cached=cached,
        failed=failed,
    )
    console.print(
        f'[bold]{len(scans):,}[/] content roots · {directories:,} '
        f'directories to resolve · resolved {resolved:,} · '
        f'cached {cached:,} · failed {failed:,}',
    )
