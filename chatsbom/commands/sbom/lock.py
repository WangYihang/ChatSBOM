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
"""
from __future__ import annotations

import shutil
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
from chatsbom.core.sandbox import LOCK_RECIPES
from chatsbom.core.sandbox import recipes_for
from chatsbom.core.sandbox import SandboxLimits
from chatsbom.services.content_service import stored_files

logger = structlog.get_logger('sbom_lock')
app = typer.Typer()


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
    limit: int | None = typer.Option(
        None, help='Resolve at most this many content roots',
    ),
    force: bool = typer.Option(
        False, help='Re-resolve even if a lockfile was already generated',
    ),
    timeout: int = typer.Option(
        300, help='Seconds before a resolution is killed',
    ),
    memory: str = typer.Option('2g', help='Container memory limit'),
    cpus: str = typer.Option('2', help='Container CPU limit'),
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
    directory read-only, no privileges, a read-only root filesystem and
    bounded resources. Composer and Bundler only, at most 10 directories
    a repository; see chatsbom/core/sandbox.py for why Maven and PyPI
    have no recipe.

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

    resolved = cached = failed = directories = 0

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
        task = progress.add_task('Locking...', total=len(scans))

        for repository_id, sha, project in scans:
            progress.advance(task)
            output_root = paths.generated_lock_path(repository_id, sha)

            # A directory that ships the lockfile is never a target: that
            # is what the project pins, and what Syft should read. Not
            # even `--force` resolves over it: that re-resolves what we
            # wrote, never what the project committed.
            for target in recipes_for(stored_files(project)):
                if ecosystem is not None and target.ecosystem != ecosystem:
                    continue
                directories += 1
                output = target.within(output_root)
                if target.recipe.generated_in(output) and not force:
                    cached += 1
                    continue
                if force:
                    for lock in target.recipe.generated_in(output):
                        lock.unlink()

                result = generate_lockfile(
                    target.ecosystem, target.within(project), output, limits,
                )
                if result.ok:
                    resolved += 1
                    logger.info(
                        'Lockfile resolved',
                        repository_id=repository_id,
                        directory=target.directory or '.',
                        files=[p.name for p in result.produced],
                    )
                else:
                    failed += 1
                    # Nothing half-made is left for `sbom generate`.
                    if output.is_dir() and not any(output.iterdir()):
                        shutil.rmtree(output, ignore_errors=True)

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
