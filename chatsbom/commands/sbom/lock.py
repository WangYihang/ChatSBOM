import shutil
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
from chatsbom.core.decorators import handle_errors
from chatsbom.core.logging import console
from chatsbom.core.sandbox import docker_available
from chatsbom.core.sandbox import generate_lockfile
from chatsbom.core.sandbox import lock_recipe_for
from chatsbom.core.sandbox import SandboxLimits
from chatsbom.core.storage import load_jsonl
from chatsbom.models.language import Language

logger = structlog.get_logger('sbom_lock')
app = typer.Typer()


@app.callback(invoke_without_command=True)
@handle_errors
def main(
    language: Language | None = typer.Option(None, help='Target Language'),
    limit: int | None = typer.Option(None, help='Limit number of items'),
    force: bool = typer.Option(
        False, help='Re-resolve even if a lockfile was already generated',
    ),
    timeout: int = typer.Option(
        300, help='Seconds before a resolution is killed',
    ),
    memory: str = typer.Option('2g', help='Container memory limit'),
    cpus: str = typer.Option('2', help='Container CPU limit'),
) -> None:
    """
    Resolve lockfiles for projects that ship none, inside a container.

    Syft reports a dependency closure only when a lockfile exists, which
    is why Maven (39% coverage, 7.9 packages per repo) and Composer (22%)
    come back thin. Resolving one means running the ecosystem's own
    resolver — and a Gemfile is Ruby, a POM runs build plugins — so every
    resolution runs with the project read-only, no privileges, a read-only
    root filesystem and bounded resources. See chatsbom/core/sandbox.py.

    Reads from: data/06-github-content
    Writes to:  data/10-generated-lock
    """
    if not docker_available():
        console.print(
            '[bold red]Error:[/] Docker is required to resolve lockfiles '
            'in isolation.\n\n'
            '[green]Solution:[/] install Docker and ensure the daemon is '
            'running, then re-run this command.',
        )
        raise typer.Exit(1)

    container = get_container()
    config = container.config
    limits = SandboxLimits(memory=memory, cpus=cpus, timeout=timeout)

    for lang in [language] if language else list(Language):
        lang_str = str(lang)
        try:
            recipe = lock_recipe_for(lang)
        except ValueError:
            # Go, Rust and npm commit lockfiles as a matter of course.
            logger.info('No lockfile recipe; skipping', language=lang_str)
            continue

        input_path = config.paths.get_content_list_path(lang_str)
        if not input_path.exists():
            logger.warning(
                f"No content list for {lang_str}", path=str(input_path),
            )
            continue

        repos = load_jsonl(input_path)
        if limit:
            repos = repos[:limit]
        if not repos:
            continue

        console.print(
            f'[bold green]Resolving {lang_str} lockfiles[/] '
            f'in [cyan]{recipe.image}[/] ({len(repos)} repositories)',
        )

        resolved = cached = failed = skipped = 0

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
            console=console,
        ) as progress:
            task = progress.add_task(
                f"Locking {lang_str}...", total=len(repos),
            )

            for repo in repos:
                progress.advance(task)

                content = repo.local_content_path
                target = repo.download_target
                if not content or not target:
                    skipped += 1
                    continue

                project = Path(content)
                if not project.is_dir():
                    skipped += 1
                    continue

                output = config.paths.get_generated_lock_dir(
                    lang_str, repo.owner, repo.repo, target.commit_sha,
                )
                existing = [
                    output / name for name in recipe.produces
                    if (output / name).exists()
                ]
                if existing and not force:
                    cached += 1
                    continue
                if force and output.exists():
                    shutil.rmtree(output)

                result = generate_lockfile(lang, project, output, limits)
                if result.ok:
                    resolved += 1
                    logger.info(
                        'Lockfile resolved',
                        repo=f'{repo.owner}/{repo.repo}',
                        files=[p.name for p in result.produced],
                    )
                else:
                    failed += 1

        logger.info(
            'Lockfile generation complete',
            language=lang_str,
            resolved=resolved,
            cached=cached,
            failed=failed,
            skipped=skipped,
        )
