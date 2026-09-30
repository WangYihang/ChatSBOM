"""`chatsbom sbom lock`: the resolver (#168; #128 section 2.1).

Resolves lockfiles for the directories of the store that ship none: each
directory due at its repository's current commit, the most-starred
repositories first (`resolver/due.py`). A recipe is chosen per
*directory* from the manifests present there, not per repository from
its language (`sandbox.recipes_for`): a directory holding
`composer.json` and no `composer.lock` is resolved by Composer, one
holding a `Gemfile` and no `Gemfile.lock` by Bundler, wherever it is in
the repository and whatever the repository is labelled. Each result is
written under the directory it was resolved for:

    data/10-generated-lock/<repository_id>/<sha>/<directory>/<lockfile>

and the SBOM stage merges it back at that same directory: the
collector's due set makes that commit's SBOM due again once the
lockfile is written (#161).

Without `--once` it runs as a service, as compose's `resolver` runs it:
a pass whenever something is due, and a sleep of
CHATSBOM_RESOLVE_INTERVAL while nothing is (`resolver/service.py`). A
failure is kept in data/resolver.sqlite with its backoff, and SIGTERM
stops it at once, with nothing half-written.

`--workers` resolves that many directories at once, each in a container
of its own, on a network of its own whose one way out is its proxy, to
the recipe's registries (`sandbox.generate_lockfile`).
"""
from __future__ import annotations

import contextlib
import signal
import threading
from collections.abc import Iterator
from pathlib import Path
from types import FrameType
from typing import TYPE_CHECKING

import structlog
import typer
from rich.markup import escape

from chatsbom.core.config import get_config
from chatsbom.core.decorators import handle_errors
from chatsbom.core.diagnostics import fail
from chatsbom.core.diagnostics import say
from chatsbom.core.logging import console
from chatsbom.core.sandbox import DISABLED_RECIPES
from chatsbom.core.sandbox import docker_available
from chatsbom.core.sandbox import LOCK_RECIPES
from chatsbom.core.sandbox import SandboxLimits

if TYPE_CHECKING:
    from chatsbom.resolver.service import Passed

logger = structlog.get_logger('sbom_lock')
app = typer.Typer()


def _with_once_alone(context: typer.Context, force: bool) -> bool:
    """`--force` resolves again all that was resolved: pass after pass,
    unless there is one."""
    if force and not context.params.get('once'):
        raise typer.BadParameter(
            'resolves again all that was resolved, pass after pass: with '
            '--once alone',
        )
    return force


@contextlib.contextmanager
def _stopped_by_sigterm(stop: threading.Event) -> Iterator[None]:
    """SIGTERM, which a stop sends, sets `stop` for as long as this
    lasts: the resolutions in flight are told, and the loop ends. The
    handler before it is put back after. None can be set but in the
    main thread, and none is set elsewhere."""
    if threading.current_thread() is not threading.main_thread():
        yield
        return

    def stopping(signum: int, frame: FrameType | None) -> None:
        stop.set()

    before = signal.signal(signal.SIGTERM, stopping)
    try:
        yield
    finally:
        signal.signal(signal.SIGTERM, before)


@app.callback(invoke_without_command=True)
@handle_errors
def main(
    once: bool = typer.Option(
        False, '--once',
        help=(
            'One pass, then exit. Without it, a pass whenever something is '
            'due, and a sleep of CHATSBOM_RESOLVE_INTERVAL (1h) while '
            'nothing is'
        ),
    ),
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
            'Resolve at most this many directories a pass, 1 or more, the '
            'most-starred repositories first; leave it out to resolve '
            'every one due'
        ),
    ),
    force: bool = typer.Option(
        False, callback=_with_once_alone,
        help=(
            'Resolve again what was resolved already, never what a '
            'project ships; with --once alone'
        ),
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
        help=(
            'Only these repositories: one owner/repo (or id) per line, '
            'found in the universe, the newest complete search snapshot'
        ),
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
    network of its own whose one way out is a proxy to the recipe's
    registries. Composer and Bundler only, at most 10 directories a
    repository; see chatsbom/core/sandbox.py for why Maven and PyPI have
    no recipe.

    What is due is each directory at its repository's current commit
    with a manifest a recipe reads, no lockfile, shipped or resolved,
    and no failure still backing off; the repositories are the
    universe's, the most starred first. Without --once this runs as the
    resolver service: a pass whenever something is due, and a sleep of
    CHATSBOM_RESOLVE_INTERVAL while nothing is. SIGTERM stops it.

    Reads from: data/01-github-search, the release and commit decisions,
                data/06-github-content
    Writes to:  data/10-generated-lock, data/resolver.sqlite
    """
    # Here, not at the top: the CLI imports every command at start-up
    # (#26), and only this one needs the resolver.
    from chatsbom.collector.settings import SettingsError
    from chatsbom.collector.state import StateError
    from chatsbom.resolver.service import resolve_interval
    from chatsbom.resolver.service import run_pass
    from chatsbom.resolver.service import serve
    from chatsbom.resolver.state import ResolverState
    from chatsbom.resolver.state import state_path

    # Said, as every refusal and error below, where the logs go: stdout
    # is for the counts a pass reports, and these were printed there
    # (#124).
    if ecosystem is not None and ecosystem not in LOCK_RECIPES:
        reason = DISABLED_RECIPES.get(ecosystem)
        supported = [str(name) for name in sorted(LOCK_RECIPES)]
        # Nothing to resolve is no failure, and the status stays 0.
        say(
            f'[yellow]Nothing to resolve:[/] no lockfile recipe for '
            f'{escape(ecosystem)}'
            + (f': {escape(reason)}' if reason else '')
            + f'. Supported: {", ".join(supported)}.',
            'Nothing to resolve', logger,
            ecosystem=ecosystem, reason=reason, supported=supported,
        )
        return

    try:
        every = resolve_interval()
    except SettingsError as error:
        fail(
            f'[bold red]Error:[/] {escape(str(error))}',
            'The resolver is not configured', logger,
            setting=error.setting, problem=str(error),
        )

    if not docker_available():
        fail(
            '[bold red]Error:[/] Docker is required to resolve lockfiles '
            'in isolation.\n\n'
            '[green]Solution:[/] install Docker and ensure the daemon is '
            'running, then re-run this command.',
            'Docker is required to resolve lockfiles', logger,
            hint='install Docker and ensure the daemon is running',
        )

    paths = get_config().paths
    try:
        state = ResolverState.open(state_path(paths.base_data_dir))
    except StateError as error:
        fail(
            f'[bold red]Error:[/] {escape(str(error))}',
            'resolver.sqlite cannot be opened', logger, error=str(error),
        )

    names = (
        None if repos_file is None
        else repos_file.read_text(encoding='utf-8').splitlines()
    )
    limits = SandboxLimits(memory=memory, cpus=cpus, timeout=timeout)
    stop = threading.Event()
    last: list[Passed] = []

    def one_pass() -> Passed:
        passed = run_pass(
            paths, state, stop=stop, limits=limits, workers=workers,
            ecosystems=None if ecosystem is None else {ecosystem},
            names=names, limit=limit, force=force,
        )
        last[:] = [passed]
        if passed.halted is None:
            _report(passed)
        return passed

    with state, _stopped_by_sigterm(stop):
        serve(one_pass, interval=every, stop=stop, once=once)

    if stop.is_set():
        logger.info('Stopped, with nothing half-written')
    elif once and last and last[0].halted is not None:
        halted = last[0].halted
        fail(
            f'[bold red]Error:[/] {escape(halted)}',
            'The sandbox cannot be set up', logger, error=halted,
        )


def _report(passed: Passed) -> None:
    """What a pass did: logged, and on stdout, the command's output."""
    walked = passed.walk
    logger.info(
        'Lockfile generation complete',
        universe=walked.universe, roots=walked.roots,
        directories=walked.directories, resolved=passed.resolved,
        cached=walked.resolved, failed=passed.failed,
        backing_off=walked.backing_off,
    )
    console.print(
        f'[bold]{walked.roots:,}[/] content roots · '
        f'{walked.directories:,} directories to resolve · '
        f'resolved {passed.resolved:,} · cached {walked.resolved:,} · '
        f'failed {passed.failed:,} · backing off {walked.backing_off:,}',
    )
