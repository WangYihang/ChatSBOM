"""`chatsbom collect`: the collector, until SIGTERM or SIGINT (#171).

What the process does is `chatsbom/collector/process.py`'s to say. What
this does is start it, once what it cannot run without is there, each
refusal naming the fix, as the collector's loop checked before it began:

- **a token:** GITHUB_TOKEN, or CHATSBOM_GITHUB_TOKENS, and every other
  setting it reads, each a value it can use;
- **`data/` and `.cache/`, written:** the store and collector.sqlite,
  and the Syft cache. Compose mounts both from the checkout, and Docker
  makes a missing source owned by root, which the collector, run as the
  invoking user, cannot write;
- **collector.sqlite, its own:** one process holds it at a time.

A Syft it cannot find is said, and not refused: every SBOM stage fails
until there is one, and the rest goes on.
"""
from __future__ import annotations

import asyncio
import os
import shutil
from pathlib import Path

import structlog
from rich.markup import escape

from chatsbom.core.config import get_config
from chatsbom.core.decorators import say_what_to_mount
from chatsbom.core.diagnostics import fail

logger = structlog.get_logger('collect')


def _writable(directory: Path) -> str:
    """Why the collector cannot write `directory`, or '' when it can:
    made where it is not, as a run from a checkout would have it."""
    try:
        directory.mkdir(parents=True, exist_ok=True)
    except OSError as error:
        return error.strerror or str(error)
    if not os.access(directory, os.W_OK | os.X_OK):
        return 'Permission denied'
    return ''


def check_writable(directories: list[Path]) -> None:
    """Each of `directories` written, or the command stops, saying how
    to make them so, as any command says it (`say_what_to_mount`)."""
    for directory in directories:
        why = _writable(directory)
        if why:
            say_what_to_mount(directory, why)


def collect() -> None:
    """Start the collector, and exit with its status."""
    # Here, not at the top: the CLI imports every command at start-up
    # (#26), and only this one runs the collector.
    import typer

    from chatsbom.collector import process
    from chatsbom.collector.depgraph import depgraph_settings
    from chatsbom.collector.settings import settings_from
    from chatsbom.collector.settings import SettingsError
    from chatsbom.collector.state import CollectorState
    from chatsbom.collector.state import state_path
    from chatsbom.collector.state import StateError
    from chatsbom.collector.syftpool import syft_settings

    try:
        settings = settings_from()
        syft = syft_settings()
        depgraph = depgraph_settings()
    except SettingsError as error:
        fail(
            f'[bold red]Error:[/] {escape(str(error))}',
            'The collector is not configured', logger,
            setting=error.setting, problem=str(error),
        )

    paths = get_config().paths
    check_writable([paths.base_data_dir, paths.cache_dir])
    if shutil.which(syft.command) is None:
        logger.warning(
            'No Syft on PATH: every SBOM stage fails until there is one',
            syft=syft.command,
        )

    try:
        state = CollectorState.open(state_path(paths.base_data_dir))
    except StateError as error:
        fail(
            f'[bold red]Error:[/] {escape(str(error))}',
            'collector.sqlite cannot be opened', logger, error=str(error),
        )
    with state:
        status = asyncio.run(
            process.run(paths, state, settings, syft, depgraph),
        )
    if status:
        raise typer.Exit(status)
