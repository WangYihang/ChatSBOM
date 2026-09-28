"""What a command needs from an extra, and what it says without it (#27).

The libraries only some commands use are extras: the Claude Agent SDK,
218 MB of it, and textual for `chat`; instructor and openai for `github
classify`; pandas, matplotlib and tiktoken for the `openapi` analyses;
pyarrow, 152 MB, for `export parquet`. Everyone installed all of it, and
so did the collector's image, which runs none of those commands.

A command that needs an extra calls `require_extra` before anything
else: before it asks for a key or a database, neither of which would get
it anywhere without the libraries. `--help` never gets that far, so it
works without them.
"""
import importlib

import structlog
import typer
from rich.text import Text

from chatsbom.core.logging import logs_are_json
from chatsbom.core.logging import stderr_console

logger = structlog.get_logger('extras')


def install_command(extra: str) -> str:
    """How to install `extra` where chatsbom was installed with pip."""
    return f"pip install 'chatsbom[{extra}]'"


def require_extra(extra: str, *modules: str) -> None:
    """Import `modules`, which `extra` installs, or stop the command and
    say how to install it.

    Imported rather than looked for: the command imports them next in
    any case, and one that is there but does not import is as missing.

    On stderr, as `handle_errors` reports: as `Text`, since the error is
    an exception's and not markup, and as the log alone when logs are
    JSON, when a machine reads stderr.
    """
    for module in modules:
        try:
            importlib.import_module(module)
        except ImportError as e:
            if logs_are_json():
                logger.error(
                    'Optional dependencies not installed',
                    requires=f'chatsbom[{extra}]',
                    error=str(e),
                    install=install_command(extra),
                )
            else:
                stderr_console.print(
                    Text.assemble(
                        ('Error:', 'bold red'),
                        f' this command needs the `{extra}` extra: {e}\n',
                        ('Solution:', 'green'),
                        f' {install_command(extra)}'
                        f'  (in a checkout: uv sync --extra {extra})',
                    ),
                    soft_wrap=True,
                )
            raise typer.Exit(1) from e
