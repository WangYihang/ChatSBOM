"""Where a command says anything that is not its output (#114).

stdout is for what a command prints for its reader: its tables, its
CSV, its report. Whatever else it has to say goes to stderr, where the
logs go: an error, a warning, that nothing was found, a question. `db
status` printed "Error fetching status" among its tables and exited 0,
so a script reading its stdout took the error for the status, and one
checking how it exited took the failure for success.

When logs are JSON a machine reads stderr, and a message for a person
is lines it cannot parse: the log says it alone then, as one event with
what it concerns in its fields, as `handle_errors` and `require_extra`
do. The ClickHouse connection check (#104) did it first, before the
server went (#153).
"""
from typing import Any
from typing import Literal
from typing import NoReturn

import typer
from rich.console import Console
from rich.console import RenderableType

from chatsbom.core.logging import logs_are_json
from chatsbom.core.logging import stderr_console

#: How much a message matters, as a logger's methods name it.
Level = Literal['info', 'warning', 'error']


def say(
    message: RenderableType,
    event: str,
    logger: Any,
    level: Level = 'warning',
    *,
    console: Console | None = None,
    **fields: Any,
) -> None:
    """`message`, for a person, on stderr; or, when logs are JSON,
    `event` and `fields` alone, logged by `logger` at `level`.

    `message` is markup, and whatever in it came from data is escaped
    by the caller, as everywhere else; or whatever else Rich prints, as
    a missing GitHub token's panel is. `console` is another to print it
    on, which the connection check takes from its caller.
    """
    if logs_are_json():
        getattr(logger, level)(event, **fields)
    else:
        (console or stderr_console).print(message)


def fail(
    message: RenderableType,
    event: str,
    logger: Any,
    *,
    console: Console | None = None,
    **fields: Any,
) -> NoReturn:
    """Say why the command cannot go on, as `say` does at `error` and on
    the `console` it is given, and stop it with status 1: whatever runs
    a command that fails and exits 0 takes the failure for a success."""
    say(message, event, logger, 'error', console=console, **fields)
    raise typer.Exit(1)
