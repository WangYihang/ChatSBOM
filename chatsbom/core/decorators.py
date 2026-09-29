import errno
import functools
import os
from collections.abc import Callable
from pathlib import Path
from typing import Any
from typing import NoReturn

import structlog
import typer
from rich.markup import escape
from rich.text import Text

from chatsbom.core.diagnostics import fail
from chatsbom.core.logging import logs_are_json
from chatsbom.core.logging import stderr_console

logger = structlog.get_logger()

#: Where the CLI keeps what it collects, in the directory it runs in:
#: data/ (`PathConfig.base_data_dir`), .cache/ (`PathConfig.cache_dir`)
#: and .requests-cache/ (`core/client.py`). The collector's image has
#: none of its own: compose mounts a checkout's over them, in the
#: image's WORKDIR, for `cli` as for the collector (bare_run_test).
STATE_DIRECTORIES = ('data', '.cache', '.requests-cache')
IMAGE_WORKDIR = '/app'

#: How an OSError says a directory could not be written to: denied, or
#: on a read-only filesystem (`docker run --read-only`).
_UNWRITABLE = {errno.EACCES, errno.EPERM, errno.EROFS}


def _unwritable_state(error: OSError) -> Path | None:
    """What `error` could not write, relative to the working directory,
    if it is in or is one of `STATE_DIRECTORIES`; None otherwise."""
    if error.errno not in _UNWRITABLE:
        return None
    try:
        path = Path(os.path.abspath(os.fsdecode(error.filename)))
        relative = path.relative_to(Path.cwd())
    except (TypeError, ValueError):
        return None
    if relative.parts[:1] and relative.parts[0] in STATE_DIRECTORIES:
        return relative
    return None


def _say_what_to_mount(path: Path, error: OSError) -> NoReturn:
    """Why a command cannot write where the CLI keeps its state, and what
    to mount where the collector's image runs it.

    The image run bare, `docker run <image>`, has none of it: its
    `queue status` went to make data/ in /app, which the image's uid
    cannot write, and stopped on a traceback (#118). The directories
    must exist before Docker mounts them, or it makes them root's, as
    `deploy/collector-loop.sh` says of the same mounts.
    """
    where, uid, gid = Path.cwd(), os.getuid(), os.getgid()
    reason = error.strerror or str(error)
    mounts = ''.join(
        f'          -v "$PWD/{name}:{IMAGE_WORKDIR}/{name}" \\\n'
        for name in STATE_DIRECTORIES
    )
    fail(
        f'[bold red]Error:[/] cannot write {escape(str(path))} in '
        f'{escape(str(where))} as uid {uid} (gid {gid}): '
        f'{escape(reason)}.\n'
        '    chatsbom keeps what it collects in '
        + ', '.join(f'{name}/' for name in STATE_DIRECTORIES[:-1])
        + f' and {STATE_DIRECTORIES[-1]}/,\n'
        '    in the directory it runs in. The collector\'s image has none '
        'of its own:\n'
        '    mount a checkout\'s, and run as their owner,\n'
        '        docker run --rm --user "$(id -u):$(id -g)" \\\n'
        f'{mounts}'
        '          IMAGE COMMAND\n'
        '    or, from the checkout, where compose mounts them:\n'
        '        docker compose --profile tools run --rm cli COMMAND\n'
        '    Make them there first, or Docker makes them owned by root:\n'
        f'        mkdir -p {" ".join(STATE_DIRECTORIES)}',
        'Cannot write where the CLI keeps its state', logger,
        path=str(path), directory=str(where), uid=uid, gid=gid,
        error=reason,
    )


def _say(title: str, error: BaseException) -> None:
    """`title: error`, for a person, on stderr where the logs go.

    As `Text`, never markup: the message is an exception's, and as
    markup a `[/dim]` in it raised MarkupError here, in place of the
    error being reported.
    """
    stderr_console.print(Text.assemble((f'{title}:', 'bold red'), f' {error}'))


def handle_errors(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator to handle exceptions in CLI commands nicely.

    What stopped the command goes to stderr, never stdout: a command
    whose stdout is read — `queue status --metrics`, by a scraper — then
    prints nothing there when it fails. When logs are JSON the log says
    it alone: a machine reads stderr then, and a line for a person is one
    it cannot parse.
    """
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except typer.Exit:
            raise
        except ValueError as e:
            # Why, for everyone; the traceback, for --debug.
            if logs_are_json():
                logger.error('Validation error', error=str(e))
            else:
                _say('Validation Error', e)
            logger.debug('Validation error', exc_info=True)
            raise typer.Exit(1)
        except KeyboardInterrupt:
            if logs_are_json():
                logger.warning('Operation cancelled by user')
            else:
                stderr_console.print(
                    '\n[yellow]Operation cancelled by user.[/]',
                )
            raise typer.Exit(130)
        except Exception as e:
            if isinstance(e, OSError):
                unwritable = _unwritable_state(e)
                if unwritable is not None:
                    logger.debug(
                        'Cannot write a state directory', exc_info=True,
                    )
                    _say_what_to_mount(unwritable, e)
            if not logs_are_json():
                _say('Unexpected Error', e)
            logger.exception('Unexpected error')
            raise typer.Exit(1)
    return wrapper
