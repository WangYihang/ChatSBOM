import functools
from collections.abc import Callable
from typing import Any

import structlog
import typer
from rich.text import Text

from chatsbom.core.logging import logs_are_json
from chatsbom.core.logging import stderr_console

logger = structlog.get_logger()


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
            if not logs_are_json():
                _say('Unexpected Error', e)
            logger.exception('Unexpected error')
            raise typer.Exit(1)
    return wrapper
