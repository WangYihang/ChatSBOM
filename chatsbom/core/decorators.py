import functools
from collections.abc import Callable
from typing import Any

import structlog
import typer

from chatsbom.core.logging import console
logger = structlog.get_logger()


def handle_errors(func: Callable[..., Any]) -> Callable[..., Any]:
    """Decorator to handle exceptions in CLI commands nicely."""
    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        try:
            return func(*args, **kwargs)
        except typer.Exit:
            raise
        except ValueError as e:
            console.print(f"[bold red]Validation Error:[/] {e}")
            logger.debug('Validation error', exc_info=True)
            raise typer.Exit(1)
        except KeyboardInterrupt:
            console.print('\n[yellow]Operation cancelled by user.[/]')
            raise typer.Exit(130)
        except Exception as e:
            console.print(f"[bold red]Unexpected Error:[/] {e}")
            logger.exception('Unexpected error')
            raise typer.Exit(1)
    return wrapper
