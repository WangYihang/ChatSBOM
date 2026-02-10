import logging
import os
import sys
from typing import Any

import structlog
from rich.console import Console

# Central console for rich output
console = Console()


class RichConsoleRenderer:
    """
    A structlog renderer that uses rich.Console to render events.
    It formats events as key=value pairs and applies rich styling based on
    an '_style' key in the event dict, and standard log levels.
    """

    def __init__(self):
        self._console = Console()
        self._level_styles = {
            'debug': 'dim',
            'info': 'green',
            'warning': 'yellow',
            'error': 'bold red',
            'critical': 'bold magenta',
        }

    def __call__(self, logger, name, event_dict):
        # Pop custom style hint - this ensures it's not printed as a key-value pair
        custom_style = event_dict.pop('_style', None)

        # Extract standard log elements
        event = event_dict.pop('event', '')
        log_level = event_dict.pop('level', 'info')
        logger_name = event_dict.pop('logger', 'root')
        timestamp = event_dict.pop('timestamp', '')
        exc_info = event_dict.pop('exc_info', None)
        exception = event_dict.pop('exception', None)
        stack_info = event_dict.pop('stack_info', None)

        parts = []
        if timestamp:
            parts.append(f"[dim]{timestamp}[/dim]")
        if logger_name:
            parts.append(f"[bold]{logger_name}[/bold]")

        # Apply base style for level
        level_style = self._level_styles.get(log_level, 'white')
        parts.append(f"[{level_style}]{log_level:<8}[/{level_style}]")

        # Add event message
        parts.append(event)

        # Add remaining key=value pairs
        for key, value in event_dict.items():
            parts.append(f"[cyan]{key}[/cyan]=[green]{value!r}[/green]")

        final_msg = ' '.join(parts)

        # Add exception info if present
        if exception:
            final_msg += f"\n[red]{exception}[/red]"
        elif exc_info:
            final_msg += f"\n[red]{exc_info}[/red]"

        if stack_info:
            final_msg += f"\n[dim]{stack_info}[/dim]"

        # Apply custom style if provided, otherwise rely on rich's default for console.print
        self._console.print(final_msg, style=custom_style)

        # Raise DropEvent to prevent the logger factory from printing an empty line
        raise structlog.DropEvent


def drop_style_processor(logger, method_name, event_dict):
    """
    Remove the internal '_style' key if it exists.
    Used as a fallback to ensure it never leaks into JSON/standard logs.
    """
    event_dict.pop('_style', None)
    return event_dict


def setup_logging(level: str = 'INFO') -> None:
    """
    Configure structured logging for the application.
    SSOT for logging configuration.
    """
    # Disable standard logging handlers to avoid duplication
    logging.basicConfig(format='%(message)s', stream=sys.stdout, level=level)

    shared_processors: list[Any] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt='iso'),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    # Different formatters for Dev (Console) vs Prod (JSON)
    if os.getenv('ENV') == 'production':
        processors = shared_processors + [
            drop_style_processor,
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ]
    else:
        # Development mode: Nice colored console output with rich.Console
        processors = shared_processors + [
            structlog.processors.format_exc_info,
            RichConsoleRenderer(),
        ]

    structlog.configure(
        processors=processors,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )
