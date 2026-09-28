import logging
import os
import sys
from typing import Any
from typing import NoReturn

import structlog
from rich.console import Console
from rich.progress import Progress
from rich.progress import ProgressColumn
from rich.text import Text
from structlog.typing import EventDict

from chatsbom.core.redact import redact_urls

# What a command prints for its reader: tables, reports, results.
console = Console()
# Logs, and the progress bars drawn beside them. Not stdout, which is
# for what a command prints: `queue status --metrics` is read by a
# scraper and `export schema` by `json.loads`, and a log line among
# either was a line neither could read. One console for the logs and
# the bars, because Rich keeps a live display in place only around what
# is printed through its own console: a line from any other is written
# where the cursor is, at the end of the bar, and each refresh leaves a
# copy of the bar behind it.
stderr_console = Console(stderr=True)

#: What CHATSBOM_LOG_FORMAT may say: for a person, or for a machine.
LOG_FORMATS = ('console', 'json')

#: Whether `setup_logging` chose JSON. A machine reads stderr then, and
#: what else is written there — a progress bar, an error for a person —
#: is a line it cannot parse.
_json = False


def logs_are_json() -> bool:
    """Whether logs are JSON, as `setup_logging` last chose."""
    return _json


def progress_bar(*columns: str | ProgressColumn, **options: Any) -> Progress:
    """A progress bar, drawn where the logs go.

    On `stderr_console`, with the logs: Rich keeps a bar in place only
    around what is printed through its own console. Not drawn at all
    when logs are JSON: without a terminal Rich prints each bar once, as
    it ends, and that is a line a machine reading stderr cannot parse.
    Kept for the console format when stderr is not a terminal either,
    where that one line says how many there were and how long they
    took, to whoever reads the file.
    """
    return Progress(*columns, console=stderr_console, disable=_json, **options)


class RichConsoleRenderer:
    """
    A structlog renderer that uses rich.Console to render events.
    It formats events as key=value pairs and applies rich styling based on
    an '_style' key in the event dict, and standard log levels.

    The line is built as `Text`, never as markup. What is logged is data
    — an exception, a path, a server's answer — and in markup `[/dim]`
    is a closing tag: Rich raised MarkupError from inside the log call,
    in an `except` in place of the error being logged, and `[link=...]`
    made a hyperlink of what followed it.
    """

    def __init__(self, console: Console | None = None) -> None:
        self._console = console or stderr_console
        self._level_styles = {
            'debug': 'dim',
            'info': 'green',
            'warning': 'yellow',
            'error': 'bold red',
            'critical': 'bold magenta',
        }

    def __call__(
        self, logger: Any, name: str, event_dict: EventDict,
    ) -> NoReturn:
        # Pop custom style hint - this ensures it's not printed as a key-value pair
        custom_style = event_dict.pop('_style', None)

        # Extract standard log elements
        event = event_dict.pop('event', '')
        log_level = event_dict.pop('level', 'info')
        logger_name = event_dict.pop('logger', 'root')
        timestamp = event_dict.pop('timestamp', '')
        exc_info = event_dict.pop('exc_info', None)
        exception = event_dict.pop('exception', None)
        # What `StackInfoRenderer` makes of `stack_info=True`.
        stack = event_dict.pop('stack', None)

        line = Text()
        if timestamp:
            line.append(f'{timestamp} ', style='dim')
        if logger_name:
            line.append(f'{logger_name} ', style='bold')

        # Apply base style for level
        level_style = self._level_styles.get(log_level, 'white')
        line.append(f'{log_level:<8}', style=level_style)

        # Add event message
        line.append(f' {event}')

        # Add remaining key=value pairs
        for key, value in event_dict.items():
            line.append(' ')
            line.append(key, style='cyan')
            line.append('=')
            if key == 'status_code' and isinstance(value, int):
                if 200 <= value < 300:
                    value_style = 'green'
                elif 300 <= value < 400:
                    value_style = 'blue'
                elif 400 <= value < 500:
                    value_style = 'yellow'
                elif 500 <= value:
                    value_style = 'red'
                else:
                    value_style = 'cyan'
                line.append(str(value), style=value_style)
            else:
                line.append(repr(value), style='green')

        # Add exception info if present
        if exception or exc_info:
            line.append(f'\n{exception or exc_info}', style='red')

        if stack:
            line.append(f'\n{stack}', style='dim')

        # Highlighted as markup was, numbers and strings picked out; that
        # styles what is there and never reads it as anything else. Soft
        # wrapped: without a terminal — journald, CI, a file — the width
        # is 80, and Rich broke one event into several lines. A terminal
        # still wraps what does not fit, as it does anything else.
        self._console.print(
            self._console.highlighter(line), style=custom_style,
            soft_wrap=True,
        )

        # Raise DropEvent to prevent the logger factory from printing an empty line
        raise structlog.DropEvent


def drop_style_processor(logger, method_name, event_dict):
    """
    Remove the internal '_style' key if it exists.
    Used as a fallback to ensure it never leaks into JSON/standard logs.
    """
    event_dict.pop('_style', None)
    return event_dict


def redact_urls_processor(
    logger: Any, method_name: str, event_dict: EventDict,
) -> EventDict:
    """Every string in an event, its URLs without what could fetch them.

    The request log redacts its own; this is for the rest. requests and
    urllib3 quote the request in their errors, query and all, and a
    report's download link is signed in its query. After
    `format_exc_info`, so that a traceback is a string by then.
    """
    for key, value in event_dict.items():
        if isinstance(value, str):
            event_dict[key] = redact_urls(value)
    return event_dict


class _RedactingFormatter(logging.Formatter):
    """`%(message)s`, and a traceback after it, with `redact_urls`.

    For what libraries log, which nothing of ours is called for: urllib3
    warns as it retries a request, naming its path and query.
    """

    def format(self, record: logging.LogRecord) -> str:
        return redact_urls(super().format(record))


class _StderrHandler(logging.Handler):
    """Writes to `sys.stderr` as it is when a record comes, as
    `logging.lastResort` does.

    A `StreamHandler` keeps the stream it was made with. A progress bar
    drawn on a terminal puts one of its own in `sys.stderr`, which
    prints above the bar rather than through it, and a test runner puts
    one there for every test.
    """

    def emit(self, record: logging.LogRecord) -> None:
        try:
            stream = sys.stderr
            stream.write(self.format(record) + '\n')
            stream.flush()
        except Exception:
            self.handleError(record)


def log_format() -> str:
    """`console` or `json`, as CHATSBOM_LOG_FORMAT says.

    Without it, `ENV=production` still means `json`: it was the only
    switch before this one, and a deployment may rely on it. A value it
    cannot read counts as unset, and `setup_logging` says so: a typo is
    not a reason to stop.

    Read when logging is set up, not at import: `.env` is loaded by the
    root callback, after every module has been imported.
    """
    setting = (os.getenv('CHATSBOM_LOG_FORMAT') or '').strip().lower()
    if setting in LOG_FORMATS:
        return setting
    return 'json' if os.getenv('ENV') == 'production' else 'console'


#: Every logger's processors. `setup_logging` changes this list rather
#: than configuring a new one: a logger keeps the list it was first used
#: with (`cache_logger_on_first_use`), so a new list would reach only the
#: loggers not used yet.
_processors: list[Any] = []


def setup_logging(level: str = 'INFO') -> None:
    """
    Configure structured logging for the application.
    SSOT for logging configuration.

    Everything logged goes to stderr — structlog's events and what
    libraries log through `logging` alike — at `level` and above, in
    `log_format()`.
    """
    global _json
    json_format = _json = log_format() == 'json'
    timestamper = structlog.processors.TimeStamper(fmt='iso')
    processors: list[Any] = [
        # First, so that nothing below the level is rendered: the console
        # renderer prints as it goes, and it used to print every event
        # before anything had looked at its level.
        structlog.stdlib.filter_by_level,
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_logger_name,
        structlog.stdlib.add_log_level,
        structlog.stdlib.PositionalArgumentsFormatter(),
        timestamper,
        structlog.processors.StackInfoRenderer(),
        structlog.processors.UnicodeDecoder(),
    ]

    formatter: logging.Formatter
    if json_format:
        # Handed to `logging`, to be rendered below with what libraries
        # log, so that every line is one JSON object: a line that is not
        # is one a log collector cannot read.
        processors += [
            drop_style_processor,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ]
        formatter = structlog.stdlib.ProcessorFormatter(
            foreign_pre_chain=[
                structlog.stdlib.add_logger_name,
                structlog.stdlib.add_log_level,
                timestamper,
            ],
            processors=[
                structlog.stdlib.ProcessorFormatter.remove_processors_meta,
                structlog.processors.format_exc_info,
                redact_urls_processor,
                structlog.processors.JSONRenderer(),
            ],
        )
    else:
        # Development mode: Nice colored console output with rich.Console
        processors += [
            structlog.processors.format_exc_info,
            redact_urls_processor,
            RichConsoleRenderer(),
        ]
        formatter = _RedactingFormatter('%(message)s')
    _processors[:] = processors

    structlog.configure(
        processors=_processors,
        logger_factory=structlog.stdlib.LoggerFactory(),
        wrapper_class=structlog.stdlib.BoundLogger,
        cache_logger_on_first_use=True,
    )

    # What libraries log through `logging`: urllib3's retries, for one.
    # A handler of our own, replaced on each call, where `basicConfig`
    # does nothing at all once the root logger has any handler.
    root = logging.getLogger()
    for handler in root.handlers[:]:
        if isinstance(handler, _StderrHandler):
            root.removeHandler(handler)
    stderr = _StderrHandler()
    stderr.setFormatter(formatter)
    root.addHandler(stderr)
    root.setLevel(level)

    setting = os.getenv('CHATSBOM_LOG_FORMAT')
    if setting and setting.strip().lower() not in LOG_FORMATS:
        structlog.get_logger('logging').warning(
            f'Unknown CHATSBOM_LOG_FORMAT, using {log_format()}',
            setting=setting,
            choices=list(LOG_FORMATS),
        )
