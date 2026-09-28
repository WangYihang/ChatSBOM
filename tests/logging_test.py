"""Logs: on stderr, printed as they were given, at the level asked for.

They went to stdout, where `queue status --metrics` is read by a
scraper: a ledger migrated as the command opened it, and `Ledger
migrated added_column='default_branch'` was printed among the samples.
The renderer made Rich markup of whatever it was given, so a message
holding `[/dim]` — an exception, a path in brackets in Syft's stderr —
raised `MarkupError` from inside the log call, in an `except` in place
of the error it was logging, and `[link=...]` became a hyperlink. It
printed each event before anything had looked at its level, so
`logger.debug` was printed without `--debug`. And JSON was `ENV=
production`, which nothing documented and no container set (#25).
"""
import ast
import json
import logging
import re
import sqlite3
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
import structlog
from typer.testing import CliRunner

import chatsbom
from chatsbom.__main__ import app
from chatsbom.core.ledger import Ledger
from chatsbom.core.logging import setup_logging
from chatsbom.export.schema import EXPORT_SCHEMA

runner = CliRunner()


@pytest.fixture(autouse=True)
def console_format(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """The console format at INFO, for each test and after it.

    `setup_logging` configures the whole process, so a test that set up
    JSON would otherwise be what the next one logs through.
    """
    for name in ('CHATSBOM_LOG_FORMAT', 'ENV'):
        monkeypatch.delenv(name, raising=False)
    yield
    for name in ('CHATSBOM_LOG_FORMAT', 'ENV'):
        monkeypatch.delenv(name, raising=False)
    setup_logging('INFO')


def log(level: str = 'INFO') -> Any:
    setup_logging(level)
    return structlog.get_logger('logging_test')


def printed(capsys: pytest.CaptureFixture[str]) -> str:
    """Everything printed, on either stream, as words.

    For what was printed rather than where: Rich wraps a long line.
    """
    captured = capsys.readouterr()
    return ' '.join((captured.out + captured.err).split())


def said(text: str) -> str:
    return ' '.join(text.split())


def json_lines(text: str) -> list[dict[str, Any]]:
    return [json.loads(line) for line in text.splitlines()]


# --- printed as it was given ----------------------------------------------

#: Rich markup, as it turns up in what is logged. A closing tag that
#: nothing opened raises; an opening one styles what follows it and is
#: not printed; a link makes a hyperlink of what follows.
MARKUP = ['[/dim]', '[bold]', '[link=x]']


@pytest.mark.parametrize('markup', MARKUP)
def test_markup_in_a_message_is_printed_as_it_is(markup, capsys):
    log().warning(f'Failed to use global cache: {markup} is unreadable')

    assert f'Failed to use global cache: {markup} is unreadable' in (
        printed(capsys)
    )


@pytest.mark.parametrize('markup', MARKUP)
def test_markup_in_a_value_is_printed_as_it_is(markup, capsys):
    log().error('SYFT Command Failed', error_output=f'{markup} /app/data')

    assert repr(f'{markup} /app/data') in printed(capsys)


@pytest.mark.parametrize('markup', MARKUP)
def test_markup_in_an_exception_is_printed_as_it_is(markup, capsys):
    """Logged from the `except`, where raising replaced the error."""
    try:
        raise OSError(f'cannot read {markup}')
    except OSError:
        log().exception('Scan failed')

    assert f'OSError: cannot read {markup}' in printed(capsys)


# --- on stderr ------------------------------------------------------------

def test_a_log_line_goes_to_stderr(capsys):
    log().warning('API Rate limit hit (Reactive)')

    captured = capsys.readouterr()
    assert 'API Rate limit hit (Reactive)' in captured.err
    assert captured.out == ''


def test_what_a_library_logs_goes_to_stderr_too(capsys):
    """urllib3 reports a retry through `logging`, not structlog."""
    setup_logging('INFO')

    logging.getLogger('urllib3.connectionpool').warning(
        'Retrying (Retry(total=2))',
    )

    captured = capsys.readouterr()
    assert 'Retrying (Retry(total=2))' in captured.err
    assert captured.out == ''


#: A line of Prometheus' text format: a HELP or TYPE comment, or a sample.
PROMETHEUS = re.compile(
    r'# (HELP|TYPE) \w+ .+|\w+(\{[^}]*\})? -?[0-9.]+(e[+-]?[0-9]+)?',
)


@pytest.fixture
def older_ledger(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """`queue status`'s ledger, written before `default_branch` was a
    column: opening it adds the column, and logs that it did."""
    path = tmp_path / 'ledger.sqlite3'
    Ledger(path).close()
    db = sqlite3.connect(path)
    db.execute('ALTER TABLE repository_state DROP COLUMN default_branch')
    db.commit()
    db.close()
    paths = SimpleNamespace(ledger_path=path)
    monkeypatch.setattr(
        'chatsbom.commands.queue.status.get_container',
        lambda: SimpleNamespace(config=SimpleNamespace(paths=paths)),
    )
    return path


def metrics() -> Any:
    result = runner.invoke(app, ['queue', 'status', '--metrics'])
    assert result.exit_code == 0, result.output
    assert 'chatsbom_queue_tracked 0' in result.stdout
    return result


def test_queue_status_metrics_prints_nothing_but_metrics(older_ledger):
    """What a textfile collector reads: a log line among the samples is
    a line it cannot parse."""
    result = metrics()

    assert [
        line for line in result.stdout.splitlines()
        if not PROMETHEUS.fullmatch(line)
    ] == []
    assert 'Ledger migrated' in result.stderr


def test_queue_status_metrics_logs_json_beside_them(
    older_ledger, monkeypatch,
):
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', 'json')

    result = metrics()

    assert [
        line for line in result.stdout.splitlines()
        if not PROMETHEUS.fullmatch(line)
    ] == []
    assert [
        (line['event'], line['added_column'])
        for line in json_lines(result.stderr)
    ] == [('Ledger migrated', 'default_branch')]


def test_every_progress_bar_is_drawn_where_the_logs_go():
    """On stderr, through the console the logs are printed through.

    Rich keeps a live display in place only around what is printed
    through its own console. With the bars on stdout and the logs on
    stderr, a log line was written wherever the cursor was, at the end
    of the bar, and every refresh left a copy of the bar behind it:
    `work ━━━━━  25% -:--:--lo`, then `g line 2`. Anything printed on
    stdout while a bar is drawn does the same, so it goes through the
    bar's console too.
    """
    package = Path(chatsbom.__file__).parent
    drawn: dict[str, list[str]] = {}
    printed_beside: list[str] = []
    for module in sorted(package.rglob('*.py')):
        where = module.relative_to(package)
        for node in ast.walk(ast.parse(module.read_text(encoding='utf-8'))):
            if not isinstance(node, ast.With):
                continue
            for item in node.items:
                call = item.context_expr
                if not (
                    isinstance(call, ast.Call)
                    and isinstance(call.func, ast.Name)
                    and call.func.id == 'Progress'
                ):
                    continue
                drawn[f'{where}:{node.lineno}'] = [
                    ast.unparse(keyword.value)
                    for keyword in call.keywords if keyword.arg == 'console'
                ]
                printed_beside += [
                    f'{where}:{inner.lineno}'
                    for statement in node.body
                    for inner in ast.walk(statement)
                    if isinstance(inner, ast.Call)
                    and isinstance(inner.func, ast.Attribute)
                    and isinstance(inner.func.value, ast.Name)
                    and inner.func.value.id == 'console'
                ]

    assert drawn, 'found no progress bar to check'
    assert {
        where: consoles for where, consoles in drawn.items()
        if consoles != ['stderr_console']
    } == {}
    assert printed_beside == []


# --- at the level asked for -----------------------------------------------

def test_debug_is_not_printed_at_info(capsys):
    log('INFO').debug('Repo cache expired', repo='o/r')

    assert 'Repo cache expired' not in printed(capsys)


def test_debug_is_printed_at_debug(capsys):
    log('DEBUG').debug('Repo cache expired', repo='o/r')

    assert 'Repo cache expired' in printed(capsys)


class TestTheDebugFlag:
    """`--debug` is what prints `logger.debug`, and nothing else does.

    A command given input it refuses says why, and logs the traceback at
    debug: the reason is for everyone, the traceback for whoever asks.
    """

    @staticmethod
    def run(monkeypatch: pytest.MonkeyPatch, *options: str) -> Any:
        def refuse() -> None:
            raise ValueError('no ledger to read')

        monkeypatch.setattr(
            'chatsbom.commands.queue.status.get_container', refuse,
        )
        result = runner.invoke(app, [*options, 'queue', 'status'])
        assert result.exit_code == 1, result.output
        assert 'no ledger to read' in result.output
        return result

    def test_without_it_the_traceback_is_not_printed(self, monkeypatch):
        assert 'Traceback' not in self.run(monkeypatch).output

    def test_with_it_the_traceback_is_printed(self, monkeypatch):
        result = self.run(monkeypatch, '--debug')

        assert 'Traceback (most recent call last)' in result.output


# --- JSON -----------------------------------------------------------------

def test_json_is_one_object_per_line(monkeypatch, capsys):
    """Libraries' records included: a line that is not JSON is one a log
    collector cannot read."""
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', 'json')
    logger = log()

    logger.info('Repo Saved', repo='o/r', stars=12, note='two\nlines')
    try:
        raise OSError('disk full')
    except OSError:
        logger.exception('Scan failed')
    logging.getLogger('urllib3.connectionpool').warning('Retrying')

    captured = capsys.readouterr()
    assert captured.out == ''
    saved, failed, retrying = json_lines(captured.err)
    assert saved == {
        'event': 'Repo Saved',
        'repo': 'o/r',
        'stars': 12,
        'note': 'two\nlines',
        'level': 'info',
        'logger': 'logging_test',
        'timestamp': saved['timestamp'],
    }
    assert failed['event'] == 'Scan failed'
    assert failed['level'] == 'error'
    assert 'OSError: disk full' in failed['exception']
    assert (retrying['event'], retrying['level'], retrying['logger']) == (
        'Retrying', 'warning', 'urllib3.connectionpool',
    )


def test_json_leaves_the_style_hint_out(monkeypatch, capsys):
    """`_style` is for the console."""
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', 'json')

    log().info('HTTP Request', _style='dim', status_code=200)

    [line] = json_lines(capsys.readouterr().err)
    assert '_style' not in line
    assert line['status_code'] == 200


def test_json_is_at_the_level_asked_for_too(monkeypatch, capsys):
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', 'json')

    log('INFO').debug('Repo cache expired')
    logging.getLogger('a_library').debug('Starting connection')

    assert capsys.readouterr().err == ''


def test_a_logger_already_used_follows_a_new_setup(monkeypatch, capsys):
    """A logger keeps the processors it was first used with, and logging
    is set up again for every command run in one process, as the tests
    run them: a module's logger went on in the format it first met."""
    logger = log()
    logger.info('Ledger migrated')
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', 'json')
    setup_logging('INFO')

    logger.info('Repo Saved')

    last = capsys.readouterr().err.splitlines()[-1]
    assert json.loads(last)['event'] == 'Repo Saved'


def is_json(line: str) -> bool:
    try:
        return isinstance(json.loads(line), dict)
    except json.JSONDecodeError:
        return False


@pytest.mark.parametrize(
    'environment, expected',
    [
        ({}, 'console'),
        ({'CHATSBOM_LOG_FORMAT': 'json'}, 'json'),
        ({'CHATSBOM_LOG_FORMAT': ' JSON '}, 'json'),
        ({'CHATSBOM_LOG_FORMAT': 'console'}, 'console'),
        # The older switch, still honoured.
        ({'ENV': 'production'}, 'json'),
        ({'ENV': 'development'}, 'console'),
        # The setting named for it wins.
        ({'ENV': 'production', 'CHATSBOM_LOG_FORMAT': 'console'}, 'console'),
        # One it cannot read, or an empty one, is as if unset.
        ({'CHATSBOM_LOG_FORMAT': ''}, 'console'),
        ({'CHATSBOM_LOG_FORMAT': 'jsonl'}, 'console'),
        ({'ENV': 'production', 'CHATSBOM_LOG_FORMAT': 'jsonl'}, 'json'),
    ],
)
def test_the_format_is_its_setting_then_env_production(
    environment, expected, monkeypatch, capsys,
):
    for name, value in environment.items():
        monkeypatch.setenv(name, value)

    log().info('Repo Saved')

    captured = capsys.readouterr()
    last = (captured.out + captured.err).splitlines()[-1]
    assert is_json(last) == (expected == 'json'), last


def test_a_format_it_cannot_read_is_said_where_logs_go(monkeypatch):
    """As for CHATSBOM_DEPGRAPH_API: a typo is no reason to stop, and is
    said. On stderr, so `export schema > schema.json` is still the
    schema."""
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', 'jsonl')
    monkeypatch.setenv('COLUMNS', '80')

    result = runner.invoke(app, ['export', 'schema'])

    assert result.exit_code == 0, result.output
    assert result.stdout == EXPORT_SCHEMA.to_json()
    assert "Unknown CHATSBOM_LOG_FORMAT, using console setting='jsonl'" in (
        said(result.stderr)
    )
