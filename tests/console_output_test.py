"""What a command prints: errors on stderr, and data as it is.

`handle_errors` printed "Unexpected Error: {e}" through the stdout
console as markup. An exception whose message held `[/dim]` raised
MarkupError from inside the `except` that was reporting it, in place of
the error; and a failing `queue status --metrics` printed its error
among the metrics a scraper reads. The same f-strings put paths,
exception text and server answers into markup all over the commands: a
path holding `[bold]` lost it, and one holding `[/dim]` raised (#25).
"""
import contextlib
import io
import json
import sqlite3
from collections.abc import Iterator
from datetime import datetime
from datetime import timezone
from types import SimpleNamespace
from typing import Any

import pytest
import typer
from rich.console import Console
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.core import clickhouse
from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import Stage
from chatsbom.core.logging import setup_logging

runner = CliRunner()

#: Rich markup, as it turns up in an error: a closing tag nothing opened
#: raises, an opening one styles what follows and is not printed.
MARKUP = ['[/dim]', '[bold]', '[link=x]']


@pytest.fixture(autouse=True)
def console_format(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    for name in ('CHATSBOM_LOG_FORMAT', 'ENV'):
        monkeypatch.delenv(name, raising=False)
    yield
    for name in ('CHATSBOM_LOG_FORMAT', 'ENV'):
        monkeypatch.delenv(name, raising=False)
    setup_logging('INFO')


def unbroken(text: str) -> str:
    """`text` without whitespace: Rich wraps, and folds a long path."""
    return ''.join(text.split())


def failing_status(
    monkeypatch: pytest.MonkeyPatch, error: Exception, *options: str,
) -> Any:
    """`queue status`, over a ledger that cannot be opened."""
    def refuse() -> None:
        raise error

    monkeypatch.setattr('chatsbom.commands.queue.status.get_container', refuse)
    result = runner.invoke(app, ['queue', 'status', *options])
    # An exit, not MarkupError escaping the handler.
    assert isinstance(result.exception, SystemExit), repr(result.exception)
    assert result.exit_code == 1
    return result


# --- a command that fails ---------------------------------------------------

@pytest.mark.parametrize('markup', MARKUP)
@pytest.mark.parametrize(
    'error, title',
    [(RuntimeError, 'Unexpected Error'), (ValueError, 'Validation Error')],
)
def test_an_error_holding_markup_is_reported_as_it_is(
    monkeypatch, error, title, markup,
):
    result = failing_status(monkeypatch, error(f'unreadable {markup} ledger'))

    assert f'{title}: unreadable {markup} ledger' in result.stderr


def test_queue_status_metrics_prints_nothing_when_it_fails(monkeypatch):
    """A scraper reads stdout: an error there is a line it cannot
    parse. On stderr, beside the logs."""
    result = failing_status(
        monkeypatch,
        sqlite3.OperationalError('unable to open database file'),
        '--metrics',
    )

    assert result.stdout == ''
    assert 'unable to open database file' in result.stderr


def test_a_failure_is_one_json_object_when_logs_are_json(monkeypatch):
    """A machine reads stderr then: a line for a person is one it
    cannot parse."""
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', 'json')

    result = failing_status(
        monkeypatch, RuntimeError('unreadable [/dim] ledger'), '--metrics',
    )

    assert result.stdout == ''
    [line] = [json.loads(line) for line in result.stderr.splitlines()]
    assert line['event'] == 'Unexpected error'
    assert 'RuntimeError: unreadable [/dim] ledger' in line['exception']


def test_a_refused_input_is_one_json_object_when_logs_are_json(monkeypatch):
    """Its traceback is for `--debug`, as on a terminal; its reason is
    not, or a machine reading stderr would see nothing at all."""
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', 'json')

    result = failing_status(monkeypatch, ValueError('no ledger [bold] here'))

    [line] = [json.loads(line) for line in result.stderr.splitlines()]
    assert (line['event'], line['error']) == (
        'Validation error', 'no ledger [bold] here',
    )


# --- data printed amid markup ------------------------------------------------

@pytest.mark.parametrize('markup', ['[bold]', '[/dim]'])
def test_a_path_holding_markup_is_printed_as_it_is(tmp_path, markup):
    path = tmp_path / markup / 'schema.json'

    result = runner.invoke(app, ['export', 'schema', '--json', str(path)])

    assert result.exit_code == 0, result.output
    assert path.exists()
    assert f'Wrote{unbroken(str(path))}' in unbroken(result.stdout)


def test_an_error_stored_with_markup_is_shown_as_it_is(tmp_path, monkeypatch):
    """`queue status` shows each repository's last error: requests'
    text, a server's answer, a path in brackets."""
    path = tmp_path / 'ledger.sqlite3'
    with Ledger(path) as ledger:
        ledger.track(1, 'o', 'a', 'go')
        ledger.record_failure(
            1, Stage.REPO, datetime.now(timezone.utc), 'not found: [/dim]',
        )
    paths = SimpleNamespace(ledger_path=path)
    monkeypatch.setattr(
        'chatsbom.commands.queue.status.get_container',
        lambda: SimpleNamespace(config=SimpleNamespace(paths=paths)),
    )
    monkeypatch.setenv('COLUMNS', '200')

    result = runner.invoke(app, ['queue', 'status'])

    assert result.exit_code == 0, result.output
    assert 'repo: not found: [/dim]' in result.stdout


@pytest.mark.parametrize('markup', MARKUP)
def test_a_clickhouse_error_holding_markup_is_printed_as_it_is(
    monkeypatch, markup,
):
    """The server's own words, after "Auth failed:"."""
    def refuse(**kwargs: object) -> None:
        raise ConnectionError(f'Code: 999. {markup} gone')

    monkeypatch.setattr(
        clickhouse.socket, 'create_connection',
        lambda *args, **kwargs: contextlib.nullcontext(),
    )
    monkeypatch.setattr(clickhouse.clickhouse_connect, 'get_client', refuse)
    out = io.StringIO()

    with pytest.raises(typer.Exit):
        clickhouse.check_clickhouse_connection(
            'localhost', 8123, 'admin', 'admin',
            console=Console(file=out, width=200),
        )

    assert f'Auth failed: Code: 999. {markup} gone' in out.getvalue()
