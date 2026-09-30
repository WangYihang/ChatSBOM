"""What a command prints: errors on stderr, and data as it is.

`handle_errors` printed "Unexpected Error: {e}" through the stdout
console as markup. An exception whose message held `[/dim]` raised
MarkupError from inside the `except` that was reporting it, in place of
the error; and a failing `queue status --metrics` printed its error
among the metrics a scraper reads. The same f-strings put paths,
exception text and server answers into markup all over the commands: a
path holding `[bold]` lost it, and one holding `[/dim]` raised (#25).

`queue status` went with the old pipeline (#171): the command that fails
here is `data prune`, which reads its configuration first.
"""
import json
import sqlite3
from collections.abc import Iterator
from typing import Any

import pytest
from typer.testing import CliRunner

from chatsbom.__main__ import app
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


def failing(monkeypatch: pytest.MonkeyPatch, error: Exception) -> Any:
    """`data prune`, with a configuration that cannot be read."""
    def refuse() -> None:
        raise error

    monkeypatch.setattr('chatsbom.commands.data.prune.get_config', refuse)
    result = runner.invoke(app, ['data', 'prune'])
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
    result = failing(monkeypatch, error(f'unreadable {markup} store'))

    assert f'{title}: unreadable {markup} store' in result.stderr


def test_a_command_that_fails_prints_nothing_on_stdout(monkeypatch):
    """What a command prints for its reader goes to stdout, and a reader
    may be a program: an error there is a line it cannot parse. On
    stderr, beside the logs."""
    result = failing(
        monkeypatch,
        sqlite3.OperationalError('unable to open database file'),
    )

    assert result.stdout == ''
    assert 'unable to open database file' in result.stderr


def test_a_failure_is_one_json_object_when_logs_are_json(monkeypatch):
    """A machine reads stderr then: a line for a person is one it
    cannot parse."""
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', 'json')

    result = failing(monkeypatch, RuntimeError('unreadable [/dim] store'))

    assert result.stdout == ''
    [line] = [json.loads(line) for line in result.stderr.splitlines()]
    assert line['event'] == 'Unexpected error'
    assert 'RuntimeError: unreadable [/dim] store' in line['exception']


def test_a_refused_input_is_one_json_object_when_logs_are_json(monkeypatch):
    """Its traceback is for `--debug`, as on a terminal; its reason is
    not, or a machine reading stderr would see nothing at all."""
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', 'json')

    result = failing(monkeypatch, ValueError('no store [bold] here'))

    [line] = [json.loads(line) for line in result.stderr.splitlines()]
    assert (line['event'], line['error']) == (
        'Validation error', 'no store [bold] here',
    )


# --- data printed amid markup ------------------------------------------------

@pytest.mark.parametrize('markup', ['[bold]', '[/dim]'])
def test_a_path_holding_markup_is_printed_as_it_is(tmp_path, markup):
    path = tmp_path / markup / 'schema.json'

    result = runner.invoke(app, ['export', 'schema', '--json', str(path)])

    assert result.exit_code == 0, result.output
    assert path.exists()
    assert f'Wrote{unbroken(str(path))}' in unbroken(result.stdout)
