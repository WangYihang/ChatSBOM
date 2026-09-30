"""What a command prints: errors on stderr, and data as it is.

`handle_errors` printed "Unexpected Error: {e}" through the stdout
console as markup. An exception whose message held `[/dim]` raised
MarkupError from inside the `except` that was reporting it, in place of
the error; and a failing `queue status --metrics` printed its error
among the metrics a scraper reads. The same f-strings put paths,
exception text and server answers into markup all over the commands: a
path holding `[bold]` lost it, and one holding `[/dim]` raised (#25).
"""
import json
import sqlite3
from collections.abc import Iterator
from datetime import datetime
from datetime import timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.core.container import Container
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


# --- run: what stops it before it starts (#124) ----------------------------

@pytest.fixture
def verified(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    """`run` in a directory of its own, where the ledger it makes is
    empty, and with its token taken as verified: nothing is asked of
    GitHub."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr('chatsbom.core.config._config', None)
    monkeypatch.setattr(Container, '_instance', None)
    monkeypatch.setattr(
        'chatsbom.commands.run.verify_github_token', lambda *a, **k: 'o',
    )


#: What stops `run` before it collects anything: its options, what it
#: says, the event it is with JSON logs, and the status. A stage that
#: does not run alone is a usage error.
STOPPED = {
    'unknown stage': (
        ['--stage', 'lock'], "Unknown stage 'lock': one of release,",
        'Unknown stage', 2,
    ),
    'empty queue': (
        [], 'The queue is empty. Run chatsbom queue track first.',
        'The queue is empty', 1,
    ),
}


@pytest.mark.parametrize(
    'options, said, event, code', STOPPED.values(), ids=list(STOPPED),
)
def test_what_stops_run_is_said_on_stderr(
    verified, options, said, event, code,
):
    """Each was printed on stdout, where the pass is reported."""
    result = runner.invoke(app, ['run', '--token', 'tok', *options])

    assert result.exit_code == code, result.output
    assert result.stdout == ''
    assert said in ' '.join(result.stderr.split())


@pytest.mark.parametrize(
    'options, said, event, code', STOPPED.values(), ids=list(STOPPED),
)
def test_what_stops_run_is_one_json_object_when_logs_are_json(
    verified, monkeypatch, options, said, event, code,
):
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', 'json')

    result = runner.invoke(app, ['run', '--token', 'tok', *options])

    assert result.exit_code == code, result.output
    assert result.stdout == ''
    [line] = [json.loads(line) for line in result.stderr.splitlines()]
    assert (line['event'], line['level'], line['logger']) == (
        event, 'error', 'run',
    )
