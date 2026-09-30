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
import socket
import sqlite3
from collections.abc import Iterator
from datetime import datetime
from datetime import timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import clickhouse_connect
import pytest
import typer
from rich.console import Console
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.core import clickhouse
from chatsbom.core.container import Container
from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import Stage
from chatsbom.core.logging import setup_logging
from chatsbom.models.query import Dependent
from chatsbom.models.query import LibraryCandidate
from chatsbom.models.relationship import DIRECT

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
    monkeypatch.setattr(clickhouse_connect, 'get_client', refuse)
    out = io.StringIO()

    with pytest.raises(typer.Exit):
        clickhouse.check_clickhouse_connection(
            'localhost', 8123, 'admin', 'admin',
            console=Console(file=out, width=200),
        )

    assert f'Auth failed: Code: 999. {markup} gone' in out.getvalue()


# --- a database that does not answer --------------------------------------

#: The commands that check the connection before they do anything else.
CONNECTING = {
    'db status': ['db', 'status'],
    'db index': ['db', 'index'],
    'db edges': ['db', 'edges'],
    'db export': ['db', 'export'],
    'db query': ['db', 'query', 'mail'],
    # Without the check, typer's traceback of the refused connection,
    # JSON logs or not (#114). Its dry run connects to nothing.
    'db raw --apply': ['db', 'raw', '--apply'],
    'export parquet': ['export', 'parquet'],
    'export d1': ['export', 'd1'],
}


@pytest.fixture
def unreachable(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> Iterator[int]:
    """CLICKHOUSE_HOST and CLICKHOUSE_PORT naming a closed local port.

    A socket bound and never listened on refuses a connection, and held
    for the test, it keeps anything else from listening there. The
    configuration is read again, as a new process reads it.
    """
    closed = socket.socket()
    closed.bind(('127.0.0.1', 0))
    port = closed.getsockname()[1]
    monkeypatch.setenv('CLICKHOUSE_HOST', '127.0.0.1')
    monkeypatch.setenv('CLICKHOUSE_PORT', str(port))
    monkeypatch.setattr('chatsbom.core.config._config', None)
    monkeypatch.setattr(Container, '_instance', None)
    monkeypatch.chdir(tmp_path)
    try:
        yield port
    finally:
        closed.close()


@pytest.mark.parametrize('command', CONNECTING.values(), ids=list(CONNECTING))
def test_a_database_that_does_not_answer_is_said_on_stderr(
    unreachable, command,
):
    """stdout is for what a command prints: `db status` printed "Cannot
    reach" there, where its tables go."""
    result = runner.invoke(app, command)

    assert result.exit_code == 1, result.output
    assert result.stdout == ''
    said = ' '.join(result.stderr.split())
    assert f'Error: Cannot reach 127.0.0.1:{unreachable}' in said
    assert 'Connection refused' in said
    assert 'docker compose up -d clickhouse' in said


@pytest.mark.parametrize('command', CONNECTING.values(), ids=list(CONNECTING))
def test_a_database_that_does_not_answer_is_one_json_object_when_logs_are_json(
    unreachable, command, monkeypatch,
):
    """A machine reads stderr then, and nothing else is printed."""
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', 'json')

    result = runner.invoke(app, command)

    assert result.exit_code == 1, result.output
    assert result.stdout == ''
    [line] = [json.loads(line) for line in result.stderr.splitlines()]
    assert (
        line['event'], line['level'], line['host'], line['port'],
    ) == ('Cannot reach ClickHouse', 'error', '127.0.0.1', unreachable)
    assert 'Connection refused' in line['error']


# --- a query that fails once connected -------------------------------------

#: What the server says when a query fails, markup and all.
GONE = 'Code: 999. [/dim] gone'


class Unanswered:
    """A query repository whose every query fails, as when the server
    goes away after the connection check has passed."""

    def __getattr__(self, name: str) -> Any:
        def query(*args: object, **kwargs: object) -> Any:
            raise ConnectionError(GONE)
        return query


def connected(
    monkeypatch: pytest.MonkeyPatch, command: str, repository: object,
) -> None:
    """`db <command>` past its connection check, reading `repository`."""
    config = SimpleNamespace(
        get_db_config=lambda role: SimpleNamespace(
            host='clickhouse', port=8123, user=role, password='',
            database='chatsbom',
        ),
    )
    container = SimpleNamespace(
        config=config, get_query_repository=lambda: repository,
    )
    module = f'chatsbom.commands.db.{command}'
    monkeypatch.setattr(f'{module}.get_container', lambda: container)
    monkeypatch.setattr(
        f'{module}.check_clickhouse_connection', lambda **_: True,
    )


#: A `db` command that reads, and how it says that a query failed.
READING = {
    'db status': (['db', 'status'], 'status', 'Error fetching status'),
    'db query': (['db', 'query', 'mail'], 'query', 'Error querying'),
    'db export': (['db', 'export'], 'export', 'Error exporting'),
}


@pytest.mark.parametrize(
    'command, module, said', READING.values(), ids=list(READING),
)
def test_a_query_that_fails_is_said_on_stderr_and_fails_the_command(
    tmp_path, monkeypatch, command, module, said,
):
    """`db status` printed "Error fetching status" on stdout, where its
    tables go, and exited 0: a script reading its stdout took the error
    for the status, and one checking its exit took it for success."""
    monkeypatch.chdir(tmp_path)
    connected(monkeypatch, module, Unanswered())

    result = runner.invoke(app, command)

    assert result.exit_code == 1, result.output
    assert 'Error' not in result.stdout
    assert f'{said}: {GONE}' in ' '.join(result.stderr.split())


@pytest.mark.parametrize(
    'command, module, said', READING.values(), ids=list(READING),
)
def test_a_query_that_fails_is_one_json_object_when_logs_are_json(
    tmp_path, monkeypatch, command, module, said,
):
    """A machine reads stderr then, and what it reads is one event."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', 'json')
    connected(monkeypatch, module, Unanswered())

    result = runner.invoke(app, command)

    assert result.exit_code == 1, result.output
    [line] = [json.loads(line) for line in result.stderr.splitlines()]
    assert (line['event'], line['level'], line['logger']) == (
        said, 'error', f'db_{module}',
    )
    assert line['error'] == GONE


# --- db: an exception no command catches (#124) ---------------------------

#: Each `db` command, and the module it runs in.
DB = {
    'db status': (['db', 'status'], 'status'),
    'db query': (['db', 'query', 'mail'], 'query'),
    'db export': (['db', 'export'], 'export'),
    'db index': (['db', 'index'], 'index'),
    'db edges': (['db', 'edges'], 'edges'),
    'db raw': (['db', 'raw'], 'raw'),
}


def broken(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, module: str,
) -> None:
    """`db <module>`, in a directory of its own, stopped by what no
    command catches: its container cannot be made."""
    def refuse() -> None:
        raise RuntimeError('unreadable [/dim] configuration')

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(f'chatsbom.commands.db.{module}.get_container', refuse)


@pytest.mark.parametrize('command, module', DB.values(), ids=list(DB))
def test_what_a_db_command_does_not_catch_is_reported_on_stderr(
    tmp_path, monkeypatch, command, module,
):
    """As `handle_errors` reports it for every other command. The `db`
    commands had none, so it was typer's traceback, 25 lines of it for
    a ledger `db index` could not read."""
    broken(tmp_path, monkeypatch, module)

    result = runner.invoke(app, command)

    # An exit, not the exception escaping the command.
    assert isinstance(result.exception, SystemExit), repr(result.exception)
    assert result.exit_code == 1
    assert result.stdout == ''
    assert 'Unexpected Error: unreadable [/dim] configuration' in (
        result.stderr
    )


@pytest.mark.parametrize('command, module', DB.values(), ids=list(DB))
def test_what_a_db_command_does_not_catch_is_one_json_object(
    tmp_path, monkeypatch, command, module,
):
    """A machine reads stderr then: the traceback was lines it could
    not parse, JSON logs or not."""
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', 'json')
    broken(tmp_path, monkeypatch, module)

    result = runner.invoke(app, command)

    assert result.exit_code == 1, result.output
    assert result.stdout == ''
    [line] = [json.loads(line) for line in result.stderr.splitlines()]
    assert (line['event'], line['level']) == ('Unexpected error', 'error')
    assert 'RuntimeError: unreadable [/dim] configuration' in (
        line['exception']
    )


# --- db query: the question on stderr, the answer on stdout ---------------

class Libraries:
    """A query repository whose search finds `found`, and which knows
    one repository depending on whichever is chosen, or none."""

    def __init__(self, *found: str, dependents: bool = True) -> None:
        self.found = found
        self.dependents = dependents
        #: How the dependents were asked for, when they were.
        self.asked: dict[str, object] = {}

    def search_library_candidates(
        self, component: str, **kwargs: object,
    ) -> list[LibraryCandidate]:
        return [LibraryCandidate(name, 3) for name in self.found]

    def get_dependents(
        self, library: str, **kwargs: object,
    ) -> list[Dependent]:
        self.asked = kwargs
        if not self.dependents:
            return []
        return [
            Dependent(
                'acme', 'shop', 10, '2.1.0', 'https://github.com/acme/shop',
                DIRECT,
            ),
        ]


def query(
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: Path,
    repository: object,
    answer: str | None = None,
) -> Any:
    """`db query mail`, reading `repository`, its question answered
    with `answer`: nothing at all when it is None."""
    monkeypatch.chdir(tmp_path)
    connected(monkeypatch, 'query', repository)
    return runner.invoke(app, ['db', 'query', 'mail'], input=answer)


def test_the_dependents_are_the_output_and_the_question_is_not(
    tmp_path, monkeypatch,
):
    """The candidates and the prompt went to stdout, before the table
    that answers them. On stderr, as a shell's `select` puts its menu:
    with stdout redirected they are still seen, and what stdout holds
    is the dependents alone."""
    result = query(monkeypatch, tmp_path, Libraries('mail', 'mailer'), '1\n')

    assert result.exit_code == 0, result.output
    assert 'Dependents of mail' in result.stdout
    assert 'acme/shop' in result.stdout
    for question in ('Library Candidates', 'mailer', 'Select a library'):
        assert question not in result.stdout
        assert question in result.stderr


@pytest.mark.parametrize(
    'repository, answer, said',
    [
        pytest.param(
            Libraries(), None, "No libraries found matching 'mail'",
            id='no library',
        ),
        pytest.param(
            Libraries('mail', dependents=False), '1\n',
            "No dependents found for 'mail'", id='no dependent',
        ),
        pytest.param(
            Libraries('mail'), '0\n', 'No selection made', id='cancelled',
        ),
        pytest.param(
            Libraries('mail'), '\n', 'No selection made', id='no choice',
        ),
    ],
)
def test_nothing_to_show_is_said_on_stderr_and_is_no_failure(
    tmp_path, monkeypatch, repository, answer, said,
):
    """An answer, if an empty one: stdout holds nothing, and the status
    is 0. On stdout, a script read the notice as a result."""
    result = query(monkeypatch, tmp_path, repository, answer)

    assert result.exit_code == 0, result.output
    assert result.stdout == ''
    assert said in result.stderr


@pytest.mark.parametrize('answer', ['x\n', '2\n', '-1\n'])
def test_an_answer_that_names_no_library_fails(
    tmp_path, monkeypatch, answer,
):
    """Not a number, or a number no candidate has: it exited 0, and a
    script could not tell a typo from a query that found nothing."""
    result = query(monkeypatch, tmp_path, Libraries('mail'), answer)

    assert result.exit_code == 1, result.output
    assert result.stdout == ''
    assert 'Invalid input' in result.stderr


def test_no_answer_at_all_fails(tmp_path, monkeypatch):
    """Input that ends before an answer: the prompt's abort was caught
    as a failed query, and "Error querying: " printed on stdout with
    no error after it, exiting 0."""
    result = query(monkeypatch, tmp_path, Libraries('mail'), '')

    assert result.exit_code == 1, result.output
    assert result.stdout == ''
    assert 'Error querying' not in result.stderr
    # Typer's own abort, which `handle_errors` passes on: caught there,
    # it was an "Unexpected Error" with nothing after it (#124).
    assert 'Aborted' in result.stderr
    assert 'Unexpected' not in result.stderr


@pytest.mark.parametrize(
    'options, asked',
    [
        (['--direct-only'], {'direct_only': True}),
        (['--limit', '3'], {'limit': 3}),
        (
            ['--ecosystem', 'maven', '--language', 'java'], {
                'ecosystem': 'maven', 'language': 'java',
            },
        ),
    ],
)
def test_its_options_may_follow_the_component(
    tmp_path, monkeypatch, options, asked,
):
    """As README writes it: `chatsbom db query mail --direct-only`. The
    command is a typer group, which took no option after its argument,
    and that was a usage error, "Missing argument 'component'"."""
    monkeypatch.chdir(tmp_path)
    libraries = Libraries('mail')
    connected(monkeypatch, 'query', libraries)

    result = runner.invoke(
        app, ['db', 'query', 'mail', *options], input='1\n',
    )

    assert result.exit_code == 0, result.output
    assert 'acme/shop' in result.stdout
    assert {name: libraries.asked[name] for name in asked} == asked


@pytest.mark.parametrize('limit', ['0', '-1'])
def test_a_limit_below_one_is_refused(tmp_path, monkeypatch, limit):
    """`--limit 0` asked the server for no dependents, and said there
    were none; the server refused `--limit -1`, and it was reported as
    a failed query. A usage error now, status 2, before anything is
    asked (#114)."""
    monkeypatch.chdir(tmp_path)
    connected(monkeypatch, 'query', Libraries('mail'))

    result = runner.invoke(
        app, ['db', 'query', '--limit', limit, 'mail'], input='1\n',
    )

    assert result.exit_code == 2, result.output
    assert result.stdout == ''
    assert '--limit' in result.stderr


@pytest.mark.parametrize(
    'repository, answer, said, level',
    [
        pytest.param(
            Libraries(), None, 'No libraries found', 'warning',
            id='no library',
        ),
        pytest.param(
            Libraries('mail'), 'x\n', 'Invalid selection', 'error',
            id='invalid',
        ),
    ],
)
def test_what_db_query_says_is_an_event_when_logs_are_json(
    tmp_path, monkeypatch, repository, answer, said, level,
):
    """The candidates and the prompt are still printed for the person
    answering; what the command says of itself is one event."""
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', 'json')

    result = query(monkeypatch, tmp_path, repository, answer)

    assert result.stdout == ''
    events = [
        json.loads(line) for line in result.stderr.splitlines()
        if line.startswith('{')
    ]
    assert [(e['event'], e['level'], e['logger']) for e in events] == [
        (said, level, 'db_query'),
    ]


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
