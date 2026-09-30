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
`queue status` went with the old pipeline (#171).
"""
import ast
import json
import logging
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import requests
import structlog
from typer.testing import CliRunner

import chatsbom
from chatsbom.__main__ import app
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
def called(node: ast.AST, name: str) -> bool:
    """Whether `node` calls `name`, bare or as an attribute."""
    if not isinstance(node, ast.Call):
        return False
    func = node.func
    return (
        isinstance(func, ast.Name) and func.id == name
        or isinstance(func, ast.Attribute) and func.attr == name
    )


def test_every_progress_bar_is_one_the_logs_allow_for():
    """Drawn by `progress_bar`, and nothing printed while one is up.

    On stderr, through the console the logs are printed through: Rich
    keeps a live display in place only around what is printed through
    its own console. With the bars on stdout and the logs on stderr, a
    log line was written wherever the cursor was, at the end of the bar,
    and every refresh left a copy of the bar behind it: `work ━━━━━  25%
    -:--:--lo`, then `g line 2`.

    And not at all when logs are JSON, which only `progress_bar` knows.
    Without a terminal Rich prints a bar once, as it ends, and a notice
    printed beside one was plain text too: lines a machine reading
    stderr could not parse. What is said while a bar is up goes through
    the logger.
    """
    package = Path(chatsbom.__file__).parent
    built_elsewhere: list[str] = []
    drawn: list[str] = []
    printed_beside: list[str] = []
    for module in sorted(package.rglob('*.py')):
        where = module.relative_to(package)
        tree = ast.parse(module.read_text(encoding='utf-8'))
        if where != Path('core/logging.py'):
            built_elsewhere += [
                f'{where}:{node.lineno}'
                for node in ast.walk(tree) if called(node, 'Progress')
            ]
        for node in ast.walk(tree):
            if not isinstance(node, ast.With) or not any(
                called(item.context_expr, 'progress_bar')
                for item in node.items
            ):
                continue
            drawn.append(f'{where}:{node.lineno}')
            printed_beside += [
                f'{where}:{inner.lineno}'
                for statement in node.body
                for inner in ast.walk(statement)
                if called(inner, 'print')
            ]

    assert built_elsewhere == []
    assert drawn, 'found no progress bar to check'
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
            raise ValueError('no store to read')

        monkeypatch.setattr('chatsbom.commands.data.prune.get_config', refuse)
        result = runner.invoke(app, [*options, 'data', 'prune'])
        assert result.exit_code == 1, result.output
        assert 'no store to read' in result.output
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
    """A typo is no reason to stop, and is said. On stderr, so `export
    schema > schema.json` is still the schema."""
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', 'jsonl')
    monkeypatch.setenv('COLUMNS', '80')

    result = runner.invoke(app, ['export', 'schema'])

    assert result.exit_code == 0, result.output
    assert result.stdout == EXPORT_SCHEMA.to_json()
    assert "Unknown CHATSBOM_LOG_FORMAT, using console setting='jsonl'" in (
        said(result.stderr)
    )


# --- one event, one line ---------------------------------------------------

def test_a_long_event_is_one_line(monkeypatch, capsys):
    """Without a terminal — journald, CI, a file — Rich wrapped a line at
    80 columns, and one event read as several."""
    monkeypatch.setenv('COLUMNS', '80')

    log().info('HTTP Request', url='https://api.github.com/o/' + 'r' * 1000)

    [line] = capsys.readouterr().err.splitlines()
    assert line.endswith("r'")


# --- nothing on stderr but JSON, when logs are JSON --------------------------

@pytest.fixture
def an_unusable_record(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> Path:
    """`data/` with a record the warehouse cannot use: `warehouse build`
    draws a bar as it reads the store, and warns of it meanwhile."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr('chatsbom.core.config._config', None)
    listing = tmp_path / 'data' / '07-sbom' / 'go.jsonl'
    listing.parent.mkdir(parents=True)
    listing.write_text(
        json.dumps({'id': 1, 'owner': 'o', 'repo': 5, 'stars': 'many'})
        + '\n',
    )
    return tmp_path


def test_json_is_all_there_is_on_stderr(an_unusable_record, monkeypatch):
    """What a log collector reads, and it reads every line. Without a
    terminal Rich printed each progress bar once, as it ended, and the
    notice a command printed beside one was plain text."""
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', 'json')

    result = runner.invoke(app, ['warehouse', 'build'])

    assert result.exit_code == 0, result.output
    assert 'Built data/warehouse.duckdb' in result.stdout
    lines = result.stderr.splitlines()
    assert [line for line in lines if not is_json(line)] == []
    assert [
        (line['event'], line['repository_id'])
        for line in map(json.loads, lines)
    ] == [('Unusable record', 1)]


# --- a signed URL, whatever carries it -----------------------------------------

#: requests' text for a download that failed to connect: the request's
#: path and query, and a report's download link is signed in its query.
SIGNED_ERROR = (
    "HTTPSConnectionPool(host='sbom-exports.example', port=443): Max "
    'retries exceeded with url: /a.json?X-Amz-Signature=5ec7e75ec7e7 '
    '(Caused by NewConnectionError())'
)


@pytest.mark.parametrize('log_format', ['console', 'json'])
def test_a_signature_is_logged_nowhere(log_format, monkeypatch, capsys):
    """In a value, in an exception, or in urllib3's own warning as it
    retries a download, which names the request's path and query: that
    one comes through `logging`, where nothing of ours is called."""
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', log_format)
    logger = log()

    logger.warning('Stage failed', error=SIGNED_ERROR)
    try:
        raise requests.ConnectionError(SIGNED_ERROR)
    except requests.ConnectionError:
        logger.exception('Download failed')
    logging.getLogger('urllib3.connectionpool').warning(
        "Retrying (%r) after connection broken by '%r': %s",
        'Retry(total=2)', 'NewConnectionError()',
        '/a.json?X-Amz-Signature=5ec7e75ec7e7',
    )

    captured = capsys.readouterr()
    logged = ''.join((captured.out + captured.err).splitlines())
    assert '5ec7e75ec7e7' not in logged
    assert logged.count('/a.json?*****') == 3


# --- a URL in brackets --------------------------------------------------------

#: A URL as a message puts one, in brackets: the redaction took the `]`
#: for part of its host, and urlsplit's ValueError came out of the log
#: call. Where `logging` called the redaction — in JSON, and for what
#: libraries log — it caught the error and printed the record whole in
#: place of the line: unredacted, and not JSON.
BRACKETED = 'see [https://example.com]'


@pytest.mark.parametrize('log_format', ['console', 'json'])
def test_a_url_in_brackets_is_logged_as_it_is(log_format, monkeypatch, capsys):
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', log_format)
    logger = log()

    logger.warning(BRACKETED, note=BRACKETED)
    logging.getLogger('a_library').warning(BRACKETED)

    captured = capsys.readouterr()
    assert captured.out == ''
    lines = captured.err.splitlines()
    if log_format == 'json':
        assert [line for line in lines if not is_json(line)] == []
        assert [
            (line['event'], line.get('note')) for line in map(json.loads, lines)
        ] == [(BRACKETED, BRACKETED), (BRACKETED, None)]
    else:
        ours, theirs = lines
        assert f'{BRACKETED} note={BRACKETED!r}' in ours
        assert theirs == BRACKETED


@pytest.mark.parametrize('log_format', ['console', 'json'])
def test_a_signature_beside_a_url_in_brackets_is_logged_nowhere(
    log_format, monkeypatch, capsys,
):
    """Once one URL in an event raised, `logging` printed the event as it
    was given: the signature beside it too."""
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', log_format)
    logger = log()

    logger.warning('Stage failed', see=BRACKETED, error=SIGNED_ERROR)
    logging.getLogger('urllib3.connectionpool').warning(
        "Retrying %s after '%s'", BRACKETED,
        '/a.json?X-Amz-Signature=5ec7e75ec7e7',
    )

    captured = capsys.readouterr()
    logged = ''.join((captured.out + captured.err).splitlines())
    assert '5ec7e75ec7e7' not in logged
    assert logged.count('/a.json?*****') == 2
    assert logged.count(BRACKETED) == 2


#: requests' error for a URL it cannot parse, which it quotes; and one
#: signed URL more, in brackets.
UNPARSEABLE = (
    'Failed to parse: https://[sbom-exports.example/a.json?'
    'X-Amz-Signature=5ec7e75ec7e7 (redirected from [https://'
    'sbom-exports.example/b.json?X-Amz-Signature=5ec7e75ec7e7])'
)


@pytest.mark.parametrize('log_format', ['console', 'json'])
def test_a_traceback_quoting_urls_in_brackets_is_logged_redacted(
    log_format, monkeypatch, capsys,
):
    """requests refuses a URL it cannot parse before sending it, and
    says which: urlsplit refuses it too."""
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', log_format)
    logger = log()

    try:
        raise requests.exceptions.InvalidURL(UNPARSEABLE)
    except requests.exceptions.InvalidURL:
        logger.exception('Download failed')

    captured = capsys.readouterr()
    logged = ''.join((captured.out + captured.err).splitlines())
    assert '5ec7e75ec7e7' not in logged
    assert 'Failed to parse: https://[sbom-exports.example/a.json?*****' in (
        logged
    )
    assert '[https://sbom-exports.example/b.json?*****])' in logged


# --- a credential, whatever carries it ------------------------------------

#: A token, in the header a request would have carried it in.
CREDENTIAL = 'ghp_5ec7e75ec7e7a1b2c3d4e5f6a1b2c3d4e5f6'

#: requests' error for a header it refuses, which quotes the header: the
#: token with the carriage return of a file saved on Windows, which the
#: log printed whole (#113).
REFUSED_HEADER = (
    'Invalid leading whitespace, reserved character(s), or return '
    f"character(s) in header value: 'Bearer {CREDENTIAL}\\r'"
)


@pytest.mark.parametrize('log_format', ['console', 'json'])
def test_a_credential_is_logged_nowhere(log_format, monkeypatch, capsys):
    """In a value, as `verify_github_token` logs the error it caught; in
    a traceback; and in what a library logs, where nothing of ours is
    called."""
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', log_format)
    logger = log()

    logger.warning('Could not verify GitHub token', error=REFUSED_HEADER)
    try:
        raise requests.exceptions.InvalidHeader(REFUSED_HEADER)
    except requests.exceptions.InvalidHeader:
        logger.exception('Request failed')
    logging.getLogger('urllib3.connectionpool').warning(
        'Sent %s', f'Authorization: token {CREDENTIAL}',
    )

    captured = capsys.readouterr()
    logged = ''.join((captured.out + captured.err).splitlines())
    assert CREDENTIAL[4:] not in logged
    assert logged.count('Bearer *****') == 2
    assert logged.count('Authorization: token *****') == 1


@pytest.mark.parametrize('log_format', ['console', 'json'])
def test_what_is_said_of_a_token_is_logged_as_it_is(
    log_format, monkeypatch, capsys,
):
    """"token" is a word as well as a scheme: what the log says of one,
    and the label a token is named by in place of its value, are left
    as they are."""
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', log_format)
    logger = log()

    logger.warning(
        'Dependency graph token rejected by GitHub; not used',
        token='token 2 (octocat)',
    )
    logger.info('GitHub token verified', note='a Bearer token expired.')

    captured = capsys.readouterr()
    logged = ''.join((captured.out + captured.err).splitlines())
    for said in (
        'Dependency graph token rejected by GitHub; not used',
        'token 2 (octocat)', 'GitHub token verified',
        'a Bearer token expired.',
    ):
        assert said in logged
    assert '*****' not in logged
