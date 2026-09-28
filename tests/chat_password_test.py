"""`chat --password` still works, and says not to use it (#29).

A password on the command line is in `ps` for anyone on the machine to
read, and in the shell's history. The guest's is read from the
environment, CLICKHOUSE_GUEST_PASSWORD, as every other command that
connects as the guest reads it.
"""
import json
from collections.abc import Iterator
from typing import Any

import pytest
from textual.app import App
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.core.logging import setup_logging

runner = CliRunner()

PASSWORD = 'hunter2-on-the-command-line'


@pytest.fixture(autouse=True)
def console_logs(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Console logs for the test, and again after it: the root callback
    sets logging up for the process, as CHATSBOM_LOG_FORMAT says."""
    for name in ('CHATSBOM_LOG_FORMAT', 'ENV'):
        monkeypatch.delenv(name, raising=False)
    yield
    for name in ('CHATSBOM_LOG_FORMAT', 'ENV'):
        monkeypatch.delenv(name, raising=False)
    setup_logging('INFO')


class Started:
    """The TUIs `chat` started, and the connections it checked, neither
    of them for real."""

    def __init__(self) -> None:
        self.apps: list[Any] = []
        self.checks: list[dict[str, Any]] = []


@pytest.fixture
def started(monkeypatch: pytest.MonkeyPatch) -> Started:
    seen = Started()

    def check(**options: Any) -> bool:
        seen.checks.append(options)
        return True

    monkeypatch.setattr(App, 'run', lambda self, **_: seen.apps.append(self))
    monkeypatch.setattr(
        'chatsbom.core.clickhouse.check_clickhouse_connection', check,
    )
    monkeypatch.setenv('ANTHROPIC_API_KEY', 'sk-ant-test')
    monkeypatch.delenv('CLICKHOUSE_GUEST_PASSWORD', raising=False)
    return seen


def test_a_password_given_as_an_option_is_used_and_warned_about(started):
    result = runner.invoke(app, ['chat', '--password', PASSWORD])

    assert result.exit_code == 0, result.output
    [tui] = started.apps
    assert tui.db_config.password == PASSWORD
    [check] = started.checks
    assert check['password'] == PASSWORD
    assert 'deprecated' in result.stderr
    assert 'CLICKHOUSE_GUEST_PASSWORD' in result.stderr
    # Said to be there, not said again.
    assert PASSWORD not in result.output


def test_the_warning_is_one_json_object_when_logs_are_json(
    started, monkeypatch,
):
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', 'json')

    result = runner.invoke(app, ['chat', '--password', PASSWORD])

    assert result.exit_code == 0, result.output
    [line] = [json.loads(line) for line in result.stderr.splitlines()]
    assert line['level'] == 'warning'
    assert 'deprecated' in line['event']
    assert PASSWORD not in json.dumps(line)


def test_the_environments_password_is_used_without_a_word(
    started, monkeypatch,
):
    monkeypatch.setenv('CLICKHOUSE_GUEST_PASSWORD', 'from-the-environment')

    result = runner.invoke(app, ['chat'])

    assert result.exit_code == 0, result.output
    [tui] = started.apps
    assert tui.db_config.password == 'from-the-environment'
    assert 'deprecated' not in result.stderr


def test_help_says_the_option_is_deprecated():
    result = runner.invoke(app, ['chat', '--help'])

    assert result.exit_code == 0, result.output
    help_text = ' '.join(result.output.split())
    assert 'Deprecated' in help_text
    assert 'CLICKHOUSE_GUEST_PASSWORD' in help_text
