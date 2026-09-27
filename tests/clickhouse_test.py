"""What the CLI says when no ClickHouse answers (#20).

It printed a recipe that published 8123 on every interface and made
`admin` with the password `admin` and GRANT ALL: the exposure compose
and the README had since closed, handed out again on the first failed
connection. It now gives the README's two ways, in one place.
"""
import io
import re

import pytest
from rich.console import Console

from chatsbom.core import clickhouse

UNREACHABLE = [
    TimeoutError('timed out'),
    ConnectionRefusedError(111, 'Connection refused'),
]


def hint_for(error: OSError, monkeypatch: pytest.MonkeyPatch) -> str:
    """What `_check_network` prints when connecting fails with `error`."""
    def connect(*args: object, **kwargs: object) -> None:
        raise error

    monkeypatch.setattr(clickhouse.socket, 'create_connection', connect)
    out = io.StringIO()
    # Wide, so that no command is wrapped across lines.
    console = Console(file=out, width=10_000, color_system=None)

    assert clickhouse._check_network('127.0.0.1', 8123, console) is False
    return out.getvalue()


@pytest.mark.parametrize('error', UNREACHABLE, ids=['timeout', 'refused'])
def test_the_hint_publishes_nothing_beyond_the_loopback(error, monkeypatch):
    hint = hint_for(error, monkeypatch)

    published = re.findall(r'(?:-p|--publish)[ =](\S+)', hint)
    assert published, 'the hint no longer shows how to publish the port'
    assert [p for p in published if not p.startswith('127.0.0.1:')] == []


@pytest.mark.parametrize('error', UNREACHABLE, ids=['timeout', 'refused'])
def test_the_hint_makes_no_accounts_of_its_own(error, monkeypatch):
    """The accounts are database/config/users.d: `admin`, and a `guest`
    whose grants and cost limits a CREATE USER line would not have."""
    hint = hint_for(error, monkeypatch)

    assert 'GRANT' not in hint.upper()
    assert 'CREATE USER' not in hint.upper()
    assert 'database/config/users.d' in hint


@pytest.mark.parametrize('error', UNREACHABLE, ids=['timeout', 'refused'])
def test_the_hint_starts_the_database_as_the_readme_does(error, monkeypatch):
    hint = hint_for(error, monkeypatch)

    assert 'docker compose up -d clickhouse' in hint
    assert 'github.com/WangYihang/ChatSBOM' in hint


def test_a_timeout_and_a_refusal_get_the_same_advice(monkeypatch):
    """Two copies of the recipe were the two places to keep in step."""
    timeout, refused = (
        hint_for(error, monkeypatch).partition('Solution')[2]
        for error in UNREACHABLE
    )
    assert timeout and timeout == refused
