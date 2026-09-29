"""`chatsbom web serve` (#134): opt-in, and loud when it cannot start.

Nothing deploys it yet: the Worker serves the site until the cutover.
A setting it cannot start with stops it before it listens, on stderr,
naming the setting, rather than at the first request.
"""
import json
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import uvicorn
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.core.logging import setup_logging

runner = CliRunner()

KEY = 'k' * 32

#: Every setting the service reads, cleared for each test.
SETTINGS = (
    'ALTCHA_HMAC_KEY', 'EDGE_SUBNET', 'WEB_STATE_DIR', 'CHAT_RATE_LIMIT',
    'QUERY_RATE_LIMIT', 'DAILY_SPEND_CAP_USD',
)


@pytest.fixture
def spa(tmp_path: Path) -> Path:
    root = tmp_path / 'client'
    (root / 'assets').mkdir(parents=True)
    (root / 'index.html').write_text('<!doctype html>')
    return root


@pytest.fixture(autouse=True)
def environment(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path,
) -> Iterator[None]:
    """No setting but what the test gives, web.sqlite in scratch, and
    console logs, as the root callback sets them, before and after."""
    for name in (*SETTINGS, 'CHATSBOM_LOG_FORMAT', 'ENV'):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv('WEB_STATE_DIR', str(tmp_path / 'state'))
    yield
    for name in ('CHATSBOM_LOG_FORMAT', 'ENV'):
        monkeypatch.delenv(name, raising=False)
    setup_logging('INFO')


@pytest.fixture
def served(monkeypatch: pytest.MonkeyPatch) -> list[uvicorn.Config]:
    """How each server that would have run was configured. Each starts,
    and is stopped at once."""
    configs: list[uvicorn.Config] = []

    def run(self: uvicorn.Server, *args: Any, **kwargs: Any) -> None:
        configs.append(self.config)
        self.started = True

    monkeypatch.setattr(uvicorn.Server, 'run', run)
    return configs


def serve(*argv: str) -> Any:
    return runner.invoke(app, ['web', 'serve', *argv])


def test_serves_on_loopback_by_default(spa, served, monkeypatch):
    """Where only the machine itself reaches it: in a container, compose
    names the address, and no port is published (#130)."""
    monkeypatch.setenv('ALTCHA_HMAC_KEY', KEY)

    result = serve('--spa', str(spa))

    assert result.exit_code == 0, result.output
    [config] = served
    assert (config.host, config.port) == ('127.0.0.1', 8080)
    # The TCP peer, not X-Forwarded-For, is who a request is from.
    assert config.proxy_headers is False
    assert config.server_header is False
    # A server prints nothing for a reader: its logs go to stderr.
    assert result.stdout == ''


def test_listens_where_it_is_told(spa, served, monkeypatch):
    monkeypatch.setenv('ALTCHA_HMAC_KEY', KEY)

    result = serve('--spa', str(spa), '--host', '0.0.0.0', '--port', '9000')

    assert result.exit_code == 0, result.output
    assert (served[0].host, served[0].port) == ('0.0.0.0', 9000)


def test_serves_the_web_projects_build_by_default():
    """What `npm run build` in web/ writes, from a checkout's root."""
    result = runner.invoke(app, ['web', 'serve', '--help'])
    assert result.exit_code == 0
    assert 'web/dist/client' in result.output


def refused(result: Any, served: list[uvicorn.Config]) -> str:
    """What `web serve` said as it refused to start."""
    assert result.exit_code == 1, result.output
    assert served == []
    assert result.stdout == ''
    said: str = result.stderr
    return said


def test_refuses_to_start_without_the_altcha_key(spa, served):
    """Not at the first question: at start."""
    said = refused(serve('--spa', str(spa)), served)
    assert 'ALTCHA_HMAC_KEY is not set' in said
    assert 'openssl rand -hex 32' in said


def test_refuses_to_start_with_a_setting_that_is_not_one(
    spa, served, monkeypatch,
):
    monkeypatch.setenv('ALTCHA_HMAC_KEY', KEY)
    monkeypatch.setenv('EDGE_SUBNET', '0.0.0.0/0')

    said = refused(serve('--spa', str(spa)), served)

    assert 'EDGE_SUBNET' in said
    assert 'private' in said


def test_refuses_to_start_without_a_built_page(tmp_path, served, monkeypatch):
    monkeypatch.setenv('ALTCHA_HMAC_KEY', KEY)

    said = refused(serve('--spa', str(tmp_path / 'nowhere')), served)

    assert 'No built page' in said
    assert 'npm run build' in said


def test_refuses_to_start_where_web_sqlite_cannot_be_kept(
    spa, served, monkeypatch, tmp_path,
):
    """A file where the directory should be: web.sqlite is opened before
    the service listens."""
    monkeypatch.setenv('ALTCHA_HMAC_KEY', KEY)
    occupied = tmp_path / 'occupied'
    occupied.write_text('')
    monkeypatch.setenv('WEB_STATE_DIR', str(occupied))

    said = refused(serve('--spa', str(spa)), served)

    assert 'WEB_STATE_DIR' in said
    assert str(occupied) in said.replace('\n', '')


def test_fails_when_the_server_does_not_start(spa, monkeypatch):
    """A port already taken: uvicorn says why, and returns rather than
    exiting, where `uvicorn.run` would have exited for it."""
    monkeypatch.setenv('ALTCHA_HMAC_KEY', KEY)
    monkeypatch.setattr(uvicorn.Server, 'run', lambda self: None)

    result = serve('--spa', str(spa))

    assert result.exit_code == 1
    assert 'did not start on 127.0.0.1:8080' in result.stderr


def test_says_why_as_one_json_object_when_logs_are_json(
    spa, served, monkeypatch,
):
    """A machine reads stderr then, and a line for a person is one it
    cannot parse."""
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', 'json')

    said = refused(serve('--spa', str(spa)), served)

    [line] = [json.loads(line) for line in said.splitlines()]
    assert line['level'] == 'error'
    assert line['setting'] == 'ALTCHA_HMAC_KEY'
    assert 'not set' in line['problem']
