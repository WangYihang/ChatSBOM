"""The CLI starts without importing what only some commands use (#26).

Every command paid for every other's imports. `__main__` imports each
command group, each group its commands, and those imported their
libraries at the top: the Claude Agent SDK and textual for `chat`,
matplotlib for `openapi plot-drift`, instructor and openai for `github
classify`, tiktoken for `openapi stats`, and pandas and pyarrow for
everything, because clickhouse-connect imports both when they are
installed. `chatsbom --help` took 2.9 s, twelve times what typer, rich
and structlog take to import; and the collector and the systemd timers
start the CLI every 15 minutes.

What start-up imports is measured in a fresh interpreter: the suite's
own imported all of it long ago.
"""
import json
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest
import typer
from textual.app import App
from typer.core import TyperGroup
from typer.testing import CliRunner

from chatsbom.__main__ import app

#: What only some commands use: each is imported by the command that
#: needs it, when it runs.
HEAVY = (
    'litellm',
    'claude_agent_sdk',
    'textual',
    'pandas',
    'pyarrow',
    'instructor',
    'openai',
    'tiktoken',
    'altcha',
    'fastapi',
    'starlette',
    'uvicorn',
)

#: Imports the CLI, and prints which of the modules named on its command
#: line that loaded.
PROBE = """
import json
import sys

import chatsbom.__main__

print(json.dumps(sorted(set(sys.argv[1:]) & set(sys.modules))))
"""


def test_importing_the_cli_loads_no_heavy_library(tmp_path):
    result = subprocess.run(
        [sys.executable, '-c', PROBE, *HEAVY],
        cwd=tmp_path, capture_output=True, text=True, check=True,
    )

    loaded = json.loads(result.stdout.splitlines()[-1])
    assert loaded == [], (
        f"importing the CLI loaded {', '.join(loaded)}; "
        "`python -X importtime -c 'import chatsbom.__main__'` shows through "
        'which module'
    )


#: How long `chatsbom --help` may take: LIMIT, or RATIO times what typer,
#: rich and structlog take to import on the same machine if that is more.
#: Measured on 4 vCPUs, best of three: 2.8 s before #26 and 0.7 s after,
#: against 0.2 s for the three; 14 and 3.5 times as long.
#:
#: The CLI cannot start faster than the libraries every command needs,
#: and what slows a machine slows both: with nothing byte-compiled,
#: `--help` took 2.2 s here, past LIMIT alone, yet only 2.6 times the
#: three. So the bound grows with the machine rather than flaking on it.
#: It is loose everywhere: the import test above is the precise one, and
#: this catches a heavy import that one does not name.
LIMIT = 1.5
RATIO = 6


def seconds(argv: list[str], cwd: Path) -> float:
    started = time.perf_counter()
    subprocess.run(argv, cwd=cwd, capture_output=True, check=True)
    return time.perf_counter() - started


def test_help_starts_quickly(tmp_path):
    """Best of three, interleaved: load on the machine only ever adds."""
    baseline = []
    help_ = []
    for _ in range(3):
        baseline.append(
            seconds(
                [sys.executable, '-c', 'import typer, rich, structlog'],
                tmp_path,
            ),
        )
        help_.append(
            seconds([sys.executable, '-m', 'chatsbom', '--help'], tmp_path),
        )

    bound = max(LIMIT, RATIO * min(baseline))
    assert min(help_) < bound, (
        f'`chatsbom --help` took {min(help_):.2f} s, best of three; '
        f'the bound is {bound:.2f} s (typer, rich and structlog: '
        f'{min(baseline):.2f} s)'
    )


def every_help() -> list[list[str]]:
    """The arguments for each command's `--help`, the CLI's own first."""
    found: list[list[str]] = []

    def walk(command: object, path: list[str]) -> None:
        found.append([*path, '--help'])
        # typer's group, not click's: from 0.26 typer carries a click of
        # its own, and its commands are no `click.Group`.
        if isinstance(command, TyperGroup):
            context = typer.Context(command)
            for name in command.list_commands(context):
                sub = command.get_command(context, name)
                if sub is not None:
                    walk(sub, [*path, name])

    walk(typer.main.get_command(app), [])
    return found


def test_no_help_reaches_the_network(monkeypatch: pytest.MonkeyPatch):
    """`sbom generate` connects to ClickHouse as soon as it starts, and
    importing litellm fetched a price list: `--help` gets to neither."""
    tried: list[tuple[str, object]] = []
    helping = ''

    def refuse(self: socket.socket, address: object, *args: object) -> None:
        tried.append((helping, address))
        raise OSError('--help has no business on the network')

    monkeypatch.setattr(socket.socket, 'connect', refuse)
    runner = CliRunner()
    for argv in every_help():
        helping = ' '.join(argv)
        result = runner.invoke(app, argv)
        assert result.exit_code == 0, f'{helping}: {result.output}'

    assert len(every_help()) > 40
    assert tried == []


def test_chat_still_starts_its_tui(monkeypatch: pytest.MonkeyPatch):
    """The TUI is imported when `chat` runs now, not when the CLI starts;
    it is handed the database the options name, as it was."""
    started: list[Any] = []
    monkeypatch.setattr(App, 'run', lambda self, **_: started.append(self))
    monkeypatch.setattr(
        'chatsbom.core.clickhouse.check_clickhouse_connection',
        lambda **_: True,
    )
    monkeypatch.setenv('ANTHROPIC_API_KEY', 'sk-ant-test')

    result = CliRunner().invoke(
        app, ['chat', '--host', 'clickhouse.test', '--port', '18123'],
    )

    assert result.exit_code == 0, result.output
    [tui] = started
    assert type(tui).__name__ == 'ChatSBOMApp'
    assert tui.db_config.host == 'clickhouse.test'
    assert tui.db_config.port == 18123
