"""The CLI reads the `.env` nearest its working directory, first thing.

It used to be loaded as a side effect of importing `chat`, by a
`find_dotenv()` that walked up from the *source file* rather than from
where the command ran. So a checkout read the repo root's `.env` from
any directory, a pip, pipx or uvx install never read a project's at all,
and every other command had one only because `__main__` imported `chat`.
"""
import json
import os
import subprocess
import sys
from unittest import mock

import pytest
import typer
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.collector import settings
from chatsbom.warehouse import limits

runner = CliRunner()


@pytest.fixture
def tokens(monkeypatch: pytest.MonkeyPatch) -> list[str]:
    """Each token `collect` was given, as it reached the command.

    Reading its settings is the command's first step, and here its last:
    the fake records the token and ends the run, so nothing leaves the
    machine. Without a token it refuses, as the command does.
    """
    seen: list[str] = []
    real = settings.settings_from

    def settings_from(environ: object = None) -> object:
        token = os.environ.get('GITHUB_TOKEN')
        if not token:
            return real()
        seen.append(token)
        raise typer.Exit(0)

    monkeypatch.setattr(settings, 'settings_from', settings_from)
    monkeypatch.delenv('GITHUB_TOKEN', raising=False)
    monkeypatch.delenv('CHATSBOM_GITHUB_TOKENS', raising=False)
    return seen


def run_collect():
    """`collect`, which takes its token from GITHUB_TOKEN."""
    return runner.invoke(app, ['collect'])


def test_a_setting_a_command_reads_takes_its_value_from_the_env_file(
    env_file_workdir, tokens,
):
    """A command reads the environment as it runs, which is after the
    root callback has run — so a `.env` loaded there reaches it. Click
    read an option's `envvar=` then too, when a command had one."""
    (env_file_workdir / '.env').write_text('GITHUB_TOKEN=from-the-file\n')

    result = run_collect()

    assert tokens == ['from-the-file'], result.output


def test_the_nearest_env_file_up_the_tree_is_read(
    env_file_workdir, tokens, monkeypatch,
):
    """As git finds `.git`: from anywhere inside the project."""
    (env_file_workdir / '.env').write_text('GITHUB_TOKEN=from-the-project\n')
    deeper = env_file_workdir / 'data' / 'reports'
    deeper.mkdir(parents=True)
    monkeypatch.chdir(deeper)

    result = run_collect()

    assert tokens == ['from-the-project'], result.output


def test_the_environment_wins_over_the_env_file(
    env_file_workdir, tokens, monkeypatch,
):
    """An `export`, or a secret a service manager injects, is a decision;
    the file is a default."""
    (env_file_workdir / '.env').write_text(
        'GITHUB_TOKEN=from-the-file\n'
        'CHATSBOM_COST_SYMBOL=from-the-file\n',
    )
    monkeypatch.setenv('GITHUB_TOKEN', 'from-the-environment')
    monkeypatch.delenv('CHATSBOM_COST_SYMBOL', raising=False)

    run_collect()

    assert tokens == ['from-the-environment']
    # The file was read all the same, for what the environment lacked.
    assert os.environ.get('CHATSBOM_COST_SYMBOL') == 'from-the-file'


def test_logging_is_set_up_after_the_env_file_is_read(
    env_file_workdir, tokens, monkeypatch,
):
    """`setup_logging` chooses JSON or console output by ENV."""
    (env_file_workdir / '.env').write_text('ENV=production\n')
    monkeypatch.delenv('ENV', raising=False)
    seen: list[str | None] = []
    monkeypatch.setattr(
        'chatsbom.__main__.setup_logging',
        lambda level: seen.append(os.getenv('ENV')),
    )

    run_collect()

    assert seen == ['production']


def test_a_setting_read_where_it_is_used_follows_the_env_file(
    env_file_workdir, tokens, monkeypatch,
):
    """Every module is imported before the root callback runs, so a
    setting read at import could never see `.env`: DuckDB's limits are
    read as each connection is made."""
    for key in ('CHATSBOM_DUCKDB_MEMORY_LIMIT', 'CHATSBOM_DUCKDB_THREADS'):
        monkeypatch.delenv(key, raising=False)
    (env_file_workdir / '.env').write_text(
        'CHATSBOM_DUCKDB_MEMORY_LIMIT=1500MB\nCHATSBOM_DUCKDB_THREADS=3\n',
        encoding='utf-8',
    )

    run_collect()

    assert limits() == {'memory_limit': '1500MB', 'threads': 3}


#: Run in a fresh interpreter: imports the module named on its command
#: line and prints where python-dotenv's loader was called from while it
#: did. Loads nothing itself.
SPY = """
import json
import sys

import dotenv
import dotenv.main

callers = []


def spy(*args, **kwargs):
    callers.append(sys._getframe(1).f_code.co_filename)
    return False


dotenv.load_dotenv = dotenv.main.load_dotenv = spy
__import__(sys.argv[1])
print(json.dumps(callers))
"""


@pytest.mark.parametrize('module', ['chatsbom.__main__', 'chatsbom.server.app'])
def test_importing_reads_no_env_file(module, tmp_path):
    """Importing is not configuring.

    Anything loaded at import runs before the root callback, and wins
    over the file it means to read: `load_dotenv` never replaces a
    variable already set. `chat` did it, and so did litellm, looking up
    from wherever it was installed — for a checkout, the repo root.
    """
    result = subprocess.run(
        [sys.executable, '-c', SPY, module],
        cwd=tmp_path,
        capture_output=True, text=True, check=True,
    )

    assert json.loads(result.stdout.splitlines()[-1]) == []


def test_the_suite_reads_no_env_file_unless_a_test_asks(
    tmp_path, tokens, monkeypatch,
):
    """Without `env_file_workdir`, a `.env` where the suite runs is left
    alone — and that is the repo root, where a developer's own may be."""
    (tmp_path / '.env').write_text('GITHUB_TOKEN=from-a-stray-file\n')
    monkeypatch.chdir(tmp_path)

    with mock.patch.dict(os.environ):
        result = run_collect()

    assert result.exit_code == 1
    assert tokens == []
