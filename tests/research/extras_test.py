"""A research command without its extra says how to install it (#27).

As the core's commands do without theirs (tests/extras_test.py, whose
fixtures conftest.py borrows). The research tools' libraries are one
extra since they left the core (#167), `research`: `classify` needs
instructor and openai, and `openapi drift`, `list-paths` and `stats`
pandas or tiktoken. Without it, the command stops before it asks for
anything else, a key or a CSV, and its `--help` works regardless.
"""
from pathlib import Path

import pytest
from typer.testing import CliRunner

from chatsbom.research.__main__ import app
from tests.cli_startup_test import every_help
from tests.extras_test import EXTRAS

runner = CliRunner()


@pytest.fixture
def workdir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Where the command runs: its defaults, `data/` and the CSVs, are
    relative, and none of them is here."""
    monkeypatch.chdir(tmp_path)
    return tmp_path


#: Each research command that needs an extra, as a person would run it,
#: and the extra. Each is given the key it asks for, so that the extra
#: is all it lacks. They were `chatsbom`'s, `github classify` and
#: `openapi ...`, and needed `classify` and `openapi`.
NEEDS = [
    (['classify', '--api-key', 'sk-test'], 'research'),
    (['openapi', 'drift'], 'research'),
    (['openapi', 'list-paths'], 'research'),
    (['openapi', 'stats'], 'research'),
]


@pytest.mark.parametrize(
    'argv,extra', NEEDS, ids=[' '.join(argv[:2]) for argv, _ in NEEDS],
)
def test_a_command_without_its_extra_says_how_to_install_it(
    argv, extra, uninstall, offline, workdir, logs_reset,
):
    uninstall(extra)

    result = runner.invoke(app, argv)

    # An exit, not a traceback.
    assert isinstance(result.exception, SystemExit), repr(result.exception)
    assert result.exit_code == 1
    assert f"pip install 'chatsbom[{extra}]'" in result.stderr
    assert f'uv sync --extra {extra}' in result.stderr
    # First: nothing printed, nothing connected to, before it.
    assert result.stdout == ''
    assert offline == []


@pytest.mark.parametrize(
    'argv,extra', NEEDS, ids=[' '.join(argv[:2]) for argv, _ in NEEDS],
)
def test_help_needs_no_extra(argv, extra, uninstall):
    uninstall(extra)

    result = runner.invoke(app, [*argv, '--help'])

    assert result.exit_code == 0, result.output
    assert 'Usage' in result.output


def test_every_help_needs_no_extra_at_all(uninstall):
    """`chatsbom-research` starts without any of them: a plain `pip
    install chatsbom` has it, and none of the extras."""
    for extra in EXTRAS:
        uninstall(extra)

    for argv in every_help(app):
        result = runner.invoke(app, argv)
        assert result.exit_code == 0, f"{' '.join(argv)}: {result.output}"
