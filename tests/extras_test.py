"""A command whose extra is not installed says how to install it (#27).

What only some commands use is an extra now: `export parquet` needs
`chatsbom[export]`, and `web serve` `[web]`; the research tools,
`chatsbom-research`'s, need `[research]`, which was `[classify]` and
`[openapi]` until they became one (#167; tests/research/extras_test.py).
Without it, the command stops before it asks for anything else, a key
or a dataset, since neither would help; and its `--help` works
regardless.

Uninstalled is simulated by `None` in `sys.modules` for every module the
extra's distributions install: Python then raises ModuleNotFoundError
for each, and for anything under it, as if it had never been there.
"""
import json
import socket
import sys
from collections.abc import Callable
from collections.abc import Iterator
from pathlib import Path

import pytest
from packaging.utils import canonicalize_name
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.core.logging import setup_logging
from chatsbom.export.parquet import export_warehouse
from tests.cli_startup_test import every_help
from tests.dependencies_test import declared
from tests.dependencies_test import DISTRIBUTIONS

runner = CliRunner()

#: Each extra, and the distributions it installs: what uninstalling it
#: takes away. As pyproject.toml declares them (checked below).
EXTRAS = {
    'research': {'instructor', 'openai', 'pandas', 'tiktoken'},
    'export': {'pyarrow'},
    'web': {'altcha', 'fastapi', 'openai', 'starlette', 'uvicorn'},
}

#: Each command that needs an extra, as a person would run it, and the
#: extra. Each is given the key it asks for, so that the extra is all it
#: lacks. The research tools' are `chatsbom-research`'s since #167
#: (tests/research/extras_test.py).
NEEDS = [
    (['export', 'parquet'], 'export'),
    (['web', 'serve'], 'web'),
]


def modules_of(extra: str) -> set[str]:
    """What installing `extra` puts on the import path, by top-level name."""
    return {
        module for module, names in DISTRIBUTIONS.items()
        if EXTRAS[extra] & {canonicalize_name(name) for name in names}
    }


@pytest.fixture
def uninstall(monkeypatch: pytest.MonkeyPatch) -> Callable[[str], None]:
    """Takes an extra away for the test, as if it were never installed."""
    def uninstall(extra: str) -> None:
        for module in modules_of(extra):
            loaded = [
                name for name in sys.modules
                if name == module or name.startswith(f'{module}.')
            ]
            for name in {module, *loaded}:
                monkeypatch.setitem(sys.modules, name, None)
    return uninstall


@pytest.fixture
def offline(monkeypatch: pytest.MonkeyPatch) -> list[object]:
    """Every connection a command tries, each refused."""
    tried: list[object] = []

    def refuse(self: socket.socket, address: object, *args: object) -> None:
        tried.append(address)
        raise OSError('no connection before the extra is there')

    monkeypatch.setattr(socket.socket, 'connect', refuse)
    return tried


@pytest.fixture
def workdir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Where the command runs: its defaults, `data/` and the CSVs, are
    relative, and none of them is here."""
    monkeypatch.chdir(tmp_path)
    return tmp_path


@pytest.fixture
def logs_reset(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Console logs for the test, and again after it: the root callback
    sets logging up for the process, as CHATSBOM_LOG_FORMAT says."""
    for name in ('CHATSBOM_LOG_FORMAT', 'ENV'):
        monkeypatch.delenv(name, raising=False)
    yield
    for name in ('CHATSBOM_LOG_FORMAT', 'ENV'):
        monkeypatch.delenv(name, raising=False)
    setup_logging('INFO')


def test_the_extras_are_the_ones_pyproject_declares():
    found = declared()
    assert {extra: found.get(extra) for extra in EXTRAS} == EXTRAS


def test_uninstalling_an_extra_takes_its_modules_away(uninstall):
    """The simulation, checked: each extra installs something here, and
    none of it imports once taken away."""
    for extra in EXTRAS:
        assert modules_of(extra), extra
        uninstall(extra)
        for module in modules_of(extra):
            with pytest.raises(ModuleNotFoundError):
                __import__(module)


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
    """The CLI starts without any of them: the collector's image and a
    plain `pip install chatsbom` have none."""
    for extra in EXTRAS:
        uninstall(extra)

    for argv in every_help():
        result = runner.invoke(app, argv)
        assert result.exit_code == 0, f"{' '.join(argv)}: {result.output}"


def test_the_hint_is_one_json_object_when_logs_are_json(
    uninstall, offline, workdir, logs_reset, monkeypatch,
):
    """A machine reads stderr then, and a line for a person is one it
    cannot parse."""
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', 'json')
    uninstall('export')

    result = runner.invoke(app, ['export', 'parquet'])

    assert result.exit_code == 1
    [line] = [json.loads(line) for line in result.stderr.splitlines()]
    assert line['level'] == 'error'
    assert line['requires'] == 'chatsbom[export]'
    assert line['install'] == "pip install 'chatsbom[export]'"
    assert 'pyarrow' in line['error']


def test_the_parquet_writer_names_the_extra_too(uninstall, tmp_path):
    """For a caller that is not the command, which checks first."""
    uninstall('export')

    with pytest.raises(RuntimeError, match=r"pip install 'chatsbom\[export\]'"):
        export_warehouse(tmp_path / 'warehouse.duckdb', tmp_path)
