"""Shared fixtures, and the suite's own rule: in CI every test runs."""
import builtins
import errno
import gc
import io
import os
from collections.abc import Iterator
from pathlib import Path
from typing import Any
from unittest import mock

import pytest

from chatsbom.core.config import load_env_file
from chatsbom.core.logging import setup_logging


def in_ci() -> bool:
    """Whether this is a CI run, where every test must run.

    GitHub Actions sets `CI=true`, as most CI services do. Empty, `0`
    and `false` read as unset, so that setting it to one of those turns
    it off rather than on.
    """
    return os.getenv('CI', '').strip().lower() not in ('', '0', 'false')


class EveryTestRuns:
    """In CI, a skip fails the run.

    CI provides what every test needs, so a skip there is a test that
    did not run: a server not started, a tool not installed, a module
    that no longer imports. Reported only as a skip, the run stayed
    green. Locally a skip is expected, and `-ra` says why.
    """

    def __init__(self) -> None:
        self.skipped: list[str] = []

    def _note(self, report: pytest.CollectReport | pytest.TestReport) -> None:
        # An xfail is reported as skipped too, but it ran.
        if report.skipped and not hasattr(report, 'wasxfail'):
            self.skipped.append(report.nodeid)

    def pytest_collectreport(self, report: pytest.CollectReport) -> None:
        self._note(report)

    def pytest_runtest_logreport(self, report: pytest.TestReport) -> None:
        self._note(report)

    def pytest_terminal_summary(self, terminalreporter: Any) -> None:
        if not self.skipped:
            return
        terminalreporter.write_sep(
            '=',
            f'{len(self.skipped)} skipped with CI set, where every test '
            'must run',
            red=True, bold=True,
        )
        for nodeid in self.skipped:
            terminalreporter.write_line(nodeid)

    def pytest_sessionfinish(self, session: pytest.Session) -> None:
        # A run whose only module was skipped collected nothing, and
        # would exit 5; that is a failed run here too.
        passing = (pytest.ExitCode.OK, pytest.ExitCode.NO_TESTS_COLLECTED)
        if self.skipped and session.exitstatus in passing:
            session.exitstatus = pytest.ExitCode.TESTS_FAILED


def pytest_configure(config: pytest.Config) -> None:
    if in_ci():
        config.pluginmanager.register(EveryTestRuns(), 'every-test-runs')


@pytest.fixture(autouse=True, scope='session')
def left_open_at_the_end() -> Iterator[None]:
    """The garbage collector run once more, before the last test ends.

    What a test leaves open is found when the collector reaches it, and
    fails the test running then (`filterwarnings`, pyproject.toml). One
    not reached before the session ended was only printed under `--cov`,
    as CI runs the suite: outside the tests, pytest-cov makes an
    unclosed SQLite connection a warning again, for coverage's own. Found
    here, it fails the last test instead.
    """
    yield
    gc.collect()


@pytest.fixture(autouse=True)
def no_env_file(monkeypatch: pytest.MonkeyPatch) -> None:
    """No `.env` is read in the suite unless a test asks for one.

    The CLI's root callback loads the `.env` nearest the working
    directory, and the suite runs from the repo root — where a
    developer's own, with real tokens in it, may well be. Every
    `CliRunner` run would load it, and leave it in the environment for
    every test after.
    """
    monkeypatch.setattr('chatsbom.core.config.load_env_file', lambda: None)


@pytest.fixture
def env_file_workdir(
    no_env_file: None,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Path]:
    """An empty working directory, with the real `.env` loader back.

    A test writes its `.env` here. The loader sets `os.environ` itself,
    which `monkeypatch` does not track, so the environment is put back
    as a whole afterwards.
    """
    monkeypatch.setattr('chatsbom.core.config.load_env_file', load_env_file)
    monkeypatch.chdir(tmp_path)
    with mock.patch.dict(os.environ):
        yield tmp_path


@pytest.fixture
def json_logs(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Logs as JSON for a command run in the test, and for a person
    again after it.

    The CLI's root callback reads CHATSBOM_LOG_FORMAT on every run, but
    what it chooses is the process's, and would outlast the test until
    the next `setup_logging`.
    """
    monkeypatch.setenv('CHATSBOM_LOG_FORMAT', 'json')
    yield
    for name in ('CHATSBOM_LOG_FORMAT', 'ENV'):
        monkeypatch.delenv(name, raising=False)
    setup_logging('INFO')


class HalfWrite:
    """A file on a disk that fills up partway through a write.

    It takes the first half of what it is given and then fails, as a full
    disk does. A file opened with 'w' was emptied before that, so an
    in-place write leaves only that half behind.
    """

    def __init__(self, handle: Any) -> None:
        self._handle = handle

    def write(self, data: Any) -> int:
        self._handle.write(data[:len(data) // 2])
        self._handle.flush()
        raise OSError(errno.ENOSPC, os.strerror(errno.ENOSPC))

    def __enter__(self) -> 'HalfWrite':
        return self

    def __exit__(self, *exc_info: object) -> None:
        self._handle.close()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._handle, name)


class FullDisk:
    """The directories where the disk is full."""

    def __init__(self) -> None:
        self.directories: list[Path] = []
        #: Every file a write failed in, in order, as it was opened.
        self.failed: list[Path] = []

    def fill(self, directory: Path) -> None:
        self.directories.append(Path(directory).resolve())

    def free(self) -> None:
        self.directories.clear()

    def covers(self, file: object, mode: str) -> bool:
        if not isinstance(file, (str, os.PathLike)):
            return False
        if not set(mode) & set('wxa+'):
            return False
        path = Path(file).resolve()
        return any(path.is_relative_to(full) for full in self.directories)


@pytest.fixture
def full_disk(monkeypatch: pytest.MonkeyPatch) -> FullDisk:
    """A disk that fills up under the directories given to `fill`.

    Every write to a file under them stops halfway with ENOSPC. The patch
    is on `open`, which every writer in the package goes through,
    `Path.write_text` included (via `io.open`). So the in-place writes
    this replaced and the atomic ones fail at the same point, and a test
    can check what each leaves on disk.
    """
    disk = FullDisk()
    real_open = io.open

    def opener(file: Any, mode: str = 'r', *args: Any, **kwargs: Any) -> Any:
        handle = real_open(file, mode, *args, **kwargs)
        if disk.covers(file, mode):
            disk.failed.append(Path(file))
            return HalfWrite(handle)
        return handle

    monkeypatch.setattr(builtins, 'open', opener)
    monkeypatch.setattr(io, 'open', opener)
    return disk
