"""Shared fixtures, including a real ClickHouse for query-layer tests."""
import builtins
import errno
import io
import os
import socket
import uuid
from collections.abc import Callable
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any
from unittest import mock

import pytest

from chatsbom.core.config import DatabaseConfig
from chatsbom.core.config import load_env_file
from chatsbom.core.repository import IngestionRepository
from chatsbom.core.repository import QueryRepository

CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_TEST_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_TEST_PORT', '8123'))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_ADMIN_USER', 'admin')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_ADMIN_PASSWORD', 'admin')
#: The server's own guest, where users.d gave it one: the account the
#: dashboard and `db query` connect as, with compose's default password.
CLICKHOUSE_GUEST_USER = os.getenv('CLICKHOUSE_GUEST_USER', 'guest')
CLICKHOUSE_GUEST_PASSWORD = os.getenv('CLICKHOUSE_GUEST_PASSWORD', 'guest')


def in_ci() -> bool:
    """Whether this is a CI run, where every test must run.

    GitHub Actions sets `CI=true`, as most CI services do. Empty, `0`
    and `false` read as unset, so that setting it to one of those turns
    it off rather than on.
    """
    return os.getenv('CI', '').strip().lower() not in ('', '0', 'false')


def _reachable() -> bool:
    try:
        with socket.create_connection((CLICKHOUSE_HOST, CLICKHOUSE_PORT), 1.0):
            return True
    except OSError:
        return False


#: A test that needs a live ClickHouse. Without one it is skipped, and
#: says how to start one; in CI, which starts one, it fails instead
#: (`pytest_runtest_setup`). The marker is registered in pyproject.toml.
requires_clickhouse = pytest.mark.clickhouse

_CLICKHOUSE_REACHABLE = pytest.StashKey[bool]()


@pytest.hookimpl(tryfirst=True)
def pytest_runtest_setup(item: pytest.Item) -> None:
    """Skip a `clickhouse` test without a server, or in CI fail it.

    The server is probed once a run, at the first test that needs it.
    """
    if item.get_closest_marker('clickhouse') is None:
        return
    stash = item.config.stash
    if _CLICKHOUSE_REACHABLE not in stash:
        stash[_CLICKHOUSE_REACHABLE] = _reachable()
    if stash[_CLICKHOUSE_REACHABLE]:
        return
    unreachable = (
        f'ClickHouse not reachable at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT}'
    )
    if in_ci():
        pytest.fail(
            f'{unreachable}, and CI is set: every test must run',
            pytrace=False,
        )
    pytest.skip(f'{unreachable} (start it with `docker compose up -d`)')


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
def no_database(monkeypatch: pytest.MonkeyPatch) -> None:
    """No database for a command to reach.

    `sbom generate` also keeps each record it writes in `raw_documents`
    when a database answers, and the one the environment names may be a
    real one: a run of these tests put their repositories in its landing
    zone, where `db index` reads them as the corpus. Without one the
    command writes its ledger alone, as it is meant to.
    """
    from chatsbom.core.container import Container

    def refuse(self: Container) -> IngestionRepository:
        raise ConnectionError('no database in this test')

    monkeypatch.setattr(Container, 'get_ingestion_repository', refuse)


def _config(database: str) -> DatabaseConfig:
    return DatabaseConfig(
        host=CLICKHOUSE_HOST,
        port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER,
        password=CLICKHOUSE_PASSWORD,
        database=database,
    )


@pytest.fixture
def clickhouse_db() -> Iterator[str]:
    """A throwaway database with the production schema applied."""
    import clickhouse_connect

    name = f"chatsbom_test_{uuid.uuid4().hex[:12]}"
    admin = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
        database='default',
    )
    admin.command(f'CREATE DATABASE {name}')
    try:
        with IngestionRepository(_config(name)) as repo:
            repo.ensure_schema()
        yield name
    finally:
        admin.command(f'DROP DATABASE IF EXISTS {name}')
        admin.close()


@pytest.fixture
def ingest(clickhouse_db: str) -> Iterator[IngestionRepository]:
    with IngestionRepository(_config(clickhouse_db)) as repo:
        yield repo


@pytest.fixture
def query(clickhouse_db: str) -> Iterator[QueryRepository]:
    with QueryRepository(_config(clickhouse_db)) as repo:
        yield repo


class DbCommand:
    """`chatsbom db ...`, run against the test database.

    The command as written, with only its container swapped.
    """

    def __init__(self, container: Any) -> None:
        self.container = container
        #: Called with each ingestion repository the command opens, so a
        #: test can watch what it sends.
        self.on_open: list[Callable[[IngestionRepository], None]] = []

    def repository(self) -> IngestionRepository:
        repository = IngestionRepository(
            self.container.config.get_db_config('admin'),
        )
        for hook in self.on_open:
            hook(repository)
        return repository

    def __call__(self, *arguments: str, succeeds: bool = True) -> Any:
        from typer.testing import CliRunner

        from chatsbom.__main__ import app

        result = CliRunner().invoke(app, ['db', *arguments])
        if succeeds:
            assert result.exit_code == 0, result.output
        return result


@pytest.fixture
def db_command(
    clickhouse_db: str,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> DbCommand:
    """`chatsbom db index` and `db edges`, against the test database."""
    from chatsbom.core.config import ChatSBOMConfig
    from chatsbom.core.config import PathConfig
    from chatsbom.services.db_service import DbService

    config = ChatSBOMConfig(
        paths=PathConfig(base_data_dir=tmp_path / 'data'),
        _db_base=DatabaseConfig(
            host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database=clickhouse_db,
        ),
    )
    container = SimpleNamespace(config=config, get_db_service=DbService)
    command = DbCommand(container)
    container.get_ingestion_repository = command.repository
    for name in ('index', 'edges'):
        monkeypatch.setattr(
            f'chatsbom.commands.db.{name}.get_container', lambda: container,
        )
        monkeypatch.setattr(
            f'chatsbom.commands.db.{name}.check_clickhouse_connection',
            lambda **_: None,
        )
    return command


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
