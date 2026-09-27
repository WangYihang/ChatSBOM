"""Shared fixtures, including a real ClickHouse for query-layer tests."""
import builtins
import errno
import io
import os
import socket
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest

from chatsbom.core.config import DatabaseConfig
from chatsbom.core.repository import IngestionRepository
from chatsbom.core.repository import QueryRepository

CLICKHOUSE_HOST = os.getenv('CLICKHOUSE_TEST_HOST', 'localhost')
CLICKHOUSE_PORT = int(os.getenv('CLICKHOUSE_TEST_PORT', '8123'))
CLICKHOUSE_USER = os.getenv('CLICKHOUSE_ADMIN_USER', 'admin')
CLICKHOUSE_PASSWORD = os.getenv('CLICKHOUSE_ADMIN_PASSWORD', 'admin')


def _reachable() -> bool:
    try:
        with socket.create_connection((CLICKHOUSE_HOST, CLICKHOUSE_PORT), 1.0):
            return True
    except OSError:
        return False


requires_clickhouse = pytest.mark.skipif(
    not _reachable(),
    reason=(
        f'ClickHouse not reachable at {CLICKHOUSE_HOST}:{CLICKHOUSE_PORT} '
        '(start it with `docker compose up -d`)'
    ),
)


def _github_reachable() -> bool:
    """Whether the real remote is reachable.

    `git ls-remote` against a live repository passes when run alone and
    fails intermittently inside the full suite, where it competes for
    the network and can be rate-limited. A test whose outcome depends on
    the weather is worse than no test: it trains everyone to rerun
    rather than to look.
    """
    try:
        with socket.create_connection(('github.com', 443), 2.0):
            return True
    except OSError:
        return False


requires_github = pytest.mark.skipif(
    not _github_reachable(),
    reason='github.com not reachable; these tests talk to the real remote',
)


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
