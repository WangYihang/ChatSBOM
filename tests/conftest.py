"""Shared fixtures, including a real ClickHouse for query-layer tests."""
import os
import socket
import uuid
from collections.abc import Iterator

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
