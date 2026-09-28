"""A command pointed at a database that does not exist yet (#64).

README says `chatsbom db index` creates the database the first time it
runs, and the hint for a missing database (#20) sends you to it. It
failed with UNKNOWN_DATABASE instead: it opened the repository's client,
which is bound to CLICKHOUSE_DB, to read the landing zone, before
`ensure_schema` had created that database through a client of its own
on `default`. `db raw --apply` creates it first, so the collector loop
never met this; a first `db index` did, which is the run README and
that hint describe.

These run the commands as a shell does, with CLICKHOUSE_DB naming a
database that does not exist until they run.
"""
from __future__ import annotations

import io
import re
import uuid
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import clickhouse_connect
import pytest
from rich.console import Console
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.core.clickhouse import check_clickhouse_connection
from chatsbom.core.container import Container
from chatsbom.core.schema import TABLE_DDL
from tests.conftest import CLICKHOUSE_HOST
from tests.conftest import CLICKHOUSE_PASSWORD
from tests.conftest import CLICKHOUSE_PORT
from tests.conftest import CLICKHOUSE_USER
from tests.conftest import requires_clickhouse

pytestmark = requires_clickhouse

runner = CliRunner()

#: The tables the `db` commands expect, as `ensure_schema` declares them.
DECLARED = {name for name, _ in TABLE_DDL}

#: The commands that write, as admin, and so are to make the database
#: they are pointed at. `db index` did not whenever it read the landing
#: zone; the others did already, and are here to keep it so.
WRITERS = {
    'index': ['db', 'index'],
    'index --rebuild': ['db', 'index', '--rebuild'],
    'index --from-files': ['db', 'index', '--from-files'],
    'edges': ['db', 'edges'],
    'raw --apply': ['db', 'raw', '--apply'],
}

#: The commands that only read, as guest. None of them can make a
#: database, so what they say when there is none has to name one that
#: does.
READERS = {
    'status': ['db', 'status'],
    'query': ['db', 'query', 'mail'],
    'export': ['db', 'export'],
}


@pytest.fixture
def server() -> Iterator[Any]:
    """The admin account on `default`, the one database always there."""
    admin = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
        database='default',
    )
    try:
        yield admin
    finally:
        admin.close()


@pytest.fixture
def missing(
    server: Any,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> Iterator[str]:
    """CLICKHOUSE_DB, naming a database that does not exist yet.

    Read as a shell hands it over: the configuration and the container
    are made again from the environment, in an empty working directory.
    Dropped afterwards, whatever the test made of it.
    """
    name = f'chatsbom_test_{uuid.uuid4().hex[:12]}'
    assert name not in databases(server)
    monkeypatch.setenv('CLICKHOUSE_DB', name)
    monkeypatch.setenv('CLICKHOUSE_HOST', CLICKHOUSE_HOST)
    monkeypatch.setenv('CLICKHOUSE_PORT', str(CLICKHOUSE_PORT))
    # The readers connect as guest. CI's server has no such account, as
    # only users.d defines one, and the hint is the same for either.
    for role in ('ADMIN', 'GUEST'):
        monkeypatch.setenv(f'CLICKHOUSE_{role}_USER', CLICKHOUSE_USER)
        monkeypatch.setenv(f'CLICKHOUSE_{role}_PASSWORD', CLICKHOUSE_PASSWORD)
    monkeypatch.setattr('chatsbom.core.config._config', None)
    monkeypatch.setattr(Container, '_instance', None)
    monkeypatch.chdir(tmp_path)
    try:
        yield name
    finally:
        server.command(f'DROP DATABASE IF EXISTS {name}')


def databases(server: Any) -> set[str]:
    return {str(name) for (name,) in server.query('SHOW DATABASES').result_rows}


def tables(server: Any, database: str) -> set[str]:
    return {
        str(name) for (name,) in server.query(
            'SELECT name FROM system.tables WHERE database = {db:String}',
            parameters={'db': database},
        ).result_rows
    }


@pytest.mark.parametrize('command', WRITERS.values(), ids=list(WRITERS))
def test_a_writer_makes_the_database(
    server: Any, missing: str, command: list[str],
) -> None:
    # Not caught, so that a refusal fails the test in ClickHouse's own
    # words rather than as an exit code.
    result = runner.invoke(app, command, catch_exceptions=False)

    assert result.exit_code == 0, result.output
    made = tables(server, missing)
    assert DECLARED <= made
    # And nothing staged is left: `--rebuild` swapped its table in.
    assert {name for name in made if name.endswith('_next')} == set()


@pytest.mark.parametrize('command', READERS.values(), ids=list(READERS))
def test_a_reader_names_a_command_that_makes_it(
    missing: str, command: list[str],
) -> None:
    """The hint is followed as written, and the check that printed it
    then passes: the database is there, with its tables."""
    refused = runner.invoke(app, command)
    said = ' '.join(refused.output.split())
    assert refused.exit_code == 1, said
    named = re.search(r'Solution: chatsbom (.+?) creates it', said)
    assert named, said

    followed = runner.invoke(app, named[1].split(), catch_exceptions=False)

    assert followed.exit_code == 0, followed.output
    assert check_clickhouse_connection(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
        user=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
        database=missing, console=Console(file=io.StringIO()),
    )
