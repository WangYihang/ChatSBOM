"""What the guest account may read, and what it leaves running (#31).

guest is the account a public deployment exposes: the dashboard's Worker
connects as it, and so do `db query`, `db status`, `db export` and the
chat. Its profile bounded what one query may *cost*; its grants said
nothing about *which* tables, so it could read `raw_documents` — the
landing zone, every document as it was fetched, about 20 GiB
uncompressed — though nothing that connects as guest reads it. And a
query the Worker had given up on at 10 s went on holding one of the
account's 16 slots until the server's own 30 s ran out: the server was
never told that nobody was waiting.

These run against an account made from `database/config/users.d/
guest.xml` itself, by SQL: its profile and its grants, pointed at a
throwaway database. That works wherever the suite does — CI's server
loads no users.d, so it has no guest — and shares nothing with a
server's real account. The last test holds the server's own guest, where
there is one, to the same file: a server reads its users from wherever
they were installed, and a copy nobody updated is a guest nobody tested.
"""
from __future__ import annotations

import re
import secrets
import uuid
import xml.etree.ElementTree as ET
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import clickhouse_connect
import pytest
from clickhouse_connect.driver.exceptions import DatabaseError

from tests.conftest import CLICKHOUSE_HOST
from tests.conftest import CLICKHOUSE_PASSWORD
from tests.conftest import CLICKHOUSE_PORT
from tests.conftest import CLICKHOUSE_USER
from tests.conftest import requires_clickhouse

pytestmark = requires_clickhouse

ROOT = Path(__file__).resolve().parents[1]
GUEST_XML = ROOT / 'database/config/users.d/guest.xml'

#: The dashboard's ClickHouse backend: every statement the Worker sends.
DASHBOARD = ROOT / 'web/src/clickhouse/queries.ts'

#: The database guest.xml's grants name.
GRANTED = 'chatsbom'

#: The landing zone, which nothing connecting as guest reads.
LANDING = 'raw_documents'


def _guest_xml() -> ET.Element:
    return ET.parse(GUEST_XML).getroot()


def profile() -> dict[str, str]:
    """The guest profile's settings, spelled as the file spells them."""
    node = _guest_xml().find('profiles/guest_readonly')
    assert node is not None, 'guest.xml declares no guest_readonly profile'
    return {child.tag: (child.text or '').strip() for child in node}


def grants() -> list[str]:
    """guest's GRANT and REVOKE statements, in the file's order."""
    return [
        (query.text or '').strip()
        for query in _guest_xml().findall('users/guest/grants/query')
    ]


def dashboard_reads() -> tuple[set[str], set[str]]:
    """The tables, and the dictionaries, the dashboard's statements name.

    Read from the backend's source, so a panel that starts reading
    another table is held to this without a line changing here.
    """
    source = DASHBOARD.read_text()
    subqueries = set(re.findall(r'\bWITH\s+(\w+)\s+AS\s*\(', source))
    tables = set(re.findall(r'\bFROM\s+([a-z_]\w*)', source)) - subqueries
    dictionaries = set(re.findall(r"\bdict(?:Get|Has)\('(\w+)'", source))
    return tables, dictionaries


def _literal(value: str) -> str:
    """A profile value as a settings clause takes it."""
    return value if re.fullmatch(r'-?\d+', value) else f"'{value}'"


@dataclass
class Account:
    name: str
    client: Any


@pytest.fixture
def admin() -> Iterator[Any]:
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
        database='default',
    )
    try:
        yield client
    finally:
        client.close()


@pytest.fixture
def guest(admin: Any, clickhouse_db: str) -> Iterator[Account]:
    """An account made from guest.xml, whose grants name `clickhouse_db`.

    A name, profile and password of its own, so nothing is shared with a
    server's real guest, and all three are dropped afterwards.
    """
    name = f'chatsbom_test_guest_{uuid.uuid4().hex[:12]}'
    password = secrets.token_hex(16)
    settings = ', '.join(
        f'{key} = {_literal(value)}' for key, value in profile().items()
    )
    try:
        admin.command(f'CREATE SETTINGS PROFILE {name} SETTINGS {settings}')
        admin.command(
            f"CREATE USER {name} IDENTIFIED WITH sha256_password "
            f"BY '{password}' SETTINGS PROFILE '{name}'",
        )
        for statement in grants():
            statement = re.sub(
                rf'\b{GRANTED}\.',
                f'{clickhouse_db}.', statement,
            )
            grantee = 'FROM' if statement.upper().startswith('REVOKE') else 'TO'
            admin.command(f'{statement} {grantee} {name}')
        client = clickhouse_connect.get_client(
            host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
            username=name, password=password, database=clickhouse_db,
        )
        try:
            yield Account(name, client)
        finally:
            client.close()
    finally:
        admin.command(f'DROP USER IF EXISTS {name}')
        admin.command(f'DROP SETTINGS PROFILE IF EXISTS {name}')


def test_guest_cannot_read_the_landing_zone(guest: Account) -> None:
    """Refused by the server, not merely left unused by the readers."""
    for query in (
        f'SELECT count() FROM {LANDING}',
        f'SELECT body FROM {LANDING} LIMIT 1',
    ):
        with pytest.raises(DatabaseError, match='ACCESS_DENIED'):
            guest.client.query(query)


def test_guest_still_reads_what_the_dashboard_reads(guest: Account) -> None:
    tables, dictionaries = dashboard_reads()
    # The statements were found, and the landing zone is not among them.
    assert {'artifacts', 'edges', 'mv_packages', 'mv_totals'} <= tables
    assert dictionaries == {'dict_repositories'}
    assert LANDING not in tables

    for table in sorted(tables):
        guest.client.query(f'SELECT * FROM {table} LIMIT 0')
    for dictionary in sorted(dictionaries):
        guest.client.query(f"SELECT dictHas('{dictionary}', toUInt64(1))")


def test_the_revoke_takes_the_landing_zone_alone(
    guest: Account, admin: Any, clickhouse_db: str,
) -> None:
    """Every other table in the database stays readable: the readers'
    rollups, views and dictionary, and the tables behind them.

    Not the `.tmp.` tables a rollup's refresh writes into and swaps out,
    which come and go while this runs; and a table gone by the time it
    is read is no answer either way. Refused is what is counted.
    """
    names = [
        str(name) for (name,) in admin.query(
            'SELECT name FROM system.tables WHERE database = {db:String} '
            "AND NOT startsWith(name, '.tmp.')",
            parameters={'db': clickhouse_db},
        ).result_rows
    ]
    assert LANDING in names

    unreadable = []
    for name in sorted(names):
        try:
            guest.client.query(f'SELECT * FROM `{name}` LIMIT 0')
        except DatabaseError as error:
            if 'ACCESS_DENIED' in str(error):
                unreadable.append(name)
            elif 'UNKNOWN_TABLE' not in str(error):
                raise
    assert unreadable == [LANDING]


def test_guest_stops_a_query_nobody_is_waiting_for(guest: Account) -> None:
    """With this set, the server cancels a read-only query sent over HTTP
    when the connection it came on closes — which is what the Worker's
    10 s deadline does. It applies to read-only queries, and those are
    the only ones this account can send."""
    [(cancel, readonly)] = guest.client.query(
        "SELECT getSetting('cancel_http_readonly_queries_on_client_close'), "
        "getSetting('readonly')",
    ).result_rows
    assert cancel
    assert int(readonly) == 1


def test_the_server_runs_this_guest(
    guest: Account, admin: Any, clickhouse_db: str,
) -> None:
    """The server's own guest has this file's grants and profile.

    Compared through the server's own tables, so both sides are spelled
    the way the server spells them.
    """
    [(accounts,)] = admin.query(
        "SELECT count() FROM system.users WHERE name = 'guest'",
    ).result_rows
    if not accounts:
        pytest.skip('this server loads no users.d, so it has no guest account')

    def rights(user: str, database: str) -> list[tuple[Any, ...]]:
        return [
            tuple(row) for row in admin.query(
                'SELECT access_type, database = {db:String}, table, column, '
                'is_partial_revoke, grant_option FROM system.grants '
                'WHERE user_name = {user:String} ORDER BY ALL',
                parameters={'user': user, 'db': database},
            ).result_rows
        ]

    def settings(name: str) -> list[tuple[Any, ...]]:
        return [
            tuple(row) for row in admin.query(
                'SELECT setting_name, value FROM system.settings_profile_elements '
                'WHERE profile_name = {name:String} ORDER BY index',
                parameters={'name': name},
            ).result_rows
        ]

    assert rights('guest', GRANTED) == rights(guest.name, clickhouse_db)
    assert settings('guest_readonly') == settings(guest.name)
