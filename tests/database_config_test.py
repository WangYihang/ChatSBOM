"""database/config/users.d, the server's accounts and their settings.

compose mounts the directory into the ClickHouse image, and CI's test
job does the same (workflows_test), so what it says is what every
server of this project runs with. The first test reads the files; the
last asks the server, which in CI is the image compose names.
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

import clickhouse_connect

from tests.conftest import CLICKHOUSE_HOST
from tests.conftest import CLICKHOUSE_PASSWORD
from tests.conftest import CLICKHOUSE_PORT
from tests.conftest import CLICKHOUSE_USER
from tests.conftest import requires_clickhouse

ROOT = Path(__file__).resolve().parents[1]
USERS_D = ROOT / 'database' / 'config' / 'users.d'


def _files() -> list[ET.Element]:
    return [
        ET.parse(path).getroot() for path in sorted(USERS_D.glob('*.xml'))
    ]


def accounts() -> dict[str, str]:
    """Each account users.d defines, and the profile it logs in with.

    Not the image's own `default`, which admin.xml removes.
    """
    found: dict[str, str] = {}
    for root in _files():
        for user in root.findall('users/*'):
            if user.get('remove') is None:
                name = user.findtext('profile') or 'default'
                found[user.tag] = name.strip()
    return found


def profile(name: str) -> dict[str, str]:
    """A settings profile's settings, from every file that sets some.

    The server merges users.d into the image's users.xml, so a profile
    the image defines, `default`, has here only what this adds to it.
    """
    settings: dict[str, str] = {}
    for root in _files():
        for node in root.findall(f'profiles/{name}'):
            settings.update(
                (child.tag, (child.text or '').strip()) for child in node
            )
    return settings


def test_every_account_that_writes_inserts_synchronously():
    """ClickHouse turned `async_insert` on by default in 26.3 (#81).

    An INSERT then waits in a buffer until the server flushes it, which
    cost about 55 ms a small insert, and `db index`, `db raw --apply` and
    the collector's loop send many: they connect as admin
    (CLICKHOUSE_ADMIN_USER). Off in the profile of every account that
    can write, as it was before; guest is read-only.
    """
    writers = {
        user: name for user, name in accounts().items()
        if profile(name).get('readonly', '0') == '0'
    }
    assert 'admin' in writers
    for user, name in writers.items():
        assert profile(name).get('async_insert') == '0', (
            f'{user} writes with async_insert on (profile {name})'
        )


@requires_clickhouse
def test_the_server_inserts_synchronously_for_admin():
    """As the server runs the file, not only as the file says it.

    A server loads users.d from wherever it was installed, and one
    whose copy predates the setting inserts asynchronously from 26.3 on,
    whatever this directory says.
    """
    client = clickhouse_connect.get_client(
        host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
        username=CLICKHOUSE_USER, password=CLICKHOUSE_PASSWORD,
    )
    try:
        [(asynchronous, version)] = client.query(
            "SELECT getSetting('async_insert'), version()",
        ).result_rows
    finally:
        client.close()
    assert not asynchronous, (
        f'ClickHouse {version} inserts asynchronously for '
        f'{CLICKHOUSE_USER}, which {USERS_D.relative_to(ROOT)} turns off'
    )
