"""What the CLI says when ClickHouse does not let it in (#20).

When no server answered it printed a recipe that published 8123 on
every interface and made `admin` with the password `admin` and GRANT
ALL: the exposure compose and the README had since closed, handed out
again on the first failed connection. It now gives the README's two
ways, in one place.

The other hints advised CREATE USER and GRANT, which fail for the
accounts database/config/users.d defines (ACCESS_STORAGE_READONLY), and
a command that does not exist, `chatsbom index`. And the one for a
missing database looked for text ClickHouse does not send, so it never
showed.
"""
import ast
import inspect
import io
import re
from collections.abc import Callable

import pytest
from clickhouse_connect.driver.exceptions import DatabaseError
from rich.console import Console

from chatsbom.core import clickhouse
from tests.conftest import CLICKHOUSE_HOST
from tests.conftest import CLICKHOUSE_PASSWORD
from tests.conftest import CLICKHOUSE_PORT
from tests.conftest import CLICKHOUSE_USER
from tests.conftest import requires_clickhouse

#: What ClickHouse 25.12 answered, as clickhouse-connect raised it,
#: measured against a server with this repository's users.d.
WRONG_PASSWORD = (
    'Received ClickHouse exception, code: 516, server response: Code: 516. '
    'DB::Exception: guest: Authentication failed: password is incorrect, or '
    'there is no user with such name. (AUTHENTICATION_FAILED) (for url '
    'http://127.0.0.1:8123)'
)
NO_SUCH_DATABASE = (
    'Received ClickHouse exception, code: 81, server response: Code: 81. '
    'DB::Exception: Database elsewhere does not exist. (UNKNOWN_DATABASE) '
    '(version 25.12.11.4 (official build)) (for url http://127.0.0.1:8123)'
)
NOT_ALLOWED = (
    'Received ClickHouse exception, code: 497, server response: Code: 497. '
    "DB::Exception: guest: Not enough privileges. To execute this query, it's "
    'necessary to have the grant SELECT for at least one column on '
    'elsewhere.repositories. (ACCESS_DENIED) (for url http://127.0.0.1:8123)'
)

#: The two accounts' settings. The checks are handed a user, not which
#: of the two it is, so a hint about an account names both.
ACCOUNT_SETTINGS = (
    'CLICKHOUSE_ADMIN_USER', 'CLICKHOUSE_ADMIN_PASSWORD',
    'CLICKHOUSE_GUEST_USER', 'CLICKHOUSE_GUEST_PASSWORD',
)

#: Advice that cannot work: users.d accounts are read-only storage.
UNWORKABLE = re.compile(r'\bGRANT\b|\bCREATE\s+USER\b', re.IGNORECASE)

#: The command is `chatsbom db index`.
NO_SUCH_COMMAND = re.compile(r'chatsbom\s+index\b')

UNREACHABLE = [
    TimeoutError('timed out'),
    ConnectionRefusedError(111, 'Connection refused'),
]


def hint_for(error: OSError, monkeypatch: pytest.MonkeyPatch) -> str:
    """What `_check_network` prints when connecting fails with `error`."""
    def connect(*args: object, **kwargs: object) -> None:
        raise error

    monkeypatch.setattr(clickhouse.socket, 'create_connection', connect)
    out = io.StringIO()
    # Wide, so that no command is wrapped across lines.
    console = Console(file=out, width=10_000, color_system=None)

    assert clickhouse._check_network('127.0.0.1', 8123, console) is False
    return out.getvalue()


@pytest.mark.parametrize('error', UNREACHABLE, ids=['timeout', 'refused'])
def test_the_hint_publishes_nothing_beyond_the_loopback(error, monkeypatch):
    hint = hint_for(error, monkeypatch)

    published = re.findall(r'(?:-p|--publish)[ =](\S+)', hint)
    assert published, 'the hint no longer shows how to publish the port'
    assert [p for p in published if not p.startswith('127.0.0.1:')] == []


@pytest.mark.parametrize('error', UNREACHABLE, ids=['timeout', 'refused'])
def test_the_hint_makes_no_accounts_of_its_own(error, monkeypatch):
    """The accounts are database/config/users.d: `admin`, and a `guest`
    whose grants and cost limits a CREATE USER line would not have."""
    hint = hint_for(error, monkeypatch)

    assert 'GRANT' not in hint.upper()
    assert 'CREATE USER' not in hint.upper()
    assert 'database/config/users.d' in hint


@pytest.mark.parametrize('error', UNREACHABLE, ids=['timeout', 'refused'])
def test_the_hint_starts_the_database_as_the_readme_does(error, monkeypatch):
    hint = hint_for(error, monkeypatch)

    assert 'docker compose up -d clickhouse' in hint
    assert 'github.com/WangYihang/ChatSBOM' in hint


def test_a_timeout_and_a_refusal_get_the_same_advice(monkeypatch):
    """Two copies of the recipe were the two places to keep in step."""
    timeout, refused = (
        hint_for(error, monkeypatch).partition('Solution')[2]
        for error in UNREACHABLE
    )
    assert timeout and timeout == refused


# --- the account, the database, its tables ----------------------------------

def test_no_hint_advises_what_cannot_work():
    """Every string in the module, so that a branch no test reaches is
    held to it as well: no CREATE USER or GRANT, which the accounts in
    users.d refuse, and no `chatsbom index`, which does not exist."""
    tree = ast.parse(inspect.getsource(clickhouse))
    strings = [
        node.value for node in ast.walk(tree)
        if isinstance(node, ast.Constant) and isinstance(node.value, str)
    ]
    unworkable = [
        text for text in strings
        if UNWORKABLE.search(text) or NO_SUCH_COMMAND.search(text)
    ]
    assert unworkable == []


def printed(check: Callable[[Console], bool]) -> str:
    """What a check prints, having failed."""
    out = io.StringIO()
    assert check(Console(file=out, width=10_000, color_system=None)) is False
    return out.getvalue()


def refused_with(message: str) -> Callable[..., object]:
    """A `get_client` that fails as ClickHouse did."""
    def get_client(**kwargs: object) -> object:
        raise DatabaseError(message)
    return get_client


class NoTables:
    """A client, and its answer to SHOW TABLES: nothing."""
    result_rows: list[tuple[str, ...]] = []

    def query(self, sql: str) -> 'NoTables':
        return self


def test_a_refused_login_names_the_settings_it_came_from(monkeypatch):
    """The fix is `.env` agreeing with database/config/users.d. The
    check is handed a user and password, not which account they are, so
    it names the settings of both."""
    monkeypatch.setattr(
        clickhouse.clickhouse_connect, 'get_client',
        refused_with(WRONG_PASSWORD),
    )

    hint = printed(
        lambda console: clickhouse._check_auth(
            'clickhouse', 8123, 'guest', 'wrong', console,
        ),
    )

    assert 'guest' in hint
    assert [s for s in ACCOUNT_SETTINGS if s not in hint] == []
    assert 'database/config/users.d' in hint
    assert not UNWORKABLE.search(hint)


def test_a_missing_database_says_how_to_make_it(monkeypatch):
    """ClickHouse says UNKNOWN_DATABASE, and the check looked for
    `unknown database`: this hint never showed, and the error went out
    raw."""
    monkeypatch.setattr(
        clickhouse.clickhouse_connect, 'get_client',
        refused_with(NO_SUCH_DATABASE),
    )

    hint = printed(
        lambda console: clickhouse._check_database(
            'clickhouse', 8123, 'guest', 'guest', 'elsewhere', console,
        ),
    )

    assert 'chatsbom db index' in hint
    assert not NO_SUCH_COMMAND.search(hint)


def test_a_database_the_account_may_not_read_is_a_users_d_matter(
    monkeypatch,
):
    monkeypatch.setattr(
        clickhouse.clickhouse_connect, 'get_client',
        refused_with(NOT_ALLOWED),
    )

    hint = printed(
        lambda console: clickhouse._check_database(
            'clickhouse', 8123, 'guest', 'guest', 'elsewhere', console,
        ),
    )

    assert 'database/config/users.d' in hint
    assert 'CLICKHOUSE_DB' in hint
    assert not UNWORKABLE.search(hint)


def test_missing_tables_may_be_tables_the_account_cannot_see(monkeypatch):
    """`chatsbom db index` makes them. But in a database users.d does
    not declare for it, guest's SHOW TABLES comes back empty rather than
    refused — measured — so they look missing when they are not."""
    monkeypatch.setattr(
        clickhouse.clickhouse_connect, 'get_client',
        lambda **kwargs: NoTables(),
    )

    hint = printed(
        lambda console: clickhouse._check_tables(
            'clickhouse', 8123, 'guest', 'guest', 'elsewhere', console,
        ),
    )

    assert 'chatsbom db index' in hint
    assert 'database/config/users.d' in hint
    assert not NO_SUCH_COMMAND.search(hint)


@requires_clickhouse
def test_a_real_refused_login_gets_the_hint():
    """The server's own words, not the ones copied above."""
    hint = printed(
        lambda console: clickhouse._check_auth(
            CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER,
            'not-the-password', console,
        ),
    )

    assert 'CLICKHOUSE_ADMIN_PASSWORD' in hint


@requires_clickhouse
def test_a_real_missing_database_gets_the_hint():
    hint = printed(
        lambda console: clickhouse._check_database(
            CLICKHOUSE_HOST, CLICKHOUSE_PORT, CLICKHOUSE_USER,
            CLICKHOUSE_PASSWORD, 'chatsbom_no_such_database', console,
        ),
    )

    assert 'chatsbom db index' in hint
