"""ClickHouse connection utilities."""
import socket

import typer
from rich.console import Console
from rich.markup import escape

# `clickhouse_connect` is imported by each check that connects, not here:
# it imports pandas, numpy and pyarrow when they are installed, and the
# CLI imports this module at start-up, whichever command runs.

# Every value below is put into markup escaped: a host, user or database
# comes from `.env`, and an error is the server's own text. Unescaped, a
# `[/dim]` in one raised MarkupError in place of the message.

#: How to start a server, when none answers: the README's two ways
#: ("Start Database"), both from a checkout, whose database/config/users.d
#: defines the accounts: `admin`, and a read-only `guest` with its grants
#: and cost limits. Published on the loopback interface alone, as compose
#: does it. The recipe this replaced published 8123 on every interface
#: and made `admin`, password `admin`, with GRANT ALL.
START_CLICKHOUSE = (
    '[green]Solution:[/] from a checkout of the repository, '
    '[cyan]docker compose up -d clickhouse[/]\n'
    '          [dim]Or:[/dim] [cyan]docker run -d --name clickhouse '
    '-p 127.0.0.1:8123:8123 --ulimit nofile=262144:262144 '
    '-v "$PWD/database/data:/var/lib/clickhouse" '
    '-v "$PWD/database/config/users.d:/etc/clickhouse-server/users.d" '
    '-v "$PWD/database/config/config.d/logs.xml:'
    '/etc/clickhouse-server/config.d/logs.xml" '
    'clickhouse/clickhouse-server:25.12-alpine[/]\n'
    '          [dim]See:[/dim] '
    'https://github.com/WangYihang/ChatSBOM#start-database'
)

#: Where the CLI's accounts come from. The checks are handed a user and
#: a password, not which of the two accounts they are, so this names
#: the settings of both. The accounts themselves are the server's, in
#: users.d, where it holds them as read-only storage: one cannot be made
#: or changed at runtime (ACCESS_STORAGE_READONLY).
ACCOUNT_SETTINGS = (
    '[green]Solution:[/] the user and password in [cyan].env[/] must match '
    'an account in [cyan]database/config/users.d[/]:\n'
    '          [cyan]CLICKHOUSE_ADMIN_USER[/] and '
    '[cyan]CLICKHOUSE_ADMIN_PASSWORD[/] for the admin one,\n'
    '          [cyan]CLICKHOUSE_GUEST_USER[/] and '
    '[cyan]CLICKHOUSE_GUEST_PASSWORD[/] for the read-only one.\n'
    '          A password is changed in both places.'
)

#: Where what an account may read is set, for the same reason.
READABLE = (
    'the databases an account may read are declared with it in '
    '[cyan]database/config/users.d[/], and cannot be added at runtime'
)


def check_clickhouse_connection(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str = 'chatsbom',
    console: Console | None = None,
    require_database: bool = True,
) -> bool:
    """
    Check ClickHouse connection with multi-step validation.

    Steps:
        1. Network - is the server reachable?
        2. Authentication - are credentials valid?
        3. Database - does it exist and is it accessible?
        4. Tables - do required tables exist?
    """
    console = console or Console()

    if not _check_network(host, port, console):
        raise typer.Exit(1)

    if not _check_auth(host, port, user, password, console):
        raise typer.Exit(1)

    if not require_database:
        return True

    if not _check_database(host, port, user, password, database, console):
        raise typer.Exit(1)

    if not _check_tables(host, port, user, password, database, console):
        raise typer.Exit(1)

    return True


def _check_network(host: str, port: int, console: Console) -> bool:
    """Step 1: Check network connectivity."""
    try:
        with socket.create_connection((host, port), timeout=5):
            return True
    except TimeoutError:
        console.print(
            f'[bold red]Error:[/] Connection to [cyan]{escape(host)}:{port}[/] '
            'timed out.\n\n' + START_CLICKHOUSE,
        )
    except OSError as e:
        console.print(
            f'[bold red]Error:[/] Cannot reach [cyan]{escape(host)}:{port}[/]\n'
            f'[dim]{escape(str(e))}[/dim]\n\n' + START_CLICKHOUSE,
        )
    return False


def _check_auth(host: str, port: int, user: str, password: str, console: Console) -> bool:
    """Step 2: Check authentication."""
    import clickhouse_connect

    try:
        client = clickhouse_connect.get_client(
            host=host, port=port, username=user, password=password, database='default',
        )
        client.query('SELECT 1')
        return True
    except Exception as e:
        err = str(e).lower()
        if any(x in err for x in ['authentication', 'password', 'denied', 'incorrect']):
            console.print(
                f'[bold red]Error:[/] Authentication failed for [cyan]{escape(user)}[/]\n\n'
                + ACCOUNT_SETTINGS,
            )
        else:
            console.print(
                f'[bold red]Error:[/] Auth failed: [dim]{escape(str(e))}[/dim]',
            )
        return False


def _check_database(
    host: str, port: int, user: str, password: str, database: str, console: Console,
) -> bool:
    """Step 3: Check database access."""
    import clickhouse_connect

    try:
        client = clickhouse_connect.get_client(
            host=host, port=port, username=user, password=password, database=database,
        )
        client.query('SELECT 1')
        return True
    except Exception as e:
        err = str(e).lower()
        # By the name ClickHouse gives the error, `(UNKNOWN_DATABASE)`.
        # This looked for `unknown database`, which it never says, so a
        # missing database got the raw error rather than this.
        if 'unknown_database' in err:
            console.print(
                f'[bold red]Error:[/] Database [cyan]{escape(database)}[/] does not exist.\n\n'
                '[green]Solution:[/] [cyan]chatsbom db index[/] creates it, '
                'with its tables. [cyan]CLICKHOUSE_DB[/] in [cyan].env[/] '
                'names it.',
            )
        elif 'access_denied' in err or 'not enough privileges' in err:
            console.print(
                f'[bold red]Error:[/] User [cyan]{escape(user)}[/] cannot access [cyan]{escape(database)}[/]\n\n'
                f'[green]Solution:[/] {READABLE}.\n'
                '          Set [cyan]CLICKHOUSE_DB[/] in [cyan].env[/] to '
                f'one declared there, or declare [cyan]{escape(database)}[/] for '
                f'[cyan]{escape(user)}[/] in that file.',
            )
        else:
            console.print(
                f'[bold red]Error:[/] Cannot access [cyan]{escape(database)}[/]: [dim]{escape(str(e))}[/dim]',
            )
        return False


def _check_tables(
    host: str, port: int, user: str, password: str, database: str, console: Console,
) -> bool:
    """Step 4: Check required tables exist."""
    required = {'repositories', 'artifacts'}

    import clickhouse_connect

    try:
        client = clickhouse_connect.get_client(
            host=host, port=port, username=user, password=password, database=database,
        )
        result = client.query('SHOW TABLES')
        existing = {row[0] for row in result.result_rows}

        if missing := required - existing:
            # An account shown a database it may not read gets an empty
            # SHOW TABLES, not a refusal, so present tables can look
            # missing.
            console.print(
                f'[bold red]Error:[/] Missing tables: [cyan]{", ".join(sorted(missing))}[/]\n\n'
                '[green]Solution:[/] [cyan]chatsbom db index[/] creates them.\n'
                '          If they exist, this account cannot see them: '
                f'{READABLE}.',
            )
            return False
        return True
    except Exception as e:
        console.print(
            f'[bold red]Error:[/] Cannot check tables: [dim]{escape(str(e))}[/dim]',
        )
        return False
