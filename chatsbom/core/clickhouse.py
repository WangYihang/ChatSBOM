"""ClickHouse connection utilities."""
import socket

import clickhouse_connect
import typer
from rich.console import Console

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
    'clickhouse/clickhouse-server:25.12-alpine[/]\n'
    '          [dim]See:[/dim] '
    'https://github.com/WangYihang/ChatSBOM#start-database'
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
            f'[bold red]Error:[/] Connection to [cyan]{host}:{port}[/] timed out.\n\n'
            f'{START_CLICKHOUSE}',
        )
    except OSError as e:
        console.print(
            f'[bold red]Error:[/] Cannot reach [cyan]{host}:{port}[/]\n'
            f'[dim]{e}[/dim]\n\n'
            f'{START_CLICKHOUSE}',
        )
    return False


def _check_auth(host: str, port: int, user: str, password: str, console: Console) -> bool:
    """Step 2: Check authentication."""
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
                f'[bold red]Error:[/] Authentication failed for [cyan]{user}[/]\n\n'
                '[green]Solution:[/] Create user:\n'
                f'  [cyan]docker exec clickhouse clickhouse-client -q \\\n'
                f'    "CREATE USER IF NOT EXISTS {user} IDENTIFIED BY \'<password>\'"[/]',
            )
        else:
            console.print(f'[bold red]Error:[/] Auth failed: [dim]{e}[/dim]')
        return False


def _check_database(
    host: str, port: int, user: str, password: str, database: str, console: Console,
) -> bool:
    """Step 3: Check database access."""
    try:
        client = clickhouse_connect.get_client(
            host=host, port=port, username=user, password=password, database=database,
        )
        client.query('SELECT 1')
        return True
    except Exception as e:
        err = str(e).lower()
        if 'unknown database' in err:
            console.print(
                f'[bold red]Error:[/] Database [cyan]{database}[/] does not exist.\n\n'
                '[green]Solution:[/] [cyan]chatsbom index --language go[/]',
            )
        elif any(x in err for x in ['access', 'denied', 'grant', 'not allowed']):
            console.print(
                f'[bold red]Error:[/] User [cyan]{user}[/] cannot access [cyan]{database}[/]\n\n'
                '[green]Solution:[/] Grant access:\n'
                f'  [cyan]docker exec clickhouse clickhouse-client -q \\\n'
                f'    "GRANT SELECT ON {database}.* TO {user}"[/]\n',
            )
        else:
            console.print(
                f'[bold red]Error:[/] Cannot access [cyan]{database}[/]: [dim]{e}[/dim]',
            )
        return False


def _check_tables(
    host: str, port: int, user: str, password: str, database: str, console: Console,
) -> bool:
    """Step 4: Check required tables exist."""
    required = {'repositories', 'artifacts'}

    try:
        client = clickhouse_connect.get_client(
            host=host, port=port, username=user, password=password, database=database,
        )
        result = client.query('SHOW TABLES')
        existing = {row[0] for row in result.result_rows}

        if missing := required - existing:
            console.print(
                f'[bold red]Error:[/] Missing tables: [cyan]{", ".join(sorted(missing))}[/]\n\n'
                '[green]Solution:[/] [cyan]chatsbom index --language go[/]',
            )
            return False
        return True
    except Exception as e:
        console.print(
            f'[bold red]Error:[/] Cannot check tables: [dim]{e}[/dim]',
        )
        return False
