"""ClickHouse connection utilities."""
import clickhouse_connect
import typer
from rich.console import Console


def check_clickhouse_connection(
    host: str,
    port: int,
    user: str,
    password: str,
    database: str = 'sbom',
    console: Console | None = None,
    require_database: bool = True,
) -> bool:
    """
    Check ClickHouse connection and optionally verify database existence.

    Args:
        host: ClickHouse host
        port: ClickHouse HTTP port
        user: ClickHouse username
        password: ClickHouse password
        database: Database to check
        console: Rich console for output (creates one if None)
        require_database: If True, check that the database exists

    Returns:
        True if connection (and database) check passed, False otherwise

    Raises:
        typer.Exit: If connection fails
    """
    if console is None:
        console = Console()

    # Step 1: Test basic connectivity
    try:
        client = clickhouse_connect.get_client(
            host=host,
            port=port,
            username=user,
            password=password,
            database='default',
        )
        client.query('SELECT 1')
    except Exception as e:
        console.print(
            f'[bold red]Error:[/] Failed to connect to ClickHouse at '
            f'[cyan]{host}:{port}[/]\n\n'
            f'Details: {e}\n\n'
            'Please ensure ClickHouse is running:\n\n'
            '[bold]Option 1:[/] Use docker compose\n'
            '  [cyan]docker compose up -d[/]\n\n'
            '[bold]Option 2:[/] Use docker run\n'
            '  Step 1: Start ClickHouse\n'
            '  [cyan]docker run --rm -d --name clickhouse \\\n'
            '    -p 8123:8123 -p 9000:9000 \\\n'
            '    -v ./database:/var/lib/clickhouse \\\n'
            '    clickhouse/clickhouse-server[/]\n\n'
            '  Step 2: Create admin and guest users\n'
            '  [cyan]docker exec -it clickhouse clickhouse-client -q \\\n'
            "    \"CREATE USER admin IDENTIFIED BY 'admin'\"[/]\n"
            '  [cyan]docker exec -it clickhouse clickhouse-client -q \\\n'
            "    \"CREATE USER guest IDENTIFIED BY 'guest'\"[/]\n"
            '  [cyan]docker exec -it clickhouse clickhouse-client -q \\\n'
            "    \"GRANT ALL ON *.* TO admin WITH GRANT OPTION\"[/]\n"
            '  [cyan]docker exec -it clickhouse clickhouse-client -q \\\n'
            "    \"GRANT SELECT ON *.* TO guest\"[/]\n",
        )
        raise typer.Exit(1)

    # Step 2: Check database exists (if required)
    if require_database:
        try:
            result = client.query(
                'SELECT name FROM system.databases WHERE name = {db:String}',
                parameters={'db': database},
            )
            if not result.result_rows:
                console.print(
                    f'[bold red]Error:[/] Database [cyan]{database}[/] '
                    'does not exist.\n\n'
                    'Please run the import command first to create and '
                    'populate the database:\n\n'
                    '  [cyan]uv run sbom-insight import[/]',
                )
                raise typer.Exit(1)
        except typer.Exit:
            raise
        except Exception as e:
            console.print(
                f'[bold red]Error:[/] Failed to check database: {e}',
            )
            raise typer.Exit(1)

    return True
