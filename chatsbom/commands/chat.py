"""ChatSBOM Agent - TUI for querying SBOM database via Claude.

The TUI itself is `chat_tui`, imported when the command runs.
"""
import os

import typer

from chatsbom.core.config import get_config
from chatsbom.core.extras import require_extra

#: Optional display currency for cost, e.g. CHATSBOM_COST_RATE=7.2 with
#: CHATSBOM_COST_SYMBOL=¥. A rate hardcoded in source is wrong the day it
#: is written, so there is no default conversion.
#:
#: Both are read when a cost is shown, not at import: `.env` is loaded by
#: the root callback, which runs after every module has been imported.


def _cost_rate() -> float:
    raw = os.getenv('CHATSBOM_COST_RATE', '')
    try:
        return float(raw)
    except ValueError:
        # A misconfigured rate should degrade to USD, not stop the cost
        # from being shown.
        return 0.0


def format_cost(usd: float) -> str:
    """Render a cost in USD, plus a converted figure when one is configured."""
    rendered = f'${usd:.4f}'
    rate = _cost_rate()
    if rate > 0:
        symbol = os.getenv('CHATSBOM_COST_SYMBOL', '')
        rendered += f' / {symbol}{usd * rate:.4f}'
    return rendered


SYSTEM_PROMPT = (
    'You are an expert for querying the SBOM database. '
    'You can ONLY use the mcp-clickhouse tool to query the database. '
    'Do NOT attempt to read files, write files, or execute bash commands. '
    'Always use the mcp-clickhouse tool to query data. '
    'For large exports, format your answer and tell the user how many results there are.'
)

app = typer.Typer(help='Chat with your SBOM data using AI')


@app.callback(invoke_without_command=True)
def main(
    host: str = typer.Option(None, help='ClickHouse host'),
    port: int = typer.Option(None, help='ClickHouse http port'),
    user: str = typer.Option(None, help='ClickHouse user'),
    password: str = typer.Option(None, help='ClickHouse password'),
    database: str = typer.Option(None, help='ClickHouse database'),
):
    """Start an AI conversation about your SBOM data."""
    # First: without the TUI's libraries, neither a key nor a database
    # would start it.
    require_extra('chat', 'claude_agent_sdk', 'textual')

    # We need to import the central console for check_clickhouse_connection
    from chatsbom.core.logging import console

    # If context is passed (e.g. --help), don't run the TUI
    # But since we use callback(invoke_without_command=True), this runs when no subcommand.
    # Typer handles --help automatically.

    if not os.getenv('ANTHROPIC_API_KEY') and not os.getenv('ANTHROPIC_AUTH_TOKEN'):
        console.print(
            '[bold red]Error:[/] ANTHROPIC_API_KEY or ANTHROPIC_AUTH_TOKEN is not set.\n\n'
            'The Agent requires an Anthropic API key for Claude. '
            'Please set the ANTHROPIC_API_KEY or ANTHROPIC_AUTH_TOKEN environment variable:\n\n'
            '    [cyan]export ANTHROPIC_API_KEY="your_api_key"[/]\n\n'
            '    [cyan]or[/]\n\n'
            '    [cyan]export ANTHROPIC_AUTH_TOKEN="your_auth_token"[/]\n\n'
            'You can get an API key at: '
            '[link=https://console.anthropic.com/]https://console.anthropic.com/[/link]',
        )
        raise typer.Exit(1)

    config = get_config()

    # Get Guest Config
    db_config = config.get_db_config(role='guest')

    if host:
        db_config.host = host
    if port:
        db_config.port = int(port)
    if user:
        db_config.user = user
    if password:
        db_config.password = password
    if database:
        db_config.database = database

    # Check ClickHouse connection before starting TUI
    from chatsbom.core.clickhouse import check_clickhouse_connection
    check_clickhouse_connection(
        host=db_config.host, port=db_config.port, user=db_config.user, password=db_config.password,
        database=db_config.database, console=console, require_database=True,
    )

    # Imported here rather than at the top: textual and the Claude Agent
    # SDK take the better part of a second to import, and at module
    # level every command paid it at start-up.
    from chatsbom.commands.chat_tui import ChatSBOMApp

    ChatSBOMApp(db_config).run()


if __name__ == '__main__':
    app()
