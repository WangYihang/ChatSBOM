"""ChatSBOM Agent - TUI for querying SBOM database via Claude."""
import asyncio
import json
import os
from contextlib import suppress
from datetime import datetime

import dotenv
import typer
from claude_agent_sdk import ClaudeAgentOptions
from claude_agent_sdk.client import ClaudeSDKClient
from claude_agent_sdk.types import AssistantMessage
from claude_agent_sdk.types import McpStdioServerConfig
from claude_agent_sdk.types import ResultMessage
from claude_agent_sdk.types import TextBlock
from claude_agent_sdk.types import ThinkingBlock
from claude_agent_sdk.types import ToolResultBlock
from claude_agent_sdk.types import ToolUseBlock
from claude_agent_sdk.types import UserMessage
from rich.markdown import Markdown
from rich.table import Table
from textual import work
from textual.app import App
from textual.app import ComposeResult
from textual.binding import Binding
from textual.reactive import reactive
from textual.widgets import Footer
from textual.widgets import Header
from textual.widgets import Input
from textual.widgets import LoadingIndicator
from textual.widgets import RichLog
from textual.widgets import Static

from chatsbom.core.config import DatabaseConfig
from chatsbom.core.config import get_config

dotenv.load_dotenv()

SYSTEM_PROMPT = (
    'You are an expert for querying the SBOM database. '
    'You can ONLY use the mcp-clickhouse tool to query the database. '
    'Do NOT attempt to read files, write files, or execute bash commands. '
    'Always use the mcp-clickhouse tool to query data. '
    'For large exports, format your answer and tell the user how many results there are.'
)

app = typer.Typer(help='Chat with your SBOM data using AI')


class ChatSBOMApp(App):
    """ChatSBOM Agent TUI."""

    CSS = """
    Screen { layout: grid; grid-size: 1; grid-rows: 1fr auto auto auto auto; }
    RichLog { border: solid green; }
    #status { height: 1; background: $primary-background; padding: 0 1; }
    #loading { height: 1; }
    .hidden { display: none; }
    """

    BINDINGS = [
        Binding('ctrl+c', 'quit', 'Quit'),
        Binding('ctrl+l', 'clear', 'Clear'),
    ]
    is_loading = reactive(False)

    def __init__(self, db_config: DatabaseConfig):
        super().__init__()
        self.db_config = db_config
        self.client: ClaudeSDKClient | None = None
        self.stats = {'cost': 0.0, 'turns': 0, 'in': 0, 'out': 0, 'ms': 0}

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield RichLog(id='log', highlight=True, markup=True)
        yield LoadingIndicator(id='loading')
        yield Static(id='status')
        yield Input(placeholder="Enter query ('exit' to quit)...", id='input')
        yield Footer()

    def watch_is_loading(self, loading: bool) -> None:
        self.query_one('#loading').set_class(not loading, 'hidden')
        inp = self.query_one('#input', Input)
        inp.disabled = loading
        if not loading:
            inp.focus()
        self._update_status()

    async def on_mount(self) -> None:
        """Initialize the Claude Agent SDK client."""
        import tempfile
        stderr_file = tempfile.NamedTemporaryFile(
            mode='w', delete=False, suffix='.log',
        )

        # Map DatabaseConfig to env vars expected by mcp-clickhouse
        env_vars = {
            'CLICKHOUSE_HOST': self.db_config.host,
            'CLICKHOUSE_PORT': str(self.db_config.port),
            'CLICKHOUSE_USER': self.db_config.user,
            'CLICKHOUSE_PASSWORD': self.db_config.password,
            'CLICKHOUSE_DATABASE': self.db_config.database,
            'CLICKHOUSE_ROLE': '',
            'CLICKHOUSE_SECURE': 'false',
            'CLICKHOUSE_VERIFY': 'false',
            'CLICKHOUSE_CONNECT_TIMEOUT': '16',
            'CLICKHOUSE_SEND_RECEIVE_TIMEOUT': '60',
        }

        opts = ClaudeAgentOptions(
            disallowed_tools=[
                'Read', 'Write', 'Edit',
                'MultiEdit', 'Bash', 'Glob', 'Grep', 'LS',
            ],
            permission_mode='bypassPermissions',
            mcp_servers={
                'mcp-clickhouse': McpStdioServerConfig(
                    command='uvx', args=['mcp-clickhouse'], env=env_vars,
                ),
            },
            system_prompt=SYSTEM_PROMPT,
            env={
                k: v for k, v in [
                    ('ANTHROPIC_BASE_URL', os.getenv('ANTHROPIC_BASE_URL')),
                ] if v
            },
            debug_stderr=stderr_file,
        )

        self.client = ClaudeSDKClient(options=opts)
        try:
            await self.client.__aenter__()
        except Exception as e:
            self._handle_init_error(e, stderr_file.name)
            raise
        finally:
            stderr_file.close()
            with suppress(OSError):
                os.unlink(stderr_file.name)

        log = self.query_one('#log', RichLog)
        log.write('[bold green]ChatSBOM Agent[/] - Query examples:')
        log.write('  • Top 10 projects using gin framework')
        log.write('  • Top 5 Python libraries')
        self.query_one('#loading').add_class('hidden')
        self._update_status()

    def _handle_init_error(self, error: Exception, stderr_path: str) -> None:
        """Display initialization error with context."""
        from rich.console import Console
        from rich.panel import Panel
        from pathlib import Path

        lines = [
            f'[red]{error}[/red]',
            f'[dim]{type(error).__name__}[/dim]',
        ]

        if stderr := Path(stderr_path).read_text().strip():
            lines += ['', '[yellow]stderr:[/yellow]', f'[dim]{stderr}[/dim]']

        if os.geteuid() == 0:
            lines += ['', '[yellow]⚠ Cannot use bypassPermissions as root. Run as non-root user.[/yellow]']

        if url := os.getenv('ANTHROPIC_BASE_URL'):
            lines += ['', f'[dim]API: {url}[/dim]']

        Console().print(
            Panel(
                '\n'.join(lines),
                title='[red]Init Failed[/red]', border_style='red',
            ),
        )

    async def on_unmount(self) -> None:
        if self.client:
            try:
                await self.client.__aexit__(None, None, None)
            except (RuntimeError, asyncio.CancelledError):
                pass

    def _update_status(self) -> None:
        s = self.stats
        if s['turns']:
            cny = s['cost'] * 7.2
            text = (
                f"🔄 {s['turns']} turns | "
                f"📊 {s['in']:,} in / {s['out']:,} out | "
                f"⏱ {s['ms']:,}ms | "
                f"💰 ${s['cost']:.4f} / ¥{cny:.4f}"
            )
        else:
            text = '✨ Ready'
        self.query_one('#status', Static).update(text)

    def action_clear(self) -> None:
        self.query_one('#log', RichLog).clear()

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        query = event.value.strip()
        self.query_one('#input', Input).value = ''
        if not query:
            return
        if query.lower() in ('exit', 'quit'):
            self.exit()
            return
        self.query_one('#log', RichLog).write(f'[bold blue]>>> {query}[/]')
        self.process_query(query)

    @work(exclusive=True)
    async def process_query(self, query: str) -> None:
        if not self.client:
            return
        log = self.query_one('#log', RichLog)
        self.is_loading = True
        try:
            await self.client.query(query)
            async for msg in self.client.receive_response():
                self._render(msg, log)
        except Exception as e:
            log.write(f'[red]Error: {e}[/]')
        finally:
            self.is_loading = False

    def _render(self, msg, log: RichLog) -> None:
        """Render a message to the log."""
        if isinstance(msg, AssistantMessage):
            for b in msg.content:
                self._render_block(b, log)
        elif isinstance(msg, UserMessage) and isinstance(msg.content, list):
            for b in msg.content:
                self._render_block(b, log)
        elif isinstance(msg, ResultMessage):
            self.stats.update({
                'cost': msg.total_cost_usd or 0,
                'turns': msg.num_turns,
                'in': (msg.usage or {}).get('input_tokens', 0),
                'out': (msg.usage or {}).get('output_tokens', 0),
                'ms': msg.duration_ms,
            })
            s = self.stats
            cny = s['cost'] * 7.2
            log.write(
                f"[dim]{datetime.now():%H:%M:%S} | "
                f"{s['ms']:,}ms | "
                f"{s['in']:,} in / {s['out']:,} out | "
                f"${s['cost']:.4f} / ¥{cny:.4f}[/]",
            )
            self._update_status()

    def _render_block(self, block, log: RichLog) -> None:
        """Render a content block to the log."""
        if isinstance(block, TextBlock):
            log.write(Markdown(block.text))
        elif isinstance(block, ThinkingBlock):
            log.write(f"[dim]💭 {block.thinking[:80]}...[/]")
        elif isinstance(block, ToolUseBlock):
            log.write(f'[cyan]⚙ {block.name}[/] [dim]{block.input}[/]')
        elif isinstance(block, ToolResultBlock):
            self._render_tool_result(block, log)

    def _render_tool_result(self, block: ToolResultBlock, log: RichLog) -> None:
        """Render tool result, converting JSON tables to rich tables."""
        if block.is_error:
            log.write(f'[red]✗ {block.content}[/]')
            return
        if not isinstance(block.content, str):
            log.write('[green]✓[/]')
            return
        try:
            data = json.loads(block.content)
            if 'columns' in data and 'rows' in data:
                t = Table(header_style='bold cyan')
                for c in data['columns']:
                    if c.lower() == 'description':
                        t.add_column(
                            c, no_wrap=True,
                            overflow='ellipsis', max_width=50,
                        )
                    else:
                        t.add_column(c, no_wrap=True, overflow='ellipsis')
                for r in data['rows']:
                    t.add_row(*[str(x) for x in r])
                log.write(t)
            else:
                log.write(f'[green]✓[/] {block.content[:100]}')
        except (json.JSONDecodeError, TypeError):
            log.write('[green]✓[/]')


@app.callback(invoke_without_command=True)
def main(
    host: str = typer.Option(None, help='ClickHouse host'),
    port: int = typer.Option(None, help='ClickHouse http port'),
    user: str = typer.Option(None, help='ClickHouse user'),
    password: str = typer.Option(None, help='ClickHouse password'),
    database: str = typer.Option(None, help='ClickHouse database'),
):
    """Start an AI conversation about your SBOM data."""
    from rich.console import Console
    console = Console()

    # If context is passed (e.g. --help), don't run the TUI
    # But since we use callback(invoke_without_command=True), this runs when no subcommand.
    # Typer handles --help automatically.

    if not os.getenv('ANTHROPIC_API_KEY') and not os.getenv('ANTHROPIC_AUTH_TOKEN'):
        console.print(
            '[bold red]Error:[/] ANTHROPIC_API_KEY or ANTHROPIC_AUTH_TOKEN is not set.\n\n'
            'The Agent requires an Anthropic API key for Claude. '
            'Please set the ANTHROPIC_API_KEY or ANTHROPIC_AUTH_TOKEN environment variable:\n\n'
            '  [cyan]export ANTHROPIC_API_KEY="your_api_key"[/]\n\n'
            '  [cyan]or[/]\n\n'
            '  [cyan]export ANTHROPIC_AUTH_TOKEN="your_auth_token"[/]\n\n'
            'Or add it to your [cyan].env[/] file:\n\n'
            '  [cyan]ANTHROPIC_API_KEY=your_api_key[/]\n\n'
            '  [cyan]or[/]\n\n'
            '  [cyan]ANTHROPIC_AUTH_TOKEN=your_auth_token[/]\n\n'
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

    ChatSBOMApp(db_config).run()


if __name__ == '__main__':
    app()
