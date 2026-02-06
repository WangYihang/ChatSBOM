"""SBOM Insight Agent - TUI for querying SBOM database via Claude."""
import asyncio
import json
import os
from dataclasses import dataclass
from dataclasses import field
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

dotenv.load_dotenv()

SYSTEM_PROMPT = (
    'You are an expert for querying the SBOM database. '
    'You can ONLY use the mcp-clickhouse tool to query the database. '
    'Do NOT attempt to read files, write files, or execute bash commands. '
    'Always use the mcp-clickhouse tool to query data. '
    'For large exports, format your answer and tell the user how many results there are.'
)


@dataclass
class ClickHouseConfig:
    """ClickHouse connection configuration."""
    host: str = field(
        default_factory=lambda: os.getenv(
            'CLICKHOUSE_HOST', 'localhost',
        ),
    )
    port: str = field(
        default_factory=lambda: os.getenv(
            'CLICKHOUSE_PORT', '8123',
        ),
    )
    user: str = field(
        default_factory=lambda: os.getenv(
            'CLICKHOUSE_USER', 'guest',
        ),
    )
    password: str = field(
        default_factory=lambda: os.getenv(
            'CLICKHOUSE_PASSWORD', 'guest',
        ),
    )

    def to_env(self) -> dict[str, str]:
        return {
            'CLICKHOUSE_HOST': self.host,
            'CLICKHOUSE_PORT': self.port,
            'CLICKHOUSE_USER': self.user,
            'CLICKHOUSE_PASSWORD': self.password,
            'CLICKHOUSE_ROLE': '',
            'CLICKHOUSE_SECURE': 'false',
            'CLICKHOUSE_VERIFY': 'false',
            'CLICKHOUSE_CONNECT_TIMEOUT': '16',
            'CLICKHOUSE_SEND_RECEIVE_TIMEOUT': '60',
        }


class SBOMInsightApp(App):
    """SBOM Insight Agent TUI."""

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

    def __init__(self, db_config: ClickHouseConfig):
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
        opts = ClaudeAgentOptions(
            disallowed_tools=[
                'Read', 'Write', 'Edit',
                'MultiEdit', 'Bash', 'Glob', 'Grep', 'LS',
            ],
            permission_mode='bypassPermissions',
            mcp_servers={
                'mcp-clickhouse': McpStdioServerConfig(
                    command='uvx', args=['mcp-clickhouse'], env=self.db_config.to_env(),
                ),
            },
            system_prompt=SYSTEM_PROMPT,
        )
        self.client = ClaudeSDKClient(options=opts)
        await self.client.__aenter__()

        log = self.query_one('#log', RichLog)
        log.write('[bold green]SBOM Insight Agent[/] - Query examples:')
        log.write('  • Top 10 projects using gin framework')
        log.write('  • Top 5 Python libraries')
        self.query_one('#loading').add_class('hidden')
        self._update_status()

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
                    t.add_column(c, no_wrap=True, overflow='ellipsis')
                for r in data['rows']:
                    t.add_row(*[str(x) for x in r])
                log.write(t)
            else:
                log.write(f'[green]✓[/] {block.content[:100]}')
        except (json.JSONDecodeError, TypeError):
            log.write('[green]✓[/]')


def main(
    host: str = typer.Option('localhost', envvar='CLICKHOUSE_HOST'),
    port: str = typer.Option('8123', envvar='CLICKHOUSE_PORT'),
    user: str = typer.Option('guest', envvar='CLICKHOUSE_USER'),
    password: str = typer.Option('guest', envvar='CLICKHOUSE_PASSWORD'),
):
    """Run the SBOM Insight Agent TUI."""
    from rich.console import Console
    console = Console()

    if not os.getenv('ANTHROPIC_API_KEY'):
        console.print(
            '[bold red]Error:[/] ANTHROPIC_API_KEY is not set.\n\n'
            'The Agent requires an Anthropic API key for Claude. '
            'Please set the ANTHROPIC_API_KEY environment variable:\n\n'
            '  [cyan]export ANTHROPIC_API_KEY="your_api_key"[/]\n\n'
            'Or add it to your [cyan].env[/] file:\n\n'
            '  [cyan]ANTHROPIC_API_KEY=your_api_key[/]\n\n'
            'You can get an API key at: '
            '[link=https://console.anthropic.com/]https://console.anthropic.com/[/link]',
        )
        raise typer.Exit(1)

    SBOMInsightApp(ClickHouseConfig(host, port, user, password)).run()
