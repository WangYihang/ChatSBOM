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
from claude_agent_sdk.types import StreamEvent
from claude_agent_sdk.types import SystemMessage
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
from textual.widgets import Footer
from textual.widgets import Header
from textual.widgets import Input
from textual.widgets import RichLog

dotenv.load_dotenv()


@dataclass
class ClickHouseConfig:
    host: str = field(
        default_factory=lambda: os.getenv('CLICKHOUSE_HOST', 'localhost'),
    )
    port: str = field(
        default_factory=lambda: os.getenv('CLICKHOUSE_PORT', '8123'),
    )
    user: str = field(
        default_factory=lambda: os.getenv('CLICKHOUSE_USER', 'guest'),
    )
    password: str = field(
        default_factory=lambda: os.getenv('CLICKHOUSE_PASSWORD', 'guest'),
    )
    role: str = field(default_factory=lambda: os.getenv('CLICKHOUSE_ROLE', ''))
    secure: bool = field(
        default_factory=lambda: os.getenv(
            'CLICKHOUSE_SECURE', 'false',
        ).lower() == 'true',
    )
    verify: bool = field(
        default_factory=lambda: os.getenv(
            'CLICKHOUSE_VERIFY', 'true',
        ).lower() == 'true',
    )
    connect_timeout: str = field(
        default_factory=lambda: os.getenv('CLICKHOUSE_CONNECT_TIMEOUT', '30'),
    )
    send_receive_timeout: str = field(
        default_factory=lambda: os.getenv(
            'CLICKHOUSE_SEND_RECEIVE_TIMEOUT', '30',
        ),
    )

    def to_env(self) -> dict[str, str]:
        return {
            'CLICKHOUSE_HOST': self.host,
            'CLICKHOUSE_PORT': self.port,
            'CLICKHOUSE_USER': self.user,
            'CLICKHOUSE_PASSWORD': self.password,
            'CLICKHOUSE_ROLE': self.role,
            'CLICKHOUSE_SECURE': str(self.secure).lower(),
            'CLICKHOUSE_VERIFY': str(self.verify).lower(),
            'CLICKHOUSE_CONNECT_TIMEOUT': self.connect_timeout,
            'CLICKHOUSE_SEND_RECEIVE_TIMEOUT': self.send_receive_timeout,
        }


class SBOMInsightApp(App):
    """SBOM Insight Agent TUI with fixed input at bottom."""

    CSS = """
    RichLog {
        height: 1fr;
        border: solid green;
        scrollbar-gutter: stable;
    }
    Input {
        dock: bottom;
        margin: 0 1;
    }
    Footer {
        dock: bottom;
    }
    """

    BINDINGS = [
        Binding('ctrl+c', 'quit', 'Quit'),
        Binding('ctrl+l', 'clear', 'Clear'),
    ]

    def __init__(self, db_config: ClickHouseConfig):
        super().__init__()
        self.db_config = db_config
        self.client: ClaudeSDKClient | None = None
        self.total_cost = 0.0
        self.total_turns = 0

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        yield RichLog(id='output', highlight=True, markup=True)
        yield Input(placeholder="Enter your query (type 'exit' to quit)...", id='query_input')
        yield Footer()

    async def on_mount(self) -> None:
        """Initialize the Claude client when app mounts."""
        options = ClaudeAgentOptions(
            disallowed_tools=[
                'Read', 'Write', 'Edit', 'MultiEdit',
                'Bash', 'Glob', 'Grep', 'LS',
                'WebFetch', 'WebSearch',
            ],
            permission_mode='bypassPermissions',
            mcp_servers={
                'mcp-clickhouse': McpStdioServerConfig(
                    command='uvx',
                    args=['mcp-clickhouse'],
                    env=self.db_config.to_env(),
                ),
            },
            system_prompt=(
                'You are an expert for querying the SBOM database. '
                'You can ONLY use the mcp-clickhouse tool to query the database. '
                'Do NOT attempt to read files, write files, or execute bash commands. '
                'Always use the mcp-clickhouse tool to query data. '
                'For large exports, format your answer and tell the user how many results there are.'
            ),
        )
        self.client = ClaudeSDKClient(options=options)
        await self.client.__aenter__()

        # Show welcome message
        output = self.query_one('#output', RichLog)
        output.write('[bold green]Welcome to SBOM Insight Agent[/bold green]')
        output.write('Example queries:')
        output.write(
            '  • Show me the top 10 most popular projects using the gin framework.',
        )
        output.write(
            '  • List the top 5 most used libraries in Python repositories.',
        )
        output.write('')

    async def on_unmount(self) -> None:
        """Cleanup Claude client when app unmounts."""
        if self.client:
            await self.client.__aexit__(None, None, None)

    def action_clear(self) -> None:
        """Clear the output log."""
        output = self.query_one('#output', RichLog)
        output.clear()

    async def on_input_submitted(self, event: Input.Submitted) -> None:
        """Handle user query submission."""
        query = event.value.strip()
        input_widget = self.query_one('#query_input', Input)
        input_widget.value = ''

        if not query:
            return

        if query.lower() in ('exit', 'quit'):
            self.exit()
            return

        output = self.query_one('#output', RichLog)
        output.write(f"[bold blue]>>> {query}[/bold blue]")

        # Process the query in background
        self.process_query(query)

    @work(exclusive=True)
    async def process_query(self, query: str) -> None:
        """Process the query and stream results."""
        if not self.client:
            return

        output = self.query_one('#output', RichLog)

        try:
            await self.client.query(query)

            async for message in self.client.receive_response():
                self.display_message(message, output)

        except Exception as e:
            output.write(f"[red]Error: {e}[/red]")

    def display_message(self, message, output: RichLog) -> None:
        """Display a message in the output log."""
        if isinstance(message, AssistantMessage):
            for block in message.content:
                self.display_content_block(block, output)
        elif isinstance(message, UserMessage):
            if isinstance(message.content, list):
                for block in message.content:
                    self.display_content_block(block, output)
        elif isinstance(message, SystemMessage):
            output.write(f"[yellow]⚡ {message.subtype}[/yellow]")
        elif isinstance(message, ResultMessage):
            self.total_cost = message.total_cost_usd or 0.0
            self.total_turns = message.num_turns
            cost = f"${self.total_cost:.4f}"
            now = datetime.now().strftime('%H:%M:%S')
            output.write(
                f"[dim]── {now} | {message.duration_ms}ms | {self.total_turns} turns | {cost} ──[/dim]",
            )
            # Update footer
            self.sub_title = f"Turns: {self.total_turns} | Cost: {cost}"
        elif isinstance(message, StreamEvent):
            pass  # Skip stream events for cleaner output

    def display_content_block(self, block, output: RichLog) -> None:
        """Display a content block in the output log."""
        if isinstance(block, TextBlock):
            output.write(Markdown(block.text))
        elif isinstance(block, ThinkingBlock):
            thinking = block.thinking[:100] + \
                '...' if len(block.thinking) > 100 else block.thinking
            output.write(f"[dim]💭 {thinking}[/dim]")
        elif isinstance(block, ToolUseBlock):
            output.write(f"[cyan]⚙ {block.name}[/cyan]")
            output.write(f"[dim]  └─ {block.input}[/dim]")
        elif isinstance(block, ToolResultBlock):
            self.display_tool_result(block, output)

    def display_tool_result(self, block: ToolResultBlock, output: RichLog) -> None:
        """Display a tool result, formatting JSON tables nicely."""
        if block.is_error:
            output.write(f"[red]✗ Error: {block.content}[/red]")
            return

        content = block.content
        if not isinstance(content, str):
            output.write('[green]✓ OK[/green]')
            return

        try:
            data = json.loads(content)
            if 'columns' in data and 'rows' in data:
                table = Table(show_header=True, header_style='bold cyan')
                for col in data['columns']:
                    table.add_column(col)
                for row in data['rows']:
                    table.add_row(*[str(cell) for cell in row])
                output.write(table)
            else:
                output.write(f"[green]✓[/green] {content}")
        except (json.JSONDecodeError, TypeError):
            output.write('[green]✓ OK[/green]')


def main(
    timeout: int = typer.Option(600, '--timeout', help='Max time (seconds)'),
    host: str = typer.Option(
        'localhost', help='ClickHouse Host', envvar='CLICKHOUSE_HOST',
    ),
    port: str = typer.Option(
        '8123', help='ClickHouse Port', envvar='CLICKHOUSE_PORT',
    ),
    user: str = typer.Option(
        'guest', help='ClickHouse User', envvar='CLICKHOUSE_USER',
    ),
    password: str = typer.Option(
        'guest', help='ClickHouse Password', envvar='CLICKHOUSE_PASSWORD',
    ),
):
    """
    Run the AI agent with access to ClickHouse in a TUI.
    """
    db_config = ClickHouseConfig(
        host=host,
        port=port,
        user=user,
        password=password,
    )

    app = SBOMInsightApp(db_config=db_config)
    app.run()
