"""The `chat` TUI: a textual app that queries the SBOM database via Claude.

A module of its own, imported by `chat` when it runs. textual and the
Claude Agent SDK take the better part of a second to import, which every
command paid at start-up while this lived in `chat.py` (#26).

What it shows is `Text`, or made of it, and never read as markup (#29).
The log read every string written to it as Rich markup: the user's
query, the model's thinking, the SQL it wrote, a tool's error, and each
cell of a result, which is text anyone can set on GitHub. `[/bold]` in a
repository's description raised MarkupError and took the worker down;
`[link=https://evil.example]` made a live link of what followed.
"""
import asyncio
import json
import os
from collections import deque
from collections.abc import Iterable
from datetime import datetime
from typing import Any

from claude_agent_sdk.client import ClaudeSDKClient
from claude_agent_sdk.types import AssistantMessage
from claude_agent_sdk.types import ResultMessage
from claude_agent_sdk.types import TextBlock
from claude_agent_sdk.types import ThinkingBlock
from claude_agent_sdk.types import ToolResultBlock
from claude_agent_sdk.types import ToolUseBlock
from claude_agent_sdk.types import UserMessage
from rich.console import Group
from rich.console import RenderableType
from rich.markdown import Markdown
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
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

from chatsbom.commands.chat import format_cost
from chatsbom.commands.chat_agent import build_options
from chatsbom.core.config import DatabaseConfig

#: How many of the CLI's last lines on stderr are kept, for the panel
#: that says why the agent did not start.
STDERR_LINES = 50


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
        #: What the CLI last said on stderr.
        self.cli_stderr: deque[str] = deque(maxlen=STDERR_LINES)

    def compose(self) -> ComposeResult:
        yield Header(show_clock=True)
        # Everything written is `Text` already; a string written by
        # mistake is shown as it is, not read as markup.
        yield RichLog(id='log', markup=False)
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
        """Start the agent: the Claude CLI, through the SDK."""
        self.client = ClaudeSDKClient(
            options=build_options(self.db_config, self.cli_stderr.append),
        )
        try:
            await self.client.__aenter__()
        except Exception as e:
            # To `exit`, which prints it once the terminal is back.
            # Printed here, it went where textual puts what is printed
            # while it runs: nowhere, outside its devtools.
            self.exit(
                return_code=1,
                message=init_error_panel(
                    e, self.cli_stderr, os.getenv('ANTHROPIC_BASE_URL'),
                ),
            )
            return

        log = self.query_one('#log', RichLog)
        log.write(
            Text.assemble(
                ('ChatSBOM Agent', 'bold green'),
                ' - Query examples:',
            ),
        )
        log.write(Text('  • Top 10 projects using gin framework'))
        log.write(Text('  • Top 5 Python libraries'))
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
            text = (
                f"🔄 {s['turns']} turns | "
                f"📊 {s['in']:,} in / {s['out']:,} out | "
                f"⏱ {s['ms']:,}ms | "
                f"💰 {format_cost(s['cost'])}"
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
        self.query_one('#log', RichLog).write(query_line(query))
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
            log.write(error_line(e))
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
            log.write(
                Text(
                    f"{datetime.now():%H:%M:%S} | "
                    f"{s['ms']:,}ms | "
                    f"{s['in']:,} in / {s['out']:,} out | "
                    f"{format_cost(s['cost'])}",
                    style='dim',
                ),
            )
            self._update_status()

    def _render_block(self, block, log: RichLog) -> None:
        """Render a content block to the log."""
        renderable = render_block(block)
        if renderable is not None:
            log.write(renderable)


def query_line(query: str) -> Text:
    """The user's query, as the log shows it."""
    return Text(f'>>> {query}', style='bold blue')


def error_line(error: BaseException) -> Text:
    """Why a query got no answer."""
    return Text(f'Error: {error}', style='red')


def render_block(block: object) -> RenderableType | None:
    """A content block of the agent's, as the log shows it."""
    if isinstance(block, TextBlock):
        # The answer, which is Markdown, with no link in it live: what
        # the model read may have shaped it, and a link's text can hide
        # where it goes. Rich shows the address after the text instead.
        return Markdown(block.text, hyperlinks=False)
    if isinstance(block, ThinkingBlock):
        return Text(f'💭 {block.thinking[:80]}...', style='dim')
    if isinstance(block, ToolUseBlock):
        return Text.assemble(
            (f'⚙ {block.name}', 'cyan'), ' ', (str(block.input), 'dim'),
        )
    if isinstance(block, ToolResultBlock):
        return tool_result(block)
    return None


def tool_result(block: ToolResultBlock) -> RenderableType:
    """A tool's result: a table of rows, or a line on how it went."""
    text = text_of(block.content)
    if block.is_error:
        return failed(text)
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        return Text('✓', style='green')
    if not isinstance(data, dict):
        return succeeded(text)
    # The database tools say so when they fail: the flag alone is lost
    # on the way, by the SDK before 0.1.51.
    if 'error' in data:
        return failed(str(data['error']))
    columns, rows = data.get('columns'), data.get('rows')
    if isinstance(columns, list) and isinstance(rows, list):
        return result_table(data)
    return succeeded(text)


def text_of(content: str | list[dict[str, Any]] | None) -> str:
    """A tool result's text: a string, or the text blocks an MCP tool
    answers with."""
    if isinstance(content, str):
        return content
    return ''.join(
        str(part.get('text', '')) for part in content or []
        if part.get('type') == 'text'
    )


def succeeded(text: str) -> Text:
    return Text.assemble(('✓', 'green'), ' ', text[:100])


def failed(text: str) -> Text:
    return Text(f'✗ {text}', style='red')


def result_table(data: dict[str, Any]) -> RenderableType:
    """Rows from the database, and each column's name, as text; and,
    beneath them, what was left out."""
    table = Table(header_style='bold cyan')
    for column in data['columns']:
        name = str(column)
        # A description is long, and would push the rest off the screen.
        width = 50 if name.lower() == 'description' else None
        table.add_column(
            Text(name), no_wrap=True, overflow='ellipsis', max_width=width,
        )
    for row in data['rows']:
        table.add_row(*[Text(str(cell)) for cell in row])
    if not data.get('truncated'):
        return table
    # Not the table's caption, which is only as wide as the table.
    return Group(table, Text(str(data.get('note', 'Cut short.')), style='dim'))


def init_error_panel(
    error: BaseException,
    stderr: Iterable[str],
    base_url: str | None,
) -> Panel:
    """Why the agent did not start, and the last the CLI said."""
    body = Text()
    body.append(str(error), style='red')
    body.append(f'\n{type(error).__name__}', style='dim')
    if said := '\n'.join(stderr).strip():
        body.append('\n\nstderr:\n', style='yellow')
        body.append(said, style='dim')
    if base_url:
        body.append(f'\n\nAPI: {base_url}', style='dim')
    return Panel(
        body, title=Text('Init Failed', style='red'), border_style='red',
    )
