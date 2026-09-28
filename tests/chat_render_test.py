"""What the chat shows, it shows as it is (#29).

Its log read everything written to it as Rich markup: the user's query,
the model's thinking, the SQL it wrote, a tool's error, and each cell of
a result, which comes from the database, and so from whoever wrote a
repository's description on GitHub. `[/bold]` there raised MarkupError
and took the worker down with it; `[link=https://evil.example]` made a
live hyperlink of what followed. The init-failure panel read the error
and the CLI's stderr the same way.

Each is built by a function that returns what the log is given, printed
here through a recording console.
"""
import asyncio
import io
import json
import os

import pytest
from claude_agent_sdk import ClaudeAgentOptions
from claude_agent_sdk import CLIConnectionError
from claude_agent_sdk import TextBlock
from claude_agent_sdk import ThinkingBlock
from claude_agent_sdk import ToolResultBlock
from claude_agent_sdk import ToolUseBlock
from rich.console import Console
from rich.console import RenderableType

from chatsbom.commands import chat_tui
from chatsbom.commands.chat_tui import ChatSBOMApp
from chatsbom.commands.chat_tui import error_line
from chatsbom.commands.chat_tui import init_error_panel
from chatsbom.commands.chat_tui import query_line
from chatsbom.commands.chat_tui import render_block
from chatsbom.core.config import DatabaseConfig

#: Markup as it may turn up in data: a closing tag nothing opened, which
#: raised, and a link, which linked.
PAYLOADS = ['[/bold]', '[link=https://evil.example]x[/link]']


def shown(renderable: RenderableType | None) -> str:
    """What `renderable` prints, checked to link to nowhere."""
    assert renderable is not None
    console = Console(file=io.StringIO(), width=200, record=True)
    console.print(renderable)
    links = [
        segment.style.link for segment in console.render(renderable)
        if segment.style is not None and segment.style.link
    ]
    assert links == []
    return console.export_text()


def result(
    content: str | list[dict[str, str]], is_error: bool | None = None,
) -> ToolResultBlock:
    return ToolResultBlock(
        tool_use_id='toolu_1', content=content, is_error=is_error,
    )


def table(columns: list[str], rows: list[list[object]]) -> str:
    return json.dumps({'columns': columns, 'rows': rows, 'truncated': False})


@pytest.mark.parametrize('payload', PAYLOADS)
def test_a_database_value_is_shown_as_it_is(payload):
    block = result(table(['description'], [[f'a {payload} library']]))

    assert f'a {payload} library' in shown(render_block(block))


@pytest.mark.parametrize('payload', PAYLOADS)
def test_a_column_name_is_shown_as_it_is(payload):
    """The model names the columns, and what it read may name them."""
    assert payload in shown(render_block(result(table([payload], [[1]]))))


def test_a_result_is_a_table_as_the_cli_hands_on_an_mcp_tools():
    """As a list of content blocks, not a string."""
    content = [{'type': 'text', 'text': table(['name'], [['gin']])}]

    printed = shown(render_block(result(content)))

    assert 'name' in printed and 'gin' in printed


def test_a_cut_short_result_says_so():
    content = json.dumps({
        'columns': ['n'], 'rows': [[1]], 'truncated': True,
        'note': 'Only the first 1 rows are here.',
    })

    printed = shown(render_block(result(content)))

    assert 'Only the first 1 rows are here.' in printed


@pytest.mark.parametrize('payload', PAYLOADS)
def test_the_query_is_shown_as_it_is(payload):
    assert f'>>> find {payload}' in shown(query_line(f'find {payload}'))


@pytest.mark.parametrize('payload', PAYLOADS)
def test_the_sql_is_shown_as_it_is(payload):
    block = ToolUseBlock(
        id='toolu_1', name='mcp__clickhouse__run_select_query',
        input={'query': f"SELECT '{payload}'"},
    )

    assert f"SELECT '{payload}'" in shown(render_block(block))


@pytest.mark.parametrize('payload', PAYLOADS)
def test_thinking_is_shown_as_it_is(payload):
    block = ThinkingBlock(thinking=f'maybe {payload}', signature='')

    assert f'maybe {payload}' in shown(render_block(block))


@pytest.mark.parametrize('payload', PAYLOADS)
def test_a_tool_error_is_shown_as_it_is(payload):
    block = result(f'Code: 47. {payload}', is_error=True)

    assert f'Code: 47. {payload}' in shown(render_block(block))


@pytest.mark.parametrize('payload', PAYLOADS)
def test_an_error_the_sdk_did_not_flag_is_still_an_error(payload):
    """The SDK before 0.1.51 drops an in-process tool's `is_error`: the
    tool's answer says so itself."""
    block = result(json.dumps({'error': f'Code: 47. {payload}'}))

    printed = shown(render_block(block))

    assert f'✗ Code: 47. {payload}' in printed
    assert '✓' not in printed


@pytest.mark.parametrize('payload', PAYLOADS)
def test_a_result_that_is_no_table_is_shown_as_it_is(payload):
    block = result(json.dumps({'database': payload}))

    assert payload in shown(render_block(block))


@pytest.mark.parametrize('payload', PAYLOADS)
def test_a_failed_query_is_shown_as_it_is(payload):
    printed = shown(error_line(RuntimeError(f'gone {payload}')))

    assert f'Error: gone {payload}' in printed


@pytest.mark.parametrize('payload', PAYLOADS)
def test_why_the_agent_did_not_start_is_shown_as_it_is(payload):
    panel = init_error_panel(
        RuntimeError(f'failed {payload}'), [f'stderr says {payload}'], None,
    )

    printed = shown(panel)

    assert f'failed {payload}' in printed
    assert f'stderr says {payload}' in printed


def test_the_init_failure_has_no_advice_about_bypassing_permissions(monkeypatch):
    """It said not to run as root, which `bypassPermissions` refuses;
    nothing asks for that mode now."""
    monkeypatch.setattr(os, 'geteuid', lambda: 0)

    printed = shown(init_error_panel(RuntimeError('failed'), [], None))

    assert 'bypassPermissions' not in printed
    assert 'root' not in printed


def test_the_answer_links_to_nowhere_and_shows_where_it_would_have():
    """The model's answer is Markdown, and what it read may have shaped
    it: a link's text could hide where it goes."""
    block = TextBlock(text='See [gin](https://evil.example/q?rows=1).')

    printed = shown(render_block(block))

    assert 'gin' in printed
    assert 'https://evil.example/q?rows=1' in printed


# --- the app ----------------------------------------------------------------

class FailingClient:
    """A CLI that says why on stderr, and exits."""

    def __init__(self, options: ClaudeAgentOptions) -> None:
        self.options = options

    async def __aenter__(self) -> 'FailingClient':
        assert self.options.stderr is not None, 'stderr goes nowhere'
        for payload in PAYLOADS:
            self.options.stderr(f'Error: invalid {payload}')
        raise CLIConnectionError(f'Failed to start: {PAYLOADS[0]}')

    async def __aexit__(self, *exc_info: object) -> None:
        pass


def test_a_failed_start_shows_what_the_cli_said(monkeypatch):
    """Printed while the app ran, the panel went where textual captures
    what is printed: nowhere, outside its devtools. It is handed to
    `exit`, which prints it once the terminal is back."""
    monkeypatch.setattr(chat_tui, 'ClaudeSDKClient', FailingClient)
    app = ChatSBOMApp(DatabaseConfig())
    exits: list[tuple[int, RenderableType | None]] = []
    real_exit = app.exit

    def spy(
        result: object = None, return_code: int = 0,
        message: RenderableType | None = None,
    ) -> None:
        exits.append((return_code, message))
        real_exit(result, return_code, message)

    monkeypatch.setattr(app, 'exit', spy)

    async def run() -> None:
        async with app.run_test() as pilot:
            await pilot.pause()

    asyncio.run(run())

    [(return_code, message)] = exits
    assert return_code == 1
    printed = shown(message)
    assert f'Failed to start: {PAYLOADS[0]}' in printed
    for payload in PAYLOADS:
        assert f'Error: invalid {payload}' in printed
    assert app.return_code == 1
