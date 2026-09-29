"""The chat's database tools, run in-process against a real ClickHouse (#29).

They were `uvx mcp-clickhouse`, a server fetched from PyPI at whatever
version it served, with the password in its environment. They are the
SDK's in-process tools now, over the guest's `QueryRepository`.

Admin on a throwaway database here, as the fixtures connect: the guest
may read `chatsbom` alone. Admin is also the harder case, since its
profile lets it write.
"""
import asyncio
import json
from typing import Any

import pytest
from claude_agent_sdk import SdkMcpTool
from mcp import Client
from mcp.server import Server
from mcp.types import TextContent

from chatsbom.commands.chat_agent import build_options
from chatsbom.commands.chat_agent import Database
from chatsbom.commands.chat_agent import database_tools
from chatsbom.commands.chat_agent import MAX_CHARS
from chatsbom.commands.chat_agent import MAX_ROWS
from chatsbom.commands.chat_agent import SERVER
from chatsbom.core.repository import QueryRepository
from tests.conftest import requires_clickhouse

pytestmark = requires_clickhouse


@pytest.fixture
def tools(query: QueryRepository) -> dict[str, SdkMcpTool[Any]]:
    return {tool.name: tool for tool in database_tools(Database(query))}


def call(tool: SdkMcpTool[Any], **arguments: Any) -> tuple[Any, bool]:
    """What `tool` answers, parsed, and whether it says it failed."""
    # The SDK types a handler's result as any awaitable, and
    # `asyncio.run` takes a coroutine.
    async def answer() -> dict[str, Any]:
        return await tool.handler(arguments)

    result = asyncio.run(answer())
    [content] = result['content']
    assert content['type'] == 'text'
    return json.loads(content['text']), bool(result.get('is_error'))


def test_a_query_is_answered_with_its_columns_and_rows(tools):
    """The shape the TUI draws a table of."""
    answer, failed = call(
        tools['run_select_query'],
        query='SELECT number AS n, toString(number) AS s FROM numbers(3)',
    )

    assert not failed
    assert answer == {
        'columns': ['n', 's'],
        'rows': [[0, '0'], [1, '1'], [2, '2']],
        'truncated': False,
    }


def test_a_long_result_stops_at_the_row_cap_and_says_so(tools):
    """Every row is read by the model, and costs it context."""
    answer, failed = call(
        tools['run_select_query'],
        query=f'SELECT number FROM numbers({MAX_ROWS + 50})',
    )

    assert not failed
    assert answer['rows'] == [[n] for n in range(MAX_ROWS)]
    assert answer['truncated'] is True
    assert f'first {MAX_ROWS} rows' in answer['note']


def test_wide_rows_stop_at_the_size_cap_and_say_so(tools):
    """Fewer rows than the cap can still be more than the model should
    read: the CLI cuts a longer tool result short itself, silently."""
    answer, failed = call(
        tools['run_select_query'],
        query=f"SELECT repeat('x', 1000) AS wide FROM numbers({MAX_ROWS})",
    )

    assert not failed
    assert 0 < len(answer['rows']) < MAX_ROWS
    assert len(json.dumps(answer['rows'])) <= MAX_CHARS
    assert answer['truncated'] is True
    assert f"first {len(answer['rows'])} rows" in answer['note']


def test_a_failing_query_is_answered_not_raised(tools):
    """The model reads why and can try again; raised, the tool call
    ended with nothing it could act on."""
    answer, failed = call(tools['run_select_query'], query='SELECT nope')

    assert failed
    assert 'nope' in answer['error']


def test_a_write_is_refused_whoever_connects(tools, query):
    """The guest's profile is read-only; a user whose profile is not,
    given with `chat --user`, is made so for each query."""
    answer, failed = call(
        tools['run_select_query'],
        query='CREATE TABLE written (a UInt8) ENGINE = Memory',
    )

    assert failed
    assert 'readonly' in answer['error'].lower()
    assert query.client.command('EXISTS TABLE written') == 0


def test_values_json_has_no_type_for_are_given_as_text(tools):
    answer, failed = call(
        tools['run_select_query'],
        query=(
            "SELECT toUUID('00000000-0000-0000-0000-00000000002a') AS id, "
            "toDate('2024-01-02') AS day, toDecimal64(1.5, 2) AS share"
        ),
    )

    assert not failed
    assert answer['rows'] == [
        ['00000000-0000-0000-0000-00000000002a', '2024-01-02', '1.50'],
    ]


def test_the_tables_are_listed_with_their_columns(tools, clickhouse_db):
    answer, failed = call(tools['list_tables'])

    assert not failed
    assert answer['database'] == clickhouse_db
    tables = {table['name']: table for table in answer['tables']}
    assert {'repositories', 'artifacts', 'releases'} <= set(tables)
    # A materialized view's own storage is not one to query.
    assert not [name for name in tables if name.startswith('.inner')]
    columns = {
        column['name']: column for column in tables['repositories']['columns']
    }
    assert columns['id']['type'] == 'UInt64'
    assert columns['id']['comment']


def served(
    server: Server[Any], *calls: tuple[str, dict[str, Any]],
) -> tuple[set[str], list[tuple[Any, bool]]]:
    """The tools `server` lists, and what it answers to each of `calls`,
    parsed, with whether it says it failed.

    Asked as the CLI asks: the SDK hands the CLI's JSON-RPC to
    `Server.run` over mcp's in-memory transport, after an `initialize`,
    which `legacy` is.
    """
    async def ask() -> tuple[set[str], list[tuple[Any, bool]]]:
        async with Client(server, mode='legacy') as client:
            listed = await client.list_tools()
            answers = []
            for name, arguments in calls:
                result = await client.call_tool(name, arguments)
                [content] = result.content
                assert isinstance(content, TextContent)
                answers.append(
                    (json.loads(content.text), bool(result.is_error)),
                )
        return {tool.name for tool in listed.tools}, answers

    return asyncio.run(ask())


def test_the_server_the_cli_is_given_answers_from_the_database(
    query, clickhouse_db,
):
    """What the model is sent: the tools above, as the SDK's own server
    runs them and makes MCP's results of their answers, the error flag
    among them. It does that one way on mcp 1 and another on mcp 2."""
    query.client.command(
        'CREATE TABLE probe (n UInt8, s String) ENGINE = Memory',
    )
    query.client.command("INSERT INTO probe VALUES (1, 'one'), (2, 'two')")
    server = build_options(query.config, print).mcp_servers[SERVER]

    names, [tables, rows, failed] = served(
        server['instance'],
        ('list_tables', {}),
        ('run_select_query', {'query': 'SELECT n, s FROM probe ORDER BY n'}),
        ('run_select_query', {'query': 'SELECT nope'}),
    )

    assert names == {'list_tables', 'run_select_query'}
    answer, failing = tables
    assert not failing
    assert answer['database'] == clickhouse_db
    assert 'probe' in {table['name'] for table in answer['tables']}
    assert rows == (
        {
            'columns': ['n', 's'],
            'rows': [[1, 'one'], [2, 'two']],
            'truncated': False,
        },
        False,
    )
    answer, failing = failed
    assert failing
    assert 'nope' in answer['error']
