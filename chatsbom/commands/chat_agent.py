"""What the `chat` agent may do, and the tools it does it with (#29).

A module of its own, imported by the TUI when `chat` runs: the Claude
Agent SDK takes the better part of a second to import (#26).

The model has the database tools and nothing else. It had a list of
eight built-in tools it could not use, with permission prompts off, so
WebFetch, WebSearch, Task and every tool added since were there to use
unasked; and what it reads is text anyone can set on GitHub, a
repository's description or a package's name. Now no built-in tool is
there at all, the database tools are allowed by name, and the CLI asks
about anything else, to be told no.

The tools run in this process, over the guest's `QueryRepository`. They
were `uvx mcp-clickhouse`: whatever version PyPI served, outside the
lock, with the ClickHouse password in its environment.

The CLI is given what it needs of this process's environment, and no
value of anything else. It was given all of it: GITHUB_TOKEN,
OPENAI_API_KEY and whatever else this process holds, for a CLI with no
tool that could use one (#113).
"""
import asyncio
import json
import os
import threading
import warnings
from collections.abc import Callable
from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Sequence
from typing import Any
from typing import TYPE_CHECKING

from claude_agent_sdk import CanUseToolShadowedWarning
from claude_agent_sdk import ClaudeAgentOptions
from claude_agent_sdk import create_sdk_mcp_server
from claude_agent_sdk import PermissionResult
from claude_agent_sdk import PermissionResultAllow
from claude_agent_sdk import PermissionResultDeny
from claude_agent_sdk import SdkMcpTool
from claude_agent_sdk import tool
from claude_agent_sdk import ToolPermissionContext

from chatsbom.commands.chat import SYSTEM_PROMPT
from chatsbom.core.config import DatabaseConfig
from chatsbom.core.repository import QueryRepository

if TYPE_CHECKING:
    from clickhouse_connect.driver.client import Client

#: The MCP server the database tools are on, which the CLI names each
#: of them after: `mcp__clickhouse__run_select_query`.
SERVER = 'clickhouse'

#: The tools, by the names the CLI gives them.
DATABASE_TOOLS = tuple(
    f'mcp__{SERVER}__{name}' for name in ('run_select_query', 'list_tables')
)

#: How much of a result the model is given. It reads every row it is
#: given, and pays for each in context. In characters as well as rows,
#: since one row can hold a page of text; and past 50,000 characters
#: the CLI cuts a tool's result short itself, without saying so (the
#: SDK's changelog, 0.1.55).
MAX_ROWS = 200
MAX_CHARS = 40_000

#: What the CLI is left of this process's environment, by name, in any
#: case: what a program needs to run, on Linux, macOS and Windows; a
#: proxy, and the certificates it is trusted by; and the CLI's settings
#: that have no prefix of their own.
CLI_ENVIRONMENT = frozenset({
    # Where programs, home and temporary files are; who runs it, and in
    # what shell and terminal.
    'PATH', 'HOME', 'PWD', 'TMPDIR', 'TMP', 'TEMP',
    'USER', 'LOGNAME', 'SHELL', 'TERM', 'NO_COLOR', 'FORCE_COLOR',
    # The same, as Windows names them.
    'SYSTEMROOT', 'SYSTEMDRIVE', 'WINDIR', 'COMSPEC', 'PATHEXT',
    'USERPROFILE', 'HOMEDRIVE', 'HOMEPATH', 'APPDATA', 'LOCALAPPDATA',
    'USERNAME',
    # The locale, and the time zone.
    'LANG', 'LANGUAGE', 'TZ',
    # A proxy, and what to trust it by.
    'HTTP_PROXY', 'HTTPS_PROXY', 'ALL_PROXY', 'NO_PROXY',
    'NODE_EXTRA_CA_CERTS', 'SSL_CERT_FILE', 'SSL_CERT_DIR',
    # The CLI's own.
    'MAX_THINKING_TOKENS', 'MAX_MCP_OUTPUT_TOKENS', 'MCP_TIMEOUT',
    'MCP_TOOL_TIMEOUT',
})

#: And by prefix: the API's key and settings, the CLI's own, what it is
#: told to turn off (`DISABLE_TELEMETRY`), and the locale's parts.
CLI_ENVIRONMENT_PREFIXES = ('ANTHROPIC_', 'CLAUDE_', 'DISABLE_', 'LC_')


def for_the_cli(name: str) -> bool:
    """Whether the CLI is started with the variable `name`, as this
    process has it."""
    upper = name.upper()
    return (
        upper in CLI_ENVIRONMENT
        or upper.startswith(CLI_ENVIRONMENT_PREFIXES)
    )


def cli_env(environ: Mapping[str, str]) -> dict[str, str]:
    """`env` for the CLI to be started with `environ`'s variables that
    are `for_the_cli`, and with no value of any other.

    The SDK starts the CLI with this process's environment and `env`
    over it, and has no option to leave a variable out: short of writing
    its subprocess transport again, an entry here is how a variable is
    changed, and it cannot remove one. So each other variable is given
    here as empty: the CLI is left its name, and no value. Those it
    keeps are not given here at all, but inherited, so that what the
    SDK sets over them, `CLAUDE_CODE_ENTRYPOINT` for one, is still the
    SDK's. `CLAUDECODE`, which the SDK already leaves out of what the
    CLI inherits, is left out here too: named, it would be put back.

    The SDK's version check, a `claude -v` it runs before the CLI, is
    started with the whole environment all the same: no option reaches
    it.
    """
    return {
        name: '' for name in environ
        if not for_the_cli(name) and name != 'CLAUDECODE'
    }


class Database:
    """The database, as the tools read it: one query at a time.

    clickhouse-connect sends every query in its client's session, and
    ClickHouse refuses a second one in a session while one runs.
    """

    def __init__(self, repository: QueryRepository) -> None:
        self.repository = repository
        self._lock = threading.Lock()

    def select(self, sql: str) -> dict[str, Any]:
        """`sql`'s columns, and its rows up to the caps."""
        with self._lock:
            client = self.repository.client
            with client.query_row_block_stream(
                sql, settings=read_only(client),
            ) as stream:
                rows, truncated = capped(
                    row for block in stream for row in block
                )
                columns = list(stream.source.column_names)
        answer: dict[str, Any] = {
            'columns': columns, 'rows': rows, 'truncated': truncated,
        }
        if truncated:
            answer['note'] = (
                f'Only the first {len(rows)} rows are here: the result has '
                'more. Aggregate, or add a LIMIT, for an answer that fits.'
            )
        return answer

    def tables(self) -> dict[str, Any]:
        """Each table's engine, and its columns' names, types and
        comments."""
        database = self.repository.config.database
        parameters = {'database': database}
        with self._lock:
            client = self.repository.client
            engines: dict[str, str] = {
                name: engine for name, engine in client.query(
                    'SELECT name, engine FROM system.tables '
                    'WHERE database = {database:String}',
                    parameters=parameters,
                ).result_rows
            }
            # A materialized view's own storage, `.inner_id.<uuid>`, is
            # its view's business: the view is the one to query.
            columns = client.query(
                'SELECT table, name, type, comment FROM system.columns '
                'WHERE database = {database:String} '
                "AND NOT startsWith(table, '.inner') "
                'ORDER BY table, position',
                parameters=parameters,
            ).result_rows
        tables: dict[str, dict[str, Any]] = {}
        for table, name, type_, comment in columns:
            if table not in tables:
                tables[table] = {
                    'name': table,
                    'engine': engines.get(table, ''),
                    'columns': [],
                }
            tables[table]['columns'].append(
                {'name': name, 'type': type_, 'comment': comment},
            )
        return {'database': database, 'tables': list(tables.values())}


def read_only(client: 'Client') -> dict[str, str]:
    """The setting that keeps a query from writing, unless the user's
    profile keeps it from writing already.

    The guest's does: `readonly=1` is what stops a write. It also
    refuses any setting a query asks for, which is why the caps are
    kept here, as rows arrive, and not asked of the server as
    `max_result_rows`. A user whose profile may write, given with `chat
    --user`, is made read-only query by query, as mcp-clickhouse did.
    """
    setting = client.server_settings.get('readonly')
    if setting is not None and setting.value != '0':
        return {}
    return {'readonly': '1'}


def capped(rows: Iterable[Sequence[Any]]) -> tuple[list[list[Any]], bool]:
    """The first rows, up to MAX_ROWS and MAX_CHARS of JSON, and whether
    there were more.

    Stopping here does not stop the query: clickhouse-connect reads the
    rest of the answer when the stream closes, to use the connection
    again. The guest's profile bounds that as it bounds the query, by
    `max_result_rows`, `max_result_bytes` and `max_execution_time`.
    """
    kept: list[list[Any]] = []
    size = 0
    for row in rows:
        # And the comma between one row and the next.
        size += len(to_json(row)) + len(', ')
        if len(kept) == MAX_ROWS or size > MAX_CHARS:
            return kept, True
        kept.append(list(row))
    return kept, False


def to_json(value: object) -> str:
    """JSON, with what it has no type for (a date, a UUID, a decimal)
    as text."""
    return json.dumps(value, default=str, ensure_ascii=False)


async def answer(
    read: Callable[..., dict[str, Any]], *args: Any,
) -> dict[str, Any]:
    """A tool's result: what `read` found, or why it found nothing.

    In a thread: clickhouse-connect blocks, and this runs on the TUI's
    event loop, which would stand still until the query ended. A query
    still running when the TUI quits holds its exit up until it ends,
    as the guest's `max_execution_time` bounds it.
    """
    try:
        found = await asyncio.to_thread(read, *args)
    except Exception as e:
        # Whatever stopped it, the model is told, and can try again;
        # raised, the call ended with nothing it could act on. In the
        # text as well as the flag: the SDK before 0.1.51 drops
        # `is_error` on its way to the CLI.
        return {
            'content': [{'type': 'text', 'text': to_json({'error': str(e)})}],
            'is_error': True,
        }
    return {'content': [{'type': 'text', 'text': to_json(found)}]}


def database_tools(database: Database) -> list[SdkMcpTool[Any]]:
    """The tools the model has, over `database`."""

    @tool(
        'run_select_query',
        'Run one read-only SQL query, in ClickHouse\'s dialect, on the SBOM '
        'database. Answers with JSON: {"columns": [...], "rows": [[...]], '
        f'"truncated": false}}. At most {MAX_ROWS} rows, and fewer when they '
        'are long: a longer result is cut short, with "truncated": true and '
        'a note. Aggregate, or add a LIMIT, rather than read every row.',
        {'query': str},
    )
    async def run_select_query(args: dict[str, Any]) -> dict[str, Any]:
        return await answer(database.select, str(args.get('query', '')))

    @tool(
        'list_tables',
        'List the tables of the SBOM database, each with its engine and '
        'its columns: their names, types and comments.',
        {},
    )
    async def list_tables(args: dict[str, Any]) -> dict[str, Any]:
        return await answer(database.tables)

    return [run_select_query, list_tables]


async def allow_database_tools_only(
    tool_name: str,
    tool_input: dict[str, Any],
    context: ToolPermissionContext,
) -> PermissionResult:
    """What the CLI is told when it asks whether a tool may be used.

    `allowed_tools` lets the database tools through without a question,
    so it asks about any other; there should be none, with no built-in
    tool and no other MCP server, but the answer is no regardless.
    """
    if tool_name in DATABASE_TOOLS:
        return PermissionResultAllow()
    return PermissionResultDeny(
        message=(
            f'{tool_name} is not available: this chat can only query the '
            'SBOM database, with list_tables and run_select_query.'
        ),
    )


def build_options(
    db_config: DatabaseConfig,
    on_stderr: Callable[[str], None],
) -> ClaudeAgentOptions:
    """How the agent is started: with the database tools, and nothing
    else.

    Nothing connects, and nothing is started, until the client is.
    """
    server = create_sdk_mcp_server(
        SERVER, tools=database_tools(Database(QueryRepository(db_config))),
    )
    # What the CLI needs of this process's environment, and no value of
    # the rest: the ClickHouse passwords are for this process, which
    # runs the tools, and the CLI has no use for any other secret.
    env = cli_env(os.environ)
    # The SDK warns, as the client connects, that `can_use_tool` is not
    # asked about the tools `allowed_tools` names whole: the database
    # tools, which are allowed without a question by design. The callback
    # is there for every other tool, to say no. That warning's category
    # alone is silenced, for the process, which connects no other client.
    warnings.filterwarnings('ignore', category=CanUseToolShadowedWarning)
    return ClaudeAgentOptions(
        # No built-in tool: an empty list is none (`--tools ""`), where
        # unset is every one.
        tools=[],
        # The database tools, without a question each time; and for
        # anything else a question, which is answered no.
        allowed_tools=list(DATABASE_TOOLS),
        permission_mode='default',
        can_use_tool=allow_database_tools_only,
        # None of the user's or the project's settings, or MCP servers:
        # their permission rules and hooks are for their own sessions.
        # An empty list, not unset: from 0.1.53 the SDK passes nothing
        # for None, and the CLI then reads every settings file it finds.
        setting_sources=[],
        extra_args={'strict-mcp-config': None},
        mcp_servers={SERVER: server},
        system_prompt=SYSTEM_PROMPT,
        env=env,
        # The SDK reads the CLI's stderr only for a callback: without
        # one, it went to the terminal, under the TUI.
        stderr=on_stderr,
    )
