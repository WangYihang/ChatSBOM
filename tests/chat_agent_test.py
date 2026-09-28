"""What `chat`'s agent may do: query the database, and nothing else (#29).

It could do far more. Its options named eight built-in tools it could
not use and switched permission prompts off (`bypassPermissions`), so
WebFetch, WebSearch, Task, Skill and every tool added since were there
to be used, unasked. What it reads holds text anyone can set on GitHub,
a repository's description or a package's name, and instructions in
it could have it fetch a URL carrying what a query had found. The
database was reached through `uvx mcp-clickhouse`: whatever version PyPI
served, outside the lock, and given the ClickHouse password in its
environment.

The options are built by a function of their own, tested here without
starting the CLI. What the installed SDK makes of them is read back as
well, since that has changed under them before.
"""
import asyncio
import dataclasses
import json
import os
import tomllib
from collections.abc import Callable
from pathlib import Path
from typing import Any

import pytest
from claude_agent_sdk import ClaudeAgentOptions
from claude_agent_sdk import PermissionResultAllow
from claude_agent_sdk import PermissionResultDeny
from claude_agent_sdk import ToolPermissionContext
from claude_agent_sdk._internal.transport.subprocess_cli import (
    SubprocessCLITransport,
)
from mcp.types import ListToolsRequest
from packaging.requirements import Requirement

from chatsbom.commands.chat import SYSTEM_PROMPT
from chatsbom.commands.chat_agent import build_options
from chatsbom.commands.chat_agent import DATABASE_TOOLS
from chatsbom.commands.chat_agent import SERVER
from chatsbom.core.config import DatabaseConfig

ROOT = Path(__file__).resolve().parent.parent

#: Tools the model must not have, built in or not: the ones it could
#: use unasked before, the ones the deny-list did name, and one another
#: MCP server might offer.
REFUSED = [
    'WebFetch', 'WebSearch', 'Task', 'Agent', 'Skill', 'TodoWrite',
    'NotebookEdit', 'Bash', 'Read', 'Write', 'Edit', 'Glob', 'Grep',
    'mcp__github__create_issue', f'mcp__{SERVER}__drop_table',
]

#: Passwords, one from each place the chat may be given one.
SECRETS = {
    'CLICKHOUSE_GUEST_PASSWORD': 'guest-secret-from-env',
    'CLICKHOUSE_ADMIN_PASSWORD': 'admin-secret-from-env',
}
FROM_THE_OPTION = 'secret-from-the-option'


def ignore(line: str) -> None:
    """Where the CLI's stderr goes, when a test does not read it."""


@pytest.fixture
def options(monkeypatch: pytest.MonkeyPatch) -> ClaudeAgentOptions:
    """The agent's options, with a password everywhere one may be."""
    for name, value in SECRETS.items():
        monkeypatch.setenv(name, value)
    config = DatabaseConfig(
        host='clickhouse.test', port=18123, user='guest',
        password=FROM_THE_OPTION, database='chatsbom',
    )
    return build_options(config, ignore)


def decide(options: ClaudeAgentOptions, tool: str) -> object:
    """What the options' permission callback says to `tool`."""
    ask = options.can_use_tool
    assert ask is not None, 'nothing is ever asked'

    # The SDK types the callback's result as any awaitable, and
    # `asyncio.run` takes a coroutine.
    async def answer() -> object:
        return await ask(tool, {}, ToolPermissionContext())

    return asyncio.run(answer())


def cli_arguments(options: ClaudeAgentOptions) -> list[str]:
    """The command line the installed SDK starts the CLI with.

    The SDK's own, and private: what an option becomes on it is the
    SDK's business, and it has changed. 0.1.53 to 0.1.59 dropped an
    empty `setting_sources`, and the CLI read every settings file it
    found. So it is read back here, not taken on trust.
    """
    transport = SubprocessCLITransport(
        prompt='', options=dataclasses.replace(options, cli_path='claude'),
    )
    return transport._build_command()


def flag(arguments: list[str], name: str) -> str | None:
    """What `name` is given, as `--name value` or `--name=value`."""
    for index, argument in enumerate(arguments):
        if argument == name:
            return arguments[index + 1]
        if argument.startswith(f'{name}='):
            return argument.partition('=')[2]
    return None


# --- tools ------------------------------------------------------------------

def test_no_built_in_tool_is_there_to_use(options):
    """An empty list, which is none; unset is every one."""
    assert options.tools == []


def test_the_database_tools_are_the_only_ones_allowed(options):
    assert sorted(options.allowed_tools) == sorted(DATABASE_TOOLS)


def test_the_database_tools_are_the_servers_own(options):
    """By the names the CLI gives them: allowing a name no tool has
    would leave the model nothing to query with."""
    server = options.mcp_servers[SERVER]
    handler = server['instance'].request_handlers[ListToolsRequest]
    listed = asyncio.run(handler(ListToolsRequest(method='tools/list')))

    names = {f'mcp__{SERVER}__{tool.name}' for tool in listed.root.tools}
    assert names == set(DATABASE_TOOLS)


def test_the_database_server_runs_in_process(options):
    """No process to start, so nothing to fetch from PyPI and nothing
    outside the lock: the SDK's own server, in this one."""
    assert set(options.mcp_servers) == {SERVER}
    for server in options.mcp_servers.values():
        assert server['type'] == 'sdk'
        assert 'command' not in server


# --- permissions ------------------------------------------------------------

def test_permissions_are_asked_for_not_bypassed(options):
    """In the default mode a tool nothing allows is asked about, and the
    callback below is what answers."""
    assert options.permission_mode == 'default'


@pytest.mark.parametrize('tool', REFUSED)
def test_every_other_tool_is_refused_with_a_reason(options, tool):
    answer = decide(options, tool)

    assert isinstance(answer, PermissionResultDeny)
    assert answer.message


@pytest.mark.parametrize('tool', DATABASE_TOOLS)
def test_the_database_tools_are_allowed_when_asked_about(options, tool):
    assert isinstance(decide(options, tool), PermissionResultAllow)


def test_no_settings_of_the_users_or_the_projects(options):
    """Their permission rules, hooks and MCP servers are theirs, for
    their own sessions: none of them is this chat's."""
    assert options.setting_sources == []
    assert 'strict-mcp-config' in options.extra_args


# --- passwords --------------------------------------------------------------

def test_no_password_reaches_a_child_process(options):
    """The CLI is started with this process's environment, and `env`
    over it (the SDK merges them so), and it starts no other."""
    secrets = [*SECRETS.values(), FROM_THE_OPTION]
    child = {**os.environ, **options.env}

    for secret in secrets:
        assert not [name for name, value in child.items() if secret in value]
    for server in options.mcp_servers.values():
        assert 'env' not in server


# --- the CLI's command line -------------------------------------------------

def test_the_cli_is_started_with_the_database_tools_alone(options):
    arguments = cli_arguments(options)

    assert flag(arguments, '--tools') == ''
    allowed = flag(arguments, '--allowedTools') or ''
    assert set(allowed.split(',')) == set(DATABASE_TOOLS)
    assert flag(arguments, '--permission-mode') == 'default'
    assert flag(arguments, '--setting-sources') == ''
    assert '--strict-mcp-config' in arguments
    assert '--dangerously-skip-permissions' not in arguments
    servers = flag(arguments, '--mcp-config')
    assert servers is not None
    assert json.loads(servers) == {
        'mcpServers': {SERVER: {'type': 'sdk', 'name': SERVER}},
    }
    for secret in [*SECRETS.values(), FROM_THE_OPTION]:
        assert not [argument for argument in arguments if secret in argument]


# --- the rest ---------------------------------------------------------------

def test_what_the_cli_says_on_stderr_is_handed_on():
    """The init failure shows it. `debug_stderr` was a temporary file
    the SDK never wrote to: it pipes stderr only for a callback."""
    said: list[str] = []
    collect: Callable[[str], None] = said.append

    options = build_options(DatabaseConfig(), collect)

    assert options.stderr is collect


def test_the_system_prompt_names_the_tools_the_model_has():
    for tool in DATABASE_TOOLS:
        assert tool.rpartition('__')[2] in SYSTEM_PROMPT
    assert 'mcp-clickhouse' not in SYSTEM_PROMPT


def chat_sdk_requirement() -> Any:
    project = tomllib.loads((ROOT / 'pyproject.toml').read_text('utf-8'))
    for spec in project['project']['optional-dependencies']['chat']:
        requirement = Requirement(spec)
        if requirement.name == 'claude-agent-sdk':
            return requirement.specifier
    raise AssertionError('the chat extra has no claude-agent-sdk')


@pytest.mark.parametrize(
    'version', [f'0.1.{patch}' for patch in range(53, 60)],
)
def test_the_chat_extra_admits_no_sdk_that_drops_empty_setting_sources(
    version,
):
    """From 0.1.53 to 0.1.59 `setting_sources=[]` was dropped as falsy,
    and the CLI read the user's and the project's settings (fixed in
    0.1.60)."""
    assert version not in chat_sdk_requirement()


def test_the_chat_extra_asks_for_an_sdk_with_the_tools_option():
    """`tools`, the newest option the chat sets, came in 0.1.12; the
    lock has 0.1.44, and 0.1.60 fixed the empty `setting_sources`."""
    specifier = chat_sdk_requirement()

    assert '0.1.11' not in specifier
    for version in ('0.1.12', '0.1.44', '0.1.60'):
        assert version in specifier
