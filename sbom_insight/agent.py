import asyncio
import os
from dataclasses import dataclass
from dataclasses import field

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
from prompt_toolkit import PromptSession
from prompt_toolkit.patch_stdout import patch_stdout
from rich.console import Console
from rich.markdown import Markdown
from rich.panel import Panel

from sbom_insight.schema import ALL_DDL

dotenv.load_dotenv()
console = Console()


@dataclass
class ClickHouseConfig:
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
        default_factory=lambda: os.getenv(
            'CLICKHOUSE_CONNECT_TIMEOUT', '30',
        ),
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


async def _main_async(
    timeout: int,
    db_config: ClickHouseConfig,
):
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
                args=[
                    'mcp-clickhouse',
                ],
                env=db_config.to_env(),
            ),
        },
        system_prompt=(
            'You are an expert for querying the SBOM database. '
            'You can ONLY use the mcp-clickhouse tool to query the database. '
            'Do NOT attempt to read files, write files, or execute bash commands. '
            'The database schema is as follows:\n\n'
            '```sql\n'
            f'{ALL_DDL}\n'
            '```\n\n'
            'Always use the mcp-clickhouse tool to query data. '
            'For large exports, format your answer and tell the user how many results there are.'
        ),
    )

    session: PromptSession = PromptSession()

    console.print(
        Panel(
            '\n'.join([
                'Welcome to SBOM Insight Agent REPL.',
                "Type 'exit' or 'quit' to leave.",
                '',
                'Example Queries:',
                '- Show me the top 10 most popular projects using the gin framework.',
                '- How many artifacts are there in the database?',
                '- List the top 5 most used libraries in Python repositories.',
                '- Find all repositories that depend on "log4j".',
            ]),
            title='SBOM Insight Agent',
            border_style='bold green',
        ),
    )

    async with ClaudeSDKClient(options=options) as client:
        while True:
            try:
                with patch_stdout():
                    user_input = await session.prompt_async('>>> ')

                if user_input.strip().lower() in ('exit', 'quit'):
                    break

                if not user_input.strip():
                    continue

                await client.query(user_input)

                async for message in client.receive_response():
                    if isinstance(message, AssistantMessage):
                        for block in message.content:
                            if isinstance(block, TextBlock):
                                console.print(Markdown(block.text))
                            elif isinstance(block, ThinkingBlock):
                                console.print(
                                    Panel(
                                        block.thinking, title='Thinking',
                                        border_style='dim',
                                    ),
                                )
                            elif isinstance(block, ToolUseBlock):
                                console.print(
                                    Panel(
                                        f"Tool: {block.name}\nInput: {block.input}", title='Tool Use', border_style='cyan',
                                    ),
                                )
                            elif isinstance(block, ToolResultBlock):
                                style = 'red' if block.is_error else 'green'
                                console.print(
                                    Panel(
                                        f"{block.content}", title='Tool Result', border_style=style,
                                    ),
                                )
                    elif isinstance(message, UserMessage):
                        if isinstance(message.content, str):
                            console.print(
                                Panel(
                                    message.content, title='User Message', border_style='blue',
                                ),
                            )
                        else:
                            # content is list[ContentBlock]
                            for block in message.content:
                                if isinstance(block, TextBlock):
                                    console.print(
                                        Panel(
                                            block.text, title='User Message', border_style='blue',
                                        ),
                                    )
                                elif isinstance(block, ToolResultBlock):
                                    style = 'red' if block.is_error else 'green'
                                    console.print(
                                        Panel(
                                            f"{block.content}", title='Tool Result', border_style=style,
                                        ),
                                    )
                    elif isinstance(message, SystemMessage):
                        console.print(
                            Panel(
                                f"Subtype: {message.subtype}\nData: {message.data}",
                                title='System Message', border_style='yellow',
                            ),
                        )
                    elif isinstance(message, ResultMessage):
                        result_info = [
                            f"Subtype: {message.subtype}",
                            f"Duration: {message.duration_ms}ms (API: {message.duration_api_ms}ms)",
                            f"Turns: {message.num_turns}",
                            f"Is Error: {message.is_error}",
                        ]
                        if message.total_cost_usd is not None:
                            result_info.append(
                                f"Cost: ${message.total_cost_usd:.4f}",
                            )
                        if message.result:
                            result_info.append(f"Result: {message.result}")
                        console.print(
                            Panel(
                                '\n'.join(result_info),
                                title='Result', border_style='magenta',
                            ),
                        )
                    elif isinstance(message, StreamEvent):
                        console.print(
                            Panel(
                                f"Event: {message.event}",
                                title='Stream Event', border_style='dim cyan',
                            ),
                        )

            except KeyboardInterrupt:
                continue
            except EOFError:
                break
            except Exception as e:
                typer.echo(f"Error: {e}")


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
    Run the AI agent with access to ClickHouse in a REPL loop.
    """
    db_config = ClickHouseConfig(
        host=host,
        port=port,
        user=user,
        password=password,
    )

    asyncio.run(
        _main_async(
            timeout,
            db_config,
        ),
    )
