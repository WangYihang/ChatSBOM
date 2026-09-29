"""`chatsbom web serve`: the Python web service (#128, section 2.5).

Opt-in: nothing deploys it yet, and the Worker serves the site until
the cutover. What it serves so far: the page, an ALTCHA challenge,
/healthz (#134), and the chat, on DeepSeek, once DEEPSEEK_API_KEY and
WEB_SNAPSHOT are set (#140). `chatsbom/server/` is the service.

A setting it cannot start with stops it before it listens, on stderr,
naming the setting. The Worker could only refuse every request.

FastAPI, uvicorn, ALTCHA and the OpenAI SDK are the `web` extra's,
imported once it is checked for, so that the rest of the CLI starts
without them (#26).
"""
import os
import sqlite3
from pathlib import Path

import structlog
import typer
from rich.markup import escape

from chatsbom.core.diagnostics import fail
from chatsbom.core.extras import require_extra

logger = structlog.get_logger('web')

app = typer.Typer(
    help='The Python web service, opt-in: the Worker still serves the site.',
    no_args_is_help=True,
)
serve_app = typer.Typer()
app.add_typer(serve_app, name='serve')


@serve_app.callback(invoke_without_command=True)
def serve(
    host: str = typer.Option(
        '127.0.0.1',
        help=(
            'The address to listen on. In a container, the one the edge '
            'network reaches it on'
        ),
    ),
    port: int = typer.Option(8080, min=1, max=65535, help='The port'),
    spa: Path = typer.Option(
        Path('web/dist/client'),
        help='The built page, which `npm run build` in web/ writes',
    ),
) -> None:
    """Serve the page, an ALTCHA challenge, the chat and /healthz.

    Configured by the environment and .env: ALTCHA_HMAC_KEY, which it
    needs, and EDGE_SUBNET, WEB_STATE_DIR, CHAT_RATE_LIMIT,
    QUERY_RATE_LIMIT and DAILY_SPEND_CAP_USD; and for the chat,
    DEEPSEEK_API_KEY, WEB_SNAPSHOT and the rest (.env.example). Without
    DEEPSEEK_API_KEY the chat is off, and says so.
    """
    # First: without them, no setting would get it anywhere.
    require_extra('web', 'fastapi', 'uvicorn', 'altcha', 'openai')

    from chatsbom.server.settings import settings_from
    from chatsbom.server.settings import SettingsError

    try:
        settings = settings_from(os.environ, spa=spa)
    except SettingsError as error:
        fail(
            f'[bold red]Error:[/] {escape(str(error))}',
            'web service not configured', logger,
            setting=error.setting, problem=str(error),
        )

    from chatsbom.server.app import create_app
    from chatsbom.server.app import server
    from chatsbom.server.state import StateError

    try:
        service = create_app(settings)
    except (OSError, sqlite3.Error, StateError) as error:
        fail(
            f'[bold red]Error:[/] web.sqlite cannot be kept in '
            f'{escape(str(settings.state_dir))} (WEB_STATE_DIR): '
            f'{escape(str(error))}',
            'web state cannot be kept', logger,
            setting='WEB_STATE_DIR', state_dir=str(settings.state_dir),
            error=str(error),
        )

    running = server(service, host, port)
    running.run()
    # uvicorn returns, rather than exiting, when the service cannot
    # start: a port taken, or its lifespan failing. It has said why.
    if not running.started:
        fail(
            f'[bold red]Error:[/] the web service did not start on '
            f'{escape(host)}:{port}',
            'web service did not start', logger, host=host, port=port,
        )
