import typer

from chatsbom.commands import chat
from chatsbom.commands import data
from chatsbom.commands import db
from chatsbom.commands import export
from chatsbom.commands import github
from chatsbom.commands import openapi
from chatsbom.commands import queue
from chatsbom.commands import run
from chatsbom.commands import sbom
from chatsbom.commands import web
from chatsbom.core import config
from chatsbom.core.logging import setup_logging

app = typer.Typer(
    help='ChatSBOM: Talk to your Supply Chain. Chat with SBOMs.',
    no_args_is_help=True,
    pretty_exceptions_show_locals=False,
)

app.add_typer(github.app, name='github')
app.add_typer(sbom.app, name='sbom')
app.add_typer(db.app, name='db')
app.add_typer(data.app, name='data')
app.add_typer(export.app, name='export')
app.add_typer(openapi.app, name='openapi')
app.add_typer(queue.app, name='queue')
app.add_typer(run.app, name='run')
app.add_typer(chat.app, name='chat')
app.add_typer(web.app, name='web')


@app.callback()
def main(
    debug: bool = typer.Option(False, '--debug', help='Enable debug logging'),
):
    """
    ChatSBOM CLI - Talk to your Supply Chain.
    """
    # First, before anything reads the environment: logging reads
    # CHATSBOM_LOG_FORMAT and ENV just below, and each subcommand's
    # options — `--token` from GITHUB_TOKEN among them — are resolved
    # after this returns.
    config.load_env_file()
    level = 'DEBUG' if debug else 'INFO'
    setup_logging(level=level)


if __name__ == '__main__':
    app()
