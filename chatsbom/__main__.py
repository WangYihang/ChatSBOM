import typer

from chatsbom.commands import chat
from chatsbom.commands import db
from chatsbom.commands import framework
from chatsbom.commands import sbom
from chatsbom.commands.github import app as github_app
from chatsbom.core.logging import setup_logging

app = typer.Typer(
    help='ChatSBOM: Talk to your Supply Chain. Chat with SBOMs.',
    no_args_is_help=True,
    pretty_exceptions_show_locals=False,
)

app.add_typer(github_app, name='github')
app.add_typer(sbom.app, name='sbom')
app.add_typer(db.app, name='db')
app.add_typer(framework.app, name='framework')
app.add_typer(chat.app, name='chat')


@app.callback()
def main(
    debug: bool = typer.Option(False, '--debug', help='Enable debug logging'),
):
    """
    ChatSBOM CLI - Talk to your Supply Chain.
    """
    level = 'DEBUG' if debug else 'INFO'
    setup_logging(level=level)


if __name__ == '__main__':
    app()
