import typer

from chatsbom.commands import chat
from chatsbom.commands import db
from chatsbom.commands import sbom
from chatsbom.commands.github import app as github_app

app = typer.Typer(
    help='ChatSBOM: Talk to your Supply Chain. Chat with SBOMs.',
    no_args_is_help=True,
    pretty_exceptions_show_locals=False,
)

app.add_typer(github_app, name='github')
app.add_typer(sbom.app, name='sbom')
app.add_typer(db.app, name='db')
app.add_typer(chat.app, name='chat')

if __name__ == '__main__':
    app()
