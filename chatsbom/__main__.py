import typer

from chatsbom.__version__ import __version__
from chatsbom.commands import chat
from chatsbom.commands import collect
from chatsbom.commands import convert
from chatsbom.commands import download
from chatsbom.commands import enrich
from chatsbom.commands import index
from chatsbom.commands import query
from chatsbom.commands import status


def version_callback(value: bool):
    """Print version and exit."""
    if value:
        typer.echo(f"ChatSBOM version {__version__}")
        raise typer.Exit()


app = typer.Typer(
    help='ChatSBOM - Talk to your Supply Chain. Chat with SBOMs.',
    no_args_is_help=True,
    add_completion=False,
)


@app.callback()
def main(
    version: bool = typer.Option(
        None,
        '--version',
        '-v',
        help='Show version and exit',
        callback=version_callback,
        is_eager=True,
    ),
):
    """ChatSBOM - Talk to your Supply Chain. Chat with SBOMs."""


app.command(name='collect')(collect.main)
app.command(name='enrich')(enrich.main)
app.command(name='download')(download.main)
app.command(name='convert')(convert.main)
app.command(name='index')(index.main)
app.command(name='status')(status.main)
app.command(name='chat')(chat.main)
app.command(name='query')(query.main)

if __name__ == '__main__':
    app()
