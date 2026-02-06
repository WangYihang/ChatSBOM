import typer

from chatsbom.commands import chat
from chatsbom.commands import collect
from chatsbom.commands import convert
from chatsbom.commands import download
from chatsbom.commands import index
from chatsbom.commands import query
from chatsbom.commands import status

app = typer.Typer(
    help='ChatSBOM - Talk to your Supply Chain. Chat with SBOMs.',
    no_args_is_help=True,
    add_completion=False,
)

# Commands ordered by typical workflow
app.command(name='collect')(collect.main)
app.command(name='download')(download.main)
app.command(name='convert')(convert.main)
app.command(name='index')(index.main)
app.command(name='status')(status.main)
app.command(name='chat')(chat.main)
app.command(name='query')(query.main)

if __name__ == '__main__':
    app()
