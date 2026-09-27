import typer

from . import prune
from . import slim

app = typer.Typer(
    help='Housekeeping for the on-disk pipeline stages.',
    no_args_is_help=True,
)

app.add_typer(prune.app, name='prune')
app.add_typer(slim.app, name='slim')
