import typer

from . import prune

app = typer.Typer(
    help='Housekeeping for the store.',
    no_args_is_help=True,
)

app.add_typer(prune.app, name='prune')
