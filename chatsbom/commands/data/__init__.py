import typer

from . import prune

app = typer.Typer(
    help='Housekeeping for the on-disk pipeline stages.',
    no_args_is_help=True,
)

app.add_typer(prune.app, name='prune')
