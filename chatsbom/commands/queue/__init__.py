import typer

from . import backfill
from . import status
from . import sync
from . import track

app = typer.Typer(
    help='Continuous collection: the work queue and its slices.',
    no_args_is_help=True,
)

app.add_typer(track.app, name='track')
app.add_typer(sync.app, name='sync')
app.add_typer(status.app, name='status')
app.add_typer(backfill.app, name='backfill')
