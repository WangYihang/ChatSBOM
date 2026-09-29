import typer

from . import build

app = typer.Typer(
    help='The serving snapshot: read-only SQLite, published from the '
    'warehouse',
)

app.add_typer(build.app, name='build')
