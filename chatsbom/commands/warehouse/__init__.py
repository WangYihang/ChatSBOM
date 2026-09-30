import typer

from . import build

app = typer.Typer(
    help='The DuckDB warehouse, the index, rebuilt from the store',
)

app.add_typer(build.app, name='build')
