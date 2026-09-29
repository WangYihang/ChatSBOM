import typer

from . import build

app = typer.Typer(
    help='The DuckDB warehouse, rebuilt from the store beside ClickHouse',
)

app.add_typer(build.app, name='build')
