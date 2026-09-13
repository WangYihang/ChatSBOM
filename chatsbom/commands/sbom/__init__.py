import typer

from . import generate
from . import lock

app = typer.Typer(help='SBOM operations')

app.add_typer(generate.app, name='generate')
app.add_typer(lock.app, name='lock')
