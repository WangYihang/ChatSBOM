import typer

from . import lock

app = typer.Typer(help='SBOM operations')

app.add_typer(lock.app, name='lock')
