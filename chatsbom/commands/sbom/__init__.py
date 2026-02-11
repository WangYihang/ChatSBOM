import typer

from . import generate

app = typer.Typer(help='SBOM operations')

app.add_typer(generate.app, name='generate')
