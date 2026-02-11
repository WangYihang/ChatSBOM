import typer

from . import index
from . import query
from . import status

app = typer.Typer(help='Database operations')

app.add_typer(index.app, name='index')
app.add_typer(status.app, name='status')
app.add_typer(query.app, name='query')
