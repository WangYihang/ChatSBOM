import typer

from . import export
from . import index
from . import query
from . import status

app = typer.Typer(help='Database operations')

app.add_typer(index.app, name='index')
app.add_typer(status.app, name='status')
app.add_typer(query.app, name='query')
app.add_typer(export.app, name='export')
