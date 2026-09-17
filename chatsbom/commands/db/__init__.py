import typer

from . import edges
from . import export
from . import index
from . import query
from . import raw
from . import status

app = typer.Typer(help='Database operations')

app.add_typer(index.app, name='index')
app.add_typer(status.app, name='status')
app.add_typer(query.app, name='query')
app.add_typer(export.app, name='export')
app.add_typer(edges.app, name='edges')
app.add_typer(raw.app, name='raw')
