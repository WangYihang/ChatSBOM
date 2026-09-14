import typer

from chatsbom.commands.export import d1
from chatsbom.commands.export import parquet
from chatsbom.commands.export import schema

app = typer.Typer(
    help='Export the dataset as portable, self-describing artefacts.',
    no_args_is_help=True,
)

app.add_typer(d1.app, name='d1')
app.add_typer(parquet.app, name='parquet')
app.add_typer(schema.app, name='schema')
