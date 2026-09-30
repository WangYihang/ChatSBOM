import typer

from . import repo

app = typer.Typer(
    help='The collector: one repository by hand now; the process in 6e '
    '(#155).',
    no_args_is_help=True,
)

app.add_typer(repo.app, name='repo')
