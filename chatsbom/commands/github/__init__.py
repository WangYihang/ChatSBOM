import typer

from . import commit
from . import content
from . import release
from . import repo
from . import search
from . import tree

app = typer.Typer(name='github', help='GitHub related commands')

app.add_typer(search.app, name='search')
app.add_typer(repo.app, name='repo')
app.add_typer(release.app, name='release')
app.add_typer(commit.app, name='commit')
app.add_typer(content.app, name='content')
app.add_typer(tree.app, name='tree')
