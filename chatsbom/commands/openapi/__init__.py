import typer

from . import candidates
from . import clone
from . import drift
from . import list_paths
from . import plot_drift
from . import stats

app = typer.Typer(help='OpenAPI discovery and analysis')

app.add_typer(candidates.app, name='candidates')
app.add_typer(clone.app, name='clone')
app.add_typer(drift.app, name='drift')
app.add_typer(list_paths.app, name='list-paths')
app.add_typer(plot_drift.app, name='plot-drift')
app.add_typer(stats.app, name='stats')
