"""`chatsbom collect`: the collector, one long-running process (#171),
and `chatsbom collect repo`, one repository's stages by hand (#161)."""
import typer

from chatsbom.core.decorators import handle_errors

from . import process
from . import repo

# Its help is the process's, below: `collect repo` is one repository.
app = typer.Typer()

app.add_typer(repo.app, name='repo')


@app.callback(invoke_without_command=True)
@handle_errors
def main(ctx: typer.Context) -> None:
    """
    Run the collector until SIGTERM or SIGINT.

    One process for all of it (#128 section 2.1): the universe, searched
    weekly; the hourly sweep of it, which says what changed; the stages
    of each repository with one due, several at once; the dependency
    graph, on a clock; and the index pass, the warehouse, the snapshot,
    the weekly export and retention, once a day at most and only after
    something was collected. It holds data/collector.sqlite, and writes
    data/collector.heartbeat, which `python -m chatsbom.collector.health`
    checks. Configured by GITHUB_TOKEN and CHATSBOM_GITHUB_TOKENS, and
    the CHATSBOM_ settings .env.example describes.
    """
    if ctx.invoked_subcommand is None:
        process.collect()
