"""`chatsbom github tree`: the tree stage, on its own.

The same stage as `chatsbom run --stage tree`: what is due comes from
the ledger, for every tracked repository whatever its language, and the
file list is written to

    data/05-github-tree/<repository_id>/<sha>/tree.txt

which is what the content stage discovers manifests from
(`core/discovery.py`).
"""
from __future__ import annotations

from pathlib import Path

import structlog
import typer

from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.core.fs import is_whole_tree
from chatsbom.core.github import check_github_token
from chatsbom.core.github import verify_github_token
from chatsbom.core.ledger import Stage
from chatsbom.core.logging import console

logger = structlog.get_logger('tree_command')
app = typer.Typer()

#: Kept under its old name for callers of the stage-major command.
_is_whole_tree = is_whole_tree


@app.callback(invoke_without_command=True)
@handle_errors
def main(
    token: str = typer.Option(
        None, envvar='GITHUB_TOKEN', help='GitHub Token',
    ),
    limit: int = typer.Option(
        50, '--limit', help='Repositories to advance in this pass',
    ),
    quota: int = typer.Option(
        500, help='Maximum API requests to spend before stopping',
    ),
    force: bool = typer.Option(
        False, help='List the files again even where a whole tree is stored',
    ),
    repos_file: Path | None = typer.Option(
        None,
        '--repos-file',
        help='Only these repositories: one owner/repo (or id) per line',
        exists=True, dir_okay=False, readable=True,
    ),
):
    """
    Fetch file trees for repositories (without downloading content).

    The same as `chatsbom run --stage tree`: claims the repositories the
    tree stage is due for from the queue, and walks the release and
    commit stages before it for the commit, from their caches.

    Reads from: data/ledger.sqlite3
    Writes to:  data/05-github-tree/{repository_id}/{sha}/tree.txt
    """
    # Imported here: `run` builds on the stage commands' modules.
    from chatsbom.commands.run import advance
    from chatsbom.commands.run import report
    from chatsbom.commands.run import resolve_repos

    check_github_token(token)
    verify_github_token(token, console=console)
    container = get_container()
    repos = resolve_repos(container.config.paths.ledger_path, repos_file)
    result = advance(
        container, token, limit=limit, quota=quota, stage=Stage.TREE,
        repos=repos, force_tree=force,
    )
    report(result, quota)
