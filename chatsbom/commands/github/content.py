"""`chatsbom github content`: the content stage, on its own.

The same stage as `chatsbom run --stage content`. For every tracked
repository whatever its language, the manifests and lockfiles are
chosen from its stored tree at any depth and of every ecosystem
(`core/discovery.py`), capped at 200 files and 64 MiB, and each is
stored at its own path:

    data/06-github-content/<repository_id>/<sha>/<path in the repository>

What was selected, fetched and left out, and why, is written beside the
tree as `manifests.json`.
"""
from __future__ import annotations

from pathlib import Path

import structlog
import typer

from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.core.github import check_github_token
from chatsbom.core.github import verify_github_token
from chatsbom.core.ledger import Stage
from chatsbom.core.logging import console

logger = structlog.get_logger('content_command')
app = typer.Typer()


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
        False, help='Download again even the files already stored',
    ),
    repos_file: Path | None = typer.Option(
        None,
        '--repos-file',
        help='Only these repositories: one owner/repo (or id) per line',
        exists=True, dir_okay=False, readable=True,
    ),
):
    """
    Download every manifest and lockfile a repository's tree lists.

    The same as `chatsbom run --stage content`: claims the repositories
    the content stage is due for from the queue, whatever their
    language, and walks the stages before it for the commit and the
    tree, from their caches.

    Reads from: data/ledger.sqlite3, data/05-github-tree
    Writes to:  data/06-github-content/{repository_id}/{sha}/,
                data/05-github-tree/{repository_id}/{sha}/manifests.json
    """
    from chatsbom.commands.run import advance
    from chatsbom.commands.run import report
    from chatsbom.commands.run import resolve_repos

    token = check_github_token(token)
    verify_github_token(token, console=console)
    container = get_container()
    repos = resolve_repos(container.config.paths.ledger_path, repos_file)
    result = advance(
        container, token, limit=limit, quota=quota, stage=Stage.CONTENT,
        repos=repos, force_content=force,
    )
    report(result, quota)
