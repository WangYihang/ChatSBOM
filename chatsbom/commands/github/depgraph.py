"""`chatsbom github depgraph`: the dependency-graph stage, on its own.

The same stage as `chatsbom run --stage depgraph`: what is due comes
from the ledger, not from the `07-sbom` lists, so a repository whose
Syft SBOM failed, or that no other stage has reached, still gets its
graph. See `chatsbom/services/depgraph_stage.py`.
"""
from __future__ import annotations

import os
from datetime import datetime
from datetime import timezone

import structlog
import typer

from chatsbom.core.container import Container
from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.core.github import check_github_token
from chatsbom.core.github import depgraph_tokens
from chatsbom.core.github import token_label
from chatsbom.core.github import verify_extra_token
from chatsbom.core.github import verify_github_token
from chatsbom.core.ledger import Ledger
from chatsbom.core.logging import console
from chatsbom.services.dependency_graph_service import closed_reason
from chatsbom.services.dependency_graph_service import DependencyGraphService
from chatsbom.services.dependency_graph_service import DEPGRAPH_ENDPOINT_CLOSES
from chatsbom.services.depgraph_stage import DEFAULT_RATE
from chatsbom.services.depgraph_stage import DepgraphPass
from chatsbom.services.depgraph_stage import DepgraphStage
from chatsbom.services.depgraph_stage import Pacer
from chatsbom.services.depgraph_stage import TokenWorker
from chatsbom.services.github_service import GitHubService

logger = structlog.get_logger('depgraph_command')
app = typer.Typer()


def collect(
    container: Container,
    token: str,
    limit: int | None,
    rate: float,
    repos: set[int] | None = None,
) -> DepgraphPass:
    """One pass of the dependency-graph stage, with every usable token.

    `token` is the primary one, already verified. The others come from
    `CHATSBOM_DEPGRAPH_TOKENS`; one GitHub rejects is dropped, and the
    log says which by its position, never by its value.
    """
    now = datetime.now(timezone.utc)
    setting = os.getenv('CHATSBOM_DEPGRAPH_API')
    reason = closed_reason(setting, now.date())
    if reason is not None:
        logger.warning('Dependency graph stage disabled', reason=reason)
        return DepgraphPass(closed=reason)
    if now.date() >= DEPGRAPH_ENDPOINT_CLOSES:
        logger.info(
            'Synchronous dependency-graph endpoint closed; asking for '
            'reports instead',
            closed_on=str(DEPGRAPH_ENDPOINT_CLOSES),
            setting=setting or 'auto',
        )

    tokens = depgraph_tokens(token, os.getenv('CHATSBOM_DEPGRAPH_TOKENS'))
    workers: list[TokenWorker] = []
    for position, value in enumerate(tokens, start=1):
        login: str | None = None
        if position > 1:
            usable, login = verify_extra_token(value)
            if not usable:
                logger.warning(
                    'Dependency graph token rejected by GitHub; not used',
                    token=token_label(position, None),
                )
                continue
        github = (
            container.get_github_service(value) if position == 1
            else GitHubService(value)
        )
        workers.append(
            TokenWorker(
                label=token_label(position, login),
                service=DependencyGraphService(github),
                pacer=Pacer(rate),
            ),
        )
    logger.info(
        'Dependency graph stage',
        tokens=len(workers), rate_per_token=rate, limit=limit,
    )

    paths = container.config.paths
    git = container.get_git_service()
    with Ledger(paths.ledger_path) as ledger:
        return DepgraphStage(
            ledger, workers, paths.depgraph_dir, git.default_branch_head,
            repos=repos,
        ).run(limit)


@app.callback(invoke_without_command=True)
@handle_errors
def main(
    token: str = typer.Option(
        None, envvar='GITHUB_TOKEN', help='GitHub Token',
    ),
    limit: int | None = typer.Option(
        None, help='Repositories to fetch; every due one if unset',
    ),
    rate: float = typer.Option(
        DEFAULT_RATE, help='Requests an hour, per token',
    ),
) -> None:
    """
    Download GitHub's own dependency graph as a second SBOM source.

    Syft reads lockfiles, so Maven and Composer projects that ship none
    come back nearly empty — 0 packages for spring-boot. GitHub parses the
    manifests server-side and reports 303 for that same repository.

    Due for every repository the queue tracks (`chatsbom queue track`,
    `--snapshot` to seed from a search), whether or not its SBOM
    succeeded: never asked first, then graphs older than 30 days, then
    repositories whose "no graph" answer is 30 days old or more. Each
    fetch is kept for good, stamped with the default branch and its
    HEAD. More tokens in CHATSBOM_DEPGRAPH_TOKENS each add a worker,
    paced to --rate. The same as `chatsbom run --stage depgraph`.

    Exits non-zero if a token was refused or a repository failed; both
    are recorded in the queue, which says when each is due again.

    Reads from: data/ledger.sqlite3
    Writes to:  data/09-github-depgraph/<repository id>/, the queue
    """
    token = check_github_token(token)
    verify_github_token(token, console=console)

    result = collect(get_container(), token, limit, rate)
    report(result)
    if result.refusals or result.counts['failed']:
        raise typer.Exit(1)


def report(result: DepgraphPass) -> None:
    """Print a pass's summary."""
    for line in result.summary(datetime.now(timezone.utc)):
        console.print(line)
    if not result.closed and result.asked == 0 and not result.refusals:
        console.print(
            '[green]No dependency graph due.[/] '
            '[dim]Track more with [cyan]chatsbom queue track --snapshot[/].'
            '[/dim]',
        )
