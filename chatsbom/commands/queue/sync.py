from datetime import datetime
from datetime import timedelta
from datetime import timezone

import structlog
import typer

from chatsbom.core.conditional import conditional_get
from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.core.github import check_github_token
from chatsbom.core.github import verify_github_token
from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import RepositoryState
from chatsbom.core.logging import console
from chatsbom.models.language import Language
from chatsbom.services.sync_service import SyncService

logger = structlog.get_logger('queue_sync')
app = typer.Typer()

REPO_URL = 'https://api.github.com/repos/{owner}/{repo}'


@app.callback(invoke_without_command=True)
@handle_errors
def main(
    token: str = typer.Option(
        None, envvar='GITHUB_TOKEN', help='GitHub Token',
    ),
    language: Language | None = typer.Option(None, help='Target Language'),
    slice_size: int = typer.Option(
        500, '--slice', help='Repositories to re-check in this pass',
    ),
    quota: int = typer.Option(
        250,
        help='Maximum rate-limited requests to spend (304s are free)',
    ),
    recheck_hours: int = typer.Option(
        6, '--recheck-hours',
        help='How stale a repository must be before it is re-checked',
    ),
) -> None:
    """
    Re-check the stalest repositories and record what changed.

    Only the repository resource is fetched, conditionally: a 304 means
    nothing changed and costs no rate limit, so a slice can revalidate far
    more repositories than it could collect. A repository whose
    `pushed_at` moved becomes due for every later stage.

    Safe to interrupt — every outcome is written as it happens, and claims
    are leased, so at most the repository in flight is lost.
    """
    check_github_token(token)
    verify_github_token(token, console=console)

    container = get_container()
    config = container.config
    github = container.get_github_service(token)
    now = datetime.now(timezone.utc)

    def observe(state: RepositoryState, etag: str | None):
        return conditional_get(
            github.session,
            REPO_URL.format(owner=state.owner, repo=state.repo),
            etag=etag,
        )

    with Ledger(config.paths.ledger_path) as ledger:
        if ledger.count() == 0:
            console.print(
                '[yellow]The queue is empty.[/] Run '
                '[cyan]chatsbom queue track[/] first.',
            )
            raise typer.Exit(1)

        result = SyncService(ledger, observe).revalidate(
            now,
            limit=slice_size,
            quota_budget=quota,
            language=str(language) if language else None,
            recheck=timedelta(hours=recheck_hours),
        )

    console.print(
        f"[bold green]Checked {result.checked:,}[/] · "
        f"unchanged {result.unchanged:,} · "
        f"changed {result.changed:,} · "
        f"absent {result.absent:,} · "
        f"failed {result.failed:,}",
    )
    console.print(
        f"[dim]Rate-limited requests spent: {result.spent_quota:,} "
        f"({result.unchanged_ratio:.0%} of checks were free)[/dim]",
    )

    # Never-checked repositories sort first, so during the initial sweep
    # every check is unconditional and the free ratio reads 0%. It only
    # becomes meaningful once `queue status` shows nothing never-checked.
    if result.unchanged == 0 and result.changed:
        console.print(
            '[dim]No 304s yet — repositories without a stored ETag are '
            'checked first. The free ratio becomes meaningful once '
            '[cyan]queue status[/cyan] reports 0 never-checked.[/dim]',
        )
