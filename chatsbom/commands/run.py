"""Collect the repositories the ledger says are due.

Nine stage-major commands become one repository-major pass. What makes
that safe is the ledger: every outcome is written as it happens, claims
are leased, and a stage that fails backs its repository off rather than
the pass. Interrupting this loses at most the repository in flight.

    chatsbom queue sync      # notice what changed (304s are free)
    chatsbom run             # collect what that made due
    chatsbom db raw --apply  # land the documents
    chatsbom db index        # project them

`queue sync` and `run` are separate because they cost differently: a
revalidation is conditional and usually free, so a pass can check
thousands of repositories, while collecting one spends several
rate-limited requests. Running them together would size both to the
expensive one.

See `chatsbom/services/run_service.py` for why the whole chain is
walked for each repository rather than only its due stages.
"""
from __future__ import annotations

import json
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

import structlog
import typer

from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.core.github import check_github_token
from chatsbom.core.github import verify_github_token
from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import Stage
from chatsbom.core.logging import console
from chatsbom.models.language import Language
from chatsbom.models.repository import Repository
from chatsbom.services.commit_service import CommitStats
from chatsbom.services.dependency_graph_service import DependencyGraphService
from chatsbom.services.release_service import ReleaseStats
from chatsbom.services.run_service import RunService
from chatsbom.services.sbom_service import SbomStats

logger = structlog.get_logger('run')
app = typer.Typer()


@app.callback(invoke_without_command=True)
@handle_errors
def main(
    token: str = typer.Option(
        None, envvar='GITHUB_TOKEN', help='GitHub Token',
    ),
    language: Language | None = typer.Option(None, help='Target Language'),
    limit: int = typer.Option(
        50, '--limit', help='Repositories to advance in this pass',
    ),
    quota: int = typer.Option(
        500, help='Maximum API requests to spend before stopping',
    ),
) -> None:
    """
    Advance the due repositories through their outstanding stages.

    Reads what is due from the queue, so `chatsbom queue track` and
    `chatsbom queue sync` come first — without a push to compare
    against, no derived stage is ever due.

    Safe to interrupt. Stops between repositories rather than inside
    one: a partially collected repository whose watermarks say it is
    done is worse than one plainly not done yet.

    `--quota` counts core API requests. Note that the dependency-graph
    endpoint is metered separately and far more tightly — measured at
    100 per hour against the core 5,000 — so a backlog of dependency
    graphs is paced by that bucket regardless of what `--quota` allows.
    """
    check_github_token(token)
    verify_github_token(token, console=console)

    container = get_container()
    config = container.config
    paths = config.paths

    repo_stats = ReleaseStats()
    commit_stats = CommitStats()
    sbom_stats = SbomStats()

    release_service = container.get_release_service(token)
    commit_service = container.get_commit_service(token)
    content_service = container.get_content_service(token)
    git_service = container.get_git_service(token)
    sbom_service = container.get_sbom_service()
    depgraph_service = DependencyGraphService(
        container.get_github_service(token),
    )

    def language_of(repository: Repository) -> str:
        """The language the paths are keyed by.

        The ledger's value, lowercased, because that is what the
        stage-major commands used to build these paths and a different
        spelling would write a second copy beside the first.
        """
        return str(repository.language or '').lower()

    def run_release(repository: Repository, carried: dict[str, Any]):
        return release_service.process_repo(
            repository, ReleaseStats(), language_of(repository),
        )

    def run_commit(repository: Repository, carried: dict[str, Any]):
        return commit_service.process_repo(
            repository, commit_stats, language_of(repository),
        )

    def run_tree(repository: Repository, carried: dict[str, Any]):
        target = repository.download_target
        if not target:
            return None
        lang = language_of(repository)
        stored = paths.get_tree_file_path(
            lang, repository.owner, repository.repo,
            target.ref, target.commit_sha,
        )
        if stored.exists() and stored.stat().st_size:
            return {}
        files = git_service.get_repository_tree(
            repository.owner, repository.repo, target.commit_sha,
            cache_path=paths.get_tree_cache_path(
                repository.owner, repository.repo,
                target.ref, target.commit_sha,
            ),
        )
        if files is None:
            return None
        stored.parent.mkdir(parents=True, exist_ok=True)
        stored.write_text(''.join(f'{path}\n' for path in files), 'utf-8')
        return {}

    def run_content(repository: Repository, carried: dict[str, Any]):
        lang = language_of(repository)
        try:
            enum = Language(lang)
        except ValueError:
            # A repository whose GitHub language is not one this project
            # has a handler for. Not an error: nothing downstream knows
            # what to do with it either.
            return None
        return content_service.process_repo(repository, enum)

    def run_depgraph(repository: Repository, carried: dict[str, Any]):
        lang = language_of(repository)
        stored = paths.get_depgraph_path(
            lang, repository.owner, repository.repo,
        )
        document = depgraph_service.fetch(repository.owner, repository.repo)
        if document is None:
            return None
        stored.parent.mkdir(parents=True, exist_ok=True)
        stored.write_text(json.dumps(document), encoding='utf-8')
        return {'depgraph_path': str(stored)}

    def run_sbom(repository: Repository, carried: dict[str, Any]):
        stored = carried.get('local_content_path')
        if not stored or not Path(stored).exists():
            return None
        record = {**repository.model_dump(mode='json'), **carried}
        return sbom_service.process_repo(
            record, sbom_stats, language_of(repository),
        )

    runners = {
        Stage.RELEASE: run_release,
        Stage.COMMIT: run_commit,
        Stage.TREE: run_tree,
        Stage.CONTENT: run_content,
        Stage.DEPGRAPH: run_depgraph,
        Stage.SBOM: run_sbom,
    }

    def spent() -> int:
        """Rate-limited requests this pass has made.

        Summed across the stages' own counters rather than tracked here:
        each service already counts what it sent, and a second count
        would drift from the first.
        """
        return (
            repo_stats.api_requests
            + commit_stats.api_requests
            + sbom_stats.api_requests
            + depgraph_service.requests
        )

    now = datetime.now(timezone.utc)
    with Ledger(config.paths.ledger_path) as ledger:
        if ledger.count() == 0:
            console.print(
                '[yellow]The queue is empty.[/] Run '
                '[cyan]chatsbom queue track[/] first.',
            )
            raise typer.Exit(1)

        result = RunService(ledger, runners, spent).advance(
            now,
            limit=limit,
            quota_budget=quota,
            language=str(language) if language else None,
        )

    if result.repositories == 0:
        console.print(
            '[green]Nothing due.[/] Every tracked repository is current '
            'for every stage.\n'
            '[dim]A stage becomes due when [cyan]queue sync[/cyan] sees '
            'a newer push than its watermark.[/dim]',
        )
        return

    console.print(
        f"[bold green]Advanced {result.repositories:,}[/] "
        f"repositories · {result.stages_run:,} stages · "
        f"failed {result.failed:,} · unusable {result.unusable:,}",
    )
    if result.completed:
        breakdown = ' · '.join(
            f'{stage} {count:,}'
            for stage, count in sorted(result.completed.items())
        )
        console.print(f'[dim]{breakdown}[/dim]')
    console.print(f'[dim]API requests spent: {result.spent_quota:,}[/dim]')
    if result.stopped_early:
        console.print(
            f'[yellow]Stopped on quota[/] after {quota:,} requests — '
            'run again to continue.',
        )
