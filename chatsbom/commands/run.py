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

from collections.abc import Callable
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

import structlog
import typer
from rich.markup import escape

from chatsbom.commands.github.depgraph import collect as collect_depgraphs
from chatsbom.commands.github.depgraph import report as report_depgraphs
from chatsbom.commands.github.tree import _is_whole_tree
from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.core.documents import RecordStore
from chatsbom.core.fs import atomic_write_text
from chatsbom.core.github import check_github_token
from chatsbom.core.github import verify_github_token
from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import Stage
from chatsbom.core.logging import console
from chatsbom.models.language import Language
from chatsbom.models.repository import Repository
from chatsbom.services.commit_service import CommitStats
from chatsbom.services.depgraph_stage import DEFAULT_RATE
from chatsbom.services.release_service import ReleaseStats
from chatsbom.services.run_service import RunService
from chatsbom.services.run_service import STAGES
from chatsbom.services.sbom_service import SbomStats

logger = structlog.get_logger('run')
app = typer.Typer()


def language_of(repository: Repository) -> str:
    """The language the paths are keyed by.

    The ledger's value, lowercased, because that is what the
    stage-major commands used to build these paths and a different
    spelling would write a second copy beside the first.
    """
    return str(repository.language or '').lower()


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
    stage: str | None = typer.Option(
        None,
        help=(
            'Run one stage only: release, commit, tree, content, sbom or '
            'depgraph. Claims what that stage is due for and records it '
            'alone.'
        ),
    ),
    repos_file: Path | None = typer.Option(
        None,
        '--repos-file',
        help='Only these repositories: one owner/repo (or id) per line',
        exists=True, dir_okay=False, readable=True,
    ),
    depgraph: bool = typer.Option(
        True,
        '--depgraph/--no-depgraph',
        help='After the walk, fetch up to --limit due dependency graphs',
    ),
    rate: float = typer.Option(
        DEFAULT_RATE, help='Dependency-graph requests an hour, per token',
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

    `--quota` counts core API requests. The dependency graph is metered
    separately and far more tightly — 100 to 200 an hour against the
    core 5,000 — so it is its own stage, due for every tracked
    repository whether or not its SBOM succeeded, and paced to `--rate`
    per token (`CHATSBOM_DEPGRAPH_TOKENS` adds tokens). It runs after
    the walk, for up to `--limit` repositories; `--stage depgraph` runs
    it alone, and `--no-depgraph` leaves it to a worker of its own.
    """
    check_github_token(token)
    verify_github_token(token, console=console)

    container = get_container()
    config = container.config
    paths = config.paths

    stages_alone = [str(s) for s in (*STAGES, Stage.DEPGRAPH)]
    if stage is not None and stage not in stages_alone:
        console.print(
            f'[bold red]Unknown stage[/] {escape(repr(stage))}: one of '
            f'[cyan]{", ".join(stages_alone)}[/] runs on its own.',
        )
        raise typer.Exit(2)

    repos: set[int] | None = None
    with Ledger(config.paths.ledger_path) as ledger:
        if ledger.count() == 0:
            console.print(
                '[yellow]The queue is empty.[/] Run '
                '[cyan]chatsbom queue track[/] first.',
            )
            raise typer.Exit(1)
        if repos_file is not None:
            repos, missing = ledger.resolve_repositories(
                repos_file.read_text(encoding='utf-8').splitlines(),
            )
            if missing:
                # A notice, so through the logger: on stderr, and as JSON
                # when a machine reads it. The lines are as the file had
                # them, which markup would have read.
                logger.warning(
                    'Not tracked, left out',
                    count=len(missing), first=missing[:10],
                )

    if stage == str(Stage.DEPGRAPH):
        alone = collect_depgraphs(container, token, limit, rate, repos=repos)
        report_depgraphs(alone)
        if alone.refusals or alone.counts['failed']:
            raise typer.Exit(1)
        return

    repo_stats = ReleaseStats()
    commit_stats = CommitStats()
    sbom_stats = SbomStats()

    release_service = container.get_release_service(token)
    commit_service = container.get_commit_service(token)
    content_service = container.get_content_service(token)
    git_service = container.get_git_service(token)
    sbom_service = container.get_sbom_service()

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
        stored = paths.tree_file(repository.id, target.commit_sha)
        # Trusted only if written to the end, as `github tree` trusts it.
        if _is_whole_tree(stored):
            return {}
        files = git_service.get_repository_tree(
            repository.owner, repository.repo, target.commit_sha,
            cache_path=paths.get_tree_cache_path(
                repository.id, target.commit_sha,
            ),
        )
        if files is None:
            return None
        atomic_write_text(stored, ''.join(f'{path}\n' for path in files))
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

    def run_sbom(repository: Repository, carried: dict[str, Any]):
        # A pure function of the repository and its commit, so this stage
        # needs nothing handed over from `content`: it can run alone.
        target = repository.download_target
        if not target:
            return None
        stored = paths.content_root(repository.id, target.commit_sha)
        if not stored.is_dir():
            return None
        record = {
            **repository.model_dump(mode='json'), **carried,
            'local_content_path': str(stored),
        }
        return sbom_service.process_repo(
            record, sbom_stats, language_of(repository),
        )

    # Annotated: one is an object, the rest functions, and the dict
    # would otherwise be inferred as holding `object`.
    runners: dict[
        Stage, Callable[[Repository, dict[str, Any]], dict[str, Any] | None],
    ] = {
        Stage.RELEASE: run_release,
        Stage.COMMIT: run_commit,
        Stage.TREE: run_tree,
        Stage.CONTENT: run_content,
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
        )

    # The record's home. Written once per repository, keyed by the
    # ledger it belongs to because that is how `RawRecords` scopes a
    # language — which language a repository is in is the pipeline's
    # judgement, not a field in the record.
    repo_db = container.get_ingestion_repository()
    repo_db.ensure_schema()
    store = RecordStore(repo_db.client)

    def remember(record: Any) -> None:
        language = str(record.get('language') or '').lower()
        if not language:
            return
        store.remember(record, paths.get_sbom_list_path(language))

    now = datetime.now(timezone.utc)
    with Ledger(config.paths.ledger_path) as ledger:
        result = RunService(
            ledger, runners, spent, remember=remember,
        ).advance(
            now,
            limit=limit,
            quota_budget=quota,
            language=str(language) if language else None,
            stage=Stage(stage) if stage else None,
            repos=repos,
        )

    # Not gated on what the walk did: the graph needs nothing from it.
    graphs = (
        collect_depgraphs(container, token, limit, rate, repos=repos)
        if depgraph and stage is None else None
    )

    if result.repositories == 0:
        console.print(
            '[green]Nothing due.[/] Every tracked repository is current '
            'for every stage.\n'
            '[dim]A stage becomes due when [cyan]queue sync[/cyan] sees '
            'a newer push than it consumed, or its stage version '
            'moves.[/dim]',
        )
        if graphs is not None:
            report_depgraphs(graphs)
        return

    console.print(
        f"[bold green]Advanced {result.repositories:,}[/] "
        f"repositories · {result.stages_run:,} stages · "
        f"recorded {result.remembered:,} · "
        f"failed {result.failed:,} · unusable {result.unusable:,}"
        + (f" · backing off {result.blocked:,}" if result.blocked else ''),
    )
    if result.completed:
        breakdown = ' · '.join(
            f'{stage} {count:,}'
            for stage, count in sorted(result.completed.items())
        )
        console.print(f'[dim]{breakdown}[/dim]')
    console.print(f'[dim]API requests spent: {result.spent_quota:,}[/dim]')
    if graphs is not None:
        report_depgraphs(graphs)
    if result.stopped_early:
        console.print(
            f'[yellow]Stopped on quota[/] after {quota:,} requests — '
            'run again to continue.',
        )
