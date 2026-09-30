"""Collect the repositories the ledger says are due.

Nine stage-major commands become one repository-major pass. What makes
that safe is the ledger: every outcome is written as it happens, claims
are leased, and a stage that fails backs its repository off rather than
the pass. Interrupting this loses at most the repository in flight.

    chatsbom queue sync      # notice what changed (304s are free)
    chatsbom run             # collect what that made due

What it collects is written to `data/` alone, the store the warehouse is
made from, by the collector loop's index pass. It kept each finished
record in ClickHouse's `raw_documents` too, which the warehouse never
read, and which went with the server (#153).

`queue sync` and `run` are separate because they cost differently: a
revalidation is conditional and usually free, so a pass can check
thousands of repositories, while collecting one spends several
rate-limited requests. Running them together would size both to the
expensive one.

See `chatsbom/services/run_service.py` for why the whole chain is
walked for each repository rather than only its due stages.
"""
from __future__ import annotations

import os
import socket
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
from chatsbom.core import decisions
from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.core.diagnostics import fail
from chatsbom.core.diagnostics import say
from chatsbom.core.fs import atomic_write_text
from chatsbom.core.fs import is_whole_tree
from chatsbom.core.github import check_github_token
from chatsbom.core.github import verify_github_token
from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import Stage
from chatsbom.core.logging import console
from chatsbom.models.repository import Repository
from chatsbom.services.commit_service import CommitStats
from chatsbom.services.depgraph_stage import DEFAULT_RATE
from chatsbom.services.release_service import ReleaseStats
from chatsbom.services.run_service import RunResult
from chatsbom.services.run_service import RunService
from chatsbom.services.run_service import STAGES
from chatsbom.services.sbom_service import SbomStats

logger = structlog.get_logger('run')
app = typer.Typer()


def language_of(repository: Repository) -> str:
    """The ledger's language, lowercased: the list a repository was
    tracked from, or '' for one a search snapshot seeded.

    It selects nothing any more: every stage runs for every tracked
    repository, and the content stage picks manifests from the tree.
    The release and commit services are handed it, as the stage
    commands hand them their list's.
    """
    return str(repository.language or '').lower()


def stage_runners(
    container: Any,
    token: str,
    *,
    release_stats: ReleaseStats | None = None,
    commit_stats: CommitStats | None = None,
    sbom_stats: SbomStats | None = None,
    force_tree: bool = False,
    force_content: bool = False,
) -> dict[Stage, Callable[[Repository, dict[str, Any]], dict[str, Any] | None]]:
    """The walk's stage callables, as `RunService` takes them.

    Shared by `chatsbom run` and the stage commands (`github tree`,
    `github content`), which are that walk with one stage.
    """
    paths = container.config.paths
    release_stats = release_stats or ReleaseStats()
    commit_stats = commit_stats or CommitStats()
    sbom_stats = sbom_stats or SbomStats()

    # Each service is made when its stage first runs, not up front: the
    # stage commands walk only the stages up to theirs, and `github
    # tree` has no business needing Syft installed.
    #
    # What the release and commit stages decide is kept in the store as
    # they decide it (#147), keyed by the push and by the tag or head,
    # which is where the warehouse reads it. A decision that cannot be
    # written fails its stage, since it is the stage's output; one the
    # store has already, differently, stands (`core/decisions.py`).
    def run_release(repository: Repository, carried: dict[str, Any]):
        # The pass's own counter: `--quota` is summed from it, and a
        # fresh one here left every release request uncounted.
        produced = container.get_release_service(token).process_repo(
            repository, release_stats, language_of(repository),
        )
        if produced is not None:
            decisions.keep_release(
                paths, {**repository.model_dump(mode='json'), **produced},
            )
        return produced

    def run_commit(repository: Repository, carried: dict[str, Any]):
        produced = container.get_commit_service(token).process_repo(
            repository, commit_stats, language_of(repository),
        )
        if produced is not None:
            decisions.keep_commit(
                paths, {**repository.model_dump(mode='json'), **produced},
            )
        return produced

    def run_tree(repository: Repository, carried: dict[str, Any]):
        target = repository.download_target
        if not target:
            return None
        stored = paths.tree_file(repository.id, target.commit_sha)
        # Trusted only if written to the end, as `github tree` trusts it.
        if not force_tree and is_whole_tree(stored):
            return {}
        files = container.get_git_service(token).get_repository_tree(
            repository.owner, repository.repo, target.commit_sha,
            cache_path=None if force_tree else paths.get_tree_cache_path(
                repository.id, target.commit_sha,
            ),
        )
        if files is None:
            return None
        atomic_write_text(stored, ''.join(f'{path}\n' for path in files))
        return {}

    def run_content(repository: Repository, carried: dict[str, Any]):
        # Every repository, whatever its language: which manifests to
        # fetch is read from its tree (`core/discovery.py`).
        return container.get_content_service(token).process_repo(
            repository, force=force_content,
        )

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
        return container.get_sbom_service().process_repo(
            record, sbom_stats,
            generated_lock_dir=paths.generated_lock_path(
                repository.id, target.commit_sha,
            ),
        )

    return {
        Stage.RELEASE: run_release,
        Stage.COMMIT: run_commit,
        Stage.TREE: run_tree,
        Stage.CONTENT: run_content,
        Stage.SBOM: run_sbom,
    }


def advance(
    container: Any,
    token: str,
    *,
    limit: int,
    quota: int,
    stage: Stage | None = None,
    repos: set[int] | None = None,
    force_tree: bool = False,
    force_content: bool = False,
) -> RunResult:
    """One pass of the walk over what the ledger says is due.

    Every tracked repository takes part, including those a search
    snapshot seeded with no language.
    """
    config = container.config
    paths = config.paths
    release_stats = ReleaseStats()
    commit_stats = CommitStats()
    sbom_stats = SbomStats()
    runners = stage_runners(
        container, token,
        release_stats=release_stats,
        commit_stats=commit_stats,
        sbom_stats=sbom_stats,
        force_tree=force_tree,
        force_content=force_content,
    )

    def spent() -> int:
        """Rate-limited requests this pass has made.

        Summed across the stages' own counters rather than tracked here:
        each service already counts what it sent, and a second count
        would drift from the first.
        """
        return (
            release_stats.api_requests
            + commit_stats.api_requests
            + sbom_stats.api_requests
        )

    now = datetime.now(timezone.utc)
    with Ledger(paths.ledger_path) as ledger:
        return RunService(
            ledger, runners, spent,
            worker=f'run@{socket.gethostname()}:{os.getpid()}',
        ).advance(
            now,
            limit=limit,
            quota_budget=quota,
            stage=stage,
            repos=repos,
        )


def report(result: RunResult, quota: int) -> None:
    """What a pass did, for the console."""
    if result.repositories == 0:
        console.print(
            '[green]Nothing due.[/] Every tracked repository is current '
            'for every stage.\n'
            '[dim]A stage becomes due when [cyan]queue sync[/cyan] sees '
            'a newer push than it consumed, or its stage version '
            'moves.[/dim]',
        )
        return
    console.print(
        f"[bold green]Advanced {result.repositories:,}[/] "
        f"repositories · {result.stages_run:,} stages · "
        f"failed {result.failed:,} · unusable {result.unusable:,}"
        + (f" · backing off {result.blocked:,}" if result.blocked else '')
        + (
            f" · taken by another worker {result.taken:,}"
            if result.taken else ''
        ),
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


def resolve_repos(ledger_path: Path, repos_file: Path | None) -> set[int] | None:
    """The ids `--repos-file` names, or None for every repository.

    Exits if the queue is empty: nothing is due without it.
    """
    with Ledger(ledger_path) as ledger:
        if ledger.count() == 0:
            # Where the logs go, and a failure as it was: stdout is for
            # what a pass reports, and this was printed there (#124).
            fail(
                '[yellow]The queue is empty.[/] Run '
                '[cyan]chatsbom queue track[/] first.',
                'The queue is empty', logger, ledger=str(ledger_path),
                hint='run chatsbom queue track first',
            )
        if repos_file is None:
            return None
        repos, missing = ledger.resolve_repositories(
            repos_file.read_text(encoding='utf-8').splitlines(),
        )
    if missing:
        # A notice, so through the logger: on stderr, and as JSON when a
        # machine reads it. The lines are as the file had them, which
        # markup would have read.
        logger.warning(
            'Not tracked, left out', count=len(missing), first=missing[:10],
        )
    return repos


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
    token = check_github_token(token)
    verify_github_token(token, console=console)

    container = get_container()
    config = container.config

    stages_alone = [str(s) for s in (*STAGES, Stage.DEPGRAPH)]
    if stage is not None and stage not in stages_alone:
        # A usage error, status 2 as it was, said where the logs go:
        # stdout is for what a pass reports, and this was printed there
        # (#124).
        say(
            f'[bold red]Unknown stage[/] {escape(repr(stage))}: one of '
            f'[cyan]{", ".join(stages_alone)}[/] runs on its own.',
            'Unknown stage', logger, 'error',
            stage=stage, stages=stages_alone,
        )
        raise typer.Exit(2)

    repos = resolve_repos(config.paths.ledger_path, repos_file)

    if stage == str(Stage.DEPGRAPH):
        alone = collect_depgraphs(container, token, limit, rate, repos=repos)
        report_depgraphs(alone)
        if alone.refusals or alone.counts['failed']:
            raise typer.Exit(1)
        return

    result = advance(
        container, token,
        limit=limit,
        quota=quota,
        stage=Stage(stage) if stage else None,
        repos=repos,
    )

    # Not gated on what the walk did: the graph needs nothing from it.
    graphs = (
        collect_depgraphs(container, token, limit, rate, repos=repos)
        if depgraph and stage is None else None
    )

    report(result, quota)
    if graphs is not None:
        report_depgraphs(graphs)
