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
import time
from collections.abc import Callable
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

import structlog
import typer

from chatsbom.commands.github.tree import _is_whole_tree
from chatsbom.core.conditional import ConditionalResult
from chatsbom.core.config import PathConfig
from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.core.documents import RecordStore
from chatsbom.core.fs import atomic_write_text
from chatsbom.core.fs import looks_like_whole_json_object
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


def language_of(repository: Repository) -> str:
    """The language the paths are keyed by.

    The ledger's value, lowercased, because that is what the
    stage-major commands used to build these paths and a different
    spelling would write a second copy beside the first.
    """
    return str(repository.language or '').lower()


class DependencyGraphStage:
    """The dependency-graph stage, one repository at a time.

    `DependencyGraphService.fetch` says which answer GitHub gave, and
    they are kept apart as `github depgraph` keeps them apart: a 404 is
    no graph, a refused token is not, and a failure is neither. Only a
    document counts as the stage's work; any other answer leaves the
    stage due, so a later pass asks again.

    A refusal stops the asking, not the pass. Every later request would
    be refused the same way until GitHub's reset, so the rest of the pass
    is not asked about, and the summary says so. The other stages go on:
    this endpoint is metered apart from the core quota, at about 100 an
    hour, and stopping on it would pace all collection by it.

    A whole document stored within `max_age` seconds is reused without
    asking, due or not. `fetch` no longer goes through the cached
    session, which slept through refusals and remembered each 404 for a
    week, and that cache was what kept a pass, which walks every stage of
    each repository it claims, from spending the scarce bucket on a graph
    fetched days before. The stored document, aged by the same
    `cache_ttl`, does that now.
    """

    def __init__(
        self,
        service: DependencyGraphService,
        paths: PathConfig,
        max_age: float,
    ) -> None:
        self._service = service
        self._paths = paths
        self._max_age = max_age
        self.fetched = self.reused = self.absent = self.failed = 0
        #: Repositories not asked about, after a refusal.
        self.unasked = 0
        #: The repository GitHub refused the token at, and its answer.
        self.refusal: tuple[str, ConditionalResult] | None = None

    def __call__(
        self,
        repository: Repository,
        carried: dict[str, Any],
    ) -> dict[str, Any] | None:
        stored = self._paths.get_depgraph_path(
            language_of(repository), repository.owner, repository.repo,
        )
        if self._recent(stored):
            self.reused += 1
            return {'depgraph_path': str(stored)}
        if self.refusal is not None:
            self.unasked += 1
            return None

        result = self._service.fetch(repository.owner, repository.repo)
        if result.rate_limited:
            self.refusal = (f'{repository.owner}/{repository.repo}', result)
            return None
        if not result.changed:
            if result.absent:
                self.absent += 1
            else:
                self.failed += 1
            return None
        # Whole or not at all: a document cut short was trusted as stored.
        atomic_write_text(
            stored, json.dumps(result.payload, ensure_ascii=False),
        )
        self.fetched += 1
        return {'depgraph_path': str(stored)}

    def _recent(self, stored: Path) -> bool:
        try:
            age = time.time() - stored.stat().st_mtime
        except OSError:
            return False
        return age < self._max_age and looks_like_whole_json_object(stored)

    def summary(self, now: datetime) -> str | None:
        """What the stage did, for after the pass; None if nothing."""
        if not (
            self.fetched or self.reused or self.absent or self.failed
            or self.refusal
        ):
            return None
        line = (
            f'[dim]Dependency graph: fetched {self.fetched:,} · '
            f'reused {self.reused:,} · no graph {self.absent:,} · '
            f'failed {self.failed:,}[/dim]'
        )
        if self.refusal is None:
            return line
        name, answer = self.refusal
        resumes_at = answer.rate_limit.resumes_at(now)
        resumes = (
            f' GitHub accepts it again at {resumes_at:%Y-%m-%d %H:%M:%S} UTC.'
            if resumes_at else ''
        )
        return (
            f'{line}\n[yellow]Dependency graph rate limited:[/] GitHub '
            f'refused the token at {name} (HTTP {answer.status}), and '
            f'{self.unasked:,} after it were not asked. Nothing was '
            f'recorded for any of them, so the stage stays due.{resumes}'
        )


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
    run_depgraph = DependencyGraphStage(
        depgraph_service, paths, config.github.cache_ttl,
    )

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
        # Trusted only if written to the end, as `github tree` trusts it.
        if _is_whole_tree(stored):
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
        stored = carried.get('local_content_path')
        if not stored or not Path(stored).exists():
            return None
        record = {**repository.model_dump(mode='json'), **carried}
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
        if ledger.count() == 0:
            console.print(
                '[yellow]The queue is empty.[/] Run '
                '[cyan]chatsbom queue track[/] first.',
            )
            raise typer.Exit(1)

        result = RunService(
            ledger, runners, spent, remember=remember,
        ).advance(
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
        f"recorded {result.remembered:,} · "
        f"failed {result.failed:,} · unusable {result.unusable:,}",
    )
    if result.completed:
        breakdown = ' · '.join(
            f'{stage} {count:,}'
            for stage, count in sorted(result.completed.items())
        )
        console.print(f'[dim]{breakdown}[/dim]')
    console.print(f'[dim]API requests spent: {result.spent_quota:,}[/dim]')
    depgraph_summary = run_depgraph.summary(datetime.now(timezone.utc))
    if depgraph_summary:
        console.print(depgraph_summary)
    if result.stopped_early:
        console.print(
            f'[yellow]Stopped on quota[/] after {quota:,} requests — '
            'run again to continue.',
        )
