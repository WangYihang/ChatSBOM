import json
from collections.abc import Iterable
from datetime import datetime
from datetime import timezone
from pathlib import Path

import structlog
import typer
from rich.progress import BarColumn
from rich.progress import MofNCompleteColumn
from rich.progress import Progress
from rich.progress import SpinnerColumn
from rich.progress import TaskProgressColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn
from rich.progress import TimeRemainingColumn

from chatsbom.core.conditional import ConditionalResult
from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.core.documents import stage_input
from chatsbom.core.fs import atomic_write_text
from chatsbom.core.fs import looks_like_whole_json_object
from chatsbom.core.github import check_github_token
from chatsbom.core.github import verify_github_token
from chatsbom.core.logging import console
from chatsbom.models.language import Language
from chatsbom.services.dependency_graph_service import DependencyGraphService

logger = structlog.get_logger('depgraph_command')
app = typer.Typer()


@app.callback(invoke_without_command=True)
@handle_errors
def main(
    token: str = typer.Option(
        None, envvar='GITHUB_TOKEN', help='GitHub Token',
    ),
    language: Language | None = typer.Option(None, help='Target Language'),
    force: bool = typer.Option(
        False, help='Re-fetch even if a stored document exists',
    ),
    limit: int | None = typer.Option(None, help='Limit number of items'),
    from_raw: bool = typer.Option(
        False,
        '--from-raw',
        help='Take the repository records from raw_documents, not data/',
    ),
) -> None:
    """
    Download GitHub's own dependency graph as a second SBOM source.

    Syft reads lockfiles, so Maven and Composer projects that ship none
    come back nearly empty — 0 packages for spring-boot. GitHub parses the
    manifests server-side and reports 303 for that same repository.

    The index is merged, never rewritten from one run: --limit bounds the
    work, not the file. The run stops at the first rate-limited answer,
    and exits non-zero if the token was refused or any repository failed.

    A report GitHub is still generating is waited for, within a bound.
    One still not ready is pending: not a failure and not "no graph", so
    nothing is recorded for it and the next run asks again.

    Reads from: data/07-sbom
    Writes to:  data/09-github-depgraph
    """
    check_github_token(token)
    verify_github_token(token, console=console)

    container = get_container()
    config = container.config
    service = DependencyGraphService(container.get_github_service(token))

    failures = waiting = 0
    #: The repository GitHub refused the token at, and its answer.
    refusal: tuple[str, ConditionalResult] | None = None

    for lang in [language] if language else list(Language):
        lang_str = str(lang)
        input_path = config.paths.get_sbom_list_path(lang_str)
        output_path = config.paths.get_depgraph_list_path(lang_str)

        if not from_raw and not input_path.exists():
            logger.warning(
                f"No input for {lang_str}", path=str(input_path),
            )
            continue

        # `--from-raw` reads the record from the landing zone
        # rather than from the previous stage's ledger. That
        # chain is what puts four copies of every release list
        # on disk, and it is currently broken in the middle:
        # `03-github-release` and `04-github-commit` are not on
        # this machine, so these stages find no input at all.
        repos = stage_input(
            container, lang_str, input_path, from_raw, limit,
        )
        if not repos:
            continue

        output_path.parent.mkdir(parents=True, exist_ok=True)
        fetched = cached = absent = failed = pending = unreached = 0

        # The index is how `db index`, `queue backfill` and `db raw` find
        # these documents, and a run may reach only some of them.
        # Rewritten from what one `--limit` run reached, it once cut Java
        # from 1,215 indexed repositories to 87. So it starts from what is
        # already indexed, and is written back whatever happens below.
        entries = _read_index(output_path)
        try:
            with Progress(
                SpinnerColumn(),
                TextColumn('[progress.description]{task.description}'),
                BarColumn(),
                TaskProgressColumn(),
                MofNCompleteColumn(),
                TextColumn('•'),
                TimeElapsedColumn(),
                TextColumn('•'),
                TimeRemainingColumn(),
                console=console,
            ) as progress:
                task = progress.add_task(
                    f"Dependency graph {lang_str}...", total=len(repos),
                )

                for position, repo in enumerate(repos):
                    stored = config.paths.get_depgraph_path(
                        lang_str, repo.owner, repo.repo,
                    )

                    # Trusted only if it looks whole. A document that an
                    # in-place write cut short used to count as cached,
                    # and `db index` then failed that repository on
                    # every run.
                    if not force and looks_like_whole_json_object(stored):
                        cached += 1
                    else:
                        result = service.fetch(repo.owner, repo.repo)
                        if result.rate_limited:
                            # The token was refused, not the repository,
                            # and every later request would be refused
                            # the same way. Stop asking, and record
                            # nothing about this one: it is not a
                            # repository without a graph.
                            refusal = (f'{repo.owner}/{repo.repo}', result)
                            unreached = len(repos) - position - 1
                            break
                        if result.pending:
                            # GitHub accepted the request and was still
                            # generating the report when the wait ran
                            # out. There is a graph and nothing failed;
                            # recording nothing is what makes the next
                            # run ask again.
                            pending += 1
                            progress.advance(task)
                            continue
                        if not result.changed:
                            if result.absent:
                                absent += 1
                            else:
                                failed += 1
                            progress.advance(task)
                            continue
                        atomic_write_text(
                            stored,
                            json.dumps(result.payload, ensure_ascii=False),
                        )
                        fetched += 1

                    record = repo.model_dump(exclude_none=True, mode='json')
                    record['depgraph_path'] = str(stored)
                    entries[repo.id] = json.dumps(record, ensure_ascii=False)
                    progress.advance(task)
        finally:
            _write_index(output_path, entries.values())

        failures += failed
        waiting += pending
        logger.info(
            'Dependency graph complete',
            language=lang_str,
            fetched=fetched,
            cached=cached,
            no_graph=absent,
            failed=failed,
            pending=pending,
            rate_limited=refusal is not None,
            not_attempted=unreached,
            indexed=len(entries),
        )
        summary = (
            f'[bold]{lang_str}[/]: fetched {fetched:,} · cached {cached:,} · '
            f'no graph {absent:,} · failed {failed:,}'
        )
        if pending:
            summary += f' · pending {pending:,}'
        if refusal is not None:
            summary += f' · rate limited 1 · not attempted {unreached:,}'
        console.print(summary)

        if refusal is not None:
            break

    if refusal is not None:
        name, answer = refusal
        resumes_at = answer.rate_limit.resumes_at(datetime.now(timezone.utc))
        resumes = (
            ' GitHub accepts it again at '
            f'{resumes_at:%Y-%m-%d %H:%M:%S} UTC.'
            if resumes_at else ''
        )
        console.print(
            f'[yellow]Rate limited:[/] GitHub refused the token at {name} '
            f'(HTTP {answer.status}), so the run stopped there rather than '
            'keep asking. Nothing was recorded as having no graph, and the '
            f'index keeps everything collected so far.{resumes}',
        )
    if waiting:
        console.print(
            f'[yellow]{waiting:,} pending:[/] GitHub accepted the request '
            'and was still generating the report when the wait ran out. '
            'None is recorded as having no graph or as failed; run again '
            'to collect them.',
        )
    if failures:
        console.print(
            f'[red]{failures:,} failed:[/] GitHub answered with an error, '
            'or not at all. None is recorded as having no graph; run again '
            'to retry them. The log names each one.',
        )
    if refusal is not None or failures:
        raise typer.Exit(1)


def _read_index(path: Path) -> dict[int, str]:
    """The index as it stands: repository id -> its line, in file order.

    Kept verbatim, except for what no reader can use — a line that is not
    a record with an integer `id` and a `depgraph_path` — and entries
    whose document is gone, which are not evidence of anything.
    """
    entries: dict[int, str] = {}
    if not path.exists():
        return entries

    unusable = gone = 0
    with path.open(encoding='utf-8') as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                unusable += 1
                continue
            if not isinstance(record, dict):
                unusable += 1
                continue
            repository_id = record.get('id')
            document = record.get('depgraph_path')
            if not isinstance(repository_id, int) or not document:
                unusable += 1
                continue
            if not Path(str(document)).is_file():
                gone += 1
                continue
            entries[repository_id] = line.rstrip('\n')

    if unusable or gone:
        # Said out loud: the index is rewritten from what is kept here.
        logger.warning(
            'Index entries dropped',
            path=str(path), unusable=unusable, document_gone=gone,
        )
    return entries


def _write_index(path: Path, lines: Iterable[str]) -> None:
    """Replace the index in one step, so it is never seen half written.

    Written beside it and renamed over it: an interrupted write leaves the
    previous index, not a truncated one. `atomic_write_text` names the
    temporary file per call, so two runs cannot write into one file, and
    never `*.jsonl`, which `db raw` and `queue backfill` glob for.
    """
    atomic_write_text(path, ''.join(line + '\n' for line in lines))
