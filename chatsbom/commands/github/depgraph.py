import json

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

from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.core.github import check_github_token
from chatsbom.core.github import verify_github_token
from chatsbom.core.logging import console
from chatsbom.core.storage import load_jsonl
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
) -> None:
    """
    Download GitHub's own dependency graph as a second SBOM source.

    Syft reads lockfiles, so Maven and Composer projects that ship none
    come back nearly empty — 0 packages for spring-boot. GitHub parses the
    manifests server-side and reports 303 for that same repository.

    Reads from: data/07-sbom
    Writes to:  data/09-github-depgraph
    """
    check_github_token(token)
    verify_github_token(token, console=console)

    container = get_container()
    config = container.config
    service = DependencyGraphService(container.get_github_service(token))

    for lang in [language] if language else list(Language):
        lang_str = str(lang)
        input_path = config.paths.get_sbom_list_path(lang_str)
        output_path = config.paths.get_depgraph_list_path(lang_str)

        if not input_path.exists():
            logger.warning(
                f"No SBOM list for {lang_str}", path=str(input_path),
            )
            continue

        repos = load_jsonl(input_path)
        if limit:
            repos = repos[:limit]
        if not repos:
            continue

        output_path.parent.mkdir(parents=True, exist_ok=True)
        fetched = cached = absent = 0

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
        ) as progress, open(output_path, 'w', encoding='utf-8') as ledger:
            task = progress.add_task(
                f"Dependency graph {lang_str}...", total=len(repos),
            )

            for repo in repos:
                stored = config.paths.get_depgraph_path(
                    lang_str, repo.owner, repo.repo,
                )

                if stored.exists() and not force:
                    cached += 1
                else:
                    payload = service.fetch(repo.owner, repo.repo)
                    if payload is None:
                        absent += 1
                        progress.advance(task)
                        continue
                    stored.parent.mkdir(parents=True, exist_ok=True)
                    stored.write_text(
                        json.dumps(payload, ensure_ascii=False),
                        encoding='utf-8',
                    )
                    fetched += 1

                record = repo.model_dump(exclude_none=True, mode='json')
                record['depgraph_path'] = str(stored)
                ledger.write(json.dumps(record, ensure_ascii=False) + '\n')
                progress.advance(task)

        logger.info(
            'Dependency graph complete',
            language=lang_str,
            fetched=fetched,
            cached=cached,
            no_graph=absent,
        )
