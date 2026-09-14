import structlog
import typer

from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.core.ledger import Ledger
from chatsbom.core.logging import console
from chatsbom.core.storage import load_jsonl
from chatsbom.models.language import Language

logger = structlog.get_logger('queue_track')
app = typer.Typer()


@app.callback(invoke_without_command=True)
@handle_errors
def main(
    language: Language | None = typer.Option(None, help='Target Language'),
) -> None:
    """
    Register the collected repositories in the work queue.

    Idempotent: existing progress, ETags and backoff are preserved, so
    this is safe to re-run after every discovery pass.
    """
    container = get_container()
    config = container.config

    with Ledger(config.paths.ledger_path) as ledger:
        before = ledger.count()

        for lang in [language] if language else list(Language):
            lang_str = str(lang)
            path = config.paths.get_repo_list_path(lang_str)
            if not path.exists():
                continue

            repos = load_jsonl(path)
            for repo in repos:
                ledger.track(repo.id, repo.owner, repo.repo, lang_str)

            logger.info('Tracked', language=lang_str, repositories=len(repos))

        after = ledger.count()

    console.print(
        f'[bold green]Queue tracks {after:,} repositories[/] '
        f'({after - before:+,} this run)',
    )
