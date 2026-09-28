import json
import re
from pathlib import Path

import structlog
import typer
from rich.markup import escape

from chatsbom.core.container import get_container
from chatsbom.core.decorators import handle_errors
from chatsbom.core.instants import mtime
from chatsbom.core.ledger import Ledger
from chatsbom.core.logging import console
from chatsbom.core.storage import load_jsonl
from chatsbom.models.language import Language

logger = structlog.get_logger('queue_track')
app = typer.Typer()

_DATED = re.compile(r'\d{4}-\d{2}-\d{2}')


@app.callback(invoke_without_command=True)
@handle_errors
def main(
    language: Language | None = typer.Option(None, help='Target Language'),
    snapshot: Path | None = typer.Option(
        None,
        '--snapshot',
        help='Seed from a search snapshot, e.g. data/01-github-search/all.jsonl',
        exists=True,
        dir_okay=False,
    ),
) -> None:
    """
    Register the collected repositories in the work queue.

    Idempotent: existing progress, ETags and backoff are preserved, so
    this is safe to re-run after every discovery pass.

    `--snapshot` seeds the queue from a `github search` snapshot instead
    of the per-language repository lists, whatever each repository's
    language: stars, default branch and GitHub's language are recorded,
    and a repository not tracked before is tracked for the stages that
    need no language, the dependency graph first. The language-keyed
    stages leave it alone until they stop being keyed by language.
    """
    container = get_container()
    config = container.config

    with Ledger(config.paths.ledger_path) as ledger:
        before = ledger.count()

        if snapshot is not None:
            _seed(ledger, snapshot)
        else:
            for lang in [language] if language else list(Language):
                lang_str = str(lang)
                path = config.paths.get_repo_list_path(lang_str)
                if not path.exists():
                    continue

                repos = load_jsonl(path)
                for repo in repos:
                    ledger.track(repo.id, repo.owner, repo.repo, lang_str)

                logger.info(
                    'Tracked', language=lang_str, repositories=len(repos),
                )

        after = ledger.count()

    console.print(
        f'[bold green]Queue tracks {after:,} repositories[/] '
        f'({after - before:+,} this run)',
    )


def snapshot_name(path: Path) -> str:
    """What a snapshot is called in the queue: its file's name, dated.

    `all-2026-10-01.jsonl` is `all-2026-10-01`. The undated `all.jsonl`
    is named for the day it was written, `all-2026-03-09`, which is the
    name PR B's migration gives the file.
    """
    stem = path.stem
    if _DATED.search(stem):
        return stem
    return f'{stem}-{mtime(path):%Y-%m-%d}'


def _seed(ledger: Ledger, path: Path) -> None:
    """Track every repository a search snapshot lists."""
    name = snapshot_name(path)
    new = seen = unusable = 0
    with path.open(encoding='utf-8') as handle, ledger.transaction():
        for line in handle:
            if not line.strip():
                continue
            try:
                record = json.loads(line)
                repository_id = int(record['id'])
                owner = str(record['owner'])
                repo = str(record.get('repo') or record['name'])
            except (ValueError, KeyError, TypeError):
                unusable += 1
                continue
            stars = record.get('stars', record.get('stargazers_count'))
            seen += 1
            if ledger.seed(
                repository_id, owner, repo,
                snapshot=name,
                github_language=str(record.get('language') or ''),
                stars=stars if isinstance(stars, int) else None,
                default_branch=str(record.get('default_branch') or ''),
            ):
                new += 1
    logger.info(
        'Seeded from snapshot', snapshot=name, listed=seen, new=new,
        unusable=unusable,
    )
    console.print(
        f'[dim]Snapshot {escape(name)}: {seen:,} listed, {new:,} new to the '
        f'queue[/dim]' +
        (f' [yellow]{unusable:,} unusable lines[/]' if unusable else ''),
    )
