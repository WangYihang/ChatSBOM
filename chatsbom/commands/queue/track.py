import json
import re
from datetime import datetime
from datetime import timezone
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
                changed = ledger.track_all(
                    (repo.id, repo.owner, repo.repo, lang_str)
                    for repo in repos
                )

                logger.info(
                    'Tracked', language=lang_str, repositories=len(repos),
                    changed=changed,
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
                pushed_at=_instant(record.get('pushed_at')),
            ):
                new += 1
        unlisted = _unlist(ledger, name, seen)
    logger.info(
        'Seeded from snapshot', snapshot=name, listed=seen, new=new,
        unlisted=unlisted, unusable=unusable,
    )
    console.print(
        f'[dim]Snapshot {escape(name)}: {seen:,} listed, {new:,} new to the '
        f'queue, {unlisted:,} no longer listed[/dim]' +
        (f' [yellow]{unusable:,} unusable lines[/]' if unusable else ''),
    )


#: A refresh that would unlist more than this share of what it lists is
#: taken for a snapshot cut short, not for the corpus shrinking: nothing
#: is unlisted, and it says so.
UNLIST_AT_MOST = 0.25


def _unlist(ledger: Ledger, name: str, listed: int) -> int:
    """Unlist what older unfiltered snapshots listed and `name` does not."""
    stale = ledger.listed_only_before(name)
    if not stale:
        return 0
    if stale > listed * UNLIST_AT_MOST:
        logger.warning(
            'Older snapshots kept: this one lists too few to replace them',
            snapshot=name, listed=listed, would_unlist=stale,
        )
        console.print(
            f'[yellow]Not unlisting {stale:,} repositories[/] that only '
            f'older snapshots list: {escape(name)} lists {listed:,}, '
            'which looks like a search cut short. Finish it '
            '(re-running `github search` the same day resumes it), then '
            'track it again.',
        )
        return 0
    return ledger.unlist_older_snapshots(name)


def _instant(value: object) -> datetime | None:
    """GitHub's `pushed_at` (`2026-09-01T00:00:00Z`), aware; else None."""
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
