"""Teach the ledger what is already on disk.

The ledger decides what to collect next by comparing each stage's
watermark against the newest push it has seen. That only works if the
watermarks reflect work that actually happened — and they did not:
measured before this existed, 467 of 24,568 rows carried any stage
watermark at all, and **none** carried a `depgraph` one, against 24,936
dependency-graph documents sitting in `data/`.

So the queue believed nothing had ever been collected. The next
continuous run would have re-fetched the whole corpus.

Nothing is re-fetched here. The documents on disk are the evidence, and
their timestamps are the watermark:

  - a dependency graph states its own, in `creationInfo.created`;
  - a syft SBOM states nothing, so its file mtime is all there is.

**Never the clock.** A watermark of `now` would say every stage
completed at the moment this command ran, which is the same lie in the
other direction: work done in February would look current, and the next
push would not overtake it.
"""
from __future__ import annotations

import json
from datetime import datetime
from datetime import timezone
from pathlib import Path

import structlog
import typer
from rich.progress import BarColumn
from rich.progress import MofNCompleteColumn
from rich.progress import Progress
from rich.progress import SpinnerColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn

from chatsbom.core.container import get_container
from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import Stage
from chatsbom.core.logging import console

logger = structlog.get_logger('queue_backfill')
app = typer.Typer()

#: Stage -> the ledger directory whose per-repository documents prove it
#: ran. Only stages whose output is stored per repository can be
#: backfilled this way; `repo`, `release` and `commit` write one ledger
#: per language and cannot say when an individual repository was seen.
STAGE_LEDGERS: tuple[tuple[Stage, str, str], ...] = (
    (Stage.SBOM, '07-sbom', 'sbom_path'),
    (Stage.DEPGRAPH, '09-github-depgraph', 'depgraph_path'),
    (Stage.CONTENT, '07-sbom', 'local_content_path'),
)


@app.callback(invoke_without_command=True)
def main(
    apply: bool = typer.Option(
        False,
        '--apply',
        help='Write the watermarks. Without this, only report.',
    ),
) -> None:
    """Record stage watermarks for work already on disk.

    Reports by default, because it rewrites scheduling state: a wrong
    watermark either re-collects the corpus or never collects it again.
    """
    container = get_container()
    paths = container.config.paths
    root = paths.base_data_dir

    found: dict[Stage, dict[int, datetime]] = {}
    names: dict[int, tuple[str, str, str]] = {}

    with Progress(
        SpinnerColumn(),
        TextColumn('[progress.description]{task.description}'),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn('•'),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        for stage, directory, field in STAGE_LEDGERS:
            listings = sorted((root / directory).glob('*.jsonl'))
            if not listings:
                console.print(
                    f'[yellow]No ledgers under {root / directory}[/] '
                    f'— skipping {stage}.',
                )
                continue

            task = progress.add_task(f'Reading {stage}...', total=None)
            seen: dict[int, datetime] = {}
            for listing in listings:
                for record in _records(listing):
                    repository_id = record.get('id')
                    stored = record.get(field)
                    if not isinstance(repository_id, int) or not stored:
                        continue
                    when = _completed_at(Path(stored), stage)
                    if when is None:
                        continue
                    # The newest evidence wins: a repository re-collected
                    # later is further along than its first scan says.
                    previous = seen.get(repository_id)
                    if previous is None or when > previous:
                        seen[repository_id] = when
                    names.setdefault(
                        repository_id,
                        (
                            str(record.get('owner') or ''),
                            str(record.get('name') or record.get('repo') or ''),
                            str(record.get('language') or '').lower(),
                        ),
                    )
                progress.advance(task)
            found[stage] = seen
            progress.update(task, total=1, completed=1)

    _report(found)
    if not apply:
        console.print(
            '\n[dim]Dry run — nothing written. Pass --apply to record.[/dim]',
        )
        return

    with Ledger(paths.ledger_path) as ledger:
        written = 0
        for stage, repositories in found.items():
            for repository_id, when in repositories.items():
                owner, repo, language = names.get(repository_id, ('', '', ''))
                if owner and repo:
                    ledger.track(repository_id, owner, repo, language)
                try:
                    ledger.record_success(repository_id, stage, when)
                except KeyError:
                    # A document for a repository the ledger has never
                    # tracked and whose listing carried no owner. Counted
                    # by its absence from `written` rather than invented.
                    continue
                written += 1
    logger.info('Watermarks recorded', written=written)
    console.print(f'[green]Recorded[/] {written:,} stage watermarks.')


def _records(listing: Path):
    """JSONL records, skipping what cannot be parsed.

    One malformed line is not a reason to lose the rest — these files
    are appended to by a long-running collector.
    """
    try:
        with listing.open(encoding='utf-8') as handle:
            for line in handle:
                if not line.strip():
                    continue
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    continue
    except OSError as error:
        logger.warning(
            'Unreadable ledger', path=str(
                listing,
            ), error=str(error),
        )


def _completed_at(stored: Path, stage: Stage) -> datetime | None:
    """When the stored artefact says it was produced.

    A dependency graph states it; everything else is dated by its file.
    Returns None when the path does not exist, because a listing entry
    for a document that is gone is not evidence the stage completed.
    """
    target = stored if stored.is_absolute() else Path(stored)
    if stage is Stage.DEPGRAPH:
        stated = _stated_creation(target)
        if stated is not None:
            return stated
    try:
        return datetime.fromtimestamp(
            target.stat().st_mtime, tz=timezone.utc,
        ).replace(tzinfo=None)
    except OSError:
        return None


def _stated_creation(path: Path) -> datetime | None:
    """`creationInfo.created` from a stored SPDX document."""
    try:
        with path.open(encoding='utf-8') as handle:
            document = json.load(handle)
    except (OSError, json.JSONDecodeError):
        return None
    sbom = document.get('sbom', document)
    if not isinstance(sbom, dict):
        return None
    info = sbom.get('creationInfo')
    if not isinstance(info, dict):
        return None
    created = info.get('created')
    if not isinstance(created, str):
        return None
    try:
        return datetime.fromisoformat(
            created.replace('Z', '+00:00'),
        ).astimezone(timezone.utc).replace(tzinfo=None)
    except ValueError:
        return None


def _report(found: dict[Stage, dict[int, datetime]]) -> None:
    from rich.table import Table

    table = Table(title='Evidence on disk')
    table.add_column('Stage')
    table.add_column('Repositories', justify='right')
    table.add_column('Earliest')
    table.add_column('Latest')
    for stage, repositories in found.items():
        if not repositories:
            table.add_row(str(stage), '0', '—', '—')
            continue
        stamps = sorted(repositories.values())
        table.add_row(
            str(stage),
            f'{len(repositories):,}',
            stamps[0].strftime('%Y-%m-%d'),
            stamps[-1].strftime('%Y-%m-%d'),
        )
    console.print(table)
