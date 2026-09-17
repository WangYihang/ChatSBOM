"""Land the collectors' documents in the database, verbatim.

The pipeline writes 31 GB of JSON into `data/07-sbom` and
`data/09-github-depgraph`, and `db index` reads about 80 bytes out of
each 820-byte package entry. The rest — `cpes`, `locations`,
`metadata`, Syft's `artifactRelationships` — is on disk and not
queryable, which has cost this project twice already: `06-github-content`
stored manifests and not sources, so PHP lockfiles were recoverable and
Java's were not, and its coverage is still 46%.

Measured rather than assumed: 600 sampled documents compress 10.1x
under `ZSTD(3)`, so the 31 GB lands in about 3.1 GB — an order of
magnitude *less* than the files it copies.

This is a landing zone, not a serving path. Nothing queries it per
request; it exists so a transform can be re-run without re-fetching,
and so a field nobody extracted yet is still there when someone wants
it.

    chatsbom db raw              # report what would be loaded
    chatsbom db raw --apply      # load it

Idempotent by content: the key is `(kind, repository_id, sha256)` on a
ReplacingMergeTree, so the same document twice is one row and a second
run of this command inserts nothing new.
"""
from __future__ import annotations

import hashlib
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
from chatsbom.core.logging import console

logger = structlog.get_logger('db_raw')
app = typer.Typer()

#: Stage directory -> the `kind` it is stored under. Only the two the
#: transform actually reads: `05-github-tree` and `06-github-content`
#: are inputs to collection rather than documents about a repository,
#: and the content directory holds source files, not JSON to query.
SOURCES: tuple[tuple[str, str, str], ...] = (
    ('07-sbom', 'syft', 'sbom_path'),
    ('09-github-depgraph', 'github-depgraph', 'depgraph_path'),
)

#: Rows per insert. Large enough that the round trips do not dominate,
#: small enough that a batch of documents fits comfortably in memory —
#: these average 600 KB each, so 200 is ~120 MB.
BATCH = 200


@app.callback(invoke_without_command=True)
def main(
    apply: bool = typer.Option(
        False, '--apply', help='Write the rows. Without this, only report.',
    ),
    language: str | None = typer.Option(
        None, help='One language, for a trial run.',
    ),
    limit: int | None = typer.Option(
        None, help='Stop after this many documents per source.',
    ),
) -> None:
    """Copy collector documents into `raw_documents`, unchanged."""
    container = get_container()
    root = container.config.paths.base_data_dir
    repo_db = container.get_ingestion_repository()
    # The other `db` commands do this too. Getting the repository builds
    # a client, not a schema — the table does not exist until something
    # asks for it, and this command failed with UNKNOWN_TABLE until it
    # did.
    if apply:
        repo_db.ensure_schema()

    planned = 0
    planned_bytes = 0
    loaded = 0

    with Progress(
        SpinnerColumn(),
        TextColumn('[progress.description]{task.description}'),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn('•'),
        TimeElapsedColumn(),
        console=console,
    ) as progress:
        for directory, kind, field in SOURCES:
            listings = sorted((root / directory).glob('*.jsonl'))
            if language:
                listings = [p for p in listings if p.stem == language]
            if not listings:
                console.print(
                    f'[yellow]No ledgers under {root / directory}[/]',
                )
                continue

            task = progress.add_task(f'Reading {kind}...', total=None)
            batch: list[list[object]] = []
            seen = 0

            for listing in listings:
                for record in _records(listing):
                    if limit is not None and seen >= limit:
                        break
                    repository_id = record.get('id')
                    stored = record.get(field)
                    if not isinstance(repository_id, int) or not stored:
                        continue
                    path = Path(str(stored))
                    document = _readable(path)
                    if document is None:
                        continue

                    seen += 1
                    planned += 1
                    planned_bytes += len(document)
                    progress.advance(task)

                    if not apply:
                        continue

                    batch.append([
                        kind,
                        repository_id,
                        str(path),
                        hashlib.sha256(document).hexdigest(),
                        _taken_at(path),
                        document.decode('utf-8', 'replace'),
                    ])
                    if len(batch) >= BATCH:
                        loaded += _flush(repo_db, batch)
                        batch.clear()
                if limit is not None and seen >= limit:
                    break

            if apply and batch:
                loaded += _flush(repo_db, batch)
                batch.clear()
            progress.update(task, total=seen, completed=seen)

    console.print(
        f'\n[bold]{planned:,}[/] documents, '
        f'{planned_bytes / 1024 ** 3:.1f} GiB on disk.',
    )
    if not apply:
        console.print(
            '[dim]Dry run — nothing written. Pass --apply to load.[/dim]',
        )
        return

    console.print(f'[green]Loaded[/] {loaded:,} rows into raw_documents.')
    logger.info('Raw documents loaded', rows=loaded, bytes=planned_bytes)


def _flush(repo_db, batch: list[list[object]]) -> int:
    """Insert one batch, reporting how many rows it carried."""
    repo_db.client.insert(
        'raw_documents',
        batch,
        column_names=[
            'kind', 'repository_id', 'path', 'sha256', 'fetched_at', 'body',
        ],
    )
    return len(batch)


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
            'Unreadable ledger', path=str(listing),
            error=str(error),
        )


def _readable(path: Path) -> bytes | None:
    """The document's bytes, or None if there is nothing worth storing.

    An empty file is skipped rather than stored: two zero-byte SBOMs in
    this corpus were the standing `failed=2` on every rebuild, and a
    landing zone that preserves them faithfully preserves nothing.
    """
    try:
        data = path.read_bytes()
    except OSError:
        return None
    return data if data.strip() else None


def _taken_at(path: Path) -> datetime:
    """When this copy was made.

    The file's mtime, not the clock. `now()` would stamp every document
    with the moment this command ran, which is the same lie the
    `observed_at` default told before it was fixed: a document collected
    in February would claim to be current.
    """
    try:
        return datetime.fromtimestamp(
            path.stat().st_mtime, tz=timezone.utc,
        ).replace(tzinfo=None)
    except OSError:
        return datetime(1970, 1, 1)
