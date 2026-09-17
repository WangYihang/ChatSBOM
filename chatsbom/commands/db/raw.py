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
from chatsbom.core.instants import mtime
from chatsbom.core.logging import console

logger = structlog.get_logger('db_raw')
app = typer.Typer()

#: Stage directory -> the `kind` it is stored under, and the ledger
#: field naming the document. One document per repository.
#:
#: `05-github-tree` is absent: it is an input to collection (which
#: manifests exist) rather than a document about a repository, and
#: `openapi_service` still reads it off disk.
SOURCES: tuple[tuple[str, str, str], ...] = (
    ('07-sbom', 'syft', 'sbom_path'),
    ('09-github-depgraph', 'github-depgraph', 'depgraph_path'),
)

#: The manifests, which are shaped differently: `local_content_path` is
#: a *directory*, and every file under it is its own row.
#:
#: They were left out of the first pass on the grounds that the content
#: directory "holds source files, not JSON to query". That was the
#: wrong test. These files are the sole evidence behind every
#: direct/transitive verdict — 46,433 of them, 9.8 GiB — so while they
#: live only on disk, `data/` cannot be discarded and the transform
#: cannot be re-run from the database.
#:
#: Measured on 400 sampled files: 4.1x under ZSTD(3), so 9.8 GiB lands
#: in about 2.4 GiB. Less than the 10.1x the SBOMs get, because a
#: lockfile is already dense JSON full of high-entropy hashes.
CONTENT_LEDGER = '07-sbom'
CONTENT_KIND = 'content'
CONTENT_FIELD = 'local_content_path'

#: The content root's fixed depth: `<language>/<owner>/<repo>/<ref>/<sha>`.
#: Everything after it is the manifest's path within the repository, which
#: is what the parser needs and what `sources` reports as the audit trail.
CONTENT_PREFIX_DEPTH = 5

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

        # The manifests. Kept as its own loop rather than folded into
        # SOURCES because the unit differs: there, one ledger record is
        # one document; here it is a directory of them.
        listings = sorted((root / CONTENT_LEDGER).glob('*.jsonl'))
        if language:
            listings = [p for p in listings if p.stem == language]

        task = progress.add_task(f'Reading {CONTENT_KIND}...', total=None)
        batch = []
        seen = 0
        for listing in listings:
            for record in _records(listing):
                if limit is not None and seen >= limit:
                    break
                repository_id = record.get('id')
                stored = record.get(CONTENT_FIELD)
                if not isinstance(repository_id, int) or not stored:
                    continue

                for path, body in _manifests(Path(str(stored))):
                    seen += 1
                    planned += 1
                    planned_bytes += len(body)
                    progress.advance(task)
                    if not apply:
                        continue
                    batch.append([
                        CONTENT_KIND,
                        repository_id,
                        str(path),
                        hashlib.sha256(body).hexdigest(),
                        _taken_at(path),
                        body.decode('utf-8', 'replace'),
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


def _manifests(root: Path):
    """Every stored manifest under a repository's content directory.

    Yields `(path, bytes)`. Descends, because 812 of the 46,433 stored
    files are nested — a monorepo declares dependencies in more than one
    place, and flattening would silently keep only one of them.

    `VENDOR_DIRS` is not filtered here on purpose: this is a landing
    zone, and deciding that `node_modules/` is uninteresting is the
    transform's judgement to make, not the copy's. Nothing vendored was
    downloaded in the first place.
    """
    if not root.is_dir():
        return
    for path in sorted(root.rglob('*')):
        if not path.is_file():
            continue
        body = _readable(path)
        if body is None:
            continue
        yield path, body


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

    Aware, and via `instants.mtime`, because stripping the zone here put
    every one of these 53,005 rows eight hours early — see
    `chatsbom/core/instants.py`.
    """
    return mtime(path)
