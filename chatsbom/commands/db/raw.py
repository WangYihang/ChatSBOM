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
    chatsbom db raw --apply --repos-file pilot.txt   # a few repositories

Idempotent by content: the key is `(kind, repository_id, sha256)` on a
ReplacingMergeTree, so the same document twice is one row and a second
run of this command inserts nothing new.
"""
from __future__ import annotations

import hashlib
import json
from collections.abc import Iterator
from datetime import datetime
from datetime import timezone
from pathlib import Path

import structlog
import typer
from rich.progress import BarColumn
from rich.progress import MofNCompleteColumn
from rich.progress import SpinnerColumn
from rich.progress import TextColumn
from rich.progress import TimeElapsedColumn

from chatsbom.core.container import get_container
from chatsbom.core.depgraph_store import stamp_of
from chatsbom.core.depgraph_store import stamp_of_path
from chatsbom.core.documents import CONTENT_PREFIX_DEPTH as DOCUMENTS_CONTENT_PREFIX_DEPTH
from chatsbom.core.instants import mtime
from chatsbom.core.instants import utc
from chatsbom.core.layout import CONTENT_ROOT
from chatsbom.core.layout import DEPGRAPH_DOCUMENT
from chatsbom.core.layout import DEPGRAPH_ROOT
from chatsbom.core.layout import is_sha
from chatsbom.core.layout import landed
from chatsbom.core.layout import LEGACY_DEPGRAPH_DIR
from chatsbom.core.layout import SBOM_ROOT
from chatsbom.core.ledger import Ledger
from chatsbom.core.logging import console
from chatsbom.core.logging import progress_bar

logger = structlog.get_logger('db_raw')
app = typer.Typer()

#: Stage directory -> the `kind` it is stored under. One document per
#: scan (`07-sbom/<repository_id>/<sha>/sbom.json`) or per fetch
#: (`09-github-depgraph/<repository_id>/<fetch>/sbom.spdx.json`, and the
#: one kept from before every fetch was, under `legacy/`).
#:
#: Found by walking the repository-keyed directories rather than the
#: per-language JSONL lists, which named each repository's document by
#: its path: the path is a pure function of the repository and its
#: commit now (`core/layout.py`), so the directory *is* the list.
#:
#: `05-github-tree` is absent: it is an input to collection (which
#: manifests exist) rather than a document about a repository, and
#: `openapi_service` still reads it off disk.
SOURCES: tuple[tuple[str, str], ...] = (
    (SBOM_ROOT, 'syft'),
    (DEPGRAPH_ROOT, 'github-depgraph'),
)

#: The metadata overlay, one row per repository.
#:
#: `GET /repos/{owner}/{repo}` as `github repo` last fetched it, 81
#: fields. It is landed separately from the record because it is
#: *fresher*: a record carries metadata from when its SBOM was
#: generated, and without this overlay a refresh never reaches the
#: database. Measured once: the ledger knew 722 repositories had been
#: pushed in September while `repositories.pushed_at` still topped out
#: at 2026-02-09.
#:
#: **`07-sbom` was here too, deriving `kind='repo'`, and had to come
#: out.** Once `data slim` strips that ledger, the derivation produces
#: a record with no `all_releases` — and because it would be the newest
#: row, `RawRecords` would serve it in preference to the complete one.
#: A 5 GB reclaim that silently empties the releases table. The record
#: now comes from `RecordStore`, written by `chatsbom run` and
#: `sbom generate` at the point where it is actually complete.
RECORD_SOURCES: tuple[tuple[str, str], ...] = (
    ('02-github-repo', 'repo-metadata'),
)

#: The manifests, which are shaped differently: a content root is a
#: *directory*, and every file under it is its own row.
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
CONTENT_KIND = 'content'

#: The content root's fixed depth below `06-github-content`:
#: `<repository_id>/<sha>`. Everything after it is the manifest's path
#: within the repository, which is what the parser needs and what
#: `sources` reports as the audit trail.
CONTENT_PREFIX_DEPTH = DOCUMENTS_CONTENT_PREFIX_DEPTH

#: Rows per insert. Large enough that the round trips do not dominate,
#: small enough that a batch of documents fits comfortably in memory —
#: these average 600 KB each, so 200 is ~120 MB.
BATCH = 200


@app.callback(invoke_without_command=True)
def main(
    apply: bool = typer.Option(
        False, '--apply', help='Write the rows. Without this, only report.',
    ),
    repos_file: Path | None = typer.Option(
        None,
        '--repos-file',
        help='Only these repositories: one owner/repo (or id) per line',
        exists=True, dir_okay=False, readable=True,
    ),
    limit: int | None = typer.Option(
        None, help='Stop after this many documents per source.',
    ),
) -> None:
    """Copy collector documents into `raw_documents`, unchanged.

    Walks the repository-keyed stage directories: every scan's SBOM and
    manifests, and every kept dependency-graph fetch. Each row's `path`
    is relative to the data directory (`07-sbom/<id>/<sha>/sbom.json`),
    with the commit it is at, and for a graph the branch, beside it.
    """
    container = get_container()
    paths = container.config.paths
    root = paths.base_data_dir
    repo_db = container.get_ingestion_repository()
    # The other `db` commands do this too. Getting the repository builds
    # a client, not a schema — the table does not exist until something
    # asks for it, and this command failed with UNKNOWN_TABLE until it
    # did.
    if apply:
        repo_db.ensure_schema()

    wanted: set[int] | None = None
    if repos_file is not None:
        with Ledger(paths.ledger_path) as ledger:
            wanted, missing = ledger.resolve_repositories(
                repos_file.read_text(encoding='utf-8').splitlines(),
            )
        if missing:
            # A notice, so through the logger: on stderr, and as JSON
            # when a machine reads it. The lines are as the file had them.
            logger.warning(
                'Not tracked, left out', count=len(missing), first=missing[:10],
            )

    planned = 0
    planned_bytes = 0
    loaded = 0
    skipped = 0

    # What is already there, so a pass reads only what changed. Only
    # when applying: a dry run reports what it *would* copy, and
    # subtracting what is stored would make it report nothing.
    newest, hashes = _already_stored(repo_db) if apply else ({}, {})

    with progress_bar(
        SpinnerColumn(),
        TextColumn('[progress.description]{task.description}'),
        BarColumn(),
        MofNCompleteColumn(),
        TextColumn('•'),
        TimeElapsedColumn(),
    ) as progress:
        for directory, kind in SOURCES:
            task = progress.add_task(f'Reading {kind}...', total=None)
            batch: list[list[object]] = []
            seen = 0

            for repository_id, path, ref, commit_sha in _documents(
                root / directory, kind, wanted,
            ):
                if limit is not None and seen >= limit:
                    break
                if _unchanged(newest, kind, repository_id, path):
                    skipped += 1
                    continue
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
                    landed(path),
                    hashlib.sha256(document).hexdigest(),
                    _taken_at(path),
                    document.decode('utf-8', 'replace'),
                    ref,
                    commit_sha,
                ])
                if len(batch) >= BATCH:
                    loaded += _flush(repo_db, batch)
                    batch.clear()

            if apply and batch:
                loaded += _flush(repo_db, batch)
                batch.clear()
            progress.update(task, total=seen, completed=seen)

        # The repository records. One row per ledger line, so the unit
        # is the line rather than a file it points at.
        for directory, kind in RECORD_SOURCES:
            listings = sorted((root / directory).glob('*.jsonl'))
            if not listings:
                # Through the logger, which prints above the bar, and as
                # JSON when a machine reads stderr.
                logger.warning('No ledgers', under=str(root / directory))
                continue

            task = progress.add_task(f'Reading {kind}...', total=None)
            batch = []
            seen = 0
            for listing in listings:
                # The ledger file's own mtime: these records carry no
                # timestamp of their own, and `pushed_at` is the
                # repository's push, not when this copy was taken.
                taken = _taken_at(listing)
                for record in _records(listing):
                    if limit is not None and seen >= limit:
                        break
                    repository_id = record.get('id')
                    if not isinstance(repository_id, int):
                        continue
                    if wanted is not None and repository_id not in wanted:
                        continue
                    # Sorted keys so the same record hashes the same
                    # across runs -- otherwise every pass looks like a
                    # change and inserts a second row for it.
                    body = json.dumps(
                        record, sort_keys=True, separators=(',', ':'),
                    ).encode('utf-8')

                    digest = hashlib.sha256(body).hexdigest()
                    if digest in hashes.get((kind, repository_id), ()):
                        skipped += 1
                        continue

                    seen += 1
                    planned += 1
                    planned_bytes += len(body)
                    progress.advance(task)
                    if not apply:
                        continue

                    batch.append([
                        kind,
                        repository_id,
                        str(listing),
                        digest,
                        taken,
                        body.decode('utf-8'),
                        '',
                        '',
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
        # SOURCES because the unit differs: there, one path is one
        # document; here it is a directory of them.
        task = progress.add_task(f'Reading {CONTENT_KIND}...', total=None)
        batch = []
        seen = 0
        for repository_id, content_root, commit_sha in _content_roots(
            root / CONTENT_ROOT, wanted,
        ):
            if limit is not None and seen >= limit:
                break
            for path, body in _manifests(content_root, newest, repository_id):
                seen += 1
                planned += 1
                planned_bytes += len(body)
                progress.advance(task)
                if not apply:
                    continue
                batch.append([
                    CONTENT_KIND,
                    repository_id,
                    landed(path),
                    hashlib.sha256(body).hexdigest(),
                    _taken_at(path),
                    body.decode('utf-8', 'replace'),
                    '',
                    commit_sha,
                ])
                if len(batch) >= BATCH:
                    loaded += _flush(repo_db, batch)
                    batch.clear()

        if apply and batch:
            loaded += _flush(repo_db, batch)
            batch.clear()
        progress.update(task, total=seen, completed=seen)

    console.print(
        f'\n[bold]{planned:,}[/] documents, '
        f'{planned_bytes / 1024 ** 3:.1f} GiB on disk.',
    )
    if skipped:
        console.print(
            f'[dim]Unchanged since the last pass, not re-read: '
            f'{skipped:,}[/dim]',
        )
    if not apply:
        console.print(
            '[dim]Dry run — nothing written. Pass --apply to load.[/dim]',
        )
        return

    console.print(f'[green]Loaded[/] {loaded:,} rows into raw_documents.')
    logger.info('Raw documents loaded', rows=loaded, bytes=planned_bytes)


def _repositories(
    root: Path, wanted: set[int] | None,
) -> Iterator[tuple[int, Path]]:
    """`(repository_id, directory)` under a stage root, in id order.

    Only directories named by an id: a tree not yet migrated, or the
    migration's own bookkeeping, is not a repository.
    """
    try:
        children = [c for c in root.iterdir() if c.name.isdigit()]
    except OSError:
        return
    for child in sorted(children, key=lambda c: int(c.name)):
        repository_id = int(child.name)
        if wanted is not None and repository_id not in wanted:
            continue
        if child.is_dir():
            yield repository_id, child


def _documents(
    root: Path, kind: str, wanted: set[int] | None,
) -> Iterator[tuple[int, Path, str, str]]:
    """`(repository_id, path, ref, commit_sha)` of each document."""
    for repository_id, directory in _repositories(root, wanted):
        try:
            children = sorted(directory.iterdir())
        except OSError:
            continue
        for child in children:
            if kind == 'syft':
                if is_sha(child.name):
                    document = child / 'sbom.json'
                    if document.is_file():
                        yield repository_id, document, '', child.name
                continue
            if child.name != LEGACY_DEPGRAPH_DIR and stamp_of(child.name) is None:
                continue
            document = child / DEPGRAPH_DOCUMENT
            if document.is_file():
                ref, commit_sha = stamp_of_path(document)
                yield repository_id, document, ref, commit_sha


def _content_roots(
    root: Path, wanted: set[int] | None,
) -> Iterator[tuple[int, Path, str]]:
    """`(repository_id, content root, commit_sha)` of each scan."""
    for repository_id, directory in _repositories(root, wanted):
        try:
            children = sorted(directory.iterdir())
        except OSError:
            continue
        for child in children:
            if is_sha(child.name) and child.is_dir():
                yield repository_id, child, child.name


def _already_stored(repo_db) -> tuple[dict, dict]:
    """What the table already holds, for skipping work rather than redoing it.

    `db raw --apply` re-read every byte on every run. Measured on the
    last full pass: it read 23.65 GiB from disk, hashed all of it, and
    inserted 99,340 rows to net-add 46,335 -- the other 53,005 were
    byte-identical re-inserts that a merge then collapsed. Idempotent,
    and wasteful, and it now runs from the collector loop daily.

    Two maps, because the two shapes need different tests:

    - `newest[(kind, repository_id, path)]` -> the newest `fetched_at`
      stored for that file. A file whose mtime is no newer cannot have
      changed, so it is skipped without being opened. That is the one
      that saves the 23.65 GiB.
    - `hashes[(kind, repository_id)]` -> the content hashes stored. For
      the ledger-derived records there is no per-record file to stat --
      one ledger holds 28,069 of them -- so the ledger is parsed and
      each record skipped by hash instead. Cheaper than it sounds
      against what it avoids: an append to the ledger otherwise
      re-lands every record in it.

    Returns empty maps on any failure. Skipping is an optimisation, and
    a pass that cannot read its own table should do the safe, slow thing
    rather than decide everything is current.
    """
    newest: dict[tuple[str, int, str], datetime] = {}
    hashes: dict[tuple[str, int], set[str]] = {}
    try:
        rows = repo_db.client.query(
            'SELECT kind, repository_id, path, sha256, max(fetched_at) '
            'FROM raw_documents '
            'GROUP BY kind, repository_id, path, sha256',
        ).result_rows
    except Exception as error:  # noqa: BLE001 - reported, not fatal
        logger.warning('Could not read stored rows', error=str(error))
        return {}, {}

    for kind, repository_id, path, sha256, fetched_at in rows:
        key = (kind, int(repository_id), str(path))
        # `clickhouse_connect` hands DateTime columns back naive, so the
        # zone is re-attached here -- comparing one of these against a
        # file's aware mtime is a TypeError, and that is the good case.
        # See `chatsbom/core/instants.py` for the bad one.
        stamp = utc(fetched_at)
        if stamp > newest.get(key, stamp.min.replace(tzinfo=timezone.utc)):
            newest[key] = stamp
        hashes.setdefault((kind, int(repository_id)), set()).add(str(sha256))
    logger.info('Stored rows read', rows=len(rows))
    return newest, hashes


def _flush(repo_db, batch: list[list[object]]) -> int:
    """Insert one batch, reporting how many rows it carried."""
    repo_db.client.insert(
        'raw_documents',
        batch,
        column_names=[
            'kind', 'repository_id', 'path', 'sha256', 'fetched_at', 'body',
            'ref', 'commit_sha',
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


def _manifests(root: Path, newest: dict, repository_id: int):
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
        if _unchanged(newest, CONTENT_KIND, repository_id, path):
            continue
        body = _readable(path)
        if body is None:
            continue
        yield path, body


def _unchanged(
    newest: dict,
    kind: str,
    repository_id: int,
    path: Path,
) -> bool:
    """Whether the stored copy is at least as new as the file.

    A `stat` rather than a read, which is the whole point: the previous
    behaviour opened and hashed 23.65 GiB to discover that almost none
    of it had moved.

    Conservative in the one direction that matters. An unreadable
    `stat`, or no stored row, answers False -- so the file gets read and
    the content hash decides. A wrong "unchanged" would silently freeze
    a document at an old version; a wrong "changed" only costs a read.
    """
    stored = newest.get((kind, repository_id, landed(path)))
    if stored is None:
        return False
    try:
        mtime = _taken_at(path)
    except OSError:
        return False
    return mtime <= stored


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
