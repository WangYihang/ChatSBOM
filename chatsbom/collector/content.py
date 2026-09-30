"""The content stage's rules, apart from how a file is downloaded (#161).

What the content stage does with a repository's manifests at a commit,
moved here from `services/content_service.py` so that the collector's
stage, which downloads on an async client of its own with no token, and
the old service, which downloads with `requests`, follow one set of
rules: which files are asked for, what an answer means, the caps, and
what `manifests.json` says of it all.

- **The walk** (`walk`) goes through the files discovery selected
  (`core/discovery.py`), in its order, and yields each one it needs
  fetched, with how many bytes its body may take. The caller fetches it
  and sends back what it got: a status and a body (`Got`), a body past
  the room it had (`TooLarge`), or no answer at all (`Lost`). A file on
  disk already, or whose last outcome stands at this commit (`settled`),
  is not asked for again unless the walk is forced.
- **The document** (`settle`): `manifests.json`, beside the tree, with
  what became of every file, the content's digest (the stage's output
  key) and, from the collector, the stage's version (`VERSION_FIELD`,
  #100 Q4). Rewritten only when it says something new.

What a content root has to be to stand (`settled_document`, `LIMITS`,
`CONTENT_VERSION`) is here too, for what is due (`collector/due.py`)
and for `core/due.py`, which reads it the same way.
"""
from __future__ import annotations

import json
from collections.abc import Generator
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Any

import structlog

from chatsbom.core.config import PathConfig
from chatsbom.core.discovery import content_digest
from chatsbom.core.discovery import discover
from chatsbom.core.discovery import Discovery
from chatsbom.core.discovery import discovery_document
from chatsbom.core.discovery import dumps
from chatsbom.core.discovery import is_safe_path
from chatsbom.core.discovery import MAX_BYTES
from chatsbom.core.discovery import MAX_FILES
from chatsbom.core.discovery import OVER_BYTE_CAP
from chatsbom.core.discovery import read_tree
from chatsbom.core.fs import atomic_write_bytes
from chatsbom.core.fs import atomic_write_text
from chatsbom.core.fs import is_whole_tree
from chatsbom.core.ledger import Stage
from chatsbom.core.ledger import STAGE_VERSION

logger = structlog.get_logger('content')

#: No one manifest is bigger than this. A lockfile of a large monorepo
#: reaches a few MiB; anything past 16 MiB is generated data under a
#: manifest's name, and would hold up Syft for nothing.
MAX_FILE_BYTES = 16 * 2**20

#: Recorded against a file larger than `MAX_FILE_BYTES`.
TOO_LARGE = 'over-file-byte-cap'

#: The discovery limits in force, as `manifests.json` records them.
LIMITS: dict[str, int] = {
    'max_files': MAX_FILES,
    'max_bytes': MAX_BYTES,
    'max_file_bytes': MAX_FILE_BYTES,
}

#: Where `manifests.json` says which version of the content stage wrote
#: it. The collector writes it; a root without it was fetched by the old
#: pipeline, and is fetched again (#100 Q4, strict).
VERSION_FIELD = 'stage_version'

#: The content stage's version, as its stamp states it.
CONTENT_VERSION = STAGE_VERSION[Stage.CONTENT]


def settled(status: str) -> bool:
    """Whether a file's last outcome stands at this commit: not there,
    too large, or refused. A server error or a rate limit may pass, and
    is asked again."""
    if status in ('absent', TOO_LARGE, 'unsafe-path'):
        return True
    if status.startswith('http-'):
        code = status.removeprefix('http-')
        return code.isdigit() and 400 <= int(code) < 500 and code != '429'
    return False


def settled_document(document: Mapping[str, Any]) -> bool:
    """Whether every selected file has an outcome that stands at this
    commit: fetched, or an answer the content stage would not ask for
    again (`settled`), or left out by the byte cap, where it stops again.
    """
    selected = document.get('selected')
    skipped = document.get('skipped')
    if not isinstance(selected, list):
        return False
    capped = {
        entry.get('path') for entry in skipped or ()
        if isinstance(entry, dict) and entry.get('reason') == OVER_BYTE_CAP
    } if isinstance(skipped, list) else set()
    for entry in selected:
        if not isinstance(entry, dict):
            return False
        status = str(entry.get('status') or '')
        if status in ('ok', OVER_BYTE_CAP) or (status and settled(status)):
            continue
        if not status and entry.get('path') in capped:
            continue
        return False
    return True


def stamp_of(document: Mapping[str, Any]) -> int | None:
    """The content stage's version a `manifests.json` states, or None
    when it states none: a number, and not `true`."""
    stamp = document.get(VERSION_FIELD)
    if isinstance(stamp, int) and not isinstance(stamp, bool):
        return stamp
    return None


# -- what the walk reads ----------------------------------------------------


def stored_discovery(
    paths: PathConfig, repository_id: int, sha: str,
    max_files: int = MAX_FILES,
) -> Discovery | None:
    """The discovery list of a stored tree, or None if there is none, or
    it was cut short (`fs.is_whole_tree`)."""
    stored = paths.tree_file(repository_id, sha)
    if not is_whole_tree(stored):
        return None
    try:
        text = stored.read_text(encoding='utf-8', errors='replace')
    except OSError:
        return None
    return discover(read_tree(text), max_files=max_files)


def read_document(index: Path) -> dict[str, Any] | None:
    """A `manifests.json`, or None when it is not there or not a JSON
    object."""
    try:
        document = json.loads(index.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return None
    return document if isinstance(document, dict) else None


def previous_outcomes(index: Path, sha: str) -> dict[str, dict[str, Any]]:
    """`path -> {status, size}` from the stored `manifests.json` of this
    commit (status `over-byte-cap` for what the byte cap left out), or {}
    if there is none."""
    return outcomes_of(read_document(index), sha)


def outcomes_of(
    document: Mapping[str, Any] | None, sha: str,
) -> dict[str, dict[str, Any]]:
    """`previous_outcomes`, of a `manifests.json` read already."""
    if document is None or document.get('commit_sha') != sha:
        return {}
    outcomes: dict[str, dict[str, Any]] = {}
    for entry in document.get('selected') or ():
        if isinstance(entry, dict) and entry.get('status'):
            outcomes[str(entry.get('path'))] = {
                key: entry[key] for key in ('status', 'size')
                if key in entry
            }
    for entry in document.get('skipped') or ():
        if isinstance(entry, dict) and entry.get('reason') == OVER_BYTE_CAP:
            outcomes[str(entry.get('path'))] = {'status': OVER_BYTE_CAP}
    return outcomes


# -- the walk ---------------------------------------------------------------


@dataclass(frozen=True)
class Wanted:
    """A file the walk needs fetched."""

    #: Its path in the repository.
    path: str
    #: The most bytes its body may take: the per-file cap, or what is
    #: left of the repository's, whichever is less.
    room: int


@dataclass(frozen=True)
class Got:
    """An answer: its status, and for a 200 its body."""

    status: int
    body: bytes | None = None


@dataclass(frozen=True)
class TooLarge:
    """A body past the room it had: `size` is what was declared, or read
    before it was stopped."""

    size: int


@dataclass(frozen=True)
class Lost:
    """No answer: the connection failed, or timed out."""

    error: str


Answer = Got | TooLarge | Lost


@dataclass
class Walked:
    """What a walk did."""

    #: `(path, size)` of every file on disk now.
    written: list[tuple[str, int]] = field(default_factory=list)
    #: What became of each selected file: `status`, and `size`.
    fetched: dict[str, dict[str, Any]] = field(default_factory=dict)
    #: `(path, reason)` of what the byte cap left out.
    capped: list[tuple[str, str]] = field(default_factory=list)
    #: The files not fetched for a reason that may pass: a server error,
    #: a rate limit, a dropped connection.
    transient: list[str] = field(default_factory=list)
    #: The bytes on disk.
    total: int = 0


def walk(
    discovery: Discovery,
    root: Path,
    *,
    known: Mapping[str, Mapping[str, Any]],
    force: bool,
    max_bytes: int = MAX_BYTES,
    max_file_bytes: int = MAX_FILE_BYTES,
    repository: str = '',
) -> Generator[Wanted, Answer, Walked]:
    """Store the files `discovery` selected under `root`, each at its own
    path, asking the caller for each it needs: it yields what it wants
    and is sent what came back. Returns what it did.

    `known` is what the last walk at this commit found
    (`previous_outcomes`), so that what was not there or was too large is
    not asked for again; `force` asks again for everything, and fetches
    again what is on disk. `repository` names it in the logs.
    """
    done = Walked()

    def cap_from(position: int) -> None:
        done.capped.extend(
            (rest.path, OVER_BYTE_CAP)
            for rest in discovery.selected[position:]
        )

    for position, item in enumerate(discovery.selected):
        path = item.path
        if not is_safe_path(path):
            # `discover` already refused it; a list built elsewhere gets
            # the same check.
            done.fetched[path] = {'status': 'unsafe-path'}
            continue
        destination = root.joinpath(*path.split('/'))

        if not force and destination.is_file():
            size = destination.stat().st_size
            if done.total + size > max_bytes:
                cap_from(position)
                break
            done.total += size
            done.written.append((path, size))
            done.fetched[path] = {'status': 'ok', 'size': size}
            continue

        previous = known.get(path, {})
        if previous.get('status') == OVER_BYTE_CAP:
            cap_from(position)
            break
        if settled(str(previous.get('status') or '')):
            done.fetched[path] = dict(previous)
            continue

        answer = yield Wanted(
            path, min(max_file_bytes, max_bytes - done.total),
        )
        if isinstance(answer, TooLarge):
            if answer.size > max_file_bytes:
                done.fetched[path] = {'status': TOO_LARGE, 'size': answer.size}
                continue
            # Within the per-file cap, past what is left of the
            # repository's: this and every file after it.
            cap_from(position)
            break
        if isinstance(answer, Lost):
            logger.warning(
                'Content download error', repo=repository, file=path,
                error=answer.error,
            )
            done.transient.append(path)
            done.fetched[path] = {'status': 'error'}
            continue

        if answer.status == 200 and answer.body is not None:
            # Whole or not at all: a file here is skipped next time, so a
            # prefix left by a kill or a full disk would be what Syft
            # scanned from then on.
            atomic_write_bytes(destination, answer.body)
            done.total += len(answer.body)
            done.written.append((path, len(answer.body)))
            done.fetched[path] = {'status': 'ok', 'size': len(answer.body)}
        elif answer.status == 404:
            # Not there at this commit after all: listed by a tree that
            # was, or a submodule path.
            done.fetched[path] = {'status': 'absent'}
        elif answer.status == 429 or answer.status >= 500:
            done.transient.append(path)
            done.fetched[path] = {'status': f'http-{answer.status}'}
        else:
            logger.warning(
                'Content download refused', repo=repository, file=path,
                status_code=answer.status,
            )
            done.fetched[path] = {'status': f'http-{answer.status}'}
    return done


def settle(
    discovery: Discovery,
    done: Walked,
    *,
    repository_id: int,
    sha: str,
    index: Path,
    max_files: int = MAX_FILES,
    max_bytes: int = MAX_BYTES,
    max_file_bytes: int = MAX_FILE_BYTES,
    stamp: int | None = None,
) -> tuple[str, dict[str, Any]]:
    """Write `manifests.json` at `index` for what `done` did, stamped
    with `stamp` where one is given; the content's digest, and the
    document. Rewritten only when it says something new, so that nothing
    that lands it lands the same list again after every walk."""
    digest = content_digest(done.written)
    document = discovery_document(
        discovery,
        repository_id=repository_id,
        commit_sha=sha,
        fetched=done.fetched,
        extra_skipped=done.capped,
        max_files=max_files,
        max_bytes=max_bytes,
        max_file_bytes=max_file_bytes,
    )
    document['bytes'] = done.total
    document['digest'] = digest
    if stamp is not None:
        document[VERSION_FIELD] = stamp
    text = dumps(document)
    try:
        unchanged = index.read_text(encoding='utf-8') == text
    except OSError:
        unchanged = False
    if not unchanged:
        atomic_write_text(index, text)
    return digest, document
