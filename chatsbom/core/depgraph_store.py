"""Where dependency-graph documents are kept: every fetch, for good.

The dependency graph used to be one file per repository,
`09-github-depgraph/<language>/<owner>/<repo>/sbom.spdx.json`, written
over by every fetch. GitHub's synchronous endpoint closes after
2026-11-13, so a document overwritten now may be one that can never be
asked for again, and a graph that changed between two fetches left no
trace of the one before.

Now each fetch has a directory of its own, keyed by the repository's
numeric id (owner decision D3 on #55: an id does not move when a
repository is renamed or transferred):

    09-github-depgraph/<repository_id>/<fetched>-<head>/sbom.spdx.json
    09-github-depgraph/<repository_id>/<fetched>-<head>/meta.json

* `<fetched>` is when it was fetched, `YYYYMMDDTHHMMSSZ` in UTC;
* `<head>` is the full sha of the default branch's HEAD that `git
  ls-remote` reported immediately before the fetch, or `unknown` when
  it could not be read. Full rather than abbreviated, so that the
  stamp survived in `raw_documents.path`, which was all `db index` had
  of a landed document until both went (#153);
* `meta.json` holds `{repository_id, owner, repo, ref, commit_sha,
  fetched_at, http_status, sha256}`: the ref is the default branch the
  graph describes.

Nothing here overwrites or deletes. A document byte-identical to the
newest one already kept is not stored again.

The legacy files stay readable where they are, and `data migrate-layout`
moves them under `<repository_id>/legacy/`, with a `meta.json` saying
when GitHub made them and that their head is unknown. A legacy document
is never a fetch: `legacy` does not parse as a fetch directory's name.
The two layouts cannot collide: a language directory is never all
digits.

`index.jsonl`, beside the per-language `<language>.jsonl` indexes, gets
one line per stored fetch. It is append-only. `db raw` and `db index`
found the new layout by it until #153 deleted both, and `queue
backfill` reads it with the stage's other `*.jsonl` listings.
"""
from __future__ import annotations

import hashlib
import json
import re
import threading
from collections.abc import Iterator
from dataclasses import dataclass
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any

import structlog

from chatsbom.core.fs import atomic_write_bytes
from chatsbom.core.fs import atomic_write_text
from chatsbom.core.fs import looks_like_whole_json_object

logger = structlog.get_logger('depgraph_store')

#: The document's name, in both layouts.
DOCUMENT = 'sbom.spdx.json'
#: The fetch's stamp, beside the document.
META = 'meta.json'
#: The append-only audit log of stored fetches.
INDEX = 'index.jsonl'
#: The head, when `git ls-remote` could not say what it was.
UNKNOWN_HEAD = 'unknown'
#: Where `data migrate-layout` puts the one document a repository had
#: before every fetch was kept: `<repository_id>/legacy/sbom.spdx.json`.
LEGACY = 'legacy'

_STAMP = '%Y%m%dT%H%M%SZ'
_FETCH_DIR = re.compile(
    r'^(?P<fetched>\d{8}T\d{6}Z)-(?P<head>[0-9a-f]{40}|' +
    UNKNOWN_HEAD + r')$',
)
_SHA = re.compile(r'^[0-9a-f]{40}$')

#: One lock for every append to `INDEX`: the depgraph stage runs a thread
#: per token, and two appends interleaving would write one broken line.
_INDEX_LOCK = threading.Lock()


@dataclass(frozen=True)
class Fetch:
    """One stored fetch of one repository's graph."""

    repository_id: int
    document: Path
    fetched_at: datetime
    #: The default branch's HEAD at fetch time; '' when unknown.
    commit_sha: str
    #: The default branch, from `meta.json`; '' when it is not there.
    ref: str = ''
    sha256: str = ''

    @property
    def directory(self) -> Path:
        return self.document.parent


@dataclass(frozen=True)
class Stored:
    """What `store` did with a document."""

    fetch: Fetch
    #: False when it was identical to the newest one kept, which stands.
    written: bool


def repository_dir(root: Path, repository_id: int) -> Path:
    """Every fetch of one repository, in the new layout."""
    return Path(root) / str(int(repository_id))


def fetch_dir(
    root: Path, repository_id: int, fetched_at: datetime, head_sha: str,
) -> Path:
    """The directory of one fetch."""
    head = head_sha if _SHA.match(head_sha or '') else UNKNOWN_HEAD
    stamp = fetched_at.astimezone(timezone.utc).strftime(_STAMP)
    return repository_dir(root, repository_id) / f'{stamp}-{head}'


def stamp_of(directory_name: str) -> tuple[datetime, str] | None:
    """`(fetched_at, commit_sha)` from a fetch directory's name.

    None when the name is not one this module writes. The sha is '' for
    a head recorded as unknown.
    """
    match = _FETCH_DIR.match(directory_name)
    if match is None:
        return None
    fetched = datetime.strptime(match['fetched'], _STAMP).replace(
        tzinfo=timezone.utc,
    )
    head = match['head']
    return fetched, '' if head == UNKNOWN_HEAD else head


def stamp_of_path(path: str | Path | None) -> tuple[str, str]:
    """`(ref, commit_sha)` a stored document was fetched at.

    From `meta.json` beside it when that is readable, else the commit
    from the directory's name, which is what a landed document still has
    (`raw_documents.path`). `('', '')` for a legacy document, which
    recorded neither.
    """
    if not path:
        return '', ''
    document = Path(str(path))
    parsed = stamp_of(document.parent.name)
    if parsed is None:
        return '', ''
    ref, sha = '', parsed[1]
    meta = _read_meta(document.parent / META)
    if meta is not None:
        ref = str(meta.get('ref') or '')
        recorded = str(meta.get('commit_sha') or '')
        if _SHA.match(recorded):
            sha = recorded
    return ref, sha


def _read_meta(path: Path) -> dict[str, Any] | None:
    try:
        meta = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return None
    return meta if isinstance(meta, dict) else None


def fetches(root: Path, repository_id: int) -> list[Fetch]:
    """Every whole stored fetch of a repository, oldest first."""
    directory = repository_dir(root, repository_id)
    try:
        children = list(directory.iterdir())
    except OSError:
        return []
    found: list[Fetch] = []
    for child in children:
        parsed = stamp_of(child.name)
        if parsed is None:
            continue
        document = child / DOCUMENT
        if not looks_like_whole_json_object(document):
            continue
        fetched_at, sha = parsed
        meta = _read_meta(child / META) or {}
        found.append(
            Fetch(
                repository_id=int(repository_id),
                document=document,
                fetched_at=fetched_at,
                commit_sha=sha,
                ref=str(meta.get('ref') or ''),
                sha256=str(meta.get('sha256') or ''),
            ),
        )
    found.sort(
        key=lambda fetch: (
            fetch.fetched_at, fetch.document.parent.name,
        ),
    )
    return found


def newest(root: Path, repository_id: int) -> Fetch | None:
    """The latest whole fetch of a repository, if there is one."""
    kept = fetches(root, repository_id)
    return kept[-1] if kept else None


def store(
    root: Path,
    *,
    repository_id: int,
    owner: str,
    repo: str,
    payload: Any,
    fetched_at: datetime,
    ref: str,
    head_sha: str,
    http_status: int,
) -> Stored:
    """Keep one fetched document, next to every one before it.

    The document is written whole or not at all, then its `meta.json`,
    then its line in `index.jsonl`. A directory already there for the
    same second is never written into: the next second is taken
    instead. A document byte-identical to the newest one kept is not
    written again; the newest one is returned, `written=False`.
    """
    body = json.dumps(payload, ensure_ascii=False).encode('utf-8')
    digest = hashlib.sha256(body).hexdigest()

    previous = newest(root, repository_id)
    if previous is not None:
        previous_digest = previous.sha256 or _digest_of(previous.document)
        if previous_digest == digest:
            return Stored(fetch=previous, written=False)

    moment = fetched_at.astimezone(timezone.utc).replace(microsecond=0)
    directory = fetch_dir(root, repository_id, moment, head_sha)
    while directory.exists():
        moment += timedelta(seconds=1)
        directory = fetch_dir(root, repository_id, moment, head_sha)

    commit_sha = head_sha if _SHA.match(head_sha or '') else ''
    document = directory / DOCUMENT
    atomic_write_bytes(document, body)
    meta = {
        'repository_id': int(repository_id),
        'owner': owner,
        'repo': repo,
        'ref': ref,
        'commit_sha': commit_sha,
        'fetched_at': moment.isoformat(),
        'http_status': int(http_status),
        'sha256': digest,
    }
    atomic_write_text(
        directory / META, json.dumps(meta, ensure_ascii=False, indent=2),
    )
    fetch = Fetch(
        repository_id=int(repository_id),
        document=document,
        fetched_at=moment,
        commit_sha=commit_sha,
        ref=ref,
        sha256=digest,
    )
    _append_index(
        Path(root) / INDEX,
        {
            'id': int(repository_id),
            'owner': owner,
            'repo': repo,
            'depgraph_path': str(document),
            'ref': ref,
            'commit_sha': commit_sha,
            'fetched_at': moment.isoformat(),
            'sha256': digest,
        },
    )
    return Stored(fetch=fetch, written=True)


def _digest_of(path: Path) -> str:
    try:
        return hashlib.sha256(path.read_bytes()).hexdigest()
    except OSError:
        return ''


def _append_index(path: Path, record: dict[str, Any]) -> None:
    """One line, appended whole: under a lock, in one write."""
    line = json.dumps(record, ensure_ascii=False) + '\n'
    path.parent.mkdir(parents=True, exist_ok=True)
    with _INDEX_LOCK:
        with path.open('a', encoding='utf-8') as handle:
            handle.write(line)


def current_documents(root: Path) -> Iterator[Path]:
    """One document per repository: the newest fetch, else the legacy one.

    For readers that walk the store on their own (`collect_edges`, as
    `db edges` did). Walking every `*.json` would read each `meta.json`
    as a document, and every fetch of a repository as another
    repository.

    A legacy document is left out when its repository has a fetch in the
    new layout; the two are matched by `owner/repo` as the fetch's
    `meta.json` spells it, case-insensitively, as GitHub treats names.
    """
    root = Path(root)
    if not root.is_dir():
        return
    covered: set[str] = set()
    legacy_roots: list[Path] = []
    for child in sorted(root.iterdir()):
        if not child.is_dir():
            continue
        if not child.name.isdigit():
            legacy_roots.append(child)
            continue
        latest = newest(root, int(child.name))
        if latest is None:
            # Only the graph kept from before every fetch was, moved
            # under the id by `data migrate-layout`.
            legacy = child / LEGACY / DOCUMENT
            if looks_like_whole_json_object(legacy):
                yield legacy
            continue
        meta = _read_meta(latest.directory / META) or {}
        owner, repo = meta.get('owner'), meta.get('repo')
        if owner and repo:
            covered.add(f'{owner}/{repo}'.lower())
        yield latest.document
    for language_root in legacy_roots:
        for document in sorted(language_root.glob(f'*/*/{DOCUMENT}')):
            name = f'{document.parent.parent.name}/{document.parent.name}'
            if name.lower() in covered:
                continue
            yield document
