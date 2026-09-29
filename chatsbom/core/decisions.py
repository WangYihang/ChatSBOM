"""The release and commit decisions, kept in the store as files (#147).

`chatsbom run` kept a repository's release list and the commit its
chain resolved to in one place only: the record `RecordStore` writes to
ClickHouse's `raw_documents`. The release and commit stages have no
scan to show for themselves, so nothing in `data/` said what they had
decided, and the warehouse, which reads the store alone (#128 §2.2),
had no releases and no refs for the repositories they had decided. The
owner decided on #100 (Q3) that their outputs are files, the release
list content-addressed, and #128 kept that:

    03-github-release/<id>/<P>/release@2.json
    03-github-release/<id>/releases/<sha256>.json
    04-github-commit/<id>/<K>/commit@1.json

`P` is the push the release stage decided for (`pushed_at`), and `K`
what the commit stage resolved for that decision: `tag:T`, the release
it chose, or `head:P`, the default branch's head, when it chose none
(#100 §2, the chain P → T → K → S). How each is spelled as a name is
`core/layout.py`'s: `20260929T122814Z`, `tag-v1.2.3`, `head-<P>`.

## The files

- **A release decision**, `{id, stage, sv, key, out, releases}`: for the
  push `key`, the tag `out` of the latest stable release the stage chose
  (null for none), and the digest of the list it chose from.
- **A release list**: the releases, as `GitHubRelease` holds them, each
  asset trimmed to what `db index` keeps of it (`ASSET_FIELDS`) less its
  download count, named by the sha256 of its bytes. A count moves on
  every fetch, and with it the same releases would be a new list each
  time; nothing reads it. So a push that decides the same releases names
  the list already there, and writes only its decision.
- **A commit decision**, `{id, stage, sv, key, out, ref, ref_type}`: for
  the key, the commit `out` and the ref it was resolved from, which is
  the default branch when the tag could not be found.

`sv` is the stage's version (`ledger.STAGE_VERSION`), which the file is
named by too: a stage whose version moves writes its decisions beside
the older ones, and the newest version is the one read.

## Written once

Each file is written whole, through a temporary file that is fsynced
and linked into place (`fs.write_once`), and never over a file that is
there. The same decision twice is one file. A different decision for a
key already decided is not written: the first stands, and is logged
(`Outcome.CONFLICT`). A list is written before the decision naming it.

## Read back

`newest` is the chain as it stands: the newest push's release decision,
its list, and the commit decision its key names. `newest_resolved` is
the newest push whose key has a commit decision, the chain the scan in
the store descends from, which `data prune` keeps (#100 Q13).
"""
from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterator
from collections.abc import Mapping
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

import structlog

from chatsbom.core.config import PathConfig
from chatsbom.core.fs import write_once
from chatsbom.core.layout import CommitKey
from chatsbom.core.layout import decision_file
from chatsbom.core.layout import decision_version
from chatsbom.core.layout import key_name
from chatsbom.core.layout import push_instant
from chatsbom.core.layout import push_name
from chatsbom.core.layout import push_of
from chatsbom.core.layout import push_text
from chatsbom.core.layout import RELEASE_LISTS
from chatsbom.core.ledger import Stage
from chatsbom.core.ledger import STAGE_VERSION
from chatsbom.models.github_release import ASSET_FIELDS
from chatsbom.models.github_release import GitHubRelease
from chatsbom.models.github_release import is_stable
from chatsbom.models.github_release import trimmed_assets

logger = structlog.get_logger('decisions')

RELEASE = str(Stage.RELEASE)
COMMIT = str(Stage.COMMIT)
RELEASE_VERSION = STAGE_VERSION[Stage.RELEASE]
COMMIT_VERSION = STAGE_VERSION[Stage.COMMIT]

#: What a release list keeps of an asset: what `db index` keeps, less
#: the download count, which moves on every fetch.
STORED_ASSET_FIELDS: frozenset[str] = ASSET_FIELDS - {'download_count'}

_DIGEST = re.compile(r'^[0-9a-f]{64}$')


class Outcome(str, Enum):
    """What keeping a decision or a list did."""

    #: Written now; or, as a report, to be written.
    WRITTEN = 'written'
    #: The same was there already.
    KEPT = 'kept'
    #: Another was there for the same key, and stands.
    CONFLICT = 'conflict'
    #: Nothing to key it by: no push, no release stage's output, no
    #: commit resolved.
    UNKEYED = 'unkeyed'

    def __str__(self) -> str:
        return self.value


@dataclass(frozen=True)
class ReleaseDecision:
    """What the release stage decided for one push."""

    repository_id: int
    push: datetime
    #: The latest stable release's tag, or None for none.
    tag: str | None
    #: The sha256 of the list it chose from.
    releases: str
    version: int = RELEASE_VERSION

    @property
    def key(self) -> CommitKey:
        """`K`: what the commit stage resolves for this decision."""
        if self.tag is not None:
            return CommitKey.tag(self.tag)
        return CommitKey.head(self.push)

    def body(self) -> dict[str, Any]:
        return {
            'id': self.repository_id, 'stage': RELEASE, 'sv': self.version,
            'key': push_text(self.push), 'out': self.tag,
            'releases': self.releases,
        }


@dataclass(frozen=True)
class CommitDecision:
    """What the commit stage resolved for one key."""

    repository_id: int
    key: CommitKey
    commit_sha: str
    ref: str
    ref_type: str
    version: int = COMMIT_VERSION

    def body(self) -> dict[str, Any]:
        return {
            'id': self.repository_id, 'stage': COMMIT, 'sv': self.version,
            'key': str(self.key), 'out': self.commit_sha, 'ref': self.ref,
            'ref_type': self.ref_type,
        }

    @property
    def download_target(self) -> dict[str, str]:
        """The record's `download_target`, as the commit stage made it."""
        return {
            'ref': self.ref, 'ref_type': self.ref_type,
            'commit_sha': self.commit_sha,
            'commit_sha_short': self.commit_sha[:7],
        }


@dataclass(frozen=True)
class Kept:
    """What keeping a release decision did, to it and to its list."""

    decision: Outcome
    #: None when there was no decision to name a list.
    releases: Outcome | None = None


# -- where they are -------------------------------------------------------


def releases_dir(paths: PathConfig, repository_id: int) -> Path:
    """A repository's release decisions and lists."""
    return paths.release_dir / str(int(repository_id))


def commits_dir(paths: PathConfig, repository_id: int) -> Path:
    """A repository's commit decisions."""
    return paths.commit_dir / str(int(repository_id))


def release_path(paths: PathConfig, decision: ReleaseDecision) -> Path:
    return (
        releases_dir(paths, decision.repository_id)
        / str(push_name(decision.push))
        / decision_file(RELEASE, decision.version)
    )


def list_path(paths: PathConfig, repository_id: int, digest: str) -> Path:
    return releases_dir(paths, repository_id) / RELEASE_LISTS / f'{digest}.json'


def commit_path(paths: PathConfig, decision: CommitDecision) -> Path:
    return (
        commits_dir(paths, decision.repository_id) / key_name(decision.key)
        / decision_file(COMMIT, decision.version)
    )


# -- from a record --------------------------------------------------------


def stored_releases(releases: Sequence[Any]) -> bytes:
    """A release list as the store keeps it, byte for byte: each release
    as `GitHubRelease` dumps it, its assets trimmed, keys sorted, no
    whitespace. The same releases are the same bytes, and so one file.

    Raises ValueError for an entry the model will not take.
    """
    entries = []
    for entry in releases:
        release = (
            entry if isinstance(entry, GitHubRelease)
            else GitHubRelease.model_validate(entry)
        )
        dumped = release.model_dump(mode='json')
        dumped['assets'] = trimmed_assets(release.assets, STORED_ASSET_FIELDS)
        entries.append(dumped)
    return json.dumps(
        entries, sort_keys=True, separators=(',', ':'), ensure_ascii=False,
    ).encode('utf-8')


def _repository_id(record: Mapping[str, Any]) -> int | None:
    value = record.get('id')
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _chosen_tag(record: Mapping[str, Any]) -> str | None:
    """The tag of the latest stable release a record says was chosen."""
    chosen = record.get('latest_stable_release')
    if isinstance(chosen, GitHubRelease):
        return chosen.tag_name
    if isinstance(chosen, Mapping) and chosen.get('tag_name') is not None:
        return str(chosen['tag_name'])
    return None


def release_of(
    record: Mapping[str, Any],
) -> tuple[ReleaseDecision, bytes] | None:
    """The release decision a record carries, and its list's bytes; None
    without a push to key it by or without the release stage's output
    (`all_releases`)."""
    repository_id = _repository_id(record)
    push = push_instant(record.get('pushed_at'))
    releases = record.get('all_releases')
    if repository_id is None or push is None or not isinstance(releases, list):
        return None
    data = stored_releases(releases)
    return ReleaseDecision(
        repository_id=repository_id, push=push, tag=_chosen_tag(record),
        releases=hashlib.sha256(data).hexdigest(),
    ), data


def commit_key(record: Mapping[str, Any]) -> CommitKey | None:
    """`K` for a record: `tag:T` for the release its release stage chose,
    or `head:P` when it chose none.

    None without the release stage's output: `run` walks on when the
    releases could not be fetched, and the commit stage then takes the
    default branch, which is no decision for a push whose release is not
    known. And None for a head with no push to key it by.
    """
    if record.get('all_releases') is None and record.get('has_releases') is None:
        return None
    tag = _chosen_tag(record)
    if tag is not None:
        return CommitKey.tag(tag)
    if push_instant(record.get('pushed_at')) is None:
        return None
    return CommitKey.head(record.get('pushed_at'))


def commit_of(record: Mapping[str, Any]) -> CommitDecision | None:
    """The commit decision a record carries: None without a commit
    resolved, or without a key (`commit_key`)."""
    repository_id = _repository_id(record)
    target = record.get('download_target')
    if repository_id is None or not isinstance(target, Mapping):
        return None
    commit = target.get('commit_sha')
    key = commit_key(record)
    if not commit or key is None:
        return None
    return CommitDecision(
        repository_id=repository_id, key=key, commit_sha=str(commit),
        ref=str(target.get('ref') or ''),
        ref_type=str(target.get('ref_type') or ''),
    )


# -- writing --------------------------------------------------------------


def _encoded(body: Mapping[str, Any]) -> bytes:
    return (
        json.dumps(body, sort_keys=True, indent=2, ensure_ascii=False) + '\n'
    ).encode('utf-8')


def _put(path: Path, data: bytes, apply: bool) -> Outcome:
    """`data` at `path` unless a file is there: which, and whether it is
    the same."""
    try:
        stored: bytes | None = path.read_bytes()
    except FileNotFoundError:
        stored = None
    if stored is None:
        if not apply:
            return Outcome.WRITTEN
        if write_once(path, data):
            return Outcome.WRITTEN
        stored = path.read_bytes()
    if stored == data:
        return Outcome.KEPT
    if apply:
        logger.warning(
            'A different decision is kept for this key, and stands',
            path=str(path),
        )
    return Outcome.CONFLICT


def _put_list(path: Path, data: bytes, apply: bool) -> Outcome:
    """A list, named by its digest: a file there is this list, and is not
    read to say so. One of another size is damaged, and said to be."""
    try:
        size: int | None = path.stat().st_size
    except FileNotFoundError:
        size = None
    if size is None:
        if not apply or write_once(path, data):
            return Outcome.WRITTEN
        size = path.stat().st_size
    if size == len(data):
        return Outcome.KEPT
    logger.error('A release list is not what its name says', path=str(path))
    return Outcome.CONFLICT


def keep_release(
    paths: PathConfig,
    record: Mapping[str, Any],
    *,
    apply: bool = True,
) -> Kept:
    """Keep the release decision `record` carries, and its list.

    `record` is a repository as the release stage leaves it
    (`Repository.model_dump(mode='json')`), or a record that stage made.
    With `apply` off nothing is written, and the outcome says what would
    be.
    """
    made = release_of(record)
    if made is None:
        return Kept(Outcome.UNKEYED)
    decision, data = made
    listed = _put_list(
        list_path(paths, decision.repository_id, decision.releases),
        data, apply,
    )
    decided = _put(
        release_path(paths, decision), _encoded(decision.body()), apply,
    )
    return Kept(decided, listed)


def keep_commit(
    paths: PathConfig,
    record: Mapping[str, Any],
    *,
    apply: bool = True,
) -> Outcome:
    """Keep the commit decision `record` carries, as `keep_release`."""
    decision = commit_of(record)
    if decision is None:
        return Outcome.UNKEYED
    return _put(
        commit_path(paths, decision), _encoded(decision.body()), apply,
    )


# -- reading --------------------------------------------------------------


def _body(path: Path) -> dict[str, Any] | None:
    try:
        loaded = json.loads(path.read_text(encoding='utf-8'))
    except (OSError, ValueError):
        return None
    return loaded if isinstance(loaded, dict) else None


def _versions(directory: Path, stage: str) -> list[tuple[int, Path]]:
    """The decision files of `stage` in `directory`, newest version first."""
    try:
        children = list(directory.iterdir())
    except OSError:
        return []
    found = [
        (version, child) for child in children
        if (version := decision_version(child.name, stage)) is not None
    ]
    return sorted(found, reverse=True)


def _is_version(value: object, version: int) -> bool:
    return isinstance(value, int) and not isinstance(value, bool) and (
        value == version
    )


def read_release(directory: Path, repository_id: int) -> ReleaseDecision | None:
    """The release decision in one push's directory, of the newest
    version whose file reads as one; None when none does, or when the
    file says it was made for another repository or push."""
    push = push_of(directory.name)
    if push is None:
        return None
    for version, file in _versions(directory, RELEASE):
        body = _body(file)
        if body is None or body.get('stage') != RELEASE:
            continue
        if not _is_version(body.get('sv'), version):
            continue
        if body.get('id') != repository_id:
            continue
        if push_instant(body.get('key')) != push:
            continue
        tag = body.get('out')
        digest = body.get('releases')
        if tag is not None and not isinstance(tag, str):
            continue
        if not isinstance(digest, str) or not _DIGEST.match(digest):
            continue
        return ReleaseDecision(repository_id, push, tag, digest, version)
    return None


def read_commit(directory: Path, repository_id: int) -> CommitDecision | None:
    """The commit decision in one key's directory, as `read_release`."""
    for version, file in _versions(directory, COMMIT):
        body = _body(file)
        if body is None or body.get('stage') != COMMIT:
            continue
        if not _is_version(body.get('sv'), version):
            continue
        if body.get('id') != repository_id:
            continue
        key = CommitKey.parse(str(body.get('key') or ''))
        if key is None or key_name(key) != directory.name:
            continue
        commit, ref, ref_type = (
            body.get('out'), body.get('ref'), body.get('ref_type'),
        )
        if not isinstance(commit, str) or not commit:
            continue
        if not isinstance(ref, str) or not isinstance(ref_type, str):
            continue
        return CommitDecision(repository_id, key, commit, ref, ref_type, version)
    return None


def pushes(paths: PathConfig, repository_id: int) -> list[Path]:
    """Every push directory of a repository, oldest first: a push's name
    sorts as its instant."""
    try:
        children = list(releases_dir(paths, repository_id).iterdir())
    except OSError:
        return []
    return sorted(
        (child for child in children if push_of(child.name) is not None),
        key=lambda child: child.name,
    )


def keys(paths: PathConfig, repository_id: int) -> list[Path]:
    """Every key directory of a repository, by name."""
    try:
        children = list(commits_dir(paths, repository_id).iterdir())
    except OSError:
        return []
    return sorted(
        (child for child in children if child.is_dir()),
        key=lambda child: child.name,
    )


def release_decisions(
    paths: PathConfig, repository_id: int,
) -> Iterator[ReleaseDecision]:
    """A repository's readable release decisions, newest push first."""
    for directory in reversed(pushes(paths, repository_id)):
        decision = read_release(directory, repository_id)
        if decision is not None:
            yield decision


def commit_decision(
    paths: PathConfig, repository_id: int, key: CommitKey,
) -> CommitDecision | None:
    """The commit decision for `key`, if the store has one."""
    return read_commit(
        commits_dir(paths, repository_id) / key_name(key), repository_id,
    )


def release_list(
    paths: PathConfig, repository_id: int, digest: str,
) -> list[dict[str, Any]] | None:
    """The list a digest names, or None when it is not there or is not
    what its name says."""
    path = list_path(paths, repository_id, digest)
    try:
        data = path.read_bytes()
    except OSError:
        return None
    if hashlib.sha256(data).hexdigest() != digest:
        logger.warning('A release list is not what its name says', path=str(path))
        return None
    try:
        loaded = json.loads(data)
    except ValueError:
        return None
    if not isinstance(loaded, list) or not all(isinstance(r, dict) for r in loaded):
        return None
    return loaded


def has_release(
    paths: PathConfig, repository_id: int, pushed_at: datetime | str | None,
) -> bool:
    """Whether the store has this version's release decision for a push."""
    name = push_name(pushed_at)
    if name is None:
        return False
    decision = read_release(releases_dir(paths, repository_id) / name, repository_id)
    return decision is not None and decision.version == RELEASE_VERSION


def has_commit(paths: PathConfig, repository_id: int, key: CommitKey) -> bool:
    """Whether the store has this version's commit decision for a key."""
    decision = commit_decision(paths, repository_id, key)
    return decision is not None and decision.version == COMMIT_VERSION


@dataclass(frozen=True)
class Chain:
    """A push's decisions, as the store has them."""

    release: ReleaseDecision
    #: The commit decision its key names; None while there is none.
    commit: CommitDecision | None
    #: The list it names; None when it was not read, or could not be.
    releases: list[dict[str, Any]] | None = None


def newest(
    paths: PathConfig, repository_id: int, *, lists: bool = True,
) -> Chain | None:
    """The chain as it stands: the newest push's release decision, the
    list it names (unless `lists` is off) and the commit decision its key
    names. None when the store has no release decision of the
    repository."""
    for decision in release_decisions(paths, repository_id):
        return Chain(
            release=decision,
            commit=commit_decision(paths, repository_id, decision.key),
            releases=(
                release_list(paths, repository_id, decision.releases)
                if lists else None
            ),
        )
    return None


def newest_resolved(
    paths: PathConfig, repository_id: int, *, lists: bool = False,
) -> Chain | None:
    """The newest push whose key has a commit decision: the chain the
    scan in the store descends from, while a newer push waits for its
    commit."""
    for decision in release_decisions(paths, repository_id):
        commit = commit_decision(paths, repository_id, decision.key)
        if commit is None:
            continue
        return Chain(
            release=decision, commit=commit,
            releases=(
                release_list(paths, repository_id, decision.releases)
                if lists else None
            ),
        )
    return None


def chosen(
    releases: Sequence[GitHubRelease], tag: str | None,
) -> GitHubRelease | None:
    """The release the release stage chose, found again by its tag.

    The first stable release in the list with that tag, which is the one
    the stage took: it takes the first stable release of the list
    (`is_stable`). A list another version of the stage chose from by
    other rules may have none that is stable now; the first with the tag
    is taken then.
    """
    if tag is None:
        return None
    tagged = [release for release in releases if release.tag_name == tag]
    return next(filter(is_stable, tagged), tagged[0] if tagged else None)


def as_record(chain: Chain) -> dict[str, Any]:
    """What a record says of the release and commit stages, as the chain
    has it: the fields `Repository` takes them in, for a reader to lay
    over a record (the warehouse, #147). The releases only where the
    list could be read; the download target only where the commit is
    decided."""
    made: dict[str, Any] = {}
    if chain.releases is not None:
        entries = list(chain.releases)
        tagged = [
            (position, GitHubRelease.model_validate(entry))
            for position, entry in enumerate(entries)
            if entry.get('tag_name') == chain.release.tag
        ]
        picked = chosen([release for _, release in tagged], chain.release.tag)
        latest = next(
            (entries[position] for position, release in tagged if release is picked),
            None,
        )
        made.update(
            all_releases=entries,
            total_releases=len(entries),
            has_releases=bool(entries),
            latest_stable_release=latest,
        )
    if chain.commit is not None:
        made['download_target'] = chain.commit.download_target
    return made
