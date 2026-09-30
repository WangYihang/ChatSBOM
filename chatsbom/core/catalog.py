"""The universe of repositories to collect, and what is known of each.

Derived scheduling (#100, step 1 of `docs/design/first-principles.md`)
computes what is due as

    due = { (repository, stage) : repository in the snapshot,
                                  output(stage, input key) not in store }

and this is the first half of it: which repositories. The newest
complete unfiltered search snapshot, `01-github-search/all-<date>.jsonl`,
which the collector's universe writes (`collector/universe.py`), as
`github search` did; not the ledger's list, which derived scheduling
replaced, and which went with the old pipeline (#171).

Complete, because a search wrote today's snapshot as it went, and a
re-run on the same day resumed it: until it ended, the file listed the
most-starred part of the corpus and none of the rest. So a snapshot
dated before today (UTC, as the search dates it) is complete, and
today's once it carries a `.complete` marker beside it: how a search
that knows it finished says so. The collector's universe writes one,
renaming the snapshot into place only once it is whole.

Each repository is described as the warehouse masters on it (`Tracked`:
name, stars, GitHub's language, default branch, the snapshot that
listed it), with the push the search saw where it gave one.
"""
from __future__ import annotations

import json
import re
from collections.abc import Iterable
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import date
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

#: Beside a snapshot, `all-<date>.jsonl.complete`: the search ended.
COMPLETE_MARKER = '.complete'

#: `all-<YYYY-MM-DD>.jsonl`, as `PathConfig.search_snapshot` names one.
_SNAPSHOT = re.compile(r'^all-(\d{4}-\d{2}-\d{2})\.jsonl$')


@dataclass(frozen=True, slots=True)
class Tracked:
    """A repository a snapshot lists, as the warehouse masters on it: what
    the ledger kept of each, until it went with the old pipeline (#171)."""

    repository_id: int
    owner: str
    repo: str
    #: GitHub's language, verbatim; '' when no snapshot or resource
    #: said.
    github_language: str = ''
    stars: int | None = None
    default_branch: str = ''
    #: The search snapshot that last listed it; '' when none did.
    snapshot: str = ''

    @property
    def full_name(self) -> str:
        return f'{self.owner}/{self.repo}'


def resolve_names(
    tracked: Mapping[int, Tracked],
    names: Iterable[str],
) -> tuple[set[int], list[str]]:
    """Ids for `owner/repo` names or ids, matched case-insensitively as
    GitHub matches names; and the names that matched nothing. A blank
    line, or one that starts with `#`, names nothing."""
    by_name = {t.full_name.lower(): i for i, t in tracked.items()}
    found: set[int] = set()
    missing: list[str] = []
    for raw in names:
        name = raw.strip()
        if not name or name.startswith('#'):
            continue
        if name.isdigit() and int(name) in tracked:
            found.add(int(name))
        elif name.lower() in by_name:
            found.add(by_name[name.lower()])
        else:
            missing.append(name)
    return found, missing


@dataclass(frozen=True, slots=True)
class Snapshot:
    """One dated unfiltered search snapshot."""

    path: Path
    day: date

    @property
    def name(self) -> str:
        """What a repository's `snapshot` says it is: `all-<date>`."""
        return self.path.stem

    @property
    def marker(self) -> Path:
        return self.path.with_name(f'{self.path.name}{COMPLETE_MARKER}')

    def complete(self, today: date) -> bool:
        """Dated before `today`, or marked complete."""
        return self.day < today or self.marker.is_file()


def snapshots(search_dir: Path) -> list[Snapshot]:
    """Every dated unfiltered snapshot in `search_dir`, oldest first.

    A language's list, the undated `all.jsonl` of older searches and
    anything else that only resembles a snapshot are not one.
    """
    try:
        children = list(Path(search_dir).iterdir())
    except OSError:
        return []
    found: list[Snapshot] = []
    for child in children:
        match = _SNAPSHOT.match(child.name)
        if match is None or not child.is_file():
            continue
        try:
            day = date.fromisoformat(match[1])
        except ValueError:
            continue
        found.append(Snapshot(child, day))
    return sorted(found, key=lambda snapshot: snapshot.day)


def newest_complete(search_dir: Path, today: date) -> Snapshot | None:
    """The newest snapshot that is complete on `today`, or None."""
    complete = [s for s in snapshots(search_dir) if s.complete(today)]
    return complete[-1] if complete else None


@dataclass(frozen=True)
class Catalog:
    """The repositories to collect, by id."""

    #: The snapshot's name (`all-<date>`).
    source: str
    repositories: Mapping[int, Tracked]
    #: The push the search saw, where it gave one.
    pushed_at: Mapping[int, datetime]
    #: Lines of the snapshot that named no repository.
    unusable: int = 0

    def __len__(self) -> int:
        return len(self.repositories)

    def __contains__(self, repository_id: object) -> bool:
        return repository_id in self.repositories

    def resolve(self, names: Iterable[str]) -> tuple[set[int], list[str]]:
        """Ids for `owner/repo` names or ids, matched case-insensitively
        as GitHub matches names; and the names that matched nothing."""
        return resolve_names(self.repositories, names)


def read_snapshot(snapshot: Snapshot) -> Catalog:
    """Every repository `snapshot` lists.

    Read as `queue track --snapshot` read it: `repo` or GitHub's
    `name`, `stars` or `stargazers_count`, and a line that names no
    repository counted rather than fatal. A repository listed twice is
    described by its last line.
    """
    repositories: dict[int, Tracked] = {}
    pushed: dict[int, datetime] = {}
    unusable = 0
    with snapshot.path.open(encoding='utf-8', errors='replace') as handle:
        for line in handle:
            if not line.strip():
                continue
            listed = _listed(line, snapshot.name)
            if listed is None:
                unusable += 1
                continue
            tracked, pushed_at = listed
            repositories[tracked.repository_id] = tracked
            if pushed_at is None:
                pushed.pop(tracked.repository_id, None)
            else:
                pushed[tracked.repository_id] = pushed_at
    return Catalog(snapshot.name, repositories, pushed, unusable)


def _listed(line: str, name: str) -> tuple[Tracked, datetime | None] | None:
    try:
        record = json.loads(line)
    except ValueError:
        return None
    if not isinstance(record, dict):
        return None
    try:
        repository_id = int(record['id'])
        owner = _owner(record['owner'])
        repo = str(record.get('repo') or record['name'])
    except (KeyError, TypeError, ValueError):
        return None
    stars = record.get('stars', record.get('stargazers_count'))
    return (
        Tracked(
            repository_id=repository_id,
            owner=owner,
            repo=repo,
            github_language=str(record.get('language') or ''),
            stars=(
                stars if isinstance(stars, int)
                and not isinstance(stars, bool) else None
            ),
            default_branch=str(record.get('default_branch') or ''),
            snapshot=name,
        ),
        _instant(record.get('pushed_at')),
    )


def _owner(value: Any) -> str:
    """The owner's login: a snapshot has it as a string, GitHub's own
    search items as an object."""
    if isinstance(value, Mapping):
        value = value.get('login')
    if not isinstance(value, str) or not value:
        raise ValueError('no owner')
    return value


def _instant(value: object) -> datetime | None:
    """GitHub's `pushed_at` (`2026-09-01T00:00:00Z`), aware; else None."""
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
