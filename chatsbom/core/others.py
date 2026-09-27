"""The `other` lane: repositories no language list took.

Every stage is keyed on a language and reads
`01-github-search/<language>.jsonl`, and those lists come from GitHub's
`language:` qualifier — the repository's *largest* language. That label
is a poor guide to what a repository depends on:

- mathesar is "Svelte" and is a Django application;
- WebGoat is "JavaScript" and is a Spring Boot application.

The first never entered the pipeline at all. The second did, and fell
out at `github content`, which fetches only the manifests of the
repository's own language — WebGoat has no `package.json`, so it had no
content, no SBOM, and no row. Measured on the 2026-09 snapshot: 25,460
of the 60,017 repositories in the unfiltered sweep are in no language
list, and 6,553 of the 34,622 that are never reached `07-sbom`.

The unfiltered sweep (`01-github-search/all.jsonl`) already holds every
repository above the star threshold, so the lane is derived from it
offline: no search calls, and the records keep GitHub's own `language`.
"""
import json
from collections import defaultdict
from collections.abc import Iterable
from collections.abc import Iterator
from collections.abc import Sequence
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger('others')

#: Label for repositories GitHub assigns no language, in summaries.
UNLABELLED = '(none)'


@dataclass(frozen=True, slots=True)
class OtherSelection:
    """The records chosen for the lane, and why the rest were not."""

    records: list[dict[str, Any]]
    #: Repositories in the sweep that no language list claimed.
    unclaimed: int
    #: Claimed by a language list, but never reached its SBOM ledger.
    orphans: int
    #: Named with `include`, placed first.
    included: list[str] = field(default_factory=list)


def _records(path: Path) -> Iterator[dict[str, Any]]:
    """JSON objects from a JSONL file; unparsable lines are skipped."""
    if not path.exists():
        return
    with path.open(encoding='utf-8') as handle:
        for line in handle:
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(record, dict) and isinstance(record.get('id'), int):
                yield record


def read_ids(paths: Iterable[Path]) -> set[int]:
    """Repository ids across several ledgers. Missing files are empty."""
    return {record['id'] for path in paths for record in _records(path)}


def full_name(record: dict[str, Any]) -> str:
    """`owner/repo`, from either the search or the pipeline record shape."""
    name = record.get('full_name')
    if isinstance(name, str) and name:
        return name
    owner = record.get('owner')
    if isinstance(owner, dict):
        owner = owner.get('login')
    repo = record.get('repo') or record.get('name')
    return f'{owner}/{repo}'


def _stars(record: dict[str, Any]) -> int:
    stars = record.get('stars', record.get('stargazers_count'))
    return stars if isinstance(stars, int) else 0


def _label(record: dict[str, Any]) -> str:
    return record.get('language') or UNLABELLED


def _spread(records: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Round-robin across GitHub languages, so a prefix samples them all.

    Languages take turns in order of how many repositories they have,
    and within a language the most-starred go first — so the first 100
    of 25,000 hold the largest few of every common label rather than
    100 C++ projects.
    """
    by_label: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        by_label[_label(record)].append(record)
    queues = sorted(
        by_label.values(),
        key=lambda group: (-len(group), _label(group[0])),
    )
    spread: list[dict[str, Any]] = []
    depth = 0
    while len(spread) < len(records):
        for queue in queues:
            if depth < len(queue):
                spread.append(queue[depth])
        depth += 1
    return spread


def select_others(
    sweep: Path,
    claimed: Iterable[Path],
    reached: Iterable[Path],
    include: Sequence[str] = (),
    include_orphans: bool = False,
    spread: bool = False,
    limit: int | None = None,
) -> OtherSelection:
    """Choose the lane's repositories from the unfiltered sweep.

    - `claimed`: every ledger that puts a repository in a language's
      lane (its search and repo lists). Those are not repeated here.
    - `reached`: the SBOM ledgers, which is what `db index` reads. A
      claimed repository absent from all of them never reached the
      database — an *orphan*, like WebGoat — and `include_orphans`
      brings it into this lane, where every ecosystem's manifests are
      fetched.
    - `include`: `owner/repo` names placed first whatever the other
      options say, provided they never reached the database. A
      repository that did would be ingested twice, and `artifacts` is
      append-only.

    Order is by stars, descending, or `spread` across languages; `limit`
    truncates after the named repositories.
    """
    claimed_ids = read_ids(claimed)
    reached_ids = read_ids(reached)

    candidates: list[dict[str, Any]] = []
    by_name: dict[str, dict[str, Any]] = {}
    unclaimed = orphans = 0
    for record in _records(sweep):
        by_name[full_name(record).lower()] = record
        repository_id = record['id']
        if repository_id not in claimed_ids:
            unclaimed += 1
            candidates.append(record)
        elif repository_id not in reached_ids:
            orphans += 1
            if include_orphans:
                candidates.append(record)

    named: list[dict[str, Any]] = []
    for name in include:
        found = by_name.get(name.lower())
        if found is None:
            raise ValueError(f'{name} is not in {sweep}')
        if found['id'] in reached_ids:
            raise ValueError(
                f'{name} is already in a language\'s SBOM ledger; '
                'indexing it again would duplicate its artifacts',
            )
        if all(found['id'] != r['id'] for r in named):
            named.append(found)

    named_ids = {record['id'] for record in named}
    rest = sorted(
        (r for r in candidates if r['id'] not in named_ids),
        key=lambda r: (-_stars(r), full_name(r).lower()),
    )
    if spread:
        rest = _spread(rest)
    if limit is not None:
        rest = rest[:max(limit - len(named), 0)]

    logger.info(
        'Other lane selected',
        unclaimed=unclaimed, orphans=orphans,
        included=len(named), selected=len(named) + len(rest),
    )
    return OtherSelection(
        records=named + rest,
        unclaimed=unclaimed,
        orphans=orphans,
        included=[full_name(r) for r in named],
    )


def write_jsonl(path: Path, records: Iterable[dict[str, Any]]) -> int:
    """Write records through a temporary file, so a crash leaves no half list."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.with_suffix(path.suffix + '.tmp')
    count = 0
    with temp.open('w', encoding='utf-8') as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=False) + '\n')
            count += 1
    temp.replace(path)
    return count
