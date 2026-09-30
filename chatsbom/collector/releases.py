"""The release stage's rules, apart from how it asks (#161).

Moved here from `services/release_service.py`, which went with the old
pipeline (#171), so that the collector's release stage, which asks the
API on the async client and git on the git remote, chose as that one
did:

- **The history** (`release_history`): every GitHub release, and every
  tag with none (a bare tag), dated by the commit it points to; newest
  first, the undated last; and the latest stable one of them, the first
  that is neither a draft nor a pre-release (`is_stable`), or none.
- **Dating the bare tags**: by the dates the list before this one had
  for them, while each still names the same commit (`carried_dates`);
  then by git's fetch of the tags (`git_dated`), which spends no quota;
  then by `/commits/{sha}` for at most `API_DATE_CAP` of those left, the
  highest versions first (`to_ask`). What nothing dates sorts last.
"""
from __future__ import annotations

import re
from collections.abc import Iterable
from collections.abc import Mapping
from collections.abc import Sequence
from datetime import datetime
from datetime import timezone
from typing import Any
from typing import Protocol

from chatsbom.models.github_release import GitHubRelease
from chatsbom.models.github_release import is_stable

# Sorts before every real date. Aware, like the dates it is compared
# with: a naive floor raised TypeError on the first undated tag, and
# the repository got no release record at all.
UNDATED = datetime.min.replace(tzinfo=timezone.utc)

#: At most this many `/commits/{sha}` lookups per repository, for the
#: tags git could not date. Measured over the corpus, a cap of 20 is a
#: mean of 6.1 calls per repository where uncapped it was 47.4 (#55).
API_DATE_CAP = 20

_VERSION_PART = re.compile(r'(\d+)')


def version_key(tag: str) -> tuple[tuple[int, int, str], ...]:
    """Sorts tags as versions: `v1.10.0` after `v1.9.0`, not before.

    Digit runs compare as numbers and everything else as text; the kind
    of each part is part of the key, so the two never meet.
    """
    return tuple(
        (1, int(part), '') if part.isdigit() else (0, 0, part)
        for part in _VERSION_PART.split(tag) if part
    )


def parse_date(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00'))
    except (ValueError, TypeError):
        return None


class Dated(Protocol):
    """What git says of a tag: `git_service.TagDate`."""

    @property
    def sha(self) -> str:
        ...

    @property
    def date(self) -> str:
        ...


def release_history(
    releases: Sequence[Mapping[str, Any]],
    bare_tags: Mapping[str, str],
    dates: Mapping[str, str],
) -> tuple[list[GitHubRelease], GitHubRelease | None]:
    """Every release and bare tag, newest first, and the latest stable
    one among them, or None.

    `releases` are GitHub's, as the API lists them; `bare_tags` the tags
    with no release, each at its commit; `dates` what dates each bare tag
    has been found to have.
    """
    entries = []
    for listed in releases:
        entry = GitHubRelease.model_validate(listed)
        entry.source = 'github_release'
        entries.append(entry)

    # Tags that have no release, dated by the commit they point to.
    for tag_name, sha in bare_tags.items():
        published = parse_date(dates.get(tag_name))
        # Pre-release and draft are flags of a GitHub release; a bare
        # tag has neither, so both stay False.
        entries.append(
            GitHubRelease(
                id=0,
                tag_name=tag_name,
                name=tag_name,
                published_at=published,
                created_at=published,
                target_commitish=sha,
                source='git_tag',
            ),
        )

    # Sort all by date, undated last.
    entries.sort(
        key=lambda x: x.published_at or x.created_at or UNDATED,
        reverse=True,
    )
    # A GitHub release says for itself whether it is a pre-release. A
    # bare tag cannot, so its name is read instead. With no stable
    # candidate at all, there is no latest release, and the commit stage
    # takes the default branch.
    return entries, next(filter(is_stable, entries), None)


def carried_dates(
    previous: Iterable[Mapping[str, Any]] | None,
    bare_tags: Mapping[str, str],
) -> dict[str, str]:
    """The dates the list before this one had for bare tags that still
    name the same commit: a tag's date is its commit's, so it holds while
    the tag does, and only the tags that are new or moved are dated."""
    carried: dict[str, str] = {}
    for entry in previous or ():
        name = entry.get('tag_name')
        date = entry.get('published_at')
        if (
            entry.get('source') == 'git_tag' and isinstance(name, str)
            and name in bare_tags and isinstance(date, str) and date
            and entry.get('target_commitish') == bare_tags[name]
        ):
            carried[name] = date
    return carried


def git_dated(
    missing: Mapping[str, str], from_git: Mapping[str, Dated] | None,
) -> dict[str, str]:
    """The dates git gave for the `missing` tags that still name the
    commit it dated."""
    dates: dict[str, str] = {}
    for name, sha in missing.items():
        found = (from_git or {}).get(name)
        if found and found.sha == sha and found.date:
            dates[name] = found.date
    return dates


def to_ask(undated: Iterable[str]) -> list[str]:
    """The tags git could not date to ask the API about: at most
    `API_DATE_CAP`, the highest versions first."""
    return sorted(undated, key=version_key, reverse=True)[:API_DATE_CAP]
