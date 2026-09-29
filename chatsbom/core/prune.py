"""Retention for the intermediate pipeline stages.

A single snapshot already occupies 46 GB under `data/` and 30 GB under
`.cache/` — `07-sbom` 16 GB, `06-github-content` 9.8 GB,
`05-github-tree` 8.3 GB. Under continuous collection every new commit
produces another content tree and another SBOM, so that growth has no
ceiling, and the failure mode is the worst kind: collection stops
silently when the disk fills.

What can safely go is the intermediate artefacts. They are *inputs* —
recomputable from GitHub, and keyed by commit — while the history that
matters has already been appended to ClickHouse. So retention keeps the
N most recent scans per repository and discards the rest.

Layout assumed throughout, which is what the stage directories
produce since `data migrate-layout` (#55, owner decision D3):

    <stage>/<repository_id>/<sha>/...

Anything else under a stage root — the language-keyed directories of a
tree not yet migrated, `_migration`, a stray file — is left alone:
deleting something because a path looked plausible is not a trade worth
making.

**What the current scan descends from is never removed** (#100 Q13):
the scan the newest resolved commit decision points to, in every scan
root, whatever its mtime; and the release decision, the commit decision
and the release list it descends from (#147). Beside those, of the
decisions, each repository keeps the `keep` newest release decisions,
as it keeps that many scans: the current one and, by default, the one
before it, which shows what the last push changed, a new release or
none, which is all the early cutoff of #128 §2.1 turns on. An older
one says nothing its list does not, each release with its date, and
kept for every push it would be two inodes and two blocks a push (see
README, "The repository-keyed layout"). A commit decision is kept while
a kept release decision leads to it or its scan is kept; a list, while
a kept release decision names it.
"""
import re
import shutil
from collections.abc import Iterator
from collections.abc import Mapping
from dataclasses import dataclass
from dataclasses import field
from dataclasses import fields
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

import structlog

from chatsbom.core import decisions
from chatsbom.core.config import PathConfig
from chatsbom.core.layout import is_sha
from chatsbom.core.layout import key_name
from chatsbom.core.layout import RELEASE_LISTS

logger = structlog.get_logger('prune')

#: A list no decision names is left this long before it goes: its writer
#: writes it before the decision that will name it, and may be between
#: the two.
UNNAMED_GRACE = timedelta(days=1)

_LIST_NAME = re.compile(r'^[0-9a-f]{64}\.json$')

#: Depth of a scan directory below a stage root: repository_id/sha.
SCAN_DEPTH = 2

#: Identity of one repository within a stage directory: its id.
RepoKey = int


@dataclass(frozen=True, slots=True)
class PruneReport:
    """What a retention pass removed, or would have removed."""

    removed: int = 0
    kept: int = 0
    bytes_freed: int = 0
    dry_run: bool = False

    def __add__(self, other: 'PruneReport') -> 'PruneReport':
        return PruneReport(
            removed=self.removed + other.removed,
            kept=self.kept + other.kept,
            bytes_freed=self.bytes_freed + other.bytes_freed,
            dry_run=self.dry_run or other.dry_run,
        )


def _directory_size(path: Path) -> int:
    total = 0
    for child in path.rglob('*'):
        try:
            if child.is_file():
                total += child.stat().st_size
        except OSError:
            continue
    return total


def _scan_dirs(root: Path) -> Iterator[tuple[int, Path]]:
    """`(repository_id, scan directory)` for every `<id>/<sha>` below
    `root`."""
    if not root.is_dir():
        return
    try:
        repositories = [
            c for c in root.iterdir() if c.is_dir() and c.name.isdigit()
        ]
    except OSError:
        return
    for repository in repositories:
        try:
            children = list(repository.iterdir())
        except OSError:
            continue
        for child in children:
            if child.is_dir() and is_sha(child.name):
                yield int(repository.name), child


def scan_dirs_for(root: Path) -> dict[RepoKey, list[Path]]:
    """Scan directories under a stage root, grouped by repository.

    Directories at an unexpected depth, or not named like an id and a
    commit, are ignored rather than guessed at.
    """
    grouped: dict[RepoKey, list[Path]] = {}
    for repository_id, scan in _scan_dirs(root):
        grouped.setdefault(repository_id, []).append(scan)
    return grouped


def prune_scan_dirs(
    root: Path,
    keep: int,
    dry_run: bool = False,
    current: Mapping[RepoKey, str] | None = None,
    retained: dict[RepoKey, set[str]] | None = None,
) -> PruneReport:
    """Keep the `keep` newest scans per repository under `root`.

    Recency is directory mtime, which the pipeline sets when it writes the
    scan. Raises rather than accepting `keep < 1`: removing every scan is
    a mistake, not a retention policy.

    `current` is each repository's current scan, the commit its newest
    resolved commit decision points to (`current_scans`): kept whatever
    its age, and counted among the `keep` (#100 Q13). `retained`, when
    given, is told the commits kept of each repository, the ones a dry
    run would keep.
    """
    if keep < 1:
        raise ValueError(f'keep must be >= 1, got {keep}')

    report = PruneReport(dry_run=dry_run)

    for repository_id, scans in scan_dirs_for(root).items():
        head = (current or {}).get(repository_id)
        by_age = sorted(
            scans, key=lambda p: (p.name == head, p.stat().st_mtime),
            reverse=True,
        )
        kept, expired = by_age[:keep], by_age[keep:]
        if retained is not None:
            retained.setdefault(repository_id, set()).update(
                scan.name for scan in kept
            )
        if not expired:
            report += PruneReport(kept=len(kept), dry_run=dry_run)
            continue

        freed = 0
        removed = 0
        for scan in expired:
            size = _directory_size(scan)
            if not dry_run:
                try:
                    shutil.rmtree(scan)
                except OSError as e:
                    logger.warning(
                        'Could not remove scan', path=str(scan), error=str(e),
                    )
                    continue
            freed += size
            removed += 1

        logger.info(
            'Pruned scans',
            repository_id=repository_id,
            removed=removed,
            kept=len(kept),
            dry_run=dry_run,
        )
        report += PruneReport(
            removed=removed,
            kept=len(kept),
            bytes_freed=freed,
            dry_run=dry_run,
        )

    return report


# -- the decisions (#147) --------------------------------------------------


@dataclass(frozen=True, slots=True)
class DecisionReport:
    """What a retention pass did to the decisions, or would have done."""

    releases_kept: int = 0
    releases_removed: int = 0
    commits_kept: int = 0
    commits_removed: int = 0
    lists_kept: int = 0
    lists_removed: int = 0
    bytes_freed: int = 0
    #: Push and key directories whose decision could not be read, left.
    unreadable: int = 0
    dry_run: bool = False

    def __add__(self, other: 'DecisionReport') -> 'DecisionReport':
        counts = {
            item.name: getattr(self, item.name) + getattr(other, item.name)
            for item in fields(self) if item.name != 'dry_run'
        }
        return DecisionReport(**counts, dry_run=self.dry_run or other.dry_run)


def current_scans(paths: PathConfig) -> dict[RepoKey, str]:
    """Each repository's current scan, as its decisions have it: the
    commit its newest resolved chain names (`decisions.newest_resolved`).
    A repository with none is not in it."""
    found: dict[RepoKey, str] = {}
    for repository_id in _numbered(paths.release_dir):
        chain = decisions.newest_resolved(paths, repository_id)
        if chain is not None and chain.commit is not None:
            found[repository_id] = chain.commit.commit_sha
    return found


def prune_decisions(
    paths: PathConfig,
    keep: int,
    *,
    scans: Mapping[RepoKey, set[str]] | None = None,
    dry_run: bool = False,
    now: datetime | None = None,
) -> DecisionReport:
    """Keep what the current scan descends from, and the `keep` newest
    release decisions of each repository, with what they name; remove
    the older ones (the module's docstring says why).

    `scans` is the commits of each repository whose scans are kept
    (`prune_scan_dirs`'s `retained`): a commit decision that resolved
    to one is kept too. `now` decides which lists no decision names are
    old enough to go (`UNNAMED_GRACE`).
    """
    if keep < 1:
        raise ValueError(f'keep must be >= 1, got {keep}')
    cutoff = (now or datetime.now(timezone.utc)) - UNNAMED_GRACE
    report = DecisionReport(dry_run=dry_run)
    ids = sorted(
        set(_numbered(paths.release_dir)) | set(_numbered(paths.commit_dir)),
    )
    for repository_id in ids:
        report += _prune_repository(
            paths, repository_id, keep,
            scans=(scans or {}).get(repository_id, set()),
            dry_run=dry_run, cutoff=cutoff,
        )
    return report


@dataclass
class _Plan:
    """One repository's decisions, sorted into kept and removed."""

    remove: list[Path] = field(default_factory=list)
    kept_releases: int = 0
    kept_commits: int = 0
    kept_lists: int = 0
    releases_removed: int = 0
    commits_removed: int = 0
    lists_removed: int = 0
    unreadable: int = 0


def _prune_repository(
    paths: PathConfig,
    repository_id: int,
    keep: int,
    *,
    scans: set[str],
    dry_run: bool,
    cutoff: datetime,
) -> DecisionReport:
    plan = _Plan()
    read: list[tuple[Path, decisions.ReleaseDecision]] = []
    for directory in reversed(decisions.pushes(paths, repository_id)):
        decision = decisions.read_release(directory, repository_id)
        if decision is None:
            # A decision this code cannot read may be a later version's,
            # which may name a list: older code deletes nothing of it.
            plan.unreadable += 1
        else:
            read.append((directory, decision))
    unread_releases = plan.unreadable

    kept = read[:keep]
    resolved = next(
        (
            (directory, decision) for directory, decision in read
            if decisions.commit_decision(
                paths, repository_id, decision.key,
            ) is not None
        ),
        None,
    )
    if resolved is not None and resolved not in kept:
        kept.append(resolved)
    plan.kept_releases = len(kept)
    for directory, decision in read:
        if (directory, decision) not in kept:
            plan.remove.append(directory)
            plan.releases_removed += 1

    # The commit decisions the kept release decisions lead to, and those
    # whose scan is kept. With no release decision to go by, all of them.
    led_to = {key_name(decision.key) for _, decision in kept}
    for directory in decisions.keys(paths, repository_id):
        commit = decisions.read_commit(directory, repository_id)
        if commit is None:
            plan.unreadable += 1
            continue
        if not read or directory.name in led_to or commit.commit_sha in scans:
            plan.kept_commits += 1
            continue
        plan.remove.append(directory)
        plan.commits_removed += 1

    named = {decision.releases for _, decision in kept}
    lists_dir = decisions.releases_dir(paths, repository_id) / RELEASE_LISTS
    for listing in sorted(_lists(lists_dir)):
        if (
            unread_releases or not read or listing.stem in named
            or _modified(listing) > cutoff
        ):
            plan.kept_lists += 1
            continue
        plan.remove.append(listing)
        plan.lists_removed += 1

    freed = 0
    for path in plan.remove:
        size = _directory_size(path) if path.is_dir() else _size(path)
        if not dry_run:
            try:
                if path.is_dir():
                    shutil.rmtree(path)
                else:
                    path.unlink()
            except OSError as e:
                logger.warning(
                    'Could not remove decision', path=str(path), error=str(e),
                )
                continue
        freed += size
    if plan.remove:
        logger.info(
            'Pruned decisions',
            repository_id=repository_id,
            releases_removed=plan.releases_removed,
            commits_removed=plan.commits_removed,
            lists_removed=plan.lists_removed,
            dry_run=dry_run,
        )
    return DecisionReport(
        releases_kept=plan.kept_releases,
        releases_removed=plan.releases_removed,
        commits_kept=plan.kept_commits,
        commits_removed=plan.commits_removed,
        lists_kept=plan.kept_lists,
        lists_removed=plan.lists_removed,
        bytes_freed=freed,
        unreadable=plan.unreadable,
        dry_run=dry_run,
    )


def _numbered(root: Path) -> list[RepoKey]:
    """The repository ids with a directory under `root`."""
    try:
        return sorted(
            int(child.name) for child in root.iterdir()
            if child.name.isdigit() and child.is_dir()
        )
    except OSError:
        return []


def _lists(directory: Path) -> list[Path]:
    """The release lists in `directory`: files named by a digest. A
    temporary file a writer left, or anything else, is not one."""
    try:
        return [
            child for child in directory.iterdir()
            if _LIST_NAME.match(child.name) and child.is_file()
        ]
    except OSError:
        return []


def _size(path: Path) -> int:
    try:
        return path.stat().st_size
    except OSError:
        return 0


def _modified(path: Path) -> datetime:
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    except OSError:
        return datetime.max.replace(tzinfo=timezone.utc)
