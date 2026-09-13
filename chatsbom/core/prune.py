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

Layout assumed throughout, which is what the stage directories already
produce:

    <stage>/<language>/<owner>/<repo>/<ref>/<sha>/...
"""
import shutil
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import structlog

logger = structlog.get_logger('prune')

#: Depth of a scan directory below a stage root: language/owner/repo/ref/sha.
SCAN_DEPTH = 5

#: Identity of one repository within a stage directory.
RepoKey = tuple[str, str, str]


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


def _scan_dirs(root: Path) -> Iterator[Path]:
    """Every directory exactly `SCAN_DEPTH` levels below `root`."""
    if not root.is_dir():
        return

    frontier = [(root, 0)]
    while frontier:
        directory, depth = frontier.pop()
        try:
            children = [c for c in directory.iterdir() if c.is_dir()]
        except OSError:
            continue

        if depth == SCAN_DEPTH - 1:
            yield from children
            continue

        frontier.extend((c, depth + 1) for c in children)


def scan_dirs_for(root: Path) -> dict[RepoKey, list[Path]]:
    """Scan directories under a stage root, grouped by repository.

    Directories at an unexpected depth are ignored rather than guessed
    at: deleting something because a path looked plausible is not a
    trade worth making.
    """
    grouped: dict[RepoKey, list[Path]] = {}
    for scan in _scan_dirs(root):
        language, owner, repo = scan.parts[-5:-2]
        grouped.setdefault((language, owner, repo), []).append(scan)
    return grouped


def prune_scan_dirs(
    root: Path,
    keep: int,
    dry_run: bool = False,
) -> PruneReport:
    """Keep the `keep` newest scans per repository under `root`.

    Recency is directory mtime, which the pipeline sets when it writes the
    scan. Raises rather than accepting `keep < 1`: removing every scan is
    a mistake, not a retention policy.
    """
    if keep < 1:
        raise ValueError(f'keep must be >= 1, got {keep}')

    report = PruneReport(dry_run=dry_run)

    for (language, owner, repo), scans in scan_dirs_for(root).items():
        if len(scans) <= keep:
            report += PruneReport(kept=len(scans), dry_run=dry_run)
            continue

        by_age = sorted(scans, key=lambda p: p.stat().st_mtime, reverse=True)
        retained, expired = by_age[:keep], by_age[keep:]

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
            repo=f'{owner}/{repo}',
            language=language,
            removed=removed,
            kept=len(retained),
            dry_run=dry_run,
        )
        report += PruneReport(
            removed=removed,
            kept=len(retained),
            bytes_freed=freed,
            dry_run=dry_run,
        )

    return report
