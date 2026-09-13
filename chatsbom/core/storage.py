import os
from pathlib import Path
from threading import Lock
from typing import Any

import structlog

from chatsbom.models.repository import Repository

logger = structlog.get_logger('storage')


class Storage:
    """Manages file persistence and deduplication for collected repository links."""

    def __init__(self, filepath: str | Path):
        self.filepath = Path(filepath)
        self.visited_ids: set[int] = set()
        self.min_stars_seen: float = float('inf')
        self._lock = Lock()
        self.filepath.parent.mkdir(parents=True, exist_ok=True)
        self._load_existing()

    def _load_existing(self):
        if not self.filepath.exists():
            return

        count = 0
        try:
            with open(self.filepath, encoding='utf-8') as f:
                for line in f:
                    if line.strip():
                        try:
                            repo = Repository.model_validate_json(line)
                            self.visited_ids.add(repo.id)
                            self.min_stars_seen = min(
                                self.min_stars_seen, repo.stars,
                            )
                            count += 1
                        except Exception:
                            pass
            logger.info(
                f"Loaded {count} existing records. Min stars: {self.min_stars_seen}",
            )
        except Exception as e:
            logger.error(f"Failed to load existing data: {e}")

    def save(self, item: Any, replace: bool = False) -> bool:
        """Persist a record. Returns True if the file was written.

        Deduplication by repository id is right for a discovery sweep —
        the same repository appearing twice in paginated search results
        should be written once. It is wrong for continuous collection,
        where re-collecting a repository is precisely the case where the
        record changed, and dropping the write silently loses its new
        releases. `replace=True` rewrites the stored record instead.
        """
        repo = Repository.model_validate(
            item,
        ) if isinstance(item, dict) else item

        with self._lock:
            known = repo.id in self.visited_ids

            if known and not replace:
                return False
            if known:
                self._rewrite_replacing(repo)
                return True

            self.visited_ids.add(repo.id)
            with open(self.filepath, 'a', encoding='utf-8') as f:
                f.write(repo.model_dump_json(exclude_none=True) + '\n')
                f.flush()
        return True

    def _rewrite_replacing(self, replacement: Repository) -> None:
        """Rewrite the ledger with one record swapped out.

        JSONL has no in-place update, so the whole file is rewritten
        through a temporary file and renamed — an interrupted rewrite
        leaves the original intact rather than a truncated ledger.
        """
        temp = self.filepath.with_suffix(self.filepath.suffix + '.tmp')
        line = replacement.model_dump_json(exclude_none=True) + '\n'

        try:
            with open(self.filepath, encoding='utf-8') as src, \
                    open(temp, 'w', encoding='utf-8') as dst:
                for raw in src:
                    if not raw.strip():
                        continue
                    try:
                        existing = Repository.model_validate_json(raw)
                    except Exception:
                        # Preserve anything we cannot parse rather than
                        # dropping it during an unrelated update.
                        dst.write(raw)
                        continue
                    dst.write(line if existing.id == replacement.id else raw)
                dst.flush()
                os.fsync(dst.fileno())
            temp.replace(self.filepath)
        except OSError as e:
            temp.unlink(missing_ok=True)
            logger.error(
                'Could not replace record',
                repo=f'{replacement.owner}/{replacement.repo}', error=str(e),
            )
            raise


def load_jsonl(filepath: str | Path) -> list[Repository]:
    """Loads records from a JSONL file into Repository objects."""
    path = Path(filepath)
    if not path.exists():
        return []

    records = []
    with path.open(encoding='utf-8') as f:
        for line in f:
            if line.strip():
                try:
                    records.append(Repository.model_validate_json(line))
                except Exception:
                    pass
    return records
