import os
from pathlib import Path
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
        os.makedirs(self.filepath.parent, exist_ok=True)
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

    def save(self, item: Any) -> bool:
        """Saves an item if it hasn't been seen before. Returns True if saved."""
        if isinstance(item, dict):
            repo = Repository.model_validate(item)
        else:
            repo = item

        if repo.id in self.visited_ids:
            return False

        self.visited_ids.add(repo.id)

        with open(self.filepath, 'a', encoding='utf-8') as f:
            f.write(repo.model_dump_json(exclude_none=True) + '\n')
            f.flush()
        return True


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
