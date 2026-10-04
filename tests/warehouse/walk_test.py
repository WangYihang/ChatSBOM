"""How `warehouse build` walks the store: with as few metadata calls per
entry as it can make (#187).

The store is on a disk where every entry visited is a seek, so a walk
that asks the file system once per entry for what `readdir` already
said, a directory or a file, costs as much again as the walk. These
hold each walk to what it found before and to the calls it makes.
"""
from __future__ import annotations

import os
from collections.abc import Iterator
from pathlib import Path

import pytest

from chatsbom.warehouse import store

A = 'a' * 40
B = 'b' * 40


@pytest.fixture
def stats(monkeypatch: pytest.MonkeyPatch) -> Iterator[list[str]]:
    """Every `os.stat` and `os.lstat` made, by path: `pathlib` asks
    through them, and `os.DirEntry` does not."""
    made: list[str] = []
    stat, lstat = os.stat, os.lstat

    def counted_stat(path, *args, **kwargs):
        made.append(str(path))
        return stat(path, *args, **kwargs)

    def counted_lstat(path, *args, **kwargs):
        made.append(str(path))
        return lstat(path, *args, **kwargs)

    monkeypatch.setattr(os, 'stat', counted_stat)
    monkeypatch.setattr(os, 'lstat', counted_lstat)
    yield made


def stage(root: Path) -> Path:
    """A stage root as the collector leaves one: repositories by id, a
    list beside them, and a repository whose directory is a link."""
    for repository_id in (30, 4, 100):
        (root / str(repository_id) / A).mkdir(parents=True)
    (root / '4' / B).mkdir()
    (root / '4' / 'notes.txt').write_text('not a scan')
    (root / 'go.jsonl').write_text('')
    (root / '_migration').mkdir()
    (root / '7').symlink_to(root / '30')
    return root


class TestAStageRoot:

    def test_its_repositories_are_in_id_order(self, tmp_path: Path) -> None:
        root = stage(tmp_path / '07-sbom')
        assert [
            (repository_id, path.name)
            for repository_id, path in store._numbered(root)
        ] == [(4, '4'), (7, '7'), (30, '30'), (100, '100')]

    def test_a_repositorys_children_are_its_directories(
        self, tmp_path: Path,
    ) -> None:
        root = stage(tmp_path / '07-sbom')
        assert sorted(p.name for p in store._children(root / '4')) == [A, B]
        assert store._children(root / 'missing') == []

    def test_it_is_listed_without_a_stat_per_entry(
        self, tmp_path: Path, stats: list[str],
    ) -> None:
        root = stage(tmp_path / '07-sbom')
        stats.clear()
        found = [path for _, path in store._numbered(root)]
        for path in found:
            store._children(path)
        assert stats == []
