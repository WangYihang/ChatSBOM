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
from datetime import datetime
from datetime import timezone
from pathlib import Path

import pytest

from chatsbom.core.documents import Document
from chatsbom.core.instants import mtime
from chatsbom.core.instants import utc
from chatsbom.warehouse import store

UTC = timezone.utc

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


def first_had_before(sbom: Document, *roots: Path) -> datetime:
    """`store._first_had` as it was before #187: an rglob, and two stats
    of each file it found. What it said is what it says."""
    earliest = sbom.observed_at
    for root in roots:
        try:
            files = [path for path in root.rglob('*') if path.is_file()]
        except OSError:
            files = []
        for path in files:
            earliest = min(earliest, mtime(path, default=earliest))
    return utc(earliest)


def written(path: Path, when: datetime, text: str = '{}') -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    os.utime(path, (when.timestamp(), when.timestamp()))
    return path


SCANNED = datetime(2026, 9, 14, 10, 0, tzinfo=UTC)


def document(observed_at: datetime = SCANNED) -> Document:
    return Document(body={}, observed_at=observed_at, origin='sbom.json')


class TestACommitsDate:
    """When the store first had a commit: the earliest of its document
    and every file under its content root and its tree's directory
    (#180). Each file is asked once, by the entry that listed it."""

    @pytest.fixture
    def roots(self, tmp_path: Path) -> tuple[Path, Path]:
        content = tmp_path / '06-github-content' / '1' / A
        tree = tmp_path / '05-github-tree' / '1' / A
        written(content / 'package.json', datetime(2026, 9, 1, tzinfo=UTC))
        written(
            content / 'a' / 'b' / 'go.mod', datetime(2026, 8, 3, tzinfo=UTC),
        )
        (content / 'empty').mkdir()
        written(tree / 'tree.txt', datetime(2026, 8, 20, tzinfo=UTC))
        written(tree / 'manifests.json', datetime(2026, 9, 2, tzinfo=UTC))
        return content, tree

    def test_it_is_the_earliest_file_under_either_root(
        self, roots: tuple[Path, Path],
    ) -> None:
        assert store._first_had(document(), *roots) == (
            datetime(2026, 8, 3, tzinfo=UTC)
        )

    def test_it_is_the_documents_where_that_is_earlier(
        self, roots: tuple[Path, Path],
    ) -> None:
        early = datetime(2026, 7, 1, 12, 30, tzinfo=UTC)
        assert store._first_had(document(early), *roots) == early

    def test_it_is_what_the_rglob_said(
        self, roots: tuple[Path, Path], tmp_path: Path,
    ) -> None:
        """Links included: a link to a file is that file, dated by it;
        a link to a directory is not walked into, nor a dangling one
        read; a root that is not there has nothing."""
        content, tree = roots
        elsewhere = tmp_path / 'elsewhere'
        written(elsewhere / 'old.txt', datetime(2025, 1, 1, tzinfo=UTC))
        (content / 'linked').symlink_to(elsewhere)
        written(tmp_path / 'target.txt', datetime(2026, 8, 1, tzinfo=UTC))
        (content / 'a' / 'file-link').symlink_to(tmp_path / 'target.txt')
        (content / 'dangling').symlink_to(tmp_path / 'nowhere')
        missing = tmp_path / 'missing'
        for sbom in (document(), document(datetime(2026, 7, 1, tzinfo=UTC))):
            assert store._first_had(sbom, content, tree, missing) == (
                first_had_before(sbom, content, tree, missing)
            )
        assert store._first_had(document(), content, tree) == (
            datetime(2026, 8, 1, tzinfo=UTC)
        )

    def test_a_file_is_not_asked_twice(
        self, roots: tuple[Path, Path], stats: list[str],
    ) -> None:
        store._first_had(document(), *roots)
        assert stats == []
