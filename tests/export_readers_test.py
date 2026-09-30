"""Who can read the Parquet export, and what they find: anyone, and a
whole export (#154).

The site serves the export as the collector writes it, from a
read-only mount of data/export, as `web`: uid 10003, neither the
collector's UID nor in its group (Dockerfile.web). So the export gives
what it writes its modes outright, as a snapshot is published (#150,
tests/snapshot/publish_test.py): the directory anyone's to list and
enter, each table's file anyone's to read and no one's to write, since
its name is its content, and the manifest anyone's to read, whatever
umask the collector runs with. A host's may be 077, where Docker gives
a container 022.

And what the site finds there is whole. The manifest is read as each
request is answered, and was written in place: a reader could find
half of it, and a full disk left half of it for a week. It is written
aside and renamed over the last now, and each file it names is on disk
before it names it: the site serves them for good.
"""
from __future__ import annotations

import json
import os
import re
import shutil
import tempfile
from collections.abc import Iterator
from pathlib import Path

import pytest

from chatsbom.export.parquet import export_warehouse
from chatsbom.export.parquet import MANIFEST_NAME
from tests.conftest import FullDisk
from tests.snapshot.conftest import Corpus
from tests.snapshot.conftest import repository
from tests.snapshot.conftest import shop
from tests.snapshot.conftest import warehouse
from tests.snapshot.publish_test import as_web
from tests.snapshot.publish_test import mode

pytest.importorskip('pyarrow')


@pytest.fixture
def closed_umask() -> Iterator[None]:
    """A umask that gives no one but the owner anything, as a host's
    may."""
    before = os.umask(0o077)
    try:
        yield
    finally:
        os.umask(before)


@pytest.fixture
def shared() -> Iterator[Path]:
    """A directory any uid can reach. tmp_path is below one only its
    owner may enter."""
    directory = Path(tempfile.mkdtemp(prefix='export-readers-'))
    directory.chmod(0o755)
    try:
        yield directory
    finally:
        shutil.rmtree(directory)


def grown() -> Corpus:
    """The shop and one repository more: another export."""
    corpus = shop()
    corpus.repositories.append(repository(5, 'acme', 'gems', 200, 'Ruby'))
    assert corpus.corpus is not None
    corpus.corpus.add(5)
    return corpus


def named(directory: Path) -> list[str]:
    """The files the manifest in `directory` names."""
    said = json.loads((directory / MANIFEST_NAME).read_text(encoding='utf-8'))
    return [entry['name'] for entry in said['files']]


class TestWhoCanRead:
    """Anyone, whatever umask the export ran with."""

    def test_anyone_whatever_the_umask(
        self, shared: Path, closed_umask: None,
    ) -> None:
        directory = shared / 'export'
        result = export_warehouse(
            warehouse(shared / 'warehouse.duckdb', shop()), directory,
        )

        assert mode(directory) == 0o755
        assert mode(directory / MANIFEST_NAME) == 0o644
        assert sorted(result.files.values()) == sorted(named(directory))
        for name in result.files.values():
            assert mode(directory / name) == 0o444, name
        # What `web` does: the manifest, then each file it names.
        with as_web():
            files = named(directory)
            sizes = [len((directory / name).read_bytes()) for name in files]
        assert sizes == [result.sizes[name] for name in files]

    def test_and_again_when_a_table_has_not_changed(
        self, shared: Path, closed_umask: None,
    ) -> None:
        """A table the next export writes the same bytes of keeps its
        name, and is written over: anyone's still."""
        directory = shared / 'export'
        store = warehouse(shared / 'warehouse.duckdb', shop())
        export_warehouse(store, directory)
        result = export_warehouse(store, directory)

        for name in result.files.values():
            assert mode(directory / name) == 0o444, name
        assert mode(directory / MANIFEST_NAME) == 0o644

    def test_a_directory_made_by_hand_is_opened_by_the_export(
        self, tmp_path: Path, closed_umask: None,
    ) -> None:
        """`web` mounts data/export and is not made without it, so it may
        be made by hand before the first export, and under such a umask
        it is its maker's alone."""
        directory = tmp_path / 'export'
        directory.mkdir()
        assert mode(directory) == 0o700

        export_warehouse(warehouse(tmp_path / 'w.duckdb', shop()), directory)

        assert mode(directory) == 0o755

    def test_what_else_the_directory_allows_stays(
        self, tmp_path: Path,
    ) -> None:
        """What a reader needs is added, and nothing taken away: a
        directory its group may write, say, stays so."""
        directory = tmp_path / 'export'
        directory.mkdir(mode=0o770)
        directory.chmod(0o2770)

        export_warehouse(warehouse(tmp_path / 'w.duckdb', shop()), directory)

        assert mode(directory) == 0o2775


class TestWhatTheSiteFinds:
    """A whole export: the last one, or the next, and never part of
    one."""

    def test_a_manifest_cut_short_leaves_the_last_one_whole(
        self, tmp_path: Path, full_disk: FullDisk,
    ) -> None:
        """A full disk, halfway through the next manifest. Written in
        place, it left half of it, which the site would serve for five
        minutes at a time until the next export, a week later; written
        aside, the last is as it was, and so are the files it names."""
        directory = tmp_path / 'export'
        export_warehouse(warehouse(tmp_path / 'a.duckdb', shop()), directory)
        before = (directory / MANIFEST_NAME).read_bytes()
        grown_store = warehouse(tmp_path / 'b.duckdb', grown())

        full_disk.fill(directory)
        with pytest.raises(OSError):
            export_warehouse(grown_store, directory)
        full_disk.free()

        assert (directory / MANIFEST_NAME).read_bytes() == before
        for name in named(directory):
            assert (directory / name).is_file(), name
        assert [
            path.name for path in directory.iterdir()
            if path.name.startswith('.')
        ] == []

    def test_each_file_is_on_disk_before_the_manifest_names_it(
        self, tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Each is synced under the name it is written under, before it
        takes its own; and the manifest, written aside, after them all.
        Otherwise a crash soon after an export could leave a manifest
        naming a file whose bytes never reached the disk, which the site
        would serve for good."""
        synced: list[str] = []
        real = os.fsync

        def fsync(descriptor: int) -> None:
            synced.append(
                Path(os.readlink(f'/proc/self/fd/{descriptor}')).name)
            real(descriptor)

        directory = tmp_path / 'export'
        store = warehouse(tmp_path / 'w.duckdb', shop())
        monkeypatch.setattr(os, 'fsync', fsync)
        result = export_warehouse(store, directory)
        monkeypatch.undo()

        def first(name: str) -> int:
            """Where the file written aside for `name` was synced."""
            aside = re.compile(rf'\.{re.escape(name)}\.[0-9a-f]{{32}}\.tmp')
            found = [i for i, seen in enumerate(
                synced) if aside.fullmatch(seen)]
            assert found, (name, synced)
            return found[0]

        tables = [first(f'{table}.parquet') for table in result.files]
        assert len(tables) == 4
        assert max(tables) < first(MANIFEST_NAME)
