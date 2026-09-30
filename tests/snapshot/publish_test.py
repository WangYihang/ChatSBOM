"""Publishing a snapshot: atomic, the last three kept (#132).

A written snapshot is renamed into `snapshots/<id>.sqlite`, then
`CURRENT` is replaced by a rename. Its first line names the snapshot
readers open, and the lines after it the two published before, which
are kept; only once it has moved is a snapshot it does not list
removed. A reader holds whatever file it opened, and `CURRENT` only
ever names a complete one, whatever step a pass stopped at.

`publish` catches nothing, so a failure at a step leaves on disk what a
crash there would. The test of each step fails every call that changes
the disk, in turn, and checks what is left.
"""
from __future__ import annotations

import dataclasses
import fcntl
import os
import shutil
import stat
import tempfile
import uuid
from collections.abc import Iterator
from contextlib import closing
from contextlib import contextmanager
from pathlib import Path
from typing import Any

import pytest

from chatsbom.dataset.open import connect
from chatsbom.dataset.open import current
from chatsbom.dataset.open import open_dataset
from chatsbom.server import settings
from chatsbom.snapshot.build import build
from chatsbom.snapshot.publish import clear
from chatsbom.snapshot.publish import KEEP
from chatsbom.snapshot.publish import publish
from chatsbom.snapshot.publish import SnapshotBusy
from chatsbom.snapshot.write import write
from chatsbom.snapshot.write import Written
from tests.snapshot.conftest import shop
from tests.snapshot.conftest import warehouse

#: Snapshots of five different contents, written once.
LIBRARY = 5


@pytest.fixture(scope='module')
def library(tmp_path_factory: pytest.TempPathFactory) -> list[Written]:
    """Snapshots of `SHOP` with app starred 300, 301, ... times."""
    written = []
    for n in range(LIBRARY):
        directory = tmp_path_factory.mktemp(f'content{n}')
        corpus = shop()
        corpus.repositories[0]['stars'] = 300 + n
        written.append(
            write(
                warehouse(directory / 'warehouse.duckdb', corpus),
                directory / 'written',
            ),
        )
    assert len({w.id for w in written}) == LIBRARY
    return written


def fresh(written: Written, directory: Path) -> Written:
    """A copy of `written`, as a pass would have left it for `publish`,
    which takes the file it is given."""
    directory.mkdir(parents=True, exist_ok=True)
    copy = directory / f'.building-{uuid.uuid4().hex}.sqlite'
    shutil.copyfile(written.path, copy)
    return dataclasses.replace(written, path=copy)


def snapshots(directory: Path) -> set[str]:
    """The ids of the snapshots in `directory`."""
    return {
        path.name.removesuffix('.sqlite') for path in directory.iterdir()
        if path.name.endswith('.sqlite') and not path.name.startswith('.')
    }


def listed(directory: Path) -> list[str]:
    """What `CURRENT` says: the current id, then those kept."""
    return (directory / 'CURRENT').read_text().splitlines()


def named(directory: Path) -> str | None:
    """The id `CURRENT` names, once the snapshot it names is found
    complete; None where nothing has been published."""
    if not (directory / 'CURRENT').exists():
        return None
    path = current(directory)
    with closing(connect(path)) as connection:
        assert connection.execute('PRAGMA integrity_check').fetchall() == [
            ('ok',),
        ]
        [(snapshot,)] = connection.execute(
            'SELECT snapshot FROM meta',
        ).fetchall()
    assert path.name == f'{snapshot}.sqlite'
    return str(snapshot)


class TestPublishing:

    def test_names_the_file_by_its_id_and_current_by_that(
        self, library: list[Written], tmp_path: Path,
    ) -> None:
        published = publish(fresh(library[0], tmp_path / 'in'), tmp_path)

        assert published.changed
        assert published.path == tmp_path / f'{library[0].id}.sqlite'
        assert (tmp_path / 'CURRENT').read_text() == f'{library[0].id}\n'
        assert named(tmp_path) == library[0].id
        assert {p.name for p in tmp_path.iterdir()} == {
            'CURRENT', f'{library[0].id}.sqlite', 'in',
        }
        assert list((tmp_path / 'in').iterdir()) == []

    def test_the_same_content_again_publishes_nothing(
        self, library: list[Written], tmp_path: Path,
    ) -> None:
        """Q11: a pass whose data did not change. What it wrote goes, and
        nothing published is touched, CURRENT or the file."""
        publish(fresh(library[0], tmp_path / 'in'), tmp_path)
        before = {
            path.name: path.stat().st_mtime_ns for path in tmp_path.iterdir()
        }

        again = publish(fresh(library[0], tmp_path / 'in'), tmp_path)

        assert not again.changed
        assert again.removed == ()
        assert {
            path.name: path.stat().st_mtime_ns for path in tmp_path.iterdir()
            if path.name != 'in'
        } == {name: at for name, at in before.items() if name != 'in'}
        assert list((tmp_path / 'in').iterdir()) == []

    def test_a_pass_publishes_what_it_wrote(
        self, tmp_path: Path, library: list[Written],
    ) -> None:
        store = warehouse(tmp_path / 'warehouse.duckdb', shop())
        report = build(store, tmp_path / 'snapshots')

        assert report.published.changed
        assert named(tmp_path / 'snapshots') == report.written.id
        # SHOP's content is the library's first.
        assert report.written.id == library[0].id
        # A second pass over the same warehouse: nothing.
        again = build(store, tmp_path / 'snapshots')
        assert not again.published.changed
        assert {p.name for p in (tmp_path / 'snapshots').iterdir()} == {
            '.lock', 'CURRENT', f'{library[0].id}.sqlite',
        }


class TestRetention:

    def test_the_last_three_are_kept(
        self, library: list[Written], tmp_path: Path,
    ) -> None:
        assert KEEP == 3
        ids = [w.id for w in library]
        for n, written in enumerate(library):
            published = publish(fresh(written, tmp_path / 'in'), tmp_path)
            kept = ids[max(0, n - 2):n + 1]
            assert named(tmp_path) == ids[n]
            assert listed(tmp_path) == kept[::-1]
            assert snapshots(tmp_path) == set(kept)
            assert [p.name for p in published.removed] == (
                [f'{ids[n - 3]}.sqlite'] if n >= 3 else []
            )

    def test_one_published_again_is_the_newest(
        self, library: list[Written], tmp_path: Path,
    ) -> None:
        """The data went back to what an earlier snapshot served: that
        one is current again, and the newest of the three kept."""
        ids = [w.id for w in library]
        for written in library[:3]:
            publish(fresh(written, tmp_path / 'in'), tmp_path)
        back = publish(fresh(library[0], tmp_path / 'in'), tmp_path)
        assert back.changed
        assert listed(tmp_path) == [ids[0], ids[2], ids[1]]

        publish(fresh(library[3], tmp_path / 'in'), tmp_path)
        # Published last: 3, then 0, then 2. 1 goes.
        assert listed(tmp_path) == [ids[3], ids[0], ids[2]]
        assert snapshots(tmp_path) == {ids[3], ids[0], ids[2]}

    def test_a_snapshot_current_does_not_list_goes(
        self, library: list[Written], tmp_path: Path,
    ) -> None:
        """One renamed into place by a pass that stopped before CURRENT
        named it, and not the content published next."""
        publish(fresh(library[0], tmp_path / 'in'), tmp_path)
        orphan = tmp_path / f'{library[4].id}.sqlite'
        shutil.copyfile(library[4].path, orphan)

        published = publish(fresh(library[1], tmp_path / 'in'), tmp_path)

        assert published.removed == (orphan,)
        assert snapshots(tmp_path) == {library[0].id, library[1].id}

    def test_what_is_not_a_snapshot_is_left_alone(
        self, library: list[Written], tmp_path: Path,
    ) -> None:
        mine = [
            'notes.txt', 'backup.sqlite', 'ABCDEF0123456789.sqlite',
            '0123456789abcdef.sqlite.bak', '.0123456789abcdef.sqlite',
        ]
        for name in mine:
            (tmp_path / name).write_text('mine')
        for written in library:
            publish(fresh(written, tmp_path / 'in'), tmp_path)
        for name in mine:
            assert (tmp_path / name).read_text() == 'mine'


class Failing:
    """`os` as `publish` sees it: each call that changes the disk is
    counted, and the `at`-th fails before it happens, or a `close`
    after it, so that nothing is left open."""

    CHANGES = frozenset({
        'open', 'write', 'close', 'fsync', 'replace', 'rename', 'utime',
        'unlink', 'remove', 'chmod', 'fchmod', 'mkdir',
    })

    def __init__(self, at: int | None = None) -> None:
        self.at = at
        self.calls: list[str] = []

    def __getattr__(self, name: str) -> Any:
        real = getattr(os, name)
        if name not in self.CHANGES:
            return real

        def call(*args: Any, **kwargs: Any) -> Any:
            self.calls.append(name)
            failing = len(self.calls) == self.at
            if failing and name != 'close':
                raise OSError(f'injected: {name}, call {self.at}')
            result = real(*args, **kwargs)
            if failing:
                raise OSError(f'injected: {name}, call {self.at}')
            return result

        return call


#: What is published before the pass that fails: nothing yet, one, or
#: as many as are kept, so that it removes one.
BEFORE = {'first': 0, 'second': 1, 'full': KEEP}


def run(
    library: list[Written],
    directory: Path,
    before: int,
    failing: Failing,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """`before` published, then the next with `os` as `failing` is."""
    for written in library[:before]:
        publish(fresh(written, directory / 'in'), directory)
    with monkeypatch.context() as patched:
        patched.setattr('chatsbom.snapshot.publish.os', failing)
        publish(fresh(library[before], directory / 'in'), directory)


class TestAFailureAtEachStep:

    @pytest.mark.parametrize('before', BEFORE.values(), ids=list(BEFORE))
    def test_leaves_current_naming_a_complete_snapshot(
        self,
        library: list[Written],
        tmp_path: Path,
        before: int,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        counting = Failing()
        run(library, tmp_path / 'counted', before, counting, monkeypatch)
        made = counting.calls
        # The file synced and renamed, and the directory synced; CURRENT
        # written aside, synced, renamed, and the directory synced; and
        # after it, what CURRENT no longer lists removed.
        assert made.count('replace') == 2
        assert made.count('fsync') == 4
        assert made.count('unlink') == (1 if before == KEEP else 0)
        if before == KEEP:
            assert made.index('unlink') > len(made) - 1 - made[::-1].index(
                'replace',
            )

        ids = [w.id for w in library]
        old = ids[before - 1] if before else None
        new = ids[before]
        for at in range(1, len(made) + 1):
            directory = tmp_path / f'at{at}'
            with pytest.raises(OSError, match='injected'):
                run(library, directory, before, Failing(at), monkeypatch)

            now = named(directory)
            assert now in {old, new}, (at, made[at - 1])
            if now == old:
                # Nothing was removed before CURRENT moved.
                assert snapshots(directory) >= set(ids[:before]), at

            # The next pass: what this one left goes, and the content is
            # published, as if nothing had stopped it.
            clear(directory)
            publish(fresh(library[before], directory / 'in'), directory)
            assert named(directory) == new
            kept = ids[max(0, before - 2):before + 1]
            assert listed(directory) == kept[::-1]
            assert snapshots(directory) == set(kept)
            assert [
                p.name for p in directory.iterdir() if p.name.startswith('.')
            ] == []

    def test_what_a_crash_left_goes_with_the_next_pass(
        self, tmp_path: Path, library: list[Written],
    ) -> None:
        """A pass killed while it wrote leaves its file, and one killed
        while it named the new snapshot, CURRENT's. The next pass, which
        holds the lock, removes them before it writes, and nothing
        else."""
        store = warehouse(tmp_path / 'warehouse.duckdb', shop())
        directory = tmp_path / 'snapshots'
        build(store, directory)
        left = [
            directory / f'.building-{uuid.uuid4().hex}.sqlite',
            directory / f'.CURRENT-{uuid.uuid4().hex}',
        ]
        for path in left:
            path.write_text('half')
        (directory / '.hidden').write_text('mine')

        build(store, directory)

        assert not any(path.exists() for path in left)
        assert (directory / '.hidden').read_text() == 'mine'
        assert named(directory) == library[0].id


class TestAPassThatFails:

    def test_while_it_writes_leaves_nothing_and_current_as_it_was(
        self,
        tmp_path: Path,
        library: list[Written],
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        directory = tmp_path / 'snapshots'
        build(warehouse(tmp_path / 'a.duckdb', shop()), directory)
        before = sorted(p.name for p in directory.iterdir())

        def broken(*args: object, **kwargs: object) -> None:
            raise RuntimeError('the index broke')

        monkeypatch.setattr('chatsbom.snapshot.write._index', broken)
        corpus = shop()
        corpus.repositories[0]['stars'] = 999
        with pytest.raises(RuntimeError, match='the index broke'):
            build(warehouse(tmp_path / 'b.duckdb', corpus), directory)

        assert sorted(p.name for p in directory.iterdir()) == before
        assert named(directory) == library[0].id

    def test_a_second_pass_while_one_runs_is_refused(
        self, tmp_path: Path,
    ) -> None:
        directory = tmp_path / 'snapshots'
        directory.mkdir()
        store = warehouse(tmp_path / 'warehouse.duckdb', shop())
        with (directory / '.lock').open('a') as held:
            fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
            with pytest.raises(SnapshotBusy):
                build(store, directory)
        assert sorted(p.name for p in directory.iterdir()) == ['.lock']


def test_a_reader_keeps_what_it_opened(
    library: list[Written], tmp_path: Path,
) -> None:
    """A reader that opened a snapshot reads it to the end, while newer
    ones are published and it is removed."""
    publish(fresh(library[0], tmp_path / 'in'), tmp_path)
    with closing(connect(current(tmp_path))) as reader:
        for written in library[1:]:
            publish(fresh(written, tmp_path / 'in'), tmp_path)
        assert library[0].id not in snapshots(tmp_path)
        assert reader.execute(
            'SELECT stars FROM repositories WHERE id = 1',
        ).fetchall() == [(300,)]


# -- who can read it (#150) ---------------------------------------------------

#: The uid `web` runs as (Dockerfile.web). It reads the snapshots
#: through a read-only mount of `data/snapshots`, which the collector
#: publishes as UID:GID: it is neither their owner nor in their group.
WEB_UID = 10003

#: Root reads as that uid; anyone else can only read as themselves, and
#: then the modes alone say what another uid could.
AS_ROOT = os.geteuid() == 0


def mode(path: Path) -> int:
    return stat.S_IMODE(path.stat().st_mode)


@contextmanager
def as_web() -> Iterator[None]:
    """What this process opens, opened as `web` would: uid and gid
    10003 and no other group, as root can take them and give them back;
    or as itself, when it is not root. In this process rather than in a
    new one run as that uid, which could not always start: an
    interpreter in root's own directory is not another uid's to run."""
    if not AS_ROOT:
        yield
        return
    groups, gid, uid = os.getgroups(), os.getegid(), os.geteuid()
    os.setgroups([])
    os.setegid(WEB_UID)
    os.seteuid(WEB_UID)
    try:
        yield
    finally:
        os.seteuid(uid)
        os.setegid(gid)
        os.setgroups(groups)


@pytest.fixture
def closed_umask() -> Iterator[None]:
    """A umask that gives no one but the owner anything, as a host's may,
    where Docker gives a container 022."""
    before = os.umask(0o077)
    try:
        yield
    finally:
        os.umask(before)


@pytest.fixture
def shared() -> Iterator[Path]:
    """A directory any uid can reach. tmp_path is below one only its
    owner may enter."""
    directory = Path(tempfile.mkdtemp(prefix='snapshot-readers-'))
    directory.chmod(0o755)
    try:
        yield directory
    finally:
        shutil.rmtree(directory)


class TestWhoCanRead:
    """Anyone: `web` reads what the collector publishes (#150), as a uid
    of its own. So the directory is anyone's to list and enter, `CURRENT`
    anyone's to read, and each snapshot anyone's to read and no one's to
    write, whatever umask the publisher ran with."""

    def test_anyone_whatever_the_umask(
        self, shared: Path, closed_umask: None,
    ) -> None:
        directory = shared / 'snapshots'
        report = build(
            warehouse(shared / 'warehouse.duckdb', shop()), directory,
        )

        assert mode(directory) & 0o755 == 0o755
        assert mode(directory / 'CURRENT') == 0o644
        assert mode(report.published.path) == 0o444
        # What `web` does with WEB_SNAPSHOT: its check as it starts,
        # then each question's pin of the snapshot `CURRENT` names.
        with as_web():
            settings.snapshot(str(directory))
            pinned = current(directory)
            with open_dataset(pinned) as dataset:
                meta = dataset.meta()
        assert pinned == report.published.path
        assert meta.schema_version == 'v8'

    def test_a_directory_made_by_hand_is_opened_by_the_first_pass(
        self, tmp_path: Path, closed_umask: None,
    ) -> None:
        """`web` mounts data/snapshots and will not start without it
        (#149), so it may be made by hand before anything is published,
        and under such a umask it is its maker's alone."""
        directory = tmp_path / 'snapshots'
        directory.mkdir()
        assert mode(directory) == 0o700

        build(warehouse(tmp_path / 'warehouse.duckdb', shop()), directory)

        assert mode(directory) == 0o755
        assert mode(directory / 'CURRENT') == 0o644

    def test_what_else_the_directory_allows_stays(
        self, tmp_path: Path,
    ) -> None:
        """What a reader needs is added, and nothing taken away: a
        directory its group may write, say, stays so."""
        store = warehouse(tmp_path / 'warehouse.duckdb', shop())
        directory = tmp_path / 'snapshots'
        directory.mkdir(mode=0o770)
        directory.chmod(0o2770)

        build(store, directory)

        assert mode(directory) == 0o2775
