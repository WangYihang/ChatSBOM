"""A pass reads only the repositories whose store changed, and carries
every other one over from the warehouse before it (#187).

What it builds is what a pass that reads the whole store builds, table
for table and row for row, scan ids included; only the `build` row,
which says how it was built, differs. That is held here across the
ways the store changes: a collection, a Syft document written again in
place, `data prune`, a fetch of the graph, a decision, a record, a
search snapshot.
"""
from __future__ import annotations

import json
import os
import shutil
from collections.abc import Callable
from collections.abc import Iterator
from datetime import date
from datetime import datetime
from pathlib import Path
from typing import Any

import duckdb
import pytest

from chatsbom.core.fs import atomic_write_bytes
from chatsbom.core.prune import prune_scan_dirs
from chatsbom.warehouse import carry
from chatsbom.warehouse.build import build
from tests.warehouse.conftest import artifact
from tests.warehouse.conftest import at
from tests.warehouse.conftest import Listed
from tests.warehouse.conftest import rows
from tests.warehouse.conftest import spdx
from tests.warehouse.conftest import Store
from tests.warehouse.conftest import TODAY

A = 'a' * 40
B = 'b' * 40
C = 'c' * 40

FEB = at(2026, 2, 11, 9, 30)
SEP = at(2026, 9, 14, 10, 0)

APP = Listed(1, 'acme', 'app', stars=500, language='Java')
WEB = Listed(2, 'acme', 'web', stars=900, language='JavaScript')
GRAPHED = Listed(3, 'acme', 'graphed', stars=1200, language='Kotlin')
LISTED = Listed(4, 'acme', 'listed', stars=50, language='Go')

PACKAGE_JSON = '{"dependencies": {"react": "^18.2.0"}}'
BUILD_GRADLE = """
dependencies {
    implementation 'com.google.guava:guava:33.0.0-jre'
}
"""


@pytest.fixture(autouse=True)
def settled(monkeypatch: pytest.MonkeyPatch) -> None:
    """What a test writes is settled at once: a directory written in the
    last seconds before a pass is otherwise read again by the next, in
    case it changes again within its timestamp's tick."""
    monkeypatch.setattr(carry, 'SETTLE_NS', 0)


@pytest.fixture
def corpus(store: Store) -> Store:
    """Four repositories of a complete snapshot: `acme/app` scanned at
    two commits, with its graph fetched; `acme/web` at one, with its
    decisions; `acme/graphed` with a graph and no record; `acme/listed`
    with nothing collected. And a repository nothing names, with a
    graph."""
    store.snapshot(date(2026, 9, 1), APP, WEB, GRAPHED, LISTED, complete=True)
    store.sbom(1, A, artifact('guava', '32.1.0-jre', 'java-archive'), at=FEB)
    store.content(1, A, {'build.gradle': BUILD_GRADLE}, at=FEB)
    store.tree(1, A, ['build.gradle'], at=FEB)
    store.sbom(1, B, artifact('guava', '33.0.0-jre', 'java-archive'), at=SEP)
    store.content(1, B, {'sub/build.gradle': BUILD_GRADLE}, at=SEP)
    store.tree(1, B, ['sub/build.gradle'], at=SEP)
    store.record(1, 'acme', 'app', commit=B, ref='v2.0.0', listing='java')
    store.graph(
        1,
        spdx(
            '2026-09-13T08:00:00Z',
            [('guava', '33.0.0', 'maven'), ('jsr305', '3.0.2', 'maven')],
            direct=['guava'], edges=[('guava', 'jsr305')],
        ),
        fetched=at(2026, 9, 13, 8, 0, 30), head=B,
    )
    store.sbom(2, A, artifact('react', '18.2.0', 'npm'), at=SEP)
    store.content(2, A, {'package.json': PACKAGE_JSON}, at=SEP)
    store.decide(
        2, pushed_at='2026-09-10T00:00:00Z',
        releases=[{
            'id': 21, 'tag_name': 'v1.0.0', 'name': 'one',
            'published_at': '2026-09-09T00:00:00Z', 'prerelease': False,
        }],
        latest='v1.0.0', commit=A, ref='v1.0.0', ref_type='tag',
    )
    store.record(2, 'acme', 'web', commit=A, listing='javascript')
    store.legacy_graph(
        3,
        spdx(
            '2026-02-01T00:00:00Z', [
                ('left', '1', 'npm'),
                ('pad', '2', 'npm'),
            ],
            direct=['left'], edges=[('left', 'pad')],
        ),
        mtime=FEB,
    )
    store.graph(
        9,
        spdx(
            '2026-08-01T00:00:00Z', [
                ('left', '1', 'npm'),
                ('pad', '2', 'npm'),
            ],
            direct=['left'], edges=[('left', 'pad')],
        ),
        fetched=at(2026, 8, 1),
    )
    return store


Built = Callable[..., duckdb.DuckDBPyConnection]


@pytest.fixture
def warehouse(store: Store, tmp_path: Path) -> Iterator[Built]:
    """A pass over `store` into `name`, from the warehouse there before
    it, if any; the file it wrote, opened read-only."""
    opened: list[duckdb.DuckDBPyConnection] = []

    def run(
        name: str = 'warehouse.duckdb', *, full: bool = False,
        today: date = TODAY,
    ) -> duckdb.DuckDBPyConnection:
        output = tmp_path / name
        for connection in opened:
            connection.close()
        opened.clear()
        build(store.paths, output, today=today, full=full)
        connection = duckdb.connect(str(output), read_only=True)
        opened.append(connection)
        return connection

    yield run
    for connection in opened:
        connection.close()


def relative(store: Store, path: Path) -> str:
    return str(path.relative_to(store.root))


class TestWhatAPassRecordsOfItsInputs:
    """What the next pass needs to tell what changed: each repository's
    directories as this pass found them, before it read what is in them,
    and what of each repository is not in its rows."""

    def test_every_directory_of_a_repository_with_its_stat(
        self, corpus: Store, warehouse: Built,
    ) -> None:
        con = warehouse()
        paths = corpus.paths
        expected = [
            paths.release_dir / '2',
            *sorted(
                path for path in (paths.release_dir / '2').rglob('*')
                if path.is_dir()
            ),
        ]
        found = rows(
            con,
            'SELECT path, inode, mtime_ns, ctime_ns FROM input_directories '
            "WHERE repository_id = 2 AND path LIKE '03-github-release/%' "
            'ORDER BY path',
        )
        assert [path for path, *_ in found] == sorted(
            relative(corpus, path) for path in expected
        )
        for path, inode, mtime_ns, ctime_ns in found:
            stat = os.stat(corpus.root / path)
            assert (inode, mtime_ns, ctime_ns) == (
                stat.st_ino, stat.st_mtime_ns, stat.st_ctime_ns,
            )
        assert {
            path for (path,) in rows(
                con,
                'SELECT path FROM input_directories WHERE repository_id = 1 '
                "AND path NOT LIKE '%/%/%' ORDER BY path",
            )
        } == {
            '05-github-tree/1', '06-github-content/1', '07-sbom/1',
            '09-github-depgraph/1',
        }
        assert rows(
            con,
            'SELECT path FROM input_directories WHERE repository_id = 1 '
            "AND path LIKE '06-github-content/%' ORDER BY path",
        ) == [
            ('06-github-content/1',), (f'06-github-content/1/{A}',),
            (f'06-github-content/1/{B}',), (f'06-github-content/1/{B}/sub',),
        ]

    def test_each_repository_with_what_is_not_in_its_rows(
        self, corpus: Store, warehouse: Built,
    ) -> None:
        con = warehouse()
        found = {
            row[0]: row[1:] for row in rows(
                con,
                'SELECT repository_id, record <> \'\', named, scans, '
                'unreadable, unnamed, graph_observed_at, trusted '
                'FROM inputs ORDER BY ALL',
            )
        }
        assert found == {
            1: (True, True, 5, 0, False, datetime(2026, 9, 13, 8), True),
            2: (True, True, 2, 0, False, None, True),
            3: (True, True, 1, 0, False, datetime(2026, 2, 1), True),
            4: (True, True, 0, 0, False, None, True),
            9: (False, False, 0, 0, True, datetime(2026, 8, 1), True),
        }

    def test_a_directory_written_as_it_was_read_is_read_again(
        self, corpus: Store, warehouse: Built,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Its next change could fall in the tick of its timestamp, and
        not move it. A repository with no directory has none to change."""
        monkeypatch.setattr(carry, 'SETTLE_NS', 10 * 60 * 10**9)
        con = warehouse()
        assert rows(
            con, 'SELECT repository_id, trusted FROM inputs ORDER BY ALL',
        ) == [(1, False), (2, False), (3, False), (4, True), (9, False)]


def contents(con: duckdb.DuckDBPyConnection) -> dict[str, list[tuple[Any, ...]]]:
    """Every table and view, each row of it, but `build`'s, which says how
    the pass went."""
    names = [
        name for (name,) in con.execute(
            'SELECT table_name FROM information_schema.tables '
            "WHERE table_name <> 'build' ORDER BY table_name",
        ).fetchall()
    ]
    return {
        name: con.execute(f'SELECT * FROM {name} ORDER BY ALL').fetchall()
        for name in names
    }


def made(con: duckdb.DuckDBPyConnection) -> tuple[int, str]:
    """How the pass went: how many repositories it carried, and why it
    read the whole store, if it did."""
    (carried, reason), = con.execute(
        'SELECT carried, full_reason FROM build',
    ).fetchall()
    return int(carried), reason


def write_again(path: Path, document: dict[str, Any]) -> None:
    """A document written over the one there, as the stages write one:
    aside, and renamed into place (`fs.atomic_write_bytes`)."""
    atomic_write_bytes(path, json.dumps(document).encode())


def syft(*artifacts: dict[str, Any], version: str) -> dict[str, Any]:
    return {
        'artifacts': list(artifacts), 'artifactRelationships': [],
        'source': {'type': 'directory'},
        'descriptor': {'name': 'syft', 'version': version},
        'schema': {'version': '16.1.0'},
    }


def collected(store: Store) -> None:
    """A new commit of `acme/web`, collected: its tree, manifests and
    Syft document, and its record."""
    store.tree(2, B, ['package.json'])
    store.content(2, B, {'package.json': PACKAGE_JSON})
    store.sbom(2, B, artifact('react', '18.3.1', 'npm'), at=SEP)
    store.record(2, 'acme', 'web', commit=B, listing='javascript')


def syft_upgraded(store: Store) -> None:
    """The SBOM stage writes an older commit's document again, after an
    upgrade of Syft: in place of the one there."""
    write_again(
        store.paths.sbom_file(1, A),
        syft(artifact('guava', '32.1.0-jre', 'java-archive'), version='1.60.0'),
    )


def pruned(store: Store) -> None:
    """`data prune --keep 1`: each repository's older scans go."""
    for root in (
        store.paths.tree_dir, store.paths.content_dir, store.paths.sbom_dir,
    ):
        prune_scan_dirs(root, keep=1)


def graph_fetched(store: Store) -> None:
    store.graph(
        3,
        spdx(
            '2026-09-20T00:00:00Z', [
                ('left', '2', 'npm'),
                ('pad', '3', 'npm'),
            ],
            direct=['left'], edges=[('pad', 'left')],
        ),
        fetched=at(2026, 9, 20),
    )


def first_fetched(store: Store) -> None:
    """The first graph of `acme/listed`, which had nothing collected: a
    directory of its id in a stage root that had none."""
    store.graph(
        4, spdx('2026-09-20T00:00:00Z', [('y', '1', 'npm')], direct=['y']),
        fetched=at(2026, 9, 20),
    )


def decided(store: Store) -> None:
    """A push of `acme/web`, and the release it chose."""
    store.decide(
        2, pushed_at='2026-09-20T00:00:00Z',
        releases=[
            {
                'id': 21, 'tag_name': 'v1.0.0', 'name': 'one',
                'published_at': '2026-09-09T00:00:00Z', 'prerelease': False,
            },
            {
                'id': 22, 'tag_name': 'v1.1.0', 'name': 'two',
                'published_at': '2026-09-19T00:00:00Z', 'prerelease': False,
            },
        ],
        latest='v1.1.0',
    )


def described(store: Store) -> None:
    """What `github repo` says of `acme/app` now."""
    store.metadata(1, 'acme', 'app', description='an app', listing='java')


def searched(store: Store) -> None:
    """A new search snapshot, complete: more stars for all."""
    store.snapshot(
        date(2026, 9, 20),
        *(
            Listed(r.id, r.owner, r.repo, r.stars + 1, r.language)
            for r in (APP, WEB, GRAPHED, LISTED)
        ),
        complete=True,
    )


def manifest_fetched(store: Store) -> None:
    """A manifest fetched into a directory of the content root already
    there, as a content stage taken up where it was left writes one."""
    path = store.paths.content_root(1, B) / 'sub' / 'settings.gradle'
    atomic_write_bytes(path, b"include 'app'\n")


def unlisted(store: Store) -> None:
    """Every directory of a repository nothing names, gone."""
    shutil.rmtree(store.paths.depgraph_dir / '9')


def appeared(store: Store) -> None:
    """A repository nothing names, with a graph."""
    store.graph(
        10, spdx('2026-09-21T00:00:00Z', [('x', '1', 'npm')], direct=['x']),
        fetched=at(2026, 9, 21),
    )


#: Each change to the store, and how many repositories a pass carries
#: after it, of those there are.
CHANGES: tuple[tuple[Callable[[Store], None], int], ...] = (
    (collected, 4),
    (syft_upgraded, 4),
    (pruned, 4),
    (graph_fetched, 4),
    (first_fetched, 4),
    (decided, 4),
    (described, 4),
    (manifest_fetched, 4),
    (unlisted, 4),
    (appeared, 5),
    (searched, 1),
)


class TestAPassCarriesWhatDidNotChange:

    def test_with_nothing_changed_nothing_is_read(
        self, corpus: Store, warehouse: Built,
    ) -> None:
        first = contents(warehouse())
        con = warehouse()
        assert made(con) == (5, '')
        assert contents(con) == first

    @pytest.mark.parametrize(
        ('change', 'carried'), CHANGES,
        ids=[change.__name__ for change, _ in CHANGES],
    )
    def test_after_a_change_it_builds_what_reading_everything_builds(
        self, corpus: Store, warehouse: Built,
        change: Callable[[Store], None], carried: int,
    ) -> None:
        before = contents(warehouse())
        change(corpus)
        con = warehouse()
        assert made(con)[0] == carried
        incremental = contents(con)
        whole = warehouse('whole.duckdb', full=True)
        assert made(whole) == (0, 'asked to read the whole store')
        assert incremental == contents(whole)
        assert incremental != before

    def test_after_every_change_in_turn(
        self, corpus: Store, warehouse: Built,
    ) -> None:
        """Each pass carrying from one that carried from another."""
        warehouse()
        for change, carried in CHANGES:
            change(corpus)
            con = warehouse()
            incremental = contents(con)
            assert made(con)[0] <= carried, change.__name__
            whole = warehouse('whole.duckdb', full=True)
            assert incremental == contents(whole), change.__name__

    def test_the_scans_are_numbered_as_a_pass_that_read_them(
        self, corpus: Store, warehouse: Built,
    ) -> None:
        """A repository read again between carried ones: theirs come
        after its scans, whose number changed."""
        warehouse()
        collected(corpus)
        sql = (
            'SELECT scan_id, repository_id, source, input_key FROM scans '
            'ORDER BY scan_id'
        )
        con = warehouse()
        assert made(con)[0] == 4
        incremental = rows(con, sql)
        assert incremental == rows(warehouse('whole.duckdb', full=True), sql)
        assert [repository for _, repository, _, _ in incremental] == [
            1, 1, 1, 1, 1, 2, 2, 2, 2, 3,
        ]


class TestWhenAPassReadsTheWholeStore:
    """It carries nothing from a warehouse it cannot trust to be what it
    would have built itself, and says why."""

    def test_with_no_warehouse_before_it(
        self, corpus: Store, warehouse: Built,
    ) -> None:
        assert made(warehouse()) == (0, 'there is no warehouse before it')

    def test_when_asked_to(self, corpus: Store, warehouse: Built) -> None:
        warehouse()
        assert made(warehouse(full=True)) == (
            0, 'asked to read the whole store',
        )

    def test_after_one_made_by_other_code(
        self, corpus: Store, warehouse: Built,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        warehouse()
        monkeypatch.setattr(carry, 'code', lambda: 'other')
        assert made(warehouse()) == (
            0, 'the warehouse before it was made by other code',
        )

    def test_after_one_that_keeps_its_inputs_otherwise(
        self, corpus: Store, warehouse: Built,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        warehouse()
        monkeypatch.setattr(carry, 'FORMAT', carry.FORMAT + 1)
        assert made(warehouse()) == (
            0, f'the warehouse before it keeps its inputs as '
            f'{carry.FORMAT - 1}',
        )

    def test_after_one_of_another_store(
        self, corpus: Store, tmp_path: Path,
    ) -> None:
        output = tmp_path / 'warehouse.duckdb'
        other = Store(tmp_path / 'other' / 'data')
        other.snapshot(date(2026, 9, 1), APP, complete=True)
        build(other.paths, output, today=TODAY)
        build(corpus.paths, output, today=TODAY)
        with duckdb.connect(str(output), read_only=True) as con:
            assert made(con) == (
                0, 'the warehouse before it is of another store',
            )

    def test_after_one_made_before_any_was_carried(
        self, corpus: Store, warehouse: Built, tmp_path: Path,
    ) -> None:
        with duckdb.connect(str(tmp_path / 'warehouse.duckdb')) as con:
            con.execute('CREATE TABLE build (version VARCHAR)')
            con.execute("INSERT INTO build VALUES ('0.5.4')")
        assert made(warehouse()) == (
            0, 'the warehouse before it keeps no inputs',
        )

    def test_after_a_file_that_is_no_warehouse(
        self, corpus: Store, warehouse: Built, tmp_path: Path,
    ) -> None:
        (tmp_path / 'warehouse.duckdb').write_bytes(b'not a database')
        carried, reason = made(warehouse())
        assert carried == 0
        assert reason.startswith('the warehouse before it cannot be opened')

    def test_after_a_pass_with_a_record_whose_id_is_not_a_number(
        self, corpus: Store, warehouse: Built,
    ) -> None:
        """`Repository` reads `"5"` as 5, and a record so named takes
        the outputs of 5 unless another took them first: which are
        whose is not one repository's to say."""
        corpus.record('5', 'acme', 'odd')  # type: ignore[arg-type]
        warehouse()
        assert made(warehouse()) == (0, 'the pass before it could not carry')

    def test_a_repository_whose_directories_had_not_settled_is_read_again(
        self, corpus: Store, warehouse: Built,
        monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        with monkeypatch.context() as patched:
            patched.setattr(carry, 'SETTLE_NS', 10 * 60 * 10**9)
            warehouse()
        con = warehouse()
        # `acme/listed`, which has no directory to settle, alone.
        assert made(con) == (1, '')
        assert contents(con) == contents(warehouse('whole.duckdb', full=True))
