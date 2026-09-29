"""A snapshot written from a small warehouse, table by table (#132).

`write` reads `warehouse.duckdb` and writes one SQLite file of the D1
schema (`D1_SCHEMA`), which the D1 backend's statements read as they
read `export d1`'s: the strings interned as `export d1` interns them,
the facts as four integers in the order `export d1` writes them, the
aggregates the D1 script computes, and a `meta` row that also says what
the file is. And one table more, the dependants table's rows in the
page's order, which `Dataset` reads for a package's dependants. The
rows here are `SHOP`'s (`conftest.py`), each worked out by hand.
"""
from __future__ import annotations

import json
import re
import shutil
import sqlite3
import stat
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest

from chatsbom.__version__ import __version__
from chatsbom.dataset import Dataset
from chatsbom.dataset import open_dataset
from chatsbom.dataset.open import connect
from chatsbom.export.d1 import aggregate_sql
from chatsbom.export.d1 import AGGREGATED
from chatsbom.export.d1 import D1_SCHEMA
from chatsbom.export.d1 import TOP_PACKAGES_DEPTH
from chatsbom.snapshot.schema import SCHEMA
from chatsbom.snapshot.write import write
from chatsbom.snapshot.write import Written
from tests.snapshot.conftest import Corpus
from tests.snapshot.conftest import shop
from tests.snapshot.conftest import warehouse
from tests.warehouse.parity_test import synthetic


@pytest.fixture(scope='module')
def written(tmp_path_factory: pytest.TempPathFactory) -> Written:
    directory = tmp_path_factory.mktemp('shop')
    return write(
        warehouse(directory / 'warehouse.duckdb', shop()),
        directory / 'snapshots',
    )


def rows(path: Path, sql: str) -> list[tuple[Any, ...]]:
    """What `sql` finds in the snapshot, opened as a reader opens it."""
    with closing(connect(path)) as connection:
        return connection.execute(sql).fetchall()


class TestTheStrings:
    """Interned as `export d1` interns them, so the ids are its ids."""

    def test_packages_in_name_order_with_their_repositories(
        self, written: Written,
    ) -> None:
        assert rows(written.path, 'SELECT * FROM packages ORDER BY id') == [
            (1, 'laravel/framework', 1), (2, 'left-pad', 1), (3, 'lodash', 1),
            (4, 'puma', 1), (5, 'rack', 2),
        ]

    def test_versions_in_the_order_the_facts_first_name_them(
        self, written: Written,
    ) -> None:
        # Not the order of the strings: `3.1.0` is fifth, after the
        # versions of the names before `rack`.
        assert rows(written.path, 'SELECT * FROM versions ORDER BY id') == [
            (1, 'v12.0.0'), (2, '1.3.0'), (3, '4.17.21'), (4, '6.4.0'),
            (5, '3.1.0'), (6, '~> 3.1'),
        ]

    def test_kinds_likewise_under_the_canonical_type(
        self, written: Written,
    ) -> None:
        # Syft's `php-composer` is `composer`, as the page names it; the
        # seventh fact's kind is the fourth's.
        assert rows(written.path, 'SELECT * FROM kinds ORDER BY id') == [
            (
                1, 'composer', 'php-composer-lock-cataloger', 'direct', 'syft',
                'resolved',
            ),
            (
                2, 'npm', 'javascript-lock-cataloger', 'direct', 'syft',
                'resolved',
            ),
            (
                3, 'npm', 'javascript-lock-cataloger', 'transitive', 'syft',
                'resolved',
            ),
            (
                4, 'gem', 'gemfile-lock-cataloger', 'transitive', 'syft',
                'resolved',
            ),
            (
                5, 'gem', 'gemfile-lock-cataloger', 'direct', 'syft',
                'resolved',
            ),
            (
                6, 'gem', 'github-dependency-graph', 'direct',
                'github-depgraph', 'constraint',
            ),
        ]


class TestTheRows:

    def test_artifacts_are_the_current_facts_by_name(
        self, written: Written,
    ) -> None:
        # One row a fact of the corpus's current scans, in the order
        # `export d1` writes them, so a package's rows are together:
        # not January's `rack`, which March's replaced, and not `cobra`,
        # whose repository is not the corpus.
        assert rows(
            written.path, 'SELECT rowid, * FROM artifacts ORDER BY rowid',
        ) == [
            (1, 2, 1, 1, 1), (2, 2, 2, 2, 2), (3, 2, 3, 3, 3),
            (4, 1, 4, 4, 4), (5, 1, 5, 5, 5), (6, 1, 5, 6, 6),
            (7, 2, 5, 5, 4),
        ]

    def test_repositories_are_the_corpus(self, written: Written) -> None:
        assert rows(
            written.path, 'SELECT * FROM repositories ORDER BY id',
        ) == [
            (
                1, 'acme', 'app', 300, 'ruby', 'Ruby', 'ruby', '["gem"]',
                'https://github.com/acme/app', 'An app', 'MIT',
                '2026-09-01',
                # Its newest current scan that saw something: the graph.
                '2026-09-13',
                # The current Syft scan's ref and commit.
                'v2.0.0', 'c2',
                1, 2,
            ),
            (
                2, 'acme', 'web', 500, 'javascript', 'JavaScript',
                'javascript', '["composer","gem","npm"]',
                'https://github.com/acme/web', 'Ünïcode — the web 🕸', '',
                '2026-08-01', '2026-02-01', 'main', 'w1', 2, 4,
            ),
            (
                # Scanned, and nothing found: dated by that scan, where
                # `export d1` has the day `db index` wrote its row.
                3, 'acme', 'idle', 100, '', '', 'none', '[]',
                'https://github.com/acme/idle', '', '', '2026-09-01',
                '2026-04-02', 'main', 'i1', 0, 0,
            ),
        ]

    def test_each_source_is_dated_by_its_current_scan(
        self, written: Written,
    ) -> None:
        # A scan that saw nothing has no row to date: idle has none.
        assert rows(
            written.path,
            'SELECT * FROM observations ORDER BY repository_id, source',
        ) == [
            (1, 'github-depgraph', '2026-09-13'), (1, 'syft', '2026-03-10'),
            (2, 'syft', '2026-02-01'),
        ]

    def test_licences_one_row_each_unknown_among_them(
        self, written: Written,
    ) -> None:
        assert rows(
            written.path, 'SELECT * FROM licenses ORDER BY rowid',
        ) == [
            ('MIT', 2, 3), ('', 1, 2), ('Ruby', 1, 1), ('WTFPL', 1, 1),
        ]

    def test_adoption_is_counted_by_intervals(self, written: Written) -> None:
        # Owner decision Q9 on #128: app's January and March scans both
        # show `rack`, so it held in February too, with web's February
        # scan: two repositories, where the months of the scans say one.
        assert rows(
            written.path,
            'SELECT * FROM history ORDER BY name, source, month',
        ) == [
            ('laravel/framework', '2026-02', 'syft', 1, 1),
            ('left-pad', '2026-02', 'syft', 1, 1),
            ('lodash', '2026-02', 'syft', 1, 0),
            ('puma', '2026-03', 'syft', 1, 0),
            ('rack', '2026-09', 'github-depgraph', 1, 1),
            ('rack', '2026-01', 'syft', 1, 0),
            ('rack', '2026-02', 'syft', 2, 0),
            ('rack', '2026-03', 'syft', 1, 1),
        ]

    def test_edges_between_packages_of_the_facts(
        self, written: Written,
    ) -> None:
        # By id, in name order; `mystery` is no package, so its edge is
        # left out.
        assert rows(
            written.path, 'SELECT * FROM agg_edges ORDER BY rowid',
        ) == [(3, 2, 1), (5, 4, 2)]


class TestThePageTable:
    """`dependants`: the dependants table's rows, in its order (#128
    §2.4), which the page reads a range of rather than grouping and
    sorting every row of a package."""

    def test_holds_a_row_for_each_line_the_page_shows(
        self, written: Written,
    ) -> None:
        # By package, then the page's order: web (500 stars) is first,
        # app (300) second. rack's March scan and its graph are two
        # lines of app's, dated by each source.
        # Its own order, which is its key's: it has no rowid.
        assert rows(written.path, 'SELECT * FROM dependants') == [
            (
                1, 1, 'v12.0.0', 'direct', 'composer', '2026-02-01', 2,
                'javascript', 1,
            ),
            (
                2, 1, '1.3.0', 'direct', 'npm', '2026-02-01', 2, 'javascript',
                1,
            ),
            (
                3, 1, '4.17.21', 'transitive', 'npm', '2026-02-01', 2,
                'javascript', 1,
            ),
            (4, 2, '6.4.0', 'transitive', 'gem', '2026-03-10', 1, 'ruby', 1),
            (
                5, 1, '3.1.0', 'transitive', 'gem', '2026-02-01', 2,
                'javascript', 1,
            ),
            (5, 2, '3.1.0', 'direct', 'gem', '2026-03-10', 1, 'ruby', 1),
            (5, 2, '~> 3.1', 'direct', 'gem', '2026-09-13', 1, 'ruby', 1),
        ]

    def test_is_stored_in_that_order(self, written: Written) -> None:
        """WITHOUT ROWID: the rows are the key's B-tree, so a page is a
        range read in order, with nothing to sort."""
        [(ddl,)] = rows(
            written.path,
            "SELECT sql FROM sqlite_master WHERE name = 'dependants'",
        )
        assert ddl.rstrip(';').endswith('WITHOUT ROWID')
        with closing(connect(written.path)) as connection:
            planned = Planned(connection)
            dataset = Dataset(planned)
            dataset.dependents_of('rack', limit=2, offset=1)
            dataset.dependents_of('rack', language='ruby')
            dataset.dependents_of('rack', type='gem', direct_only=True)
            dataset.count_dependents('rack', language='ruby')
            dataset.count_dependent_rows('rack', type='gem')
        # Each a range of the key, by the package, and no fact read.
        for steps in planned.plans:
            assert 'SEARCH d USING PRIMARY KEY (package_id=?)' in steps
            assert not [
                step for step in steps
                if 'artifacts' in step
                or step.startswith(('SCAN a', 'SEARCH a'))
            ], steps
        sorted_by = [
            [
                PART.sub('PART OF ORDER BY', step) for step in steps
                if 'TEMP B-TREE' in step
            ]
            for steps in planned.plans
        ]
        assert sorted_by == [
            [], [],
            # An ecosystem or a relationship asked for is a column inside
            # the key, which SQLite does not pass over as it does the
            # package: it orders the rows of each repository and version,
            # one or a few, by the rest. Never the whole page.
            ['USE TEMP B-TREE FOR PART OF ORDER BY'],
            # A set of the repositories counted.
            ['USE TEMP B-TREE FOR count(DISTINCT)'],
            [],
        ]


#: A sort of the last terms of an ORDER BY alone, within each run of
#: rows equal in the terms before: 3.45 says `RIGHT PART OF`, 3.53
#: `LAST 2 TERMS OF`.
PART = re.compile(r'(RIGHT PART|LAST \d+ TERMS) OF ORDER BY')


class Planned:
    """A snapshot's connection that keeps the plan of each statement it
    is asked, and answers it."""

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection
        #: Each statement's plan, a step a line.
        self.plans: list[list[str]] = []

    def execute(self, sql: str, parameters: Any, /) -> sqlite3.Cursor:
        self.plans.append([
            str(step[-1]) for step in self.connection.execute(
                f'EXPLAIN QUERY PLAN {sql}', parameters,
            ).fetchall()
        ])
        return self.connection.execute(sql, parameters)


def recomputed(written: Written, directory: Path) -> Path:
    """A copy of the snapshot, its aggregates and its packages' counts
    made again by `aggregate_sql`, D1's own script, from its rows."""
    again = directory / 'again.sqlite'
    shutil.copyfile(written.path, again)
    again.chmod(0o644)
    with closing(sqlite3.connect(again)) as connection:
        connection.execute('UPDATE packages SET repositories = 0')
        connection.executescript(aggregate_sql())
        connection.commit()
    return again


def same_aggregates(written: Written, again: Path) -> None:
    for table in (*AGGREGATED, D1_SCHEMA.table('packages')):
        ordered = f'SELECT * FROM {table.name} ORDER BY ' + ', '.join(
            table.column_names,
        )
        assert rows(written.path, ordered) == rows(again, ordered), (
            table.name
        )


class TestTheAggregates:
    """The snapshot computes them in DuckDB; D1's script, run over the
    snapshot's own rows, is the definition they have to meet."""

    def test_are_what_the_d1_script_makes_of_the_rows(
        self, written: Written, tmp_path: Path,
    ) -> None:
        same_aggregates(written, recomputed(written, tmp_path))
        # Not agreement on nothing.
        assert rows(written.path, 'SELECT * FROM agg_totals') == [
            (2, 7, 5, 7, 3),
        ]

    def test_of_a_synthetic_corpus_too(self, tmp_path: Path) -> None:
        """Hundreds of names, so that the ranking is cut at its depth;
        more than twelve languages; every source."""
        repositories, artifacts, edges, ids = synthetic()
        written = write(
            warehouse(
                tmp_path / 'warehouse.duckdb',
                Corpus(repositories, artifacts, edges, ids),
            ),
            tmp_path / 'snapshots',
        )
        same_aggregates(written, recomputed(written, tmp_path))
        [(ranked, cut)] = rows(
            written.path,
            'SELECT count(*), max(rank) FROM agg_top_packages',
        )
        assert cut == TOP_PACKAGES_DEPTH
        assert ranked > 4 * TOP_PACKAGES_DEPTH
        assert rows(
            written.path,
            'SELECT count(*) FROM agg_language_coverage '
            "WHERE language = 'other'",
        ) == [(1,)]


class TestTheMeta:

    def test_what_the_page_reads(self, written: Written) -> None:
        with open_dataset(written.path) as dataset:
            meta = dataset.meta()
        assert meta.generator == f'chatsbom/{__version__}'
        assert meta.schema_version == 'd1 v8'
        # The repositories with dependencies, as `export d1` spans them.
        assert (meta.observed_from, meta.observed_to) == (
            '2026-02-01', '2026-09-13',
        )

    def test_what_the_file_is(self, written: Written) -> None:
        [(snapshot, version, corpus, counted)] = rows(
            written.path,
            'SELECT snapshot, version, corpus, rows FROM meta',
        )
        assert snapshot == written.id
        assert version == __version__
        assert corpus == 'all-2026-09-01'
        counts = json.loads(counted)
        assert counts == {
            table.name: rows(
                written.path, f'SELECT count(*) FROM {table.name}',
            )[0][0]
            for table in SCHEMA.tables
        }
        assert counts == written.rows
        assert counts['artifacts'] == 7

    def test_the_id(self, written: Written) -> None:
        assert len(written.id) == 16
        assert set(written.id) <= set('0123456789abcdef')


class TestTheFile:
    """Closed as a reader of an immutable file needs it."""

    def test_has_every_table_and_index(self, written: Written) -> None:
        found = {
            (kind, name) for kind, name in rows(
                written.path,
                'SELECT type, name FROM sqlite_master '
                "WHERE name NOT LIKE 'sqlite_%'",
            )
        }
        assert found == {
            *(('table', table.name) for table in SCHEMA.tables),
            *(('index', index.name) for index in SCHEMA.indexes),
        }
        # D1's own, all of them: `Dataset` asks what D1 is asked.
        assert {index.name for index in D1_SCHEMA.indexes} <= {
            index.name for index in SCHEMA.indexes
        }

    def test_leaves_no_journal_and_is_no_wal_file(
        self, written: Written,
    ) -> None:
        assert sorted(p.name for p in written.path.parent.iterdir()) == [
            written.path.name,
        ]
        header = written.path.read_bytes()[:100]
        # The file format's write and read versions: 1 is a rollback
        # journal, 2 WAL, which a reader could not open without making
        # a `-shm` beside it.
        assert (header[18], header[19]) == (1, 1)

    def test_is_analysed_and_has_no_free_page(self, written: Written) -> None:
        assert rows(written.path, 'SELECT count(*) FROM sqlite_stat1')[0][0]
        assert rows(written.path, 'PRAGMA freelist_count') == [(0,)]
        assert rows(written.path, 'PRAGMA integrity_check') == [('ok',)]

    def test_cannot_be_written(self, written: Written) -> None:
        mode = written.path.stat().st_mode
        assert not mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH)

    def test_is_named_to_be_left_alone(self, written: Written) -> None:
        """Until it is published, by a name no reader or retention takes
        for a snapshot's."""
        assert written.path.name.startswith('.')
        assert written.path.name != f'{written.id}.sqlite'

    def test_answers_the_page(self, written: Written) -> None:
        with open_dataset(written.path) as dataset:
            assert dataset.count_dependents('rack') == 2
            assert [
                (row.owner, row.repo, row.version, row.relationship)
                for row in dataset.dependents_of('rack')
            ] == [
                ('acme', 'web', '3.1.0', 'transitive'),
                ('acme', 'app', '3.1.0', 'direct'),
                ('acme', 'app', '~> 3.1', 'direct'),
            ]
            assert dataset.totals().tracked == 3
