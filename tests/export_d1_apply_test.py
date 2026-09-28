"""The D1 scripts of a real export, applied to SQLite, and applied again.

A retry is how an import over a network recovers, from a failure or from
a timeout that had in fact gone through, and the scripts did not survive
one. `03-aggregates.sql` was `INSERT INTO agg_* SELECT` with nothing
before it, so a second run doubled every aggregate; `04-indexes.sql`
failed on its first line, the index being there already; and the rows
were one 831 MB `02-data.sql` that a failure anywhere sent back to the
start. Measured with sqlite3, which is what D1 runs.

The edges are here too. The export walked `data/09-github-depgraph`
under whatever directory it was run from — 74 seconds, for a table `db
edges` already keeps in ClickHouse — and a directory that was not there
gave an empty `agg_edges` and a warning.
"""
from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path
from typing import Any

import pytest

from chatsbom.core.repository import IngestionRepository
from chatsbom.core.repository import QueryRepository
from chatsbom.core.schema import ARTIFACTS
from chatsbom.core.schema import EDGES
from chatsbom.core.schema import REPOSITORIES
from chatsbom.export.d1 import D1_SCHEMA
from chatsbom.export.d1 import export_d1
from chatsbom.models.relationship import DIRECT
from chatsbom.models.relationship import TRANSITIVE
from tests.conftest import requires_clickhouse
from tests.repository_query_test import artifact_row
from tests.repository_query_test import repo_row

pytestmark = requires_clickhouse


def seed_edges(
    ingest: IngestionRepository,
    *edges: tuple[str, str, int],
) -> None:
    """Rows in `edges`, as `db edges` writes them: a pair may be there
    more than once, the table being a SummingMergeTree."""
    ingest.insert_batch(
        EDGES.name,
        EDGES.rows([
            {
                'parent': parent, 'child': child, 'repositories': count,
                'observed_at': datetime(2026, 9, 14),
            }
            for parent, child, count in edges
        ]),
        EDGES.column_names,
    )


def apply_scripts(
    directory: Path,
    names: list[str],
    connection: sqlite3.Connection,
) -> None:
    """Apply each script, in the order given."""
    for name in names:
        # executescript stops at the first error and raises, so a
        # rejected statement fails the test rather than being skipped.
        connection.executescript(
            (directory / name).read_text(encoding='utf-8'),
        )


def contents(connection: sqlite3.Connection) -> dict[str, list[Any]]:
    """Every table's rows, sorted, so the order they were written in
    does not matter."""
    return {
        table.name: sorted(
            connection.execute(
                f'SELECT * FROM {table.name}',  # noqa: S608 - schema-owned
            ).fetchall(),
        )
        for table in D1_SCHEMA.tables
    }


@pytest.fixture
def seeded(ingest: IngestionRepository, query: QueryRepository) -> QueryRepository:
    """Three repositories, two languages, both relationships, and one
    edge between two of the packages."""
    ingest.insert_batch(
        REPOSITORIES.name,
        REPOSITORIES.rows([
            repo_row(id=1, owner='mastodon', repo='mastodon', stars=300),
            repo_row(
                id=2, owner='rails', repo='rails', stars=200,
                description='依赖关系图谱 🚀',
            ),
            repo_row(id=3, owner='py', repo='app', language='python'),
        ]),
        REPOSITORIES.column_names,
    )
    ingest.insert_batch(
        ARTIFACTS.name,
        ARTIFACTS.rows([
            artifact_row(repository_id=1, artifact_id='a1', relationship=DIRECT),
            artifact_row(
                repository_id=1, artifact_id='a2', name='mini_mime',
                version='1.1.5', relationship=TRANSITIVE,
            ),
            artifact_row(repository_id=2, artifact_id='a3'),
            artifact_row(
                repository_id=3, artifact_id='a4', name='requests',
                type='python', version='2.32.0', relationship=DIRECT,
            ),
        ]),
        ARTIFACTS.column_names,
    )
    seed_edges(ingest, ('mail', 'mini_mime', 2))
    return query


class TestApplyingAgain:

    def test_every_script_twice_gives_the_same_database(
        self, seeded: QueryRepository, tmp_path: Path,
    ) -> None:
        """The whole import run a second time, from `01-schema.sql`."""
        result = export_d1(seeded, tmp_path / 'd1')
        connection = sqlite3.connect(tmp_path / 'applied.sqlite')

        apply_scripts(result.directory, sorted(result.files), connection)
        once = contents(connection)
        apply_scripts(result.directory, sorted(result.files), connection)

        assert contents(connection) == once
        assert once['agg_totals'] == [(3, 4, 3, 4)]

    def test_each_script_can_be_retried(
        self, seeded: QueryRepository, tmp_path: Path,
    ) -> None:
        """Each applied twice in a row, as a retry applies it, gives
        the database one pass gives.

        `02-data.sql` failed its second time on the first package id,
        already there; the aggregates doubled; the indexes failed.
        """
        result = export_d1(seeded, tmp_path / 'd1')
        once = sqlite3.connect(tmp_path / 'once.sqlite')
        apply_scripts(result.directory, sorted(result.files), once)

        retried = sqlite3.connect(tmp_path / 'retried.sqlite')
        apply_scripts(
            result.directory,
            [name for name in sorted(result.files) for _ in range(2)],
            retried,
        )

        assert contents(retried) == contents(once)

    def test_an_import_can_resume_from_the_part_that_failed(
        self, seeded: QueryRepository, tmp_path: Path,
    ) -> None:
        """Parts of one row each: the import is run to the end, and
        then again from every part in turn."""
        result = export_d1(seeded, tmp_path / 'd1', batch=1, chunk_bytes=1)
        names = sorted(result.files)
        once = sqlite3.connect(':memory:')
        apply_scripts(result.directory, names, once)
        expected = contents(once)

        for start in range(1, len(names)):
            connection = sqlite3.connect(':memory:')
            apply_scripts(result.directory, names, connection)
            apply_scripts(result.directory, names[start:], connection)
            assert contents(connection) == expected, names[start]


class TestTheDataParts:

    def test_the_rows_are_split_into_numbered_parts(
        self, seeded: QueryRepository, tmp_path: Path,
    ) -> None:
        """`02-<table>-0001.sql` onwards, one or more for every table the
        data script fills, and no `02-data.sql`."""
        result = export_d1(seeded, tmp_path / 'd1', batch=1, chunk_bytes=1)
        names = sorted(result.files)

        assert names[0] == '01-schema.sql'
        assert names[-2:] == ['03-aggregates.sql', '04-indexes.sql']
        parts = names[1:-2]
        assert all(name.startswith('02-') for name in parts)
        assert {name.rsplit('-', 1)[0] for name in parts} == {
            f'02-{table}' for table in result.row_counts
        }
        # One row a part: the four artifact rows are four parts.
        assert [
            name for name in parts if name.startswith('02-artifacts-')
        ] == [f'02-artifacts-{n:04d}.sql' for n in range(1, 5)]

    def test_what_the_export_reports_is_what_lands(
        self, seeded: QueryRepository, tmp_path: Path,
    ) -> None:
        result = export_d1(seeded, tmp_path / 'd1', batch=1, chunk_bytes=1)
        connection = sqlite3.connect(tmp_path / 'applied.sqlite')
        apply_scripts(result.directory, sorted(result.files), connection)
        for table, expected in result.row_counts.items():
            landed = connection.execute(
                f'SELECT count(*) FROM {table}',  # noqa: S608 - schema-owned
            ).fetchone()[0]
            assert landed == expected, table

    def test_a_reexport_leaves_nothing_of_the_one_before(
        self, seeded: QueryRepository, tmp_path: Path,
    ) -> None:
        """Parts are applied by name, so one left by an earlier, larger
        export would be applied with this one's: a stale `0042` after a
        new `0041`, or the single `02-data.sql` of before the split."""
        directory = tmp_path / 'd1'
        directory.mkdir()
        for stale in ('02-data.sql', '02-artifacts-0042.sql'):
            (directory / stale).write_text(
                'INSERT INTO artifacts VALUES (9, 9, 9, 9);\n',
            )
        (directory / 'notes.sql').write_text('-- mine\n')

        result = export_d1(seeded, directory)

        assert not (directory / '02-data.sql').exists()
        assert not (directory / '02-artifacts-0042.sql').exists()
        assert (directory / 'notes.sql').read_text() == '-- mine\n'
        assert sorted(
            path.name for path in directory.glob('0*.sql')
        ) == sorted(result.files)


class TestTheEdges:

    def test_they_are_the_ones_clickhouse_holds(
        self, seeded: QueryRepository, ingest: IngestionRepository,
        tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """Summed per pair, as the table's readers must: a
        SummingMergeTree holds a pair more than once until it merges.
        A pair naming a package no artifact names is left out."""
        # Where the export once looked for documents, with none there.
        monkeypatch.chdir(tmp_path)
        seed_edges(ingest, ('mail', 'mini_mime', 3), ('rails', 'mail', 1))

        result = export_d1(seeded, tmp_path / 'd1')
        connection = sqlite3.connect(tmp_path / 'applied.sqlite')
        apply_scripts(result.directory, sorted(result.files), connection)

        assert connection.execute(
            'SELECT p.name, c.name, e.repositories FROM agg_edges e '
            'JOIN packages p ON p.id = e.parent_id '
            'JOIN packages c ON c.id = e.child_id',
        ).fetchall() == [('mail', 'mini_mime', 5)]
        assert result.row_counts['agg_edges'] == 1

    def test_an_empty_table_fails_the_export(
        self, ingest: IngestionRepository, query: QueryRepository,
        tmp_path: Path,
    ) -> None:
        """Before anything is written, and saying what to run.

        An empty `agg_edges` is a dashboard whose edge panels are empty,
        exported as a success."""
        ingest.insert_batch(
            REPOSITORIES.name, REPOSITORIES.rows([repo_row()]),
            REPOSITORIES.column_names,
        )
        with pytest.raises(RuntimeError, match='db edges'):
            export_d1(query, tmp_path / 'd1')
        assert not list((tmp_path / 'd1').glob('*.sql'))


class TestACappedAccount:

    def test_fails_the_export_rather_than_truncating_it(
        self, seeded: QueryRepository, tmp_path: Path,
    ) -> None:
        """The guest profile caps a result at `max_result_rows` with
        `result_overflow_mode=break`, which stops returning rows without
        an error. The D1 export had no check at all: under such a cap it
        wrote the rows it was given and reported success."""
        for setting, value in (
            ('max_result_rows', 1),
            ('result_overflow_mode', 'break'),
            # A row a block, so the cap falls inside a result.
            ('max_block_size', 1),
        ):
            seeded.client.set_client_setting(setting, value)

        with pytest.raises(RuntimeError, match='stopped'):
            export_d1(seeded, tmp_path / 'd1')
