"""The D1 scripts of a real export, applied to SQLite, and applied again.

A retry is how an import over a network recovers, from a failure or from
a timeout that had in fact gone through, and the scripts did not survive
one. `03-aggregates.sql` was `INSERT INTO agg_* SELECT` with nothing
before it, so a second run doubled every aggregate; `04-indexes.sql`
failed on its first line, the index being there already; and the rows
were one `02-data.sql` of about 450 MB that a failure anywhere sent back
to the start. Measured with sqlite3, which is what D1 runs.

The edges are here too. The export walked `data/09-github-depgraph`
under whatever directory it was run from — 74 seconds, for a table `db
edges` already keeps in ClickHouse — and a directory that was not there
gave an empty `agg_edges` and a warning.
"""
from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from chatsbom.core.repository import IngestionRepository
from chatsbom.core.repository import QueryRepository
from chatsbom.core.schema import ARTIFACTS
from chatsbom.core.schema import EDGES
from chatsbom.core.schema import REPOSITORIES
from chatsbom.export.d1 import D1_SCHEMA
from chatsbom.export.d1 import export_d1
from chatsbom.models.provenance import CONSTRAINT
from chatsbom.models.provenance import DEPGRAPH
from chatsbom.models.relationship import DIRECT
from chatsbom.models.relationship import TRANSITIVE
from tests.conftest import requires_clickhouse
from tests.repository_query_test import artifact_row
from tests.repository_query_test import repo_row
from tests.repository_query_test import STALE_SHA

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
            artifact_row(
                repository_id=1, artifact_id='a1',
                relationship=DIRECT,
            ),
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
        assert once['agg_totals'] == [(3, 4, 3, 4, 3)]

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


class TestTheLicences:

    def test_each_is_one_row_whatever_the_ecosystems(
        self, seeded: QueryRepository, tmp_path: Path,
    ) -> None:
        """D1's `licenses` is one row per licence, which the panel reads
        as that licence's total. Filled from the query the Parquet export
        shares, keyed by licence and type, it held a row per licence per
        ecosystem, and the panel showed whichever sorted highest: MIT
        10,114 against a true 16,846. Here MIT is on gems in two
        repositories, and on a Python package in the third.
        """
        result = export_d1(seeded, tmp_path / 'd1')
        connection = sqlite3.connect(tmp_path / 'applied.sqlite')
        apply_scripts(result.directory, sorted(result.files), connection)

        # license, repository_count, package_count
        assert contents(connection)['licenses'] == [('MIT', 3, 3)]


class TestTheObservationDates:
    """When each collector last observed each repository (#41).

    D1's dependants table dated a row by its repository's newest
    observation from any source, so a repository Syft scanned in
    February and the dependency graph read in September showed
    September on every row; ClickHouse dates each row by its own (#24).
    `observations` holds the date per repository and source, for the
    rows to join on.
    """

    SCANNED = datetime(2026, 2, 11, 9, 30)
    #: Late in the UTC day: made in UTC+8, the date would be the 15th.
    GRAPHED = datetime(2026, 9, 14, 23, 30)
    #: The scan the February one replaced.
    EARLIER = datetime(2026, 1, 20, 9, 30)

    def test_each_source_is_dated_by_its_current_observation(
        self, ingest: IngestionRepository, query: QueryRepository,
        tmp_path: Path,
    ) -> None:
        ingest.insert_batch(
            REPOSITORIES.name,
            REPOSITORIES.rows([
                repo_row(
                    id=1, owner='rails', repo='rails',
                    depgraph_observed_at=self.GRAPHED,
                ),
                repo_row(id=2, owner='mastodon', repo='mastodon'),
            ]),
            REPOSITORIES.column_names,
        )
        ingest.insert_batch(
            ARTIFACTS.name,
            ARTIFACTS.rows([
                artifact_row(
                    repository_id=1, artifact_id='scan',
                    observed_at=self.SCANNED,
                ),
                artifact_row(
                    repository_id=1, artifact_id='before', version='2.7.0',
                    sbom_commit_sha=STALE_SHA, observed_at=self.EARLIER,
                ),
                artifact_row(
                    repository_id=1, artifact_id='graph', version='~> 2.8',
                    source=DEPGRAPH, relationship=DIRECT,
                    version_kind=CONSTRAINT, sbom_commit_sha='',
                    observed_at=self.GRAPHED,
                ),
                artifact_row(
                    repository_id=2, artifact_id='scan',
                    observed_at=self.SCANNED,
                ),
            ]),
            ARTIFACTS.column_names,
        )
        seed_edges(ingest, ('mail', 'mini_mime', 1))

        result = export_d1(query, tmp_path / 'd1')
        connection = sqlite3.connect(':memory:')
        apply_scripts(result.directory, sorted(result.files), connection)

        # Not January's scan, which is history; and in UTC.
        assert connection.execute(
            'SELECT repository_id, source, observed_at FROM observations '
            'ORDER BY repository_id, source',
        ).fetchall() == [
            (1, 'github-depgraph', '2026-09-14'),
            (1, 'syft', '2026-02-11'),
            (2, 'syft', '2026-02-11'),
        ]
        # The repository's own date is still its newest, from any source.
        assert connection.execute(
            'SELECT id, observed_at FROM repositories ORDER BY id',
        ).fetchall() == [(1, '2026-09-14'), (2, '2026-02-11')]
        assert result.row_counts['observations'] == 3


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

    def test_leaves_nothing_to_apply(
        self, seeded: QueryRepository, tmp_path: Path,
    ) -> None:
        """What it had written is removed: the files are applied by
        name, all of them, and the ones it got to would load part of a
        dataset."""
        seeded.client.set_client_setting('max_result_rows', 1)
        seeded.client.set_client_setting('max_block_size', 1)

        with pytest.raises(RuntimeError):
            export_d1(seeded, tmp_path / 'd1')

        assert not list((tmp_path / 'd1').iterdir())


class TestTheCommand:
    """`chatsbom export d1`, and the loop it prints to apply the files."""

    @pytest.fixture
    def exported(
        self, seeded: QueryRepository, tmp_path: Path,
        monkeypatch: pytest.MonkeyPatch,
    ) -> tuple[Path, str]:
        """The command's output directory, one with a space in its name,
        and what it printed."""
        from typer.testing import CliRunner

        from chatsbom.__main__ import app

        container = SimpleNamespace(
            config=SimpleNamespace(get_db_config=lambda role: seeded.config),
            get_export_repository=lambda: seeded,
        )
        monkeypatch.setattr(
            'chatsbom.commands.export.d1.get_container', lambda: container,
        )
        monkeypatch.setattr(
            'chatsbom.commands.export.d1.check_clickhouse_connection',
            lambda **_: None,
        )
        output = tmp_path / 'dist d1'
        result = CliRunner().invoke(
            app, ['export', 'd1', '--output', str(output)],
        )
        assert result.exit_code == 0, result.output
        return output, result.stdout

    def test_prints_the_loop_that_applies_the_files(
        self, exported: tuple[Path, str],
    ) -> None:
        from chatsbom.commands.export.d1 import apply_loop
        output, printed = exported
        assert ''.join(apply_loop(output).split()) in ''.join(printed.split())

    def test_the_loop_applies_every_file_in_order(
        self, exported: tuple[Path, str], tmp_path: Path,
    ) -> None:
        """Run by a shell, with `npx` standing in for wrangler: a script
        that applies the file it is given to SQLite and notes its name.
        """
        from chatsbom.commands.export.d1 import apply_loop
        output, _ = exported
        bin_directory = tmp_path / 'bin'
        bin_directory.mkdir()
        npx = bin_directory / 'npx'
        npx.write_text(
            f'#!{sys.executable}\n'
            'import os, sqlite3, sys\n'
            'path = sys.argv[-1]\n'
            "with open(os.environ['APPLIED'], 'a') as log:\n"
            "    log.write(os.path.basename(path) + '\\n')\n"
            "connection = sqlite3.connect(os.environ['DATABASE'])\n"
            "connection.executescript(open(path, encoding='utf-8').read())\n"
            'connection.close()\n',
        )
        npx.chmod(0o755)
        environment = {
            **os.environ,
            'PATH': f'{bin_directory}{os.pathsep}{os.environ["PATH"]}',
            'DATABASE': str(tmp_path / 'applied.sqlite'),
            'APPLIED': str(tmp_path / 'applied.txt'),
        }

        subprocess.run(
            ['bash', '-c', apply_loop(output)], env=environment, check=True,
        )

        applied = (tmp_path / 'applied.txt').read_text().split()
        assert applied == sorted(path.name for path in output.iterdir())
        connection = sqlite3.connect(tmp_path / 'applied.sqlite')
        assert connection.execute(
            'SELECT repositories, dependencies FROM agg_totals',
        ).fetchall() == [(3, 4)]
        connection.close()
