"""`db index --rebuild` builds the table aside and swaps it in (#23).

It dropped `artifacts` and then refilled it from each repository's
current documents. For the seven minutes that takes on the corpus, the
dashboard read a table filling up from empty; an ingest that failed
left it that way; and every older scan was gone for good, because the
ledger holds one record per repository and `data prune` deletes the
older SBOMs, so the table was the only copy of that history.

These run the command against a real database, with documents landed
in `raw_documents` as `graph_currency_test.py` lands them.
"""
from __future__ import annotations

from datetime import datetime
from datetime import timezone
from typing import Any

import pytest

from chatsbom.core.repository import IngestionRepository
from chatsbom.core.repository import QueryRepository
from chatsbom.core.schema import ARTIFACTS
from chatsbom.models.provenance import SYFT
from chatsbom.services.db_service import DbService
from tests.conftest import requires_clickhouse
from tests.current_state_test import dependants
from tests.graph_currency_test import COMMIT
from tests.graph_currency_test import CREATED
from tests.graph_currency_test import CREATED_BEFORE
from tests.graph_currency_test import land
from tests.graph_currency_test import land_graph
from tests.graph_currency_test import LANDED
from tests.graph_currency_test import LANDED_BEFORE
from tests.graph_currency_test import record
from tests.graph_currency_test import spdx
from tests.graph_currency_test import syft_sbom
from tests.repository_query_test import artifact_row

pytestmark = requires_clickhouse

#: January's scan of `acme/app`: its SBOM went in a `data prune` long
#: ago, and the ledger only ever held the current record, so these rows
#: are the one copy of what it depended on then.
JANUARY = datetime(2026, 1, 15, tzinfo=timezone.utc)
JANUARY_SCAN = [
    artifact_row(
        repository_id=1, artifact_id=f'{name}@{version}', name=name,
        version=version, purl=f'pkg:gem/{name}@{version}',
        sbom_commit_sha='a' * 40, observed_at=JANUARY,
    )
    for name, version in (('mail', '2.7.1'), ('left-pad', '1.3.0'))
]

RELEASES = [
    {'id': 11, 'tag_name': 'v4.2.0', 'published_at': '2026-01-02T03:04:05Z'},
    {'id': 12, 'tag_name': 'v4.3.0', 'published_at': '2026-09-02T03:04:05Z'},
]


def land_app(ingest: IngestionRepository) -> None:
    """`acme/app` as `db raw` lands it: the record, with its releases,
    the Syft SBOM at its commit, and a dependency graph."""
    land(
        ingest, 'repo', 1, 'data/07-sbom/ruby.jsonl',
        {**record(1, 'app', COMMIT), 'all_releases': RELEASES},
        LANDED_BEFORE,
    )
    land(
        ingest, SYFT, 1,
        f'data/07-sbom/ruby/acme/app/v4.3.0/{COMMIT}/sbom.json',
        syft_sbom(('mail', '2.9.1')), LANDED_BEFORE,
    )
    land_graph(
        ingest, 1, 'app',
        spdx(CREATED_BEFORE, ('rails', '~> 7.0'), ('sidekiq', '~> 7.0')),
        LANDED_BEFORE,
    )


def everything(query: QueryRepository) -> list[tuple[Any, ...]]:
    """Every artifact row, as the table holds it.

    Less `updated_at`, which is when a row was written, not anything
    about what it records.
    """
    return sorted(
        tuple(row) for row in query.client.query(
            'SELECT * EXCEPT (updated_at) FROM artifacts',
        ).result_rows
    )


def counts(query: QueryRepository) -> dict[str, int]:
    """How many rows each table holds, as its readers count them."""
    return {
        table: int(
            query.client.query(
                f'SELECT count() FROM {table}',
            ).result_rows[0][0],
        )
        for table in (
            'artifacts', 'repositories FINAL', 'releases FINAL',
            'raw_documents FINAL',
        )
    }


def tables(query: QueryRepository) -> set[str]:
    return {
        str(name) for (name,) in query.client.query(
            'SELECT name FROM system.tables '
            'WHERE database = currentDatabase()',
        ).result_rows
    }


@pytest.fixture
def indexed(ingest, query, db_command) -> QueryRepository:
    """`acme/app` indexed once, and January's scan beside it."""
    land_app(ingest)
    db_command('index', '--language', 'ruby')
    ingest.insert_batch(
        ARTIFACTS.name, ARTIFACTS.rows(JANUARY_SCAN), ARTIFACTS.column_names,
    )
    return query


def ask_after_every_write(
    repository: IngestionRepository,
    reader: QueryRepository,
    question: str,
    answers: list[Any],
) -> None:
    """Ask `question` after each statement and insert the repository
    sends. See `definitions_test.TestNoWindow` for why after each one."""
    for method in ('command', 'insert'):
        send = getattr(repository.client, method)

        def sent(*args: Any, _send: Any = send, **kwargs: Any) -> Any:
            result = _send(*args, **kwargs)
            try:
                answers.append(reader.client.query(question).result_rows)
            except Exception as error:  # noqa: BLE001 - the finding
                answers.append(str(error).split('\n')[0][:120])
            return result

        setattr(repository.client, method, sent)


class TestTheSameInputTwice:

    def test_indexing_it_again_changes_no_count(self, indexed, db_command):
        once = counts(indexed)
        assert once['releases FINAL'] == 2

        db_command('index', '--language', 'ruby')
        assert counts(indexed) == once

    def test_rebuilding_changes_no_count_either(self, indexed, db_command):
        once = counts(indexed)

        db_command('index', '--rebuild')
        assert counts(indexed) == once
        db_command('index', '--rebuild')
        assert counts(indexed) == once


class TestTheOldTableServesUntilTheSwap:

    def test_every_reader_answers_as_before_throughout(
        self, indexed, db_command,
    ):
        """Asked after every statement and every insert the rebuild
        sends: the old table's answer until the exchange, and the same
        answer after it, because the input did not change."""
        before = everything(indexed)
        answers: list[Any] = []
        db_command.on_open.append(
            lambda repository: ask_after_every_write(
                repository, indexed,
                'SELECT count() FROM artifacts', answers,
            ),
        )

        db_command('index', '--rebuild')

        assert len(answers) > 10
        assert {str(answer) for answer in answers} == {
            f'[({len(before)},)]',
        }
        assert everything(indexed) == before

    def test_a_failed_ingest_leaves_the_old_table(
        self, indexed, db_command, monkeypatch,
    ):
        """The rows it had written by then go with the new table."""
        before = everything(indexed)
        ingest_from_list = DbService.ingest_from_list

        def fails(self: DbService, *args: Any, **kwargs: Any) -> Any:
            ingest_from_list(self, *args, **kwargs)
            raise RuntimeError('the ingest fell over')

        monkeypatch.setattr(DbService, 'ingest_from_list', fails)

        result = db_command('index', '--rebuild', succeeds=False)

        assert result.exit_code != 0
        assert everything(indexed) == before
        assert 'artifacts_next' not in tables(indexed)

    def test_the_rest_of_the_schema_is_left_alone(
        self, indexed, clickhouse_db, db_command,
    ):
        """`repositories` is not being rebuilt, so the dictionary that
        loads it is not declared again: a rebuild of `artifacts` once
        dropped it, and the dependants panel had none until the CREATE
        after."""
        from tests.definitions_test import uuids

        before = uuids(indexed.client, clickhouse_db)

        db_command('index', '--rebuild')

        after = uuids(indexed.client, clickhouse_db)
        assert after['dict_repositories'] == before['dict_repositories']
        assert after['current_artifacts'] == before['current_artifacts']


class TestHistory:
    """A rebuild re-derives each repository's current scan from its
    documents, and keeps every other observation as the table held it:
    there is nowhere else to re-derive it from."""

    def test_an_older_scan_survives_a_rebuild(self, indexed, db_command):
        before = everything(indexed)
        january = [row for row in before if row[12] == 'a' * 40]
        assert len(january) == 2

        db_command('index', '--rebuild')

        assert everything(indexed) == before

    def test_what_reads_the_table_follows_the_swap(
        self, ingest, indexed, db_command,
    ):
        """The graph was fetched again between the two runs: the views
        and rollups read the rebuilt table by name, and are refreshed
        after the swap. `sidekiq`, which only the earlier graph listed,
        stays in the history and leaves the present."""
        land_graph(
            ingest, 1, 'app', spdx(CREATED, ('rails', '~> 7.1')), LANDED,
        )

        db_command('index', '--rebuild')

        assert dependants(indexed, 'rails') == [(1, '~> 7.1')]
        assert dependants(indexed, 'sidekiq') == []
        assert ('sidekiq',) not in indexed.client.query(
            'SELECT name FROM mv_packages',
        ).result_rows
        assert indexed.client.query(
            "SELECT month FROM mv_package_month WHERE name = 'sidekiq'",
        ).result_rows == [('2026-09',)]
        assert indexed.client.query(
            "SELECT month FROM mv_package_month WHERE name = 'left-pad'",
        ).result_rows == [('2026-01',)]


class TestATableOnAnotherEngine:

    def test_is_rebuilt_without_its_rows(self, ingest, query, db_command):
        """The drift the additive path refuses to migrate, whose rows
        the refusal says are discarded: they are not carried, and the
        documents fill the new table."""
        land_app(ingest)
        ingest.client.command('DROP TABLE artifacts')
        ingest.client.command(
            'CREATE TABLE artifacts (repository_id UInt64, name String) '
            'ENGINE = ReplacingMergeTree ORDER BY repository_id',
        )
        ingest.client.command("INSERT INTO artifacts VALUES (1, 'drifted')")

        db_command('index', '--rebuild')

        assert query.client.query(
            'SELECT engine FROM system.tables '
            "WHERE database = currentDatabase() AND name = 'artifacts'",
        ).result_rows == [('MergeTree',)]
        assert sorted(
            name for (name,) in query.client.query(
                'SELECT name FROM artifacts',
            ).result_rows
        ) == ['mail', 'rails', 'sidekiq']


class TestAnIndexMerges:

    def test_it_merges_repositories_and_not_releases(
        self, indexed, db_command,
    ):
        """`releases` is read by one query, `db status`'s count, through
        FINAL, and `OPTIMIZE ... FINAL` rewrote all of it on every run:
        1.74 s on a million rows, measured, whatever was indexed.
        `repositories` stays merged: 62 ms buys every FINAL read of it
        back its merged cost."""
        sent: list[str] = []

        def watch(repository: IngestionRepository) -> None:
            command = repository.client.command

            def recorded(sql: str, *args: Any, **kwargs: Any) -> Any:
                sent.append(' '.join(str(sql).split()))
                return command(sql, *args, **kwargs)

            setattr(repository.client, 'command', recorded)

        db_command.on_open.append(watch)

        db_command('index', '--language', 'ruby', '--limit', '1')

        optimized = [sql for sql in sent if sql.startswith('OPTIMIZE')]
        assert 'OPTIMIZE TABLE repositories FINAL' in optimized
        assert not [sql for sql in optimized if 'releases' in sql]
