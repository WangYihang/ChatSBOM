"""ensure_schema must reconcile an existing table, not just create one.

`CREATE TABLE IF NOT EXISTS` silently does nothing when the table exists,
so adding a column to the DDL left older databases behind. Ingestion then
failed inside the driver with "Unrecognized column", which says nothing
about what to do.
"""
import pytest

from chatsbom.core.schema import ARTIFACTS
from chatsbom.core.schema import ddl_column_definitions
from chatsbom.core.schema import ddl_columns
from chatsbom.core.schema import REPOSITORIES_DDL
from tests.conftest import requires_clickhouse

pytestmark = requires_clickhouse


def test_ddl_column_definitions_parse_types():
    definitions = ddl_column_definitions(REPOSITORIES_DDL)
    assert definitions['id'].startswith('UInt64')
    assert definitions['owner'].startswith('LowCardinality(String)')
    assert 'DEFAULT' in definitions['sbom_ref']


def test_definitions_cover_every_declared_column():
    assert set(ddl_column_definitions(REPOSITORIES_DDL)) == set(
        ddl_columns(REPOSITORIES_DDL),
    )


def _columns(client, table: str) -> list[str]:
    return [
        row[0] for row in client.query(
            'SELECT name FROM system.columns '
            'WHERE database = currentDatabase() AND table = {t:String} '
            'ORDER BY position',
            parameters={'t': table},
        ).result_rows
    ]


def test_missing_column_is_added_to_an_existing_table(ingest):
    """Simulate a database created before `relationship` existed."""
    ingest.client.command('DROP TABLE artifacts')
    ingest.client.command("""
        CREATE TABLE artifacts (
            repository_id UInt64,
            artifact_id String,
            name String,
            version String,
            type LowCardinality(String),
            purl String,
            found_by LowCardinality(String),
            licenses Array(LowCardinality(String)),
            sbom_ref String DEFAULT '',
            sbom_commit_sha String DEFAULT '',
            updated_at DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY (repository_id, artifact_id, name, version, sbom_commit_sha)
    """)
    assert 'relationship' not in _columns(ingest.client, 'artifacts')

    ingest.ensure_schema()

    assert 'relationship' in _columns(ingest.client, 'artifacts')


def test_added_column_takes_the_ddl_default(ingest):
    ingest.client.command('DROP TABLE artifacts')
    ingest.client.command("""
        CREATE TABLE artifacts (
            repository_id UInt64,
            artifact_id String,
            name String,
            version String,
            type LowCardinality(String),
            purl String,
            found_by LowCardinality(String),
            licenses Array(LowCardinality(String)),
            sbom_ref String DEFAULT '',
            sbom_commit_sha String DEFAULT '',
            updated_at DateTime DEFAULT now()
        ) ENGINE = ReplacingMergeTree(updated_at)
        ORDER BY (repository_id, artifact_id, name, version, sbom_commit_sha)
    """)
    ingest.client.insert(
        'artifacts',
        [[1, 'a', 'mail', '2.9.0', 'gem', '', '', [], 'v1', 'sha']],
        column_names=[
            'repository_id', 'artifact_id', 'name', 'version', 'type',
            'purl', 'found_by', 'licenses', 'sbom_ref', 'sbom_commit_sha',
        ],
    )

    ingest.ensure_schema()

    value = ingest.client.query(
        'SELECT relationship FROM artifacts',
    ).result_rows[0][0]
    assert value == 'unknown', 'pre-existing rows must get the declared default'


def test_ensure_schema_is_idempotent(ingest):
    before = _columns(ingest.client, 'artifacts')
    ingest.ensure_schema()
    ingest.ensure_schema()
    assert _columns(ingest.client, 'artifacts') == before


def test_reconciliation_covers_every_insert_column(ingest):
    ingest.ensure_schema()
    existing = set(_columns(ingest.client, 'artifacts'))
    missing = [c for c in ARTIFACTS.columns if c not in existing]
    assert not missing, missing


def test_extra_columns_in_the_table_are_left_alone(ingest):
    """Migration is additive; it must not drop what it does not know."""
    ingest.client.command(
        'ALTER TABLE artifacts ADD COLUMN extra String DEFAULT %s' % "''",
    )
    ingest.ensure_schema()
    assert 'extra' in _columns(ingest.client, 'artifacts')


def test_rebuild_drops_rows_written_under_an_older_schema(ingest):
    """The off-by-one wrote 7-char SHAs; those rows are unreachable.

    They are excluded by the scan-matching join, so queries are correct,
    but they linger — 6.1M of them in the real database. `rebuild_table`
    is the tool for discarding them.
    """
    from chatsbom.core.schema import ARTIFACTS
    ingest.insert_batch(
        ARTIFACTS.name,
        ARTIFACTS.rows([{
            'repository_id': 1, 'artifact_id': 'a', 'name': 'mail',
            'version': '2.9.0', 'type': 'gem', 'purl': '', 'found_by': '',
            'licenses': [], 'relationship': 'unknown', 'source': 'syft',
            'version_kind': 'resolved',
            'sbom_ref': 'v1', 'sbom_commit_sha': 'abc1234',
        }]),
        ARTIFACTS.column_names,
    )
    before = ingest.client.query(
        'SELECT count() FROM artifacts',
    ).result_rows[0][0]
    assert before == 1

    ingest.rebuild_table(ARTIFACTS.name)

    after = ingest.client.query(
        'SELECT count() FROM artifacts',
    ).result_rows[0][0]
    assert after == 0


def test_rebuild_leaves_the_schema_in_place(ingest):
    from chatsbom.core.schema import ARTIFACTS
    ingest.rebuild_table(ARTIFACTS.name)
    columns = _columns(ingest.client, 'artifacts')
    assert 'relationship' in columns
    assert 'source' in columns


def test_rebuild_rejects_an_unknown_table(ingest):
    with pytest.raises(ValueError, match='not a managed table'):
        ingest.rebuild_table('system.tables')
