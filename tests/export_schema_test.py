"""The export schema is the contract the TypeScript dashboard is built from."""
import json

import pytest

from chatsbom.export.schema import ColumnType
from chatsbom.export.schema import EXPORT_SCHEMA
from chatsbom.export.typescript import render_typescript
from chatsbom.models.relationship import RELATIONSHIPS


def test_schema_declares_every_exported_table():
    assert {t.name for t in EXPORT_SCHEMA.tables} == {
        'repositories', 'artifacts', 'licenses', 'history',
    }


def test_history_is_a_separate_file_from_current_state():
    """The dashboard downloads current state; history is opt-in."""
    history = EXPORT_SCHEMA.table('history')
    assert history.to_dict()['file'] == 'history.parquet'
    assert 'month' in history.column_names


def test_every_column_has_a_described_type():
    for table in EXPORT_SCHEMA.tables:
        assert table.columns, table.name
        for column in table.columns:
            assert isinstance(column.type, ColumnType)
            assert column.description, f'{table.name}.{column.name}'


def test_artifacts_join_to_repositories():
    artifacts = EXPORT_SCHEMA.table('artifacts')
    assert 'repository_id' in [c.name for c in artifacts.columns]
    assert EXPORT_SCHEMA.table('repositories').primary_key == 'id'


def test_relationship_column_enumerates_the_python_literal():
    """The enum crosses the language boundary from one definition."""
    column = EXPORT_SCHEMA.table('artifacts').column('relationship')
    assert column.enum == list(RELATIONSHIPS)


def test_unknown_table_is_an_error():
    with pytest.raises(KeyError, match='nope'):
        EXPORT_SCHEMA.table('nope')


def test_unknown_column_is_an_error():
    with pytest.raises(KeyError, match='nope'):
        EXPORT_SCHEMA.table('artifacts').column('nope')


# --- JSON manifest ---------------------------------------------------------

def test_schema_serialises_to_json():
    payload = json.loads(EXPORT_SCHEMA.to_json())
    assert payload['version'] == EXPORT_SCHEMA.version
    names = [t['name'] for t in payload['tables']]
    assert 'artifacts' in names


def test_json_round_trips_the_enum():
    payload = json.loads(EXPORT_SCHEMA.to_json())
    artifacts = next(t for t in payload['tables'] if t['name'] == 'artifacts')
    column = next(
        c for c in artifacts['columns']
        if c['name'] == 'relationship'
    )
    assert column['enum'] == list(RELATIONSHIPS)


# --- generated TypeScript --------------------------------------------------

@pytest.fixture(scope='module')
def ts() -> str:
    return render_typescript(EXPORT_SCHEMA)


def test_typescript_declares_a_row_interface_per_table(ts):
    assert 'export interface RepositoryRow {' in ts
    assert 'export interface ArtifactRow {' in ts


def test_typescript_maps_types(ts):
    assert 'owner: string;' in ts
    assert 'stars: number;' in ts


def test_typescript_emits_the_relationship_union(ts):
    assert (
        "export type Relationship = 'direct' | 'transitive' | 'unknown';" in ts
    )
    assert 'relationship: Relationship;' in ts


def test_typescript_exports_column_names_for_runtime_checks(ts):
    assert 'export const ARTIFACT_COLUMNS' in ts
    assert "'repository_id'" in ts


def test_typescript_is_marked_generated(ts):
    first_lines = ts.splitlines()[:4]
    assert any('generated' in line.lower() for line in first_lines)
    assert any(
        'chatsbom export schema' in line.lower()
        for line in first_lines
    )


def test_typescript_has_no_trailing_whitespace(ts):
    assert not any(line != line.rstrip() for line in ts.splitlines())


def test_typescript_is_deterministic(ts):
    assert render_typescript(EXPORT_SCHEMA) == ts


# --- the generated file must not drift ------------------------------------

def test_checked_in_typescript_is_up_to_date(ts):
    """`web/src/schema.ts` is generated; a stale copy is a build-time lie."""
    from pathlib import Path

    generated = Path(__file__).resolve().parent.parent / \
        'web' / 'src' / 'schema.ts'
    if not generated.exists():
        pytest.skip('web/ not present in this checkout')

    assert generated.read_text(encoding='utf-8') == ts, (
        'web/src/schema.ts is out of date; regenerate with\n'
        '  uv run chatsbom export schema --typescript web/src/schema.ts '
        '--json web/src/schema.json'
    )


def test_checked_in_schema_json_is_up_to_date():
    from pathlib import Path

    generated = Path(__file__).resolve().parent.parent / \
        'web' / 'src' / 'schema.json'
    if not generated.exists():
        pytest.skip('web/ not present in this checkout')

    assert generated.read_text(encoding='utf-8') == EXPORT_SCHEMA.to_json()


class TestLicenceQueries:
    """Two consumers, two shapes, one question.

    `QUERIES['licenses']` is keyed `(license, type)` because the
    Parquet export declares and checks that. D1's `licenses` table
    declares one row per licence and the panel reads it as licence
    totals — and it used to be filled from the shared query with the
    type column simply dropped, so it held one row per licence per
    ecosystem and the panel showed whichever slice sorted highest as
    the whole: MIT 10,114 against a true 16,846, unknown 10,121
    against 23,022.
    """

    @staticmethod
    def _sql(query: str) -> str:
        """Comments stripped, because a substring check cannot tell SQL
        from a note about SQL — learned when `-- UNION ALL` satisfied
        the test guarding `UNION ALL`."""
        return '\n'.join(
            line for line in query.split('\n')
            if not line.strip().startswith('--')
        )

    def test_the_shared_query_keeps_the_type_key(self) -> None:
        """Parquet declares it and raises `licenses query is missing
        column(s) type` without it — which is how removing it was
        caught, by sixteen failing tests rather than by review."""
        from chatsbom.export.queries import QUERIES
        assert 'GROUP BY license, type' in self._sql(QUERIES['licenses'])

    def test_the_d1_query_groups_by_licence_alone(self) -> None:
        from chatsbom.export.queries import D1_LICENSES_QUERY
        sql = self._sql(D1_LICENSES_QUERY)
        assert 'GROUP BY license\n' in sql + '\n'
        assert 'GROUP BY license, type' not in sql

    def test_neither_takes_only_the_first_licence(self) -> None:
        """`arrayElement(licenses, 1)` kept one and the corpus carries
        packages under several: 112 licences vanished outright and 28
        were undercounted, `GPL-2.0-only` by a third — 139 of 216."""
        from chatsbom.export.queries import D1_LICENSES_QUERY, QUERIES
        for query in (QUERIES['licenses'], D1_LICENSES_QUERY):
            sql = self._sql(query)
            assert 'ARRAY JOIN' in sql
            assert 'arrayElement' not in sql

    def test_both_keep_the_unknown_bucket(self) -> None:
        """23,022 of 24,339 repositories hold a package with no licence
        at all — the largest category, and the one the panel's note
        promises is "shown rather than dropped". `ARRAY JOIN` discards
        an empty array, so the second branch is what preserves it."""
        from chatsbom.export.queries import D1_LICENSES_QUERY, QUERIES
        for query in (QUERIES['licenses'], D1_LICENSES_QUERY):
            sql = self._sql(query)
            assert 'empty(a.licenses)' in sql
            assert "'' AS license" in sql

    def test_d1_reads_its_own_query(self) -> None:
        """The export loops over three tables sharing one code path;
        this is the one that must not use `QUERIES[name]`."""
        import inspect
        from chatsbom.export import d1
        source = inspect.getsource(d1.export_d1)
        assert 'D1_LICENSES_QUERY' in source
