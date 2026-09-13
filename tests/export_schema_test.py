"""The export schema is the contract the TypeScript dashboard is built from."""
import json

import pytest

from chatsbom.export.schema import ColumnType
from chatsbom.export.schema import EXPORT_SCHEMA
from chatsbom.export.typescript import render_typescript
from chatsbom.models.relationship import RELATIONSHIPS


def test_schema_declares_both_tables():
    assert {t.name for t in EXPORT_SCHEMA.tables} == {
        'repositories', 'artifacts',
    }


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
