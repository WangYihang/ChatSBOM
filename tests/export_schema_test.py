"""The export schema is the contract the TypeScript dashboard is built from."""
import json

import pytest
from typer.testing import CliRunner

from chatsbom.export.schema import ColumnType
from chatsbom.export.schema import EXPORT_SCHEMA
from chatsbom.export.typescript import render_typescript
from chatsbom.export.warehouse import QUERIES
from chatsbom.models.provenance import ARTIFACT_SOURCES
from chatsbom.models.relationship import RELATIONSHIPS


def test_schema_declares_every_exported_table():
    assert {t.name for t in EXPORT_SCHEMA.tables} == {
        'repositories', 'artifacts', 'licenses', 'history',
    }


def test_history_is_a_separate_file_from_current_state():
    """The dashboard downloads current state; history is opt-in.

    Its own table, so its own file. Which file is the export's to say
    (`test_the_contract_names_no_file`): this asserted the contract's
    `history.parquet`, a name no export writes.
    """
    history = EXPORT_SCHEMA.table('history')
    assert history.name not in {
        t.name for t in EXPORT_SCHEMA.tables if t is not history
    }
    assert 'month' in history.column_names


def test_history_is_a_series_per_source():
    """Syft resolves lockfiles and GitHub's graph parses manifests, so
    one series over both reads a change of instrument as a change in
    adoption. The query keeps them apart, as D1's `history` and
    `mv_package_month` do; the contract dropped the column that says
    which is which."""
    column = EXPORT_SCHEMA.table('history').column('source')
    assert column.enum == list(ARTIFACT_SOURCES)
    assert column.ts_type == 'ArtifactSource'


def _order_by(sql: str) -> list[str]:
    """The column names of a query's last ORDER BY, in order."""
    clause = sql[sql.rindex('ORDER BY') + len('ORDER BY'):]
    clause = clause.split('LIMIT')[0]
    return [
        term.split()[0].rsplit('.', 1)[-1]
        for term in clause.split(',')
    ]


def test_each_table_is_sorted_as_it_declares():
    """`sortedBy` is what a reader prunes row groups by, so it must be a
    prefix of the query's own ORDER BY. `history` declared
    `(name, month)` while its query sorted `(name, source, month)`."""
    for table in EXPORT_SCHEMA.tables:
        order = _order_by(QUERIES[table.name])
        assert order[:len(table.sorted_by)] == list(table.sorted_by), (
            table.name, order,
        )


def test_the_contract_names_no_file():
    """Files are named after their own content (`content_addressed_name`),
    so the contract cannot know them. It named `history.parquet`, which
    no export writes and nothing serves; the manifest an export writes
    says which file holds each table."""
    for table in EXPORT_SCHEMA.tables:
        assert 'file' not in table.to_dict(), table.name
    assert '.parquet' not in EXPORT_SCHEMA.to_json()


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


def test_typescript_history_rows_name_their_source(ts):
    row = ts[ts.index('export interface HistoryRow {'):]
    assert 'source: ArtifactSource;' in row[:row.index('}')]


def test_typescript_names_no_file(ts):
    """`DATA_FILES` mapped each table to `<table>.parquet`, and the
    export writes `<table>-<hash>.parquet`: a map to files nobody
    serves."""
    assert 'DATA_FILES' not in ts
    assert '.parquet' not in ts


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


class TestTheSchemaCommand:
    """`chatsbom export schema > schema.json` wrote JSON that did not
    parse.

    It printed through the Rich console, which wraps to the terminal's
    width; with stdout redirected there is no terminal, and the width is
    80. Every longer line was broken inside a string, and `json.loads`
    stopped at the first: `Invalid control character at: line 86
    column 79`.
    """

    @staticmethod
    def run(monkeypatch: pytest.MonkeyPatch) -> str:
        from chatsbom.__main__ import app

        # What Rich measures when stdout is not a terminal.
        monkeypatch.setenv('COLUMNS', '80')
        result = CliRunner().invoke(app, ['export', 'schema'])
        assert result.exit_code == 0, result.output
        return result.stdout

    def test_its_output_parses_as_json(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        assert json.loads(self.run(monkeypatch)) == EXPORT_SCHEMA.to_dict()

    def test_its_output_is_the_contract_verbatim(
        self, monkeypatch: pytest.MonkeyPatch,
    ) -> None:
        """What `--json` writes, byte for byte."""
        assert self.run(monkeypatch) == EXPORT_SCHEMA.to_json()


class TestLicenceQueries:
    """The licence shares the Parquet export asks the warehouse for.

    `QUERIES['licenses']` is keyed `(license, type)` because the
    Parquet export declares and checks that. A snapshot's `licenses`
    is one row a licence (`snapshot/tables.py`): D1's was once filled
    from this query with the type column simply dropped, so it held
    one row per licence per ecosystem and the panel showed whichever
    slice sorted highest as the whole: MIT 10,114 against a true
    16,846, unknown 10,121 against 23,022.
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
        assert 'GROUP BY license, type' in self._sql(QUERIES['licenses'])

    def test_it_does_not_take_only_the_first_licence(self) -> None:
        """ClickHouse's `arrayElement(licenses, 1)` kept one and the
        corpus carries packages under several: 112 licences vanished
        outright and 28 were undercounted, `GPL-2.0-only` by a third —
        139 of 216. Every one is unnested."""
        sql = self._sql(QUERIES['licenses'])
        assert 'unnest(licenses)' in sql
        assert 'licenses[1]' not in sql

    def test_it_keeps_the_unknown_bucket(self) -> None:
        """23,022 of 24,339 repositories hold a package with no licence
        at all — the largest category, and the one the panel's note
        promises is "shown rather than dropped". `unnest`, as `ARRAY
        JOIN` did, drops an empty list, so the second branch is what
        preserves it."""
        sql = self._sql(QUERIES['licenses'])
        assert 'len(licenses) = 0' in sql
        assert "'' AS license" in sql
