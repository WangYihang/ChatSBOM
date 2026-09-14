"""Parquet export: the payload the dashboard downloads."""
import json

import pytest

from chatsbom.core.schema import ARTIFACTS
from chatsbom.core.schema import REPOSITORIES
from chatsbom.export.parquet import export_dataset
from chatsbom.export.schema import EXPORT_SCHEMA
from chatsbom.models.relationship import DIRECT
from chatsbom.models.relationship import TRANSITIVE
from tests.conftest import requires_clickhouse
from tests.repository_query_test import artifact_row
from tests.repository_query_test import repo_row

pytestmark = requires_clickhouse

pa = pytest.importorskip('pyarrow')
pq = pytest.importorskip('pyarrow.parquet')


@pytest.fixture
def seeded(ingest, query):
    repos = [
        repo_row(id=1, owner='mastodon', repo='mastodon', stars=300),
        repo_row(id=2, owner='rails', repo='rails', stars=200),
    ]
    artifacts = [
        artifact_row(repository_id=1, artifact_id='a1', relationship=DIRECT),
        artifact_row(
            repository_id=1, artifact_id='a2',
            name='mini_mime', relationship=TRANSITIVE,
        ),
        artifact_row(
            repository_id=2, artifact_id='a3',
            relationship=TRANSITIVE,
        ),
    ]
    ingest.insert_batch(
        REPOSITORIES.name, REPOSITORIES.rows(repos), REPOSITORIES.column_names,
    )
    ingest.insert_batch(
        ARTIFACTS.name, ARTIFACTS.rows(artifacts), ARTIFACTS.column_names,
    )
    return query


def table_file(directory, table):
    """Locate an exported table by name, whatever its content hash.

    Filenames are content-addressed now, so a test that hard-codes
    `repositories.parquet` is asserting the very assumption that made
    `immutable` a lie. Asking the directory keeps the tests honest about
    what they actually care about: the contents of a named table.
    """
    matches = sorted(directory.glob(f'{table}-*.parquet'))
    assert len(matches) == 1, f'expected one {table} file, got {matches}'
    return matches[0]


def test_unknown_licences_are_reported_not_hidden(seeded, tmp_path):
    """'We do not know' is a finding about SBOM quality."""
    export_dataset(seeded, tmp_path)
    rows = pq.read_table(table_file(tmp_path, 'licenses')).to_pylist()
    assert rows, 'the licence table must not be empty for seeded data'
    assert all('license' in r for r in rows)


def test_history_carries_the_monthly_series(seeded, tmp_path):
    export_dataset(seeded, tmp_path)
    rows = pq.read_table(table_file(tmp_path, 'history')).to_pylist()
    mail = [r for r in rows if r['name'] == 'mail']
    assert mail, 'the monthly series is what a snapshot cannot answer'
    assert all(len(r['month']) == 7 for r in mail), 'YYYY-MM'


def test_export_writes_a_file_per_table(seeded, tmp_path):
    result = export_dataset(seeded, tmp_path)
    for table in EXPORT_SCHEMA.tables:
        assert table_file(tmp_path, table.name).exists()
    assert result.row_counts['repositories'] == 2
    assert result.row_counts['artifacts'] == 3
    assert 'history' in result.row_counts


def test_parquet_columns_match_the_declared_schema(seeded, tmp_path):
    export_dataset(seeded, tmp_path)
    for table in EXPORT_SCHEMA.tables:
        written = pq.read_schema(table_file(tmp_path, table.name))
        assert written.names == table.column_names, table.name


def test_relationship_values_are_within_the_enum(seeded, tmp_path):
    export_dataset(seeded, tmp_path)
    column = pq.read_table(
        table_file(tmp_path, 'artifacts'), columns=['relationship'],
    )['relationship'].to_pylist()
    allowed = set(EXPORT_SCHEMA.table('artifacts').column('relationship').enum)
    assert set(column) <= allowed


def test_repositories_carry_dependency_counts(seeded, tmp_path):
    export_dataset(seeded, tmp_path)
    rows = pq.read_table(table_file(tmp_path, 'repositories')).to_pylist()
    by_repo = {r['repo']: r for r in rows}
    assert by_repo['mastodon']['total_dependencies'] == 2
    assert by_repo['mastodon']['direct_dependencies'] == 1


def test_artifacts_are_sorted_for_row_group_pruning(seeded, tmp_path):
    export_dataset(seeded, tmp_path)
    names = pq.read_table(
        table_file(tmp_path, 'artifacts'), columns=['name'],
    )['name'].to_pylist()
    assert names == sorted(names), 'sorted by name keeps lookups cheap'


def test_manifest_describes_every_file(seeded, tmp_path):
    export_dataset(seeded, tmp_path)
    manifest = json.loads((tmp_path / 'manifest.json').read_text())

    assert manifest['schemaVersion'] == EXPORT_SCHEMA.version
    assert manifest['rowCounts']['repositories'] == 2
    assert manifest['rowCounts']['artifacts'] == 3
    files = {f['name']: f for f in manifest['files']}
    # Names are content-addressed, so assert the set of *tables* rather
    # than a set of literal filenames that changes with the data.
    assert {name.split('-')[0] for name in files} == {
        'repositories', 'artifacts', 'licenses', 'history',
    }
    for entry in files.values():
        assert entry['bytes'] > 0
        assert len(entry['sha256']) == 64


def test_manifest_records_the_generator_version(seeded, tmp_path):
    export_dataset(seeded, tmp_path)
    manifest = json.loads((tmp_path / 'manifest.json').read_text())
    assert manifest['generator'].startswith('chatsbom/')


def test_export_is_reproducible(seeded, tmp_path):
    a = export_dataset(seeded, tmp_path / 'a')
    b = export_dataset(seeded, tmp_path / 'b')
    assert a.checksums == b.checksums, 'same input must give the same bytes'


def test_empty_database_still_produces_valid_files(query, tmp_path):
    result = export_dataset(query, tmp_path)
    assert result.row_counts == {
        'repositories': 0, 'artifacts': 0, 'licenses': 0, 'history': 0,
    }
    for table in EXPORT_SCHEMA.tables:
        written = pq.read_schema(table_file(tmp_path, table.name))
        assert written.names == table.column_names


def test_export_creates_the_output_directory(seeded, tmp_path):
    out = tmp_path / 'nested' / 'deep'
    export_dataset(seeded, out)
    assert (out / 'manifest.json').exists()


def test_manifest_sources_reach_the_export(ingest, query, tmp_path):
    """The audit trail behind every direct/transitive verdict.

    This column was declared and then exported as a literal empty array,
    so a `transitive` label was indistinguishable from an unexamined one
    for anyone reading the Parquet.
    """
    ingest.insert_batch(
        REPOSITORIES.name,
        REPOSITORIES.rows([
            repo_row(
                id=5, owner='o', repo='r',
                manifest_sources=['Gemfile', 'x.gemspec'],
            ),
        ]),
        REPOSITORIES.column_names,
    )
    export_dataset(query, tmp_path)
    rows = pq.read_table(
        table_file(tmp_path, 'repositories'), columns=['manifest_sources'],
    )['manifest_sources'].to_pylist()
    assert rows == [['Gemfile', 'x.gemspec']]


def test_repository_with_no_manifests_exports_an_empty_list(
    ingest, query, tmp_path,
):
    ingest.insert_batch(
        REPOSITORIES.name,
        REPOSITORIES.rows([repo_row(id=6, manifest_sources=[])]),
        REPOSITORIES.column_names,
    )
    export_dataset(query, tmp_path)
    rows = pq.read_table(
        table_file(tmp_path, 'repositories'), columns=['manifest_sources'],
    )['manifest_sources'].to_pylist()
    assert rows == [[]]


# --- silent truncation ----------------------------------------------------

def test_export_verifies_it_wrote_every_row(ingest, query, tmp_path):
    """A truncated export must fail, not look like a success.

    The guest profile caps `max_result_rows` with
    `result_overflow_mode=break`, whose documented behaviour is to stop
    returning rows *without an error*. A real export silently lost 6.0M
    of 6.1M artifact rows and reported "Export Complete".
    """
    class TruncatingRepo:
        """Streams fewer rows than the table holds, as `break` would."""

        def __init__(self, inner):
            self._inner = inner
            self.client = inner.client

        def stream_rows(self, sql, parameters=None):
            rows = list(self._inner.stream_rows(sql, parameters))
            # Drop the tail, exactly as an overflow break does.
            yield from rows[: max(len(rows) - 1, 0)]

        def count_rows(self, sql, parameters=None):
            return self._inner.count_rows(sql, parameters)

    ingest.insert_batch(
        REPOSITORIES.name,
        REPOSITORIES.rows([
            repo_row(id=1, owner='a', repo='one'),
            repo_row(id=2, owner='b', repo='two'),
            repo_row(id=3, owner='c', repo='three'),
        ]),
        REPOSITORIES.column_names,
    )

    with pytest.raises(RuntimeError, match='truncated'):
        export_dataset(TruncatingRepo(query), tmp_path)


def test_a_complete_export_passes_the_check(seeded, tmp_path):
    result = export_dataset(seeded, tmp_path)
    assert result.row_counts['repositories'] == 2


def test_manifest_files_are_content_addressed(seeded, tmp_path):
    """Immutable files need immutable names.

    Parquet is served `immutable, max-age=31536000`, and with fixed
    filenames every export reused the same URLs — so a browser kept the
    previous table for a year while revalidating a manifest describing a
    different one. Measured when it happened: the manifest advertised sha
    659592a2 while the browser held e8e84bf5, and every query failed with
    `Binder Error: Table "r" does not have a column named "observed_at"`.
    A stale cache presenting as a schema bug.
    """
    export_dataset(seeded, tmp_path)
    manifest = json.loads((tmp_path / 'manifest.json').read_text())
    for entry in manifest['files']:
        assert entry['name'].endswith('.parquet')
        assert entry['name'].split('-')[-1][:8] == entry['sha256'][:8]


def test_every_file_the_manifest_names_exists(seeded, tmp_path):
    """A manifest naming a file that is not there is worse than none."""
    export_dataset(seeded, tmp_path)
    manifest = json.loads((tmp_path / 'manifest.json').read_text())
    for entry in manifest['files']:
        assert (tmp_path / entry['name']).exists(), entry['name']


def test_no_unaddressed_parquet_is_left_behind(seeded, tmp_path):
    """Otherwise an upload ships both and the stale URL stays reachable."""
    export_dataset(seeded, tmp_path)
    for path in tmp_path.glob('*.parquet'):
        assert '-' in path.stem, f'{path.name} is not content-addressed'


def test_row_counts_stay_keyed_by_table(seeded, tmp_path):
    """The filenames changed; the row-count keys must not."""
    export_dataset(seeded, tmp_path)
    manifest = json.loads((tmp_path / 'manifest.json').read_text())
    assert set(manifest['rowCounts']) == {
        'repositories', 'artifacts', 'licenses', 'history',
    }


def test_d1_export_counts_every_table_it_writes_rows_for(seeded, tmp_path):
    """A table the export writes but never counts is a silent failure.

    It happened: an edit meant to write `agg_edges` did not apply, and
    the export still reported success with a plausible file listing.
    Zero is a fine answer; absent is not.

    The SQL-computed aggregates are excluded because the export does not
    write their rows — `03-aggregates.sql` computes them inside D1, so
    their counts are not knowable here.
    """
    from chatsbom.export.d1 import D1_SCHEMA
    from chatsbom.export.d1 import export_d1

    computed_in_sql = {
        'agg_totals', 'agg_relationship_split', 'agg_language_coverage',
        'agg_top_packages', 'agg_dependency_buckets', 'agg_source_comparison',
    }
    result = export_d1(
        seeded, tmp_path / 'd1', depgraph_root=tmp_path / 'none',
    )
    for table in D1_SCHEMA.tables:
        if table.name in computed_in_sql:
            continue
        assert table.name in result.row_counts, table.name


def test_d1_aggregate_script_fills_the_tables_the_export_does_not(seeded, tmp_path):
    """The other half of the same guarantee.

    Between this and the test above, every declared table is accounted
    for by something: either the export writes its rows, or the
    aggregate script computes them.
    """
    from chatsbom.export.d1 import D1_SCHEMA
    from chatsbom.export.d1 import export_d1

    result = export_d1(
        seeded, tmp_path / 'd1', depgraph_root=tmp_path / 'none',
    )
    script = (result.directory / '03-aggregates.sql').read_text()
    for table in D1_SCHEMA.tables:
        if table.name in result.row_counts:
            continue
        assert f'INSERT INTO {table.name}' in script, table.name


def test_the_d1_scripts_actually_apply(seeded, tmp_path):
    """Apply all four to SQLite and count what lands.

    This is the test the export did not have, and its absence cost a
    shipped-broken export. Adding `packages.repositories` left the
    writer emitting two values for a three-column table; SQLite refuses
    that, but nothing here applied the SQL, so 54 assertions about the
    schema declaration and the statement text all passed while the
    `packages` table came out empty.

    The row counts the export *reports* are the counts it wrote out, not
    the counts that land. Comparing the two is the whole point.
    """
    import sqlite3

    from chatsbom.export.d1 import export_d1

    result = export_d1(
        seeded, tmp_path / 'd1', depgraph_root=tmp_path / 'none',
    )

    db = tmp_path / 'applied.sqlite'
    connection = sqlite3.connect(db)
    for name in (
        '01-schema.sql', '02-data.sql', '03-aggregates.sql', '04-indexes.sql',
    ):
        # executescript stops at the first error and raises, so a
        # rejected INSERT fails the test rather than being skipped.
        connection.executescript(
            (result.directory / name).read_text(encoding='utf-8'),
        )

    for table, expected in result.row_counts.items():
        landed = connection.execute(
            f'SELECT count(*) FROM {table}',  # noqa: S608 - schema-owned name
        ).fetchone()[0]
        assert landed == expected, f'{table}: wrote {expected}, landed {landed}'

    connection.close()


def test_the_applied_database_fills_the_package_dependant_count(seeded, tmp_path):
    """`packages.repositories` is what the search box ranks by.

    Declared in the schema, written by `03-aggregates.sql`, and read by
    a query that has no other way to sort. If the UPDATE is ever
    dropped, every package ranks as equally popular — which looks like
    working software, so it is asserted against a real applied database
    rather than against the SQL text.
    """
    import sqlite3

    from chatsbom.export.d1 import export_d1

    result = export_d1(
        seeded, tmp_path / 'd1', depgraph_root=tmp_path / 'none',
    )
    connection = sqlite3.connect(tmp_path / 'applied.sqlite')
    for name in ('01-schema.sql', '02-data.sql', '03-aggregates.sql'):
        connection.executescript(
            (result.directory / name).read_text(encoding='utf-8'),
        )

    counted, highest = connection.execute(
        'SELECT count(*), max(repositories) FROM packages',
    ).fetchone()
    assert counted > 0
    assert highest >= 1, 'every package ranks as equally unused'

    # And it must count repositories, not artifact rows: a package
    # appears once per manifest it is found in.
    rows = connection.execute(
        """SELECT p.name, p.repositories,
                  (SELECT count(DISTINCT a.repository_id) FROM artifacts a
                   WHERE a.package_id = p.id)
           FROM packages p""",
    ).fetchall()
    for name, stored, recomputed in rows:
        assert stored == recomputed, name

    connection.close()


class TestTheSummaryReportsRealCounts:
    """The "Export Complete" table printed 0 for every file.

    Exported names carry a content hash — `artifacts-5d2cc120.parquet`
    — and `row_counts` is keyed by table, so stripping only the
    extension left `artifacts-5d2cc120` and every lookup missed. A
    49 MB file was reported as 0 rows while the log line directly above
    it said 16,905,915. `row_counts`' keying has a test; the one place
    that reads it for a human did not.
    """

    def test_it_strips_the_cache_busting_hash(self) -> None:
        from chatsbom.commands.export.parquet import table_of
        assert table_of('artifacts-5d2cc120.parquet') == 'artifacts'
        assert table_of('repositories-2ab02b38.parquet') == 'repositories'

    def test_it_survives_a_name_with_no_hash(self) -> None:
        """Nothing writes these today, and a summary that raises while
        reporting a finished export would be worse than one that is
        wrong."""
        from chatsbom.commands.export.parquet import table_of
        assert table_of('artifacts.parquet') == 'artifacts'

    def test_every_exported_table_name_round_trips(self) -> None:
        """A table whose own name contains a dash would break the
        rsplit. None do — this asserts that rather than assuming it."""
        from chatsbom.commands.export.parquet import table_of
        from chatsbom.export.parquet import EXPORT_SCHEMA
        for table in EXPORT_SCHEMA.tables:
            assert '-' not in table.name
            assert table_of(f'{table.name}-deadbeef.parquet') == table.name

    def test_a_missing_count_is_not_printed_as_zero(self) -> None:
        """`0` reads as a real answer. The bug was invisible for
        exactly that reason, so an unmatched lookup has to look
        unmatched."""
        import inspect
        from chatsbom.commands.export import parquet
        source = inspect.getsource(parquet.main)
        assert 'row_counts.get(table_of(name))' in source
        assert 'row_counts.get(table_name, 0)' not in source
