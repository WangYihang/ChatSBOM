"""`export d1`, from ClickHouse: that it counts, and applies, what it
writes.

The Parquet export's tests are `parquet_warehouse_test.py`'s: it reads
the warehouse alone since `--from` went (#153).
"""
import pytest

from chatsbom.core.schema import ARTIFACTS
from chatsbom.core.schema import REPOSITORIES
from chatsbom.models.relationship import DIRECT
from chatsbom.models.relationship import TRANSITIVE
from tests.conftest import requires_clickhouse
from tests.export_d1_apply_test import apply_scripts
from tests.export_d1_apply_test import seed_edges
from tests.repository_query_test import artifact_row
from tests.repository_query_test import repo_row

pytestmark = requires_clickhouse


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


@pytest.fixture
def edged(seeded, ingest):
    """`seeded`, with an edge between two of its packages, as `db
    edges` stores it: the D1 export refuses an empty edge table."""
    seed_edges(ingest, ('mail', 'mini_mime', 1))
    return seeded


def test_d1_export_counts_every_table_it_writes_rows_for(edged, tmp_path):
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
        'agg_ecosystem_coverage', 'agg_top_packages',
        'agg_dependency_buckets', 'agg_source_comparison',
    }
    result = export_d1(edged, tmp_path / 'd1')
    for table in D1_SCHEMA.tables:
        if table.name in computed_in_sql:
            continue
        assert table.name in result.row_counts, table.name


def test_d1_aggregate_script_fills_the_tables_the_export_does_not(edged, tmp_path):
    """The other half of the same guarantee.

    Between this and the test above, every declared table is accounted
    for by something: either the export writes its rows, or the
    aggregate script computes them.
    """
    from chatsbom.export.d1 import D1_SCHEMA
    from chatsbom.export.d1 import export_d1

    result = export_d1(edged, tmp_path / 'd1')
    script = (result.directory / '03-aggregates.sql').read_text()
    for table in D1_SCHEMA.tables:
        if table.name in result.row_counts:
            continue
        assert f'INSERT INTO {table.name}' in script, table.name


def test_the_d1_scripts_actually_apply(edged, tmp_path):
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
    from contextlib import closing

    from chatsbom.export.d1 import export_d1

    result = export_d1(edged, tmp_path / 'd1')

    db = tmp_path / 'applied.sqlite'
    with closing(sqlite3.connect(db)) as connection:
        # Every script, the data's parts among them, in name order.
        apply_scripts(result.directory, sorted(result.files), connection)

        for table, expected in result.row_counts.items():
            landed = connection.execute(
                f'SELECT count(*) FROM {table}',  # noqa: S608 - schema-owned name
            ).fetchone()[0]
            assert landed == expected, f'{table}: wrote {expected}, landed {landed}'


def test_the_applied_database_fills_the_package_dependant_count(edged, tmp_path):
    """`packages.repositories` is what the search box ranks by.

    Declared in the schema, written by `03-aggregates.sql`, and read by
    a query that has no other way to sort. If the UPDATE is ever
    dropped, every package ranks as equally popular — which looks like
    working software, so it is asserted against a real applied database
    rather than against the SQL text.
    """
    import sqlite3
    from contextlib import closing

    from chatsbom.export.d1 import export_d1

    result = export_d1(edged, tmp_path / 'd1')
    with closing(sqlite3.connect(tmp_path / 'applied.sqlite')) as connection:
        apply_scripts(result.directory, sorted(result.files), connection)

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
