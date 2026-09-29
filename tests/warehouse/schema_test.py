"""The warehouse's tables, held to the ClickHouse contract they port.

The warehouse is filled by the parsers `db index` uses (#131), so its
columns are ClickHouse's, rearranged: an `artifacts` row is an
observation and the scan it belongs to, and `repositories` keeps the
metadata without the columns that point at a scan. A column added on
one side and not the other fails here, before the two drift.
"""
from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime

import duckdb
import pytest

from chatsbom.core.schema import ARTIFACTS
from chatsbom.core.schema import EDGES
from chatsbom.core.schema import RELEASES
from chatsbom.core.schema import REPOSITORIES
from chatsbom.warehouse import schema


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    connection = duckdb.connect()
    schema.create(connection)
    yield connection
    connection.close()


def columns(con: duckdb.DuckDBPyConnection, table: str) -> list[str]:
    return [
        name for (name,) in con.execute(
            'SELECT column_name FROM information_schema.columns '
            'WHERE table_name = ? ORDER BY ordinal_position',
            [table],
        ).fetchall()
    ]


def test_the_tables(con: duckdb.DuckDBPyConnection) -> None:
    tables = {
        name for (name,) in con.execute(
            'SELECT table_name FROM information_schema.tables',
        ).fetchall()
    }
    assert {
        'repositories', 'repository_history', 'scans', 'observations',
        'releases', 'edges', 'corpus', 'build',
    } <= tables


def test_a_repository_is_what_clickhouse_holds_less_its_scan_pointers(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Which scan is current is the warehouse's own rule, the newest of
    each source, so the columns `db index` writes to point at one are
    the scans' here, and nothing points."""
    assert columns(con, 'repositories') == [
        column for column in REPOSITORIES.columns
        if column not in schema.SCAN_POINTERS
    ]
    assert set(schema.SCAN_POINTERS) <= set(REPOSITORIES.columns)


def test_an_observation_and_its_scan_are_an_artifacts_row(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Every column of `artifacts` is an observation's, or its scan's
    under the name `schema.SCAN_COLUMNS` gives it."""
    observation = set(columns(con, 'observations'))
    scan = set(columns(con, 'scans'))
    for column in ARTIFACTS.columns:
        lifted = schema.SCAN_COLUMNS.get(column)
        if lifted is None:
            assert column in observation, column
        else:
            assert lifted in scan, column


def test_releases_and_edges_are_clickhouses(
    con: duckdb.DuckDBPyConnection,
) -> None:
    assert columns(con, 'releases') == list(RELEASES.columns)
    assert columns(con, 'edges') == list(EDGES.columns)


def test_a_scan_is_one_input_read_by_one_tool(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """The store keys an output by its input and tool@version (#100),
    and so does the warehouse: the same one twice is refused."""
    scan = (
        "(?, 7, 'syft', 'c1', 'syft@1.52.0', ?, 'v1', 'release', 'c1', "
        "'07-sbom/7/c1/sbom.json', [], [], 0)"
    )
    made = datetime(2026, 2, 11, 9, 30)
    con.execute(f'INSERT INTO scans VALUES {scan}', [1, made])
    with pytest.raises(duckdb.ConstraintException):
        con.execute(f'INSERT INTO scans VALUES {scan}', [2, made])
