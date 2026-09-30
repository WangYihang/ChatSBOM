"""The warehouse's tables, held to the rows the parsers make of the store.

The warehouse is filled by the parsers of `services/db_service.py`
(#131): an `artifacts` row is an observation and the scan it belongs
to, and `repositories` keeps the metadata without the columns that point
at a scan. The columns were ClickHouse's, and are the parsers' since the
server went (#153): a column made on one side and not kept on the
other fails here, before the two drift.
"""
from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime
from datetime import timezone
from typing import Any

import duckdb
import pytest

from chatsbom.core.documents import Document
from chatsbom.services.db_service import DbService
from chatsbom.warehouse import schema
from tests.db_ingest_test import make_repo

#: A Gradle build, for the rows the manifests declare.
GRADLE = (
    "dependencies { implementation 'org.springframework.boot:"
    "spring-boot-starter-web:3.2.0' }\n"
)

#: A dependency graph of one package.
GRAPH = {
    'sbom': {
        'creationInfo': {'created': '2026-09-14T03:56:20Z'},
        'packages': [{
            'SPDXID': 'p1', 'name': 'org.slf4j:slf4j-api',
            'versionInfo': '2.0.13',
            'externalRefs': [{
                'referenceType': 'purl',
                'referenceLocator': 'pkg:maven/org.slf4j/slf4j-api@2.0.13',
            }],
        }],
    },
}


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


def artifact_rows() -> list[dict[str, Any]]:
    """A row of each kind the parsers make: Syft's, a manifest's and the
    dependency graph's."""
    service = DbService()
    repo_row = service.parse_repository(make_repo())
    seen = datetime(2026, 2, 11, tzinfo=timezone.utc)
    syft = service.parse_artifacts(
        Document(
            body={'artifacts': [{'name': 'mail', 'type': 'gem'}]},
            observed_at=seen, origin='test',
        ),
        4321, repo_row,
    )
    declared = service.parse_manifests(
        [('build.gradle', GRADLE)], 4321, repo_row, observed_at=seen,
    )
    graph = service.parse_dependency_graph(
        Document(body=GRAPH, observed_at=seen, origin='test'), 4321, repo_row,
    )
    assert syft and declared and graph
    return [syft[0], declared[0], graph[0]]


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


def test_a_repository_is_the_parsers_row_less_its_scan_pointers(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Which scan is current is the warehouse's own rule, the newest of
    each source, so the columns a repository's row points at one with
    are the scans' here, and nothing points."""
    row = DbService().parse_repository(make_repo())
    assert columns(con, 'repositories') == [
        column for column in row if column not in schema.SCAN_POINTERS
    ]
    assert set(schema.SCAN_POINTERS) <= set(row)


def test_an_observation_and_its_scan_are_an_artifact_row(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Every column of a parser's artifact row is an observation's, or
    its scan's under the name `schema.SCAN_COLUMNS` gives it; and every
    observation's column but its place is made by them."""
    observation = set(columns(con, 'observations'))
    scan = set(columns(con, 'scans'))
    made: set[str] = set()
    for row in artifact_rows():
        for column in row:
            lifted = schema.SCAN_COLUMNS.get(column)
            if lifted is None:
                assert column in observation, column
            else:
                assert lifted in scan, column
        made |= set(row)
    assert observation - made == {'scan_id', 'position'}


def test_a_release_is_the_parsers_row(
    con: duckdb.DuckDBPyConnection,
) -> None:
    repo = make_repo(
        all_releases=[{'id': 9, 'tag_name': 'v1.0.0', 'assets': []}],
    )
    [release] = DbService().parse_releases(repo)
    assert columns(con, 'releases') == list(release)


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
