"""The writer: rows into the warehouse's tables, a batch at a time."""
from __future__ import annotations

from collections.abc import Iterator
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from typing import Any

import duckdb
import pytest

from chatsbom.core.instants import UNSET
from chatsbom.warehouse import connect
from chatsbom.warehouse import schema
from chatsbom.warehouse.writer import Scan
from chatsbom.warehouse.writer import Writer

MADE = datetime(2026, 2, 11, 9, 30, tzinfo=timezone.utc)


@pytest.fixture
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    with connect(':memory:') as connection:
        schema.create(connection)
        yield connection


def scan(*rows: dict[str, Any], observed_at: datetime = MADE) -> Scan:
    return Scan(
        repository_id=7, source='syft', input_key='c1', tool='syft@1.52.0',
        observed_at=observed_at, ref='v1', commit_sha='c1', rows=rows,
    )


def row(**fields: Any) -> dict[str, Any]:
    return {
        'repository_id': 7, 'name': 'rack', 'version': '3.1.0',
        'type': 'gem', 'source': 'syft', 'sbom_ref': 'v1',
        'sbom_commit_sha': 'c1', 'observed_at': MADE, **fields,
    }


def test_a_row_that_disagrees_with_its_scan_is_refused(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """It would be stored under a scan it does not belong to. Nothing of
    the scan is written: not the scan, not the rows before it."""
    for disagreeing in (
        {'sbom_ref': 'v2'}, {'sbom_commit_sha': 'c2'},
        {'observed_at': MADE + timedelta(seconds=1)},
        {'repository_id': 8}, {'source': 'github-depgraph'},
    ):
        with Writer(con) as writer, pytest.raises(ValueError):
            writer.scan(scan(row(), row(**disagreeing)))
    assert con.execute('SELECT count(*) FROM scans').fetchone() == (0,)
    assert con.execute(
        'SELECT count(*) FROM observations',
    ).fetchone() == (0,)


def test_a_second_writer_numbers_its_scans_after_the_first(
    con: duckdb.DuckDBPyConnection,
) -> None:
    for key in ('c1', 'c2'):
        with Writer(con) as writer:
            writer.scan(replace(scan(row()), input_key=key))
    assert con.execute(
        'SELECT scan_id FROM scans ORDER BY scan_id',
    ).fetchall() == [(1,), (2,)]


def test_one_instant_with_a_zone_and_without_is_one_instant(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """A naive value is UTC, as `instants.utc` reads one: the same
    instant the scan has with a zone, and in another zone."""
    east = timezone(timedelta(hours=8))
    with Writer(con) as writer:
        writer.scan(
            scan(
                row(observed_at=MADE.replace(tzinfo=None)),
                row(observed_at=MADE.astimezone(east), name='rails'),
            ),
        )
    assert con.execute(
        'SELECT observed_at, observations FROM scans',
    ).fetchall() == [(datetime(2026, 2, 11, 9, 30), 2)]


def test_a_column_a_row_lacks_holds_its_default(
    con: duckdb.DuckDBPyConnection,
) -> None:
    with Writer(con) as writer:
        writer.scan(scan({'name': 'rack'}))
        writer.add('repositories', {'id': 7, 'owner': 'acme', 'repo': 'app'})
    assert con.execute(
        'SELECT position, artifact_id, version, licenses, relationship, '
        'version_kind FROM observations',
    ).fetchall() == [(0, '', '', [], 'unknown', 'resolved')]
    assert con.execute(
        'SELECT stars, topics, created_at, is_fork FROM repositories',
    ).fetchall() == [(0, [], UNSET.replace(tzinfo=None), False)]


def test_rows_are_written_a_batch_at_a_time(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """Past a batch's size a table is written, and what is left when the
    writer closes."""
    with Writer(con, batch=10) as writer:
        writer.scan(scan(*(row(name=f'p{k}') for k in range(25))))
        assert con.execute(
            'SELECT count(*) FROM observations',
        ).fetchone() == (20,)
    assert con.execute('SELECT count(*) FROM observations').fetchone() == (25,)
    assert con.execute(
        'SELECT min(position), max(position) FROM observations',
    ).fetchone() == (0, 24)
