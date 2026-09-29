"""Adoption over time by intervals (owner decision Q9 on #128).

A repository counts for a package in every month between two
consecutive scans of one source that both show it, where the scan-month
series, `mv_package_month`, counts it only in the months it was
scanned. Between two scans the package is known to have held; before
the first, after the last, and after a scan that no longer shows it,
nothing is known, and nothing is counted.
"""
from __future__ import annotations

from collections.abc import Iterator
from datetime import datetime
from datetime import timezone
from typing import Any

import duckdb
import pytest

from chatsbom.core.instants import UNSET
from chatsbom.warehouse import connect
from chatsbom.warehouse.rollups import derive
from chatsbom.warehouse.rows import load
from tests.warehouse.conftest import artifact
from tests.warehouse.conftest import at
from tests.warehouse.conftest import Build
from tests.warehouse.conftest import Listed
from tests.warehouse.conftest import Store

UTC = timezone.utc


def month(number: int, day: int = 11) -> datetime:
    """A day in `number`'s month of 2026, in UTC."""
    return datetime(2026, number, day, 9, 30, tzinfo=UTC)


def scan(
    repository_id: int,
    when: datetime,
    *names: str | tuple[str, str],
    source: str = 'syft',
) -> list[dict[str, Any]]:
    """The rows of one scan: each name, `direct` unless given as
    `(name, relationship)`."""
    commit = f'{repository_id}-{when:%Y%m%d%H%M}' if source != (
        'github-depgraph'
    ) else ''
    rows = []
    for entry in names:
        name, relationship = (
            entry if isinstance(entry, tuple) else (entry, 'direct')
        )
        rows.append({
            'repository_id': repository_id,
            'artifact_id': f'{repository_id}-{name}',
            'name': name, 'version': '1.0', 'type': 'gem', 'purl': '',
            'found_by': 'cataloger', 'licenses': [],
            'relationship': relationship, 'source': source,
            'version_kind': 'resolved', 'sbom_ref': 'main',
            'sbom_commit_sha': commit, 'observed_at': when,
        })
    return rows


def repository(id: int, snapshot: str = 'all-2026-09-01') -> dict[str, Any]:
    return {
        'id': id, 'owner': 'acme', 'repo': f'r{id}',
        'github_language': 'Ruby', 'snapshot': snapshot,
    }


@pytest.fixture
def adoption() -> Iterator[Any]:
    """The series of a warehouse of these scans, per package and
    source: `{month: (repositories, direct_repositories)}`."""
    opened: list[duckdb.DuckDBPyConnection] = []

    def series(
        *scans: list[dict[str, Any]],
        name: str = 'mail',
        source: str = 'syft',
        table: str = 'mv_package_month_intervals',
        corpus: set[int] | None = None,
    ) -> dict[str, tuple[int, int]]:
        rows = [row for found in scans for row in found]
        ids = sorted({row['repository_id'] for row in rows})
        con = connect(':memory:')
        opened.append(con)
        load(con, [repository(i) for i in ids], rows, corpus=corpus)
        derive(con)
        return {
            month: (repositories, direct)
            for month, repositories, direct in con.execute(
                'SELECT month, repositories, direct_repositories '
                f'FROM {table} WHERE name = ? AND source = ? ORDER BY month',
                [name, source],
            ).fetchall()
        }

    yield series
    for con in opened:
        con.close()


def months(
    first: int, last: int, counts: tuple[int, int] = (1, 1),
) -> dict[str, tuple[int, int]]:
    """Every month of 2026 from `first` to `last`, each with `counts`."""
    return {f'2026-{m:02}': counts for m in range(first, last + 1)}


def test_a_repository_counts_in_every_month_between_scans_that_show_it(
    adoption: Any,
) -> None:
    """Scanned in February and in September, both times with `mail`:
    it depended on `mail` all along, and counts in the six months
    between as well."""
    assert adoption(
        scan(1, month(2), 'mail'), scan(1, month(9), 'mail'),
    ) == months(2, 9)


def test_the_scan_months_count_only_the_months_scanned(adoption: Any) -> None:
    """The series ClickHouse draws, kept for the parity check."""
    assert adoption(
        scan(1, month(2), 'mail'), scan(1, month(9), 'mail'),
        table='mv_package_month',
    ) == {'2026-02': (1, 1), '2026-09': (1, 1)}


def test_a_dependency_that_disappears_counts_until_the_last_scan_showing_it(
    adoption: Any,
) -> None:
    """`mail` is in January's scan and not April's: known in January,
    unknown after. `rack` is in all three, so it runs through."""
    scans = (
        scan(1, month(1), 'mail', 'rack'),
        scan(1, month(4), 'rack'),
        scan(1, month(7), 'rack'),
    )
    assert adoption(*scans) == months(1, 1)
    assert adoption(*scans, name='rack') == months(1, 7)


def test_one_that_comes_back_counts_again_from_the_scan_showing_it(
    adoption: Any,
) -> None:
    assert adoption(
        scan(1, month(1), 'mail', 'rack'),
        scan(1, month(4), 'rack'),
        scan(1, month(7), 'mail', 'rack'),
        scan(1, month(9), 'mail'),
    ) == {**months(1, 1), **months(7, 9)}


def test_nothing_is_known_after_the_newest_scan(adoption: Any) -> None:
    """One scan in March: counted in March, not in every month since."""
    assert adoption(scan(1, month(3), 'mail')) == months(3, 3)


def test_each_source_has_runs_of_its_own(adoption: Any) -> None:
    """A graph in June between two Syft scans neither extends nor ends
    the Syft run: the instruments measure differently (#55 §4.13)."""
    scans = (
        scan(1, month(2), 'mail'),
        scan(1, month(6), 'mail', source='github-depgraph'),
        scan(1, month(9), 'mail'),
    )
    assert adoption(*scans) == months(2, 9)
    assert adoption(*scans, source='github-depgraph') == months(6, 6)


def test_declared_counts_where_every_scan_between_declared_it(
    adoption: Any,
) -> None:
    """Direct in February and September, transitive in May: a
    dependant all along, a declared one in February and September."""
    assert adoption(
        scan(1, month(2), 'mail'),
        scan(1, month(5), ('mail', 'transitive')),
        scan(1, month(9), 'mail'),
    ) == {
        '2026-02': (1, 1), '2026-03': (1, 0), '2026-04': (1, 0),
        '2026-05': (1, 0), '2026-06': (1, 0), '2026-07': (1, 0),
        '2026-08': (1, 0), '2026-09': (1, 1),
    }


def test_a_repository_is_counted_once_a_month(adoption: Any) -> None:
    """Two scans in one month are one month, and two repositories whose
    intervals overlap are two where they do."""
    assert adoption(
        scan(1, month(2, 3), 'mail'), scan(1, month(2, 20), 'mail'),
        scan(1, month(6), 'mail'),
        scan(2, month(4), 'mail'), scan(2, month(9), 'mail'),
    ) == {
        '2026-02': (1, 1), '2026-03': (1, 1), '2026-04': (2, 2),
        '2026-05': (2, 2), '2026-06': (2, 2), '2026-07': (1, 1),
        '2026-08': (1, 1), '2026-09': (1, 1),
    }


def test_only_the_corpus_counts(adoption: Any) -> None:
    assert adoption(
        scan(1, month(2), 'mail'), scan(1, month(4), 'mail'),
        scan(2, month(2), 'mail'), scan(2, month(4), 'mail'),
        corpus={1},
    ) == months(2, 4)


def test_months_are_utcs(adoption: Any) -> None:
    """At 20:00 UTC on 31 January, in any zone, the scan is January's."""
    late = datetime(2026, 1, 31, 20, 0, tzinfo=UTC)
    assert adoption(
        scan(1, late, 'mail'), scan(1, month(3, 1), 'mail'),
    ) == months(1, 3)


def test_a_scan_with_no_date_is_left_out(adoption: Any) -> None:
    """A commit's manifests with no Syft document to date them have the
    unset date: they cannot be placed, and do not draw a line from 1970."""
    undated = scan(1, UNSET, 'mail', source='manifest')
    assert adoption(
        undated,
        scan(1, month(2), 'mail', source='manifest'),
        scan(1, month(4), 'mail', source='manifest'),
        source='manifest',
    ) == months(2, 4)


def test_a_scan_that_saw_nothing_ends_the_run(
    store: Store, built: Build,
) -> None:
    """In the store, where a Syft document that found nothing is a scan
    with no observation: `mail` was not there in May."""
    listed = Listed(1, 'acme', 'app')
    store.seed(store.snapshot(at(2026, 9, 1).date(), listed), listed)
    store.sbom(1, 'a' * 40, artifact('mail', '2.8.1', 'gem'), at=month(2))
    store.sbom(1, 'b' * 40, at=month(5))
    store.sbom(1, 'c' * 40, artifact('mail', '2.8.1', 'gem'), at=month(9))
    store.record(1, 'acme', 'app', commit='c' * 40)
    con = built()
    assert con.execute(
        'SELECT month FROM mv_package_month_intervals '
        "WHERE name = 'mail' AND source = 'syft' ORDER BY month",
    ).fetchall() == [('2026-02',), ('2026-09',)]
