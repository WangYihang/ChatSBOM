"""The page table answers the dependants as D1's statements do (#132).

`Dataset` reads a package's dependants from the snapshot's page table,
`dependants`, a range in the order the page shows them (#128 §2.4),
where D1 grouped and sorted every artifact of the package on each
request: at the documented shape, 171 ms against 14 for the most used
package, a page and its counts. The answers must not change. So here
the statements the Worker's `web/src/d1/queries.ts` asked D1 are the
oracle, run over the same snapshot's own D1 tables, and every package of a synthetic
corpus, under every filter the page offers, is asked both ways: its
count of dependants, its count of rows, and its pages, first, in the
middle and last.
"""
from __future__ import annotations

import sqlite3
from collections.abc import Iterator
from contextlib import closing
from pathlib import Path
from typing import Any

import pytest

from chatsbom.core.ecosystems import canonical
from chatsbom.dataset import Dataset
from chatsbom.dataset.open import connect
from chatsbom.dataset.shape import shape_dependant
from chatsbom.dataset.types import Dependent
from chatsbom.snapshot.write import write
from tests.snapshot.conftest import artifact
from tests.snapshot.conftest import at
from tests.snapshot.conftest import Corpus
from tests.snapshot.conftest import repository
from tests.snapshot.conftest import shop
from tests.snapshot.conftest import warehouse
from tests.warehouse.parity_test import synthetic

#: What `D1Dataset` asked D1, as `web/src/d1/queries.ts` had it: the
#: rows of a package, grouped as the table shows them and ordered by
#: every key, and both counts.
D1_FROM = """
       FROM artifacts AS a
       JOIN packages AS p ON p.id = a.package_id
       JOIN versions AS v ON v.id = a.version_id
       JOIN kinds AS k ON k.id = a.kind_id
       JOIN repositories AS r ON r.id = a.repository_id
       LEFT JOIN observations AS o
         ON o.repository_id = a.repository_id
        AND o.source = k.source"""
D1_OBSERVED = 'coalesce(o.observed_at, r.observed_at) AS observed_on'
D1_ONE_ROW = 'r.id, v.version, k.relationship, k.type, observed_on'

#: The filters the page offers, alone and together.
FILTERS: tuple[dict[str, Any], ...] = (
    {}, {'type': 'npm'}, {'type': 'php-composer'}, {'language': 'ruby'},
    {'language': 'other'}, {'direct_only': True},
    {'type': 'gem', 'language': 'ruby', 'direct_only': True},
)


def d1_where(name: str, filters: dict[str, Any]) -> tuple[str, list[Any]]:
    clauses, bound = ['p.name = ?'], [name]
    if filters.get('type'):
        clauses.append('k.type = ?')
        bound.append(canonical(filters['type']))
    if filters.get('language'):
        clauses.append('r.language_bucket = ?')
        bound.append(filters['language'].lower())
    if filters.get('direct_only'):
        clauses.append('k.relationship = ?')
        bound.append('direct')
    return ' AND '.join(clauses), bound


def d1_rows(
    db: sqlite3.Connection,
    name: str,
    filters: dict[str, Any],
    limit: int,
    offset: int,
) -> list[Dependent]:
    where, bound = d1_where(name, filters)
    cursor = db.execute(
        f"""
       SELECT r.owner AS owner, r.repo AS repo, r.stars AS stars,
              v.version AS version, r.url AS url,
              r.github_language AS language, k.type AS ecosystem,
              k.relationship AS relationship, {D1_OBSERVED},
              count(*) AS manifests
       {D1_FROM}
       WHERE {where}
       GROUP BY {D1_ONE_ROW}
       ORDER BY r.stars DESC, r.owner, r.repo, v.version, k.relationship,
                k.type, observed_on, r.id
       LIMIT ? OFFSET ?""",
        [*bound, limit, offset],
    )
    columns = [column[0] for column in cursor.description]
    return [
        shape_dependant(dict(zip(columns, row, strict=True)))
        for row in cursor.fetchall()
    ]


def d1_counts(
    db: sqlite3.Connection, name: str, filters: dict[str, Any],
) -> tuple[int, int]:
    where, bound = d1_where(name, filters)
    [(dependents,)] = db.execute(
        f"""
       SELECT count(DISTINCT a.repository_id)
       FROM artifacts AS a
       JOIN packages AS p ON p.id = a.package_id
       JOIN kinds AS k ON k.id = a.kind_id
       JOIN repositories AS r ON r.id = a.repository_id
       WHERE {where}""",
        bound,
    ).fetchall()
    [(rows,)] = db.execute(
        f"""
       SELECT count(*) FROM (
           SELECT {D1_OBSERVED}
           {D1_FROM}
           WHERE {where}
           GROUP BY {D1_ONE_ROW}
       )""",
        bound,
    ).fetchall()
    return int(dependents), int(rows)


def agree(db: sqlite3.Connection, names: list[str]) -> int:
    """Every name, under every filter, asked both ways; how many pages
    were compared."""
    dataset = Dataset(db)
    pages = 0
    for name in names:
        for filters in FILTERS:
            dependents, rows = d1_counts(db, name, filters)
            assert dataset.count_dependents(name, **filters) == dependents, (
                name, filters,
            )
            assert dataset.count_dependent_rows(name, **filters) == rows, (
                name, filters,
            )
            for limit, offset in {
                (50, 0), (7, 0), (7, 7), (7, rows // 2), (7, max(0, rows - 7)),
                (500, 0), (5, rows + 5),
            }:
                assert dataset.dependents_of(
                    name, limit=limit, offset=offset, **filters,
                ) == d1_rows(db, name, filters, limit, offset), (
                    name, filters, limit, offset,
                )
                pages += 1
    return pages


@pytest.fixture(scope='module')
def synthetic_snapshot(
    tmp_path_factory: pytest.TempPathFactory,
) -> Iterator[sqlite3.Connection]:
    directory = tmp_path_factory.mktemp('synthetic')
    repositories, artifacts, edges, ids = synthetic()
    written = write(
        warehouse(
            directory / 'warehouse.duckdb',
            Corpus(repositories, artifacts, edges, ids),
        ),
        directory / 'snapshots',
    )
    with closing(connect(written.path)) as db:
        yield db


def names(db: sqlite3.Connection) -> list[str]:
    return [
        str(name) for (name,) in db.execute(
            'SELECT name FROM packages ORDER BY repositories DESC, name',
        ).fetchall()
    ]


class TestAsD1Answers:

    def test_every_package_of_a_synthetic_corpus(
        self, synthetic_snapshot: sqlite3.Connection,
    ) -> None:
        every = names(synthetic_snapshot)
        assert len(every) > 300
        pages = agree(synthetic_snapshot, every)
        assert pages > 10_000

    def test_repositories_the_page_cannot_tell_apart(
        self, tmp_path: Path,
    ) -> None:
        """Two repositories of one name and one star count, which a
        stale record can make: D1 orders their rows by version before
        repository, and so does the page table, which gives them one
        place."""
        corpus = shop()
        corpus.repositories += [
            repository(5, 'twin', 'repo', 700, 'Ruby'),
            repository(6, 'twin', 'repo', 700, 'Ruby'),
        ]
        assert corpus.corpus is not None
        corpus.corpus |= {5, 6}
        for id, versions in ((5, ('2.0', '1.0')), (6, ('1.5', '3.0'))):
            for version in versions:
                corpus.artifacts.append(
                    artifact(
                        id, 'rack', version, 'gem', observed_at=at(2026, 5, id),
                        commit=f't{id}', relationship='direct',
                    ),
                )
        written = write(
            warehouse(tmp_path / 'warehouse.duckdb', corpus),
            tmp_path / 'snapshots',
        )
        with closing(connect(written.path)) as db:
            assert agree(db, names(db))
            assert [
                row.version for row in Dataset(db).dependents_of('rack')
            ][:4] == ['1.0', '1.5', '2.0', '3.0']
