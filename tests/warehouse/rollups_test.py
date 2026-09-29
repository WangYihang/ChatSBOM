"""The rollups, ported from `core/rollups.py`, one test each.

The corpus is small enough to count by hand, and each row is here for a
semantic the port has to keep, as #120 corrected it:

- a fact is one however many manifests report it, and a repository is
  counted once however many facts it has (`uniqExact`, here
  `count(DISTINCT)`);
- `constrained` counts repositories, not constraint strings: rails
  declares `mail` twice, `~> 2.8` and `>= 2.7`, and is one repository
  under `constraint`;
- Syft's `dart-pub` is the `pub` ecosystem, and `php-composer`
  `composer`;
- months are UTC's, whatever the machine's zone;
- only the corpus counts: `acme/outside` is in an older snapshot, with a
  scan of its own, and appears in no answer.

Each answer here is counted by hand. Whether the port answers as
ClickHouse does, on larger corpora, is the parity check's to say.
"""
from __future__ import annotations

import time
from collections.abc import Iterator
from datetime import datetime
from datetime import timezone
from typing import Any

import duckdb
import pytest

from chatsbom.warehouse import connect
from chatsbom.warehouse.rollups import derive
from chatsbom.warehouse.rows import load

UTC = timezone.utc
JAN = datetime(2026, 1, 20, 9, 30, tzinfo=UTC)
FEB = datetime(2026, 2, 11, 9, 30, tzinfo=UTC)
SEP = datetime(2026, 9, 13, 8, 0, tzinfo=UTC)

CORPUS = 'all-2026-09-01'


def repository(
    id: int, owner: str, repo: str, language: str, snapshot: str = CORPUS,
) -> dict[str, Any]:
    return {
        'id': id, 'owner': owner, 'repo': repo,
        'url': f'https://github.com/{owner}/{repo}', 'stars': 1000 + id,
        'github_language': language, 'language': language,
        'snapshot': snapshot,
    }


def row(
    repository_id: int,
    commit: str,
    name: str,
    version: str,
    type: str,
    relationship: str,
    licenses: list[str],
    observed_at: datetime = FEB,
    *,
    found_by: str = 'cataloger',
    source: str = 'syft',
    version_kind: str = 'resolved',
    artifact_id: str = '',
) -> dict[str, Any]:
    """An `artifacts` row, as `db index` writes one."""
    return {
        'repository_id': repository_id,
        'artifact_id': artifact_id or f'{repository_id}-{name}-{version}',
        'name': name, 'version': version, 'type': type,
        'purl': f'pkg:{type}/{name}@{version}', 'found_by': found_by,
        'licenses': licenses, 'relationship': relationship,
        'source': source, 'version_kind': version_kind,
        'sbom_ref': 'main', 'sbom_commit_sha': commit,
        'observed_at': observed_at,
    }


def graph(
    repository_id: int,
    name: str,
    version: str,
    type: str,
    relationship: str,
    version_kind: str = 'resolved',
    *,
    artifact_id: str = '',
    observed_at: datetime = SEP,
) -> dict[str, Any]:
    return row(
        repository_id, '', name, version, type, relationship, [],
        observed_at, found_by='github-dependency-graph',
        source='github-depgraph', version_kind=version_kind,
        artifact_id=artifact_id,
    )


REPOSITORIES = [
    repository(1, 'rails', 'rails', 'Ruby'),
    repository(2, 'discourse', 'discourse', 'Ruby'),
    repository(3, 'apache', 'james', 'Java'),
    repository(4, 'laravel', 'laravel', 'PHP'),
    repository(5, 'monicahq', 'monica', 'PHP'),
    repository(6, 'psf', 'app', 'Python'),
    repository(7, 'expressjs', 'site', ''),
    repository(8, 'golang', 'tools', 'Go'),
    repository(9, 'acme', 'outside', 'Go', snapshot='all-2026-03-01'),
]

ARTIFACTS = [
    row(1, 'r0', 'mail', '2.7.0', 'gem', 'transitive', ['MIT'], JAN),
    row(1, 'r1', 'mail', '2.8.1', 'gem', 'transitive', ['MIT']),
    row(1, 'r1', 'mini_mime', '1.1.5', 'gem', 'transitive', ['MIT']),
    graph(1, 'mail', '~> 2.8', 'gem', 'direct', 'constraint'),
    graph(
        1, 'mail', '>= 2.7', 'gem', 'direct', 'constraint',
        artifact_id='1-rails.gemspec',
    ),
    row(
        2, 'd1', 'mail', '2.8.1', 'gem', 'direct', ['MIT'],
        found_by='ruby-gemfile-cataloger',
    ),
    row(
        2, 'd1', 'mail', '2.8.1', 'gem', 'direct', ['MIT'],
        found_by='ruby-gemspec-cataloger', artifact_id='2-mail-gemspec',
    ),
    graph(2, 'debug', '4.3.4', 'npm', 'transitive'),
    graph(2, 'ms', '2.1.2', 'npm', 'transitive'),
    row(3, 'j1', 'mail', '1.4.7', 'java-archive', 'direct', ['Apache-2.0']),
    row(
        3, 'j1', 'jakarta.mail', '2.1.0', 'maven', 'direct', [],
        source='manifest', version_kind='constraint',
    ),
    graph(
        4, 'laravel/framework', '^12.0', 'composer', 'direct', 'constraint',
        artifact_id='4-composer.json',
    ),
    graph(
        4, 'laravel/framework', '^12.0', 'composer', 'direct', 'constraint',
        artifact_id='4-packages/app/composer.json',
    ),
    graph(
        4, 'laravel/framework', '', 'composer', 'transitive', 'unversioned',
    ),
    row(
        5, 'mo1', 'laravel/framework', 'v12.49.0', 'php-composer', 'direct',
        ['MIT'],
    ),
    row(6, 'p1', 'requests', '2.32.0', 'python', 'direct', ['Apache-2.0']),
    row(6, 'p1', 'mail', '0.0.1', 'python', 'transitive', []),
    row(6, 'p1', 'certifi', '2024.2.2', 'python', 'unknown', ['MPL-2.0']),
    row(7, 'e1', 'express', '4.19.2', 'npm', 'direct', ['MIT']),
    row(7, 'e1', 'flutter_bloc', '8.1.3', 'dart-pub', 'direct', ['MIT']),
    row(9, 'o1', 'mail', '9.9.9', 'gem', 'direct', ['MIT']),
]

EDGES = [
    ('express', 'body-parser', 5, SEP),
    ('body-parser', 'debug', 3, SEP),
    ('mail-dev', 'mail', 1, SEP),
]


def warehouse(
    repositories: list[dict[str, Any]] = REPOSITORIES,
    artifacts: list[dict[str, Any]] = ARTIFACTS,
) -> duckdb.DuckDBPyConnection:
    """The corpus loaded, and everything derived from it."""
    con = connect(':memory:')
    load(
        con, repositories, artifacts, EDGES,
        corpus={r['id'] for r in repositories if r['snapshot'] == CORPUS},
    )
    derive(con)
    return con


@pytest.fixture(scope='module')
def con() -> Iterator[duckdb.DuckDBPyConnection]:
    connection = warehouse()
    yield connection
    connection.close()


def rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[tuple[Any, ...]]:
    return con.execute(sql).fetchall()


def answered(
    repositories: list[dict[str, Any]],
    artifacts: list[dict[str, Any]],
    sql: str,
) -> list[tuple[Any, ...]]:
    """What `sql` answers of a warehouse of these rows alone."""
    with warehouse(repositories, artifacts) as con:
        return rows(con, sql)


def test_facts(con: duckdb.DuckDBPyConnection) -> None:
    """Eighteen current facts: rails' January scan is history, the two
    manifests declaring laravel are one fact, and acme/outside counts
    for nothing."""
    assert rows(con, 'SELECT count(*) FROM facts') == [(18,)]
    assert rows(
        con, 'SELECT DISTINCT repository_id FROM facts ORDER BY 1',
    ) == [(1,), (2,), (3,), (4,), (5,), (6,), (7,)]


def test_mv_package_ecosystem(con: duckdb.DuckDBPyConnection) -> None:
    assert rows(
        con,
        'SELECT ecosystem, name, repositories, direct_repositories, '
        'records, direct_records, transitive_records, unknown_records, '
        'syft_records, depgraph_records, manifest_records '
        'FROM mv_package_ecosystem ORDER BY ecosystem, name',
    ) == [
        ('composer', 'laravel/framework', 2, 2, 3, 2, 1, 0, 1, 2, 0),
        ('gem', 'mail', 2, 2, 5, 4, 1, 0, 3, 2, 0),
        ('gem', 'mini_mime', 1, 0, 1, 0, 1, 0, 1, 0, 0),
        ('maven', 'jakarta.mail', 1, 1, 1, 1, 0, 0, 0, 0, 1),
        ('maven', 'mail', 1, 1, 1, 1, 0, 0, 1, 0, 0),
        ('npm', 'debug', 1, 0, 1, 0, 1, 0, 0, 1, 0),
        ('npm', 'express', 1, 1, 1, 1, 0, 0, 1, 0, 0),
        ('npm', 'ms', 1, 0, 1, 0, 1, 0, 0, 1, 0),
        ('pub', 'flutter_bloc', 1, 1, 1, 1, 0, 0, 1, 0, 0),
        ('pypi', 'certifi', 1, 0, 1, 0, 0, 1, 1, 0, 0),
        ('pypi', 'mail', 1, 0, 1, 0, 1, 0, 1, 0, 0),
        ('pypi', 'requests', 1, 1, 1, 1, 0, 0, 1, 0, 0),
    ]


def test_mv_repository_deps(con: duckdb.DuckDBPyConnection) -> None:
    assert rows(
        con,
        'SELECT repository_id, packages, direct_packages, records, '
        'syft_records, depgraph_records, manifest_records '
        'FROM mv_repository_deps ORDER BY repository_id',
    ) == [
        (1, 2, 1, 4, 2, 2, 0),
        (2, 3, 1, 4, 2, 2, 0),
        (3, 2, 2, 2, 1, 0, 1),
        (4, 1, 1, 2, 0, 2, 0),
        (5, 1, 1, 1, 1, 0, 0),
        (6, 3, 1, 3, 3, 0, 0),
        (7, 2, 2, 2, 2, 0, 0),
    ]


def test_mv_licenses(con: duckdb.DuckDBPyConnection) -> None:
    """Unknown is a row, keyed empty: five repositories have a package
    with no licence, more than any licence has."""
    assert rows(
        con,
        'SELECT license, repositories, packages FROM mv_licenses '
        'ORDER BY license',
    ) == [
        ('', 5, 5),
        ('Apache-2.0', 2, 2),
        ('MIT', 4, 5),
        ('MPL-2.0', 1, 1),
    ]


def test_mv_ecosystem_totals(con: duckdb.DuckDBPyConnection) -> None:
    """Records partition by ecosystem, so these add up to the facts."""
    assert rows(
        con,
        'SELECT ecosystem, direct_records, transitive_records, '
        'unknown_records, syft_records, depgraph_records, manifest_records, '
        'records FROM mv_ecosystem_totals ORDER BY ecosystem',
    ) == [
        ('composer', 2, 1, 0, 1, 2, 0, 3),
        ('gem', 4, 2, 0, 4, 2, 0, 6),
        ('maven', 2, 0, 0, 1, 0, 1, 2),
        ('npm', 1, 2, 0, 1, 2, 0, 3),
        ('pub', 1, 0, 0, 1, 0, 0, 1),
        ('pypi', 1, 1, 1, 3, 0, 0, 3),
    ]


def test_mv_packages(con: duckdb.DuckDBPyConnection) -> None:
    """`mail` is a gem, a Maven artifact and a PyPI package: four
    repositories, each once, where the per-ecosystem rows sum to five."""
    assert rows(
        con,
        'SELECT name, repositories, direct_repositories FROM mv_packages '
        'ORDER BY name',
    ) == [
        ('certifi', 1, 0),
        ('debug', 1, 0),
        ('express', 1, 1),
        ('flutter_bloc', 1, 1),
        ('jakarta.mail', 1, 1),
        ('laravel/framework', 2, 2),
        ('mail', 4, 3),
        ('mini_mime', 1, 0),
        ('ms', 1, 0),
        ('requests', 1, 1),
    ]


def test_mv_edges_forward(con: duckdb.DuckDBPyConnection) -> None:
    assert rows(
        con,
        'SELECT parent, child, repositories FROM mv_edges_forward '
        'ORDER BY parent, child',
    ) == [
        ('body-parser', 'debug', 3),
        ('express', 'body-parser', 5),
        ('mail-dev', 'mail', 1),
    ]


def test_mv_package_month(con: duckdb.DuckDBPyConnection) -> None:
    """The scan months, kept for the parity check: every observation of
    the corpus's repositories, in the month it was scanned."""
    assert rows(
        con,
        'SELECT name, source, month, repositories, direct_repositories '
        'FROM mv_package_month ORDER BY name, source, month',
    ) == [
        ('certifi', 'syft', '2026-02', 1, 0),
        ('debug', 'github-depgraph', '2026-09', 1, 0),
        ('express', 'syft', '2026-02', 1, 1),
        ('flutter_bloc', 'syft', '2026-02', 1, 1),
        ('jakarta.mail', 'manifest', '2026-02', 1, 1),
        ('laravel/framework', 'github-depgraph', '2026-09', 1, 1),
        ('laravel/framework', 'syft', '2026-02', 1, 1),
        ('mail', 'github-depgraph', '2026-09', 1, 1),
        ('mail', 'syft', '2026-01', 1, 0),
        ('mail', 'syft', '2026-02', 4, 2),
        ('mini_mime', 'syft', '2026-02', 1, 0),
        ('ms', 'github-depgraph', '2026-09', 1, 0),
        ('requests', 'syft', '2026-02', 1, 1),
    ]


def test_mv_package_type(con: duckdb.DuckDBPyConnection) -> None:
    assert rows(
        con,
        'SELECT name, type, repositories, direct_repositories '
        'FROM mv_package_type ORDER BY name, type',
    ) == [
        ('certifi', 'python', 1, 0),
        ('debug', 'npm', 1, 0),
        ('express', 'npm', 1, 1),
        ('flutter_bloc', 'dart-pub', 1, 1),
        ('jakarta.mail', 'maven', 1, 1),
        ('laravel/framework', 'composer', 1, 1),
        ('laravel/framework', 'php-composer', 1, 1),
        ('mail', 'gem', 2, 2),
        ('mail', 'java-archive', 1, 1),
        ('mail', 'python', 1, 0),
        ('mini_mime', 'gem', 1, 0),
        ('ms', 'npm', 1, 0),
        ('requests', 'python', 1, 1),
    ]


def test_mv_package_version(con: duckdb.DuckDBPyConnection) -> None:
    """What is set aside is one row per kind: rails' two constraints on
    `mail` are one repository (#120)."""
    assert rows(
        con,
        'SELECT name, version_kind, version, repositories '
        'FROM mv_package_version ORDER BY name, version_kind, version',
    ) == [
        ('certifi', 'resolved', '2024.2.2', 1),
        ('debug', 'resolved', '4.3.4', 1),
        ('express', 'resolved', '4.19.2', 1),
        ('flutter_bloc', 'resolved', '8.1.3', 1),
        ('jakarta.mail', 'constraint', '', 1),
        ('laravel/framework', 'constraint', '', 1),
        ('laravel/framework', 'resolved', 'v12.49.0', 1),
        ('laravel/framework', 'unversioned', '', 1),
        ('mail', 'constraint', '', 1),
        ('mail', 'resolved', '0.0.1', 1),
        ('mail', 'resolved', '1.4.7', 1),
        ('mail', 'resolved', '2.8.1', 2),
        ('mini_mime', 'resolved', '1.1.5', 1),
        ('ms', 'resolved', '2.1.2', 1),
        ('requests', 'resolved', '2.32.0', 1),
    ]


def test_mv_dependency_buckets(con: duckdb.DuckDBPyConnection) -> None:
    assert rows(
        con,
        'SELECT position, bucket, repositories FROM mv_dependency_buckets '
        'ORDER BY position',
    ) == [(0, '1-9', 7)]


def test_mv_dependency_buckets_bound_each_bucket(
    con: duckdb.DuckDBPyConnection,
) -> None:
    """The boundaries, which the corpus above does not reach: a
    repository with 9, 10, 24, 25, 99, 100, 249, 250, 999 and 1000
    packages."""
    sizes = (9, 10, 24, 25, 99, 100, 249, 250, 999, 1000)
    repositories = [
        repository(100 + i, 'size', str(n), 'C') for i, n in enumerate(sizes)
    ]
    artifacts = [
        row(100 + i, 'c', f'p{k}', '1', 'npm', 'direct', [])
        for i, n in enumerate(sizes) for k in range(n)
    ]
    assert answered(
        repositories, artifacts,
        'SELECT position, bucket, repositories FROM mv_dependency_buckets '
        'ORDER BY position',
    ) == [
        (0, '1-9', 1), (1, '10-24', 2), (2, '25-99', 2),
        (3, '100-249', 2), (4, '250-999', 2), (5, '1000+', 1),
    ]


def test_mv_version_kinds(con: duckdb.DuckDBPyConnection) -> None:
    """The three add up to the dependencies tile."""
    assert rows(
        con,
        'SELECT version_kind, records FROM mv_version_kinds '
        'ORDER BY version_kind',
    ) == [('constraint', 4), ('resolved', 13), ('unversioned', 1)]


def test_mv_language_coverage(con: duckdb.DuckDBPyConnection) -> None:
    """Every repository of the corpus is in the denominator, collected
    or not; no language on GitHub is `none`."""
    assert rows(
        con,
        'SELECT language, repositories, with_sbom, with_syft, '
        'with_depgraph, with_manifest FROM mv_language_coverage '
        'ORDER BY language',
    ) == [
        ('go', 1, 0, 0, 0, 0),
        ('java', 1, 1, 1, 0, 1),
        ('none', 1, 1, 1, 0, 0),
        ('php', 2, 2, 1, 1, 0),
        ('python', 1, 1, 1, 0, 0),
        ('ruby', 2, 2, 2, 2, 0),
    ]


def test_mv_language_coverage_folds_past_the_top_twelve() -> None:
    """The twelve languages with the most repositories by name, the
    rest `other` (D7); a tie at the boundary goes by name, and GitHub's
    spelling is folded to lower case first."""
    languages = [
        'Ruby', 'ruby', 'A1', 'A2', 'A3', 'A4', 'A5', 'A6', 'A7', 'A8',
        'A9', 'B1', 'B2', 'Z1', 'Z2',
    ]
    repositories = [
        repository(200 + i, 'lang', name, name)
        for i, name in enumerate(languages)
    ]
    assert answered(
        repositories, [],
        'SELECT language, repositories FROM mv_language_coverage '
        'ORDER BY language',
    ) == [
        ('a1', 1), ('a2', 1), ('a3', 1), ('a4', 1), ('a5', 1), ('a6', 1),
        ('a7', 1), ('a8', 1), ('a9', 1), ('b1', 1), ('b2', 1),
        ('other', 2), ('ruby', 2),
    ]


def test_mv_ecosystem_coverage(con: duckdb.DuckDBPyConnection) -> None:
    """A repository is under every ecosystem its current scans show,
    so discourse is a gem repository and an npm one: the rows overlap."""
    assert rows(
        con,
        'SELECT ecosystem, repositories, with_any, with_syft, '
        'with_depgraph, with_manifest FROM mv_ecosystem_coverage '
        'ORDER BY ecosystem',
    ) == [
        ('composer', 2, 2, 1, 1, 0),
        ('gem', 2, 2, 2, 1, 0),
        ('maven', 1, 1, 1, 0, 1),
        ('npm', 2, 2, 1, 1, 0),
        ('pub', 1, 1, 1, 0, 0),
        ('pypi', 1, 1, 1, 0, 0),
    ]


def test_mv_totals(con: duckdb.DuckDBPyConnection) -> None:
    """Seven repositories with a fact out of eight tracked; 18 facts,
    17 of them classified; ten names."""
    assert rows(
        con,
        'SELECT repositories, dependencies, packages, classified, tracked '
        'FROM mv_totals',
    ) == [(7, 18, 10, 17, 8)]


def test_mv_top_packages(con: duckdb.DuckDBPyConnection) -> None:
    """Every ecosystem's ranking and the corpus's, keyed `''`, both
    ways; a tie goes by name."""
    assert rows(
        con,
        'SELECT direct_only, rank, name, repositories, direct_repositories '
        "FROM mv_top_packages WHERE ecosystem = '' "
        'ORDER BY direct_only, rank',
    ) == [
        (0, 1, 'mail', 4, 3),
        (0, 2, 'laravel/framework', 2, 2),
        (0, 3, 'certifi', 1, 0),
        (0, 4, 'debug', 1, 0),
        (0, 5, 'express', 1, 1),
        (0, 6, 'flutter_bloc', 1, 1),
        (0, 7, 'jakarta.mail', 1, 1),
        (0, 8, 'mini_mime', 1, 0),
        (0, 9, 'ms', 1, 0),
        (0, 10, 'requests', 1, 1),
        (1, 1, 'mail', 4, 3),
        (1, 2, 'laravel/framework', 2, 2),
        (1, 3, 'express', 1, 1),
        (1, 4, 'flutter_bloc', 1, 1),
        (1, 5, 'jakarta.mail', 1, 1),
        (1, 6, 'requests', 1, 1),
        (1, 7, 'certifi', 1, 0),
        (1, 8, 'debug', 1, 0),
        (1, 9, 'mini_mime', 1, 0),
        (1, 10, 'ms', 1, 0),
    ]
    assert rows(
        con,
        'SELECT ecosystem, direct_only, rank, name FROM mv_top_packages '
        "WHERE ecosystem IN ('gem', 'pub') ORDER BY ecosystem, direct_only, rank",
    ) == [
        ('gem', 0, 1, 'mail'), ('gem', 0, 2, 'mini_mime'),
        ('gem', 1, 1, 'mail'), ('gem', 1, 2, 'mini_mime'),
        ('pub', 0, 1, 'flutter_bloc'), ('pub', 1, 1, 'flutter_bloc'),
    ]


def test_mv_top_packages_stops_at_a_hundred() -> None:
    repositories = [repository(300, 'many', 'names', 'C')]
    artifacts = [
        row(300, 'c', f'p{k:03}', '1', 'npm', 'direct', [])
        for k in range(101)
    ]
    assert answered(
        repositories, artifacts,
        'SELECT count(*), max(rank) FROM mv_top_packages '
        "WHERE ecosystem = '' AND direct_only = 0",
    ) == [(100, 100)]


def test_mv_edge_ambiguity(con: duckdb.DuckDBPyConnection) -> None:
    """`mail` is a name in three ecosystems; laravel/framework under
    both of Composer's spellings is one."""
    assert rows(
        con,
        'SELECT names, ambiguous_names, edges, ambiguous_edges, '
        'largest_repository FROM mv_edge_ambiguity',
    ) == [(10, 1, 3, 1, 3)]


def test_every_clickhouse_rollup_is_ported(
    con: duckdb.DuckDBPyConnection,
) -> None:
    from chatsbom.core.rollups import REFRESH_ORDER

    tables = {
        name for (name,) in rows(
            con, 'SELECT table_name FROM information_schema.tables',
        )
    }
    assert set(REFRESH_ORDER) <= tables


def test_months_are_utcs_on_a_machine_in_another_zone(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """At 20:00 UTC on 31 January it is February in UTC+8: the month is
    January wherever the build runs (#120)."""
    monkeypatch.setenv('TZ', 'Asia/Shanghai')
    time.tzset()
    try:
        late = datetime(2026, 1, 31, 20, 0, tzinfo=UTC)
        with warehouse(
            [repository(400, 'late', 'scan', 'C')],
            [row(400, 'c', 'pkg', '1', 'npm', 'direct', [], late)],
        ) as dated:
            assert rows(
                dated, 'SELECT month FROM mv_package_month',
            ) == [('2026-01',)]
            assert rows(
                dated, 'SELECT observed_at FROM scans',
            ) == [(datetime(2026, 1, 31, 20, 0),)]
    finally:
        monkeypatch.delenv('TZ')
        time.tzset()
