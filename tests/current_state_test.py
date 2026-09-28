"""What "current" means, asked of every reader at once.

`artifacts` is append-only: a repository scanned twice keeps both scans'
rows, which is the point of the design. "Who depends on X" is then a
question about the scan each repository records now, and the CLI and the
exports always asked it that way, joining on the repository's
`sbom_commit_sha`. The rollups, the repository dictionary and the
dashboard's dependants query read every row ever appended. With
`history_test.py`'s own seed the CLI showed mail 2.9.1 alone,
`mv_package_version` showed 2.7.1 beside it, and
`mv_repository_deps.records` doubled: two halves of one product,
disagreeing as soon as a repository had a second scan.

So one fixture, two scans of one repository, and every reader asked the
same questions. Not every reader is meant to see only the new scan:
`mv_package_month` and the exported `history` are the series the table is
append-only for, and they must still see both.
"""
from __future__ import annotations

import hashlib
import importlib.util
import json
import re
import sqlite3
import sys
from datetime import datetime
from pathlib import Path
from types import ModuleType
from types import SimpleNamespace
from typing import Any

import pytest

from chatsbom.commands.db.export import COLUMNS
from chatsbom.commands.db.export import export_rows
from chatsbom.core.documents import RawDocuments
from chatsbom.core.documents import RawManifests
from chatsbom.core.documents import RawRecords
from chatsbom.core.repository import IngestionRepository
from chatsbom.core.repository import QueryRepository
from chatsbom.core.rollups import ROLLUPS
from chatsbom.core.schema import ARTIFACTS
from chatsbom.core.schema import REPOSITORIES
from chatsbom.export.d1 import export_d1
from chatsbom.models.framework_index import FrameworkIndex
from chatsbom.models.provenance import CONSTRAINT
from chatsbom.models.provenance import DEPGRAPH
from chatsbom.models.provenance import UNVERSIONED
from chatsbom.models.relationship import DIRECT
from chatsbom.models.relationship import TRANSITIVE
from chatsbom.services.db_service import DbService
from chatsbom.services.openapi_service import OpenApiService
from tests.conftest import requires_clickhouse
from tests.db_ingest_test import FakeIngestionRepository
from tests.repository_query_test import artifact_row
from tests.repository_query_test import repo_row

pytestmark = requires_clickhouse

ROOT = Path(__file__).resolve().parents[1]

JAN = datetime(2026, 1, 15)
SEP = datetime(2026, 9, 15)

#: `mastodon/mastodon`'s January scan, and the one it records now.
OLD = 'a' * 40
NEW = 'b' * 40


def syft(name: str, version: str, commit: str, seen: datetime, **over: Any):
    row: dict[str, Any] = {
        'repository_id': 1, 'artifact_id': f'{name}@{version}',
        'name': name, 'version': version,
        'purl': f'pkg:gem/{name}@{version}',
        'sbom_commit_sha': commit, 'observed_at': seen,
    }
    row.update(over)
    return artifact_row(**row)


def graph(
    repository_id: int,
    artifact_id: str,
    name: str,
    version: str,
    kind: str,
    commit: str,
    seen: datetime,
):
    """A dependency-graph row, stamped as `parse_dependency_graph` does.

    With the Syft scan's commit: that is today's key, and #22 changes
    it. Pinned here so that change starts from a tested baseline.
    """
    return artifact_row(
        repository_id=repository_id, artifact_id=artifact_id, name=name,
        version=version, type='gem', purl=f'pkg:gem/{name}',
        found_by='github-dependency-graph', licenses=[],
        relationship=DIRECT, source=DEPGRAPH, version_kind=kind,
        sbom_ref='v1' if commit else '', sbom_commit_sha=commit,
        observed_at=seen,
    )


#: `mastodon/mastodon` in January: mail 2.7.1, and left-pad, which the
#: September scan dropped.
JANUARY = [
    syft('mail', '2.7.1', OLD, JAN, relationship=DIRECT),
    syft(
        'left-pad', '1.3.0', OLD, JAN, type='npm',
        purl='pkg:npm/left-pad@1.3.0', found_by='javascript-lock-cataloger',
        licenses=['WTFPL'], relationship=TRANSITIVE,
    ),
    graph(1, 'SPDXRef-rails-a', 'rails', '~> 7.0', CONSTRAINT, OLD, JAN),
]

#: And in September, when the graph reported `rails` once per manifest.
SEPTEMBER = [
    syft('mail', '2.9.1', NEW, SEP, relationship=DIRECT),
    graph(1, 'SPDXRef-rails-b', 'rails', '~> 7.1', CONSTRAINT, NEW, SEP),
    graph(1, 'SPDXRef-rails-c', 'rails', '~> 7.1', CONSTRAINT, NEW, SEP),
]

#: `graph-only/app`: no download target, so no commit.
GRAPH_ONLY = [
    graph(2, 'SPDXRef-rack', 'rack', '', UNVERSIONED, '', SEP),
]


def seed_two_scans(ingest: IngestionRepository) -> None:
    """One repository scanned twice, and one with no commit at all.

    `mastodon/mastodon` was scanned in January at mail 2.7.1, with
    left-pad, and in September at mail 2.9.1 without it. Its January row
    in `repositories` is left unmerged, which is the window every
    `db index` opens until its OPTIMIZE: a reader without `FINAL` sees
    both rows, and both commits.

    GitHub's graph reported `rails` in both scans, and in September once
    per manifest: two rows, one fact. `graph-only/app` has no download
    target, so its commit is empty and so is its graph rows'. The data
    model allows that, and the scan-matching join has always counted
    such rows as current.
    """
    ingest.client.command('SYSTEM STOP MERGES repositories')
    ingest.insert_batch(
        REPOSITORIES.name,
        REPOSITORIES.rows([
            repo_row(
                id=1, owner='mastodon', repo='mastodon', stars=250,
                language='Ruby', sbom_commit_sha=OLD,
                sbom_commit_sha_short=OLD[:7],
            ),
        ]),
        REPOSITORIES.column_names,
    )
    ingest.insert_batch(
        REPOSITORIES.name,
        REPOSITORIES.rows([
            repo_row(
                id=1, owner='mastodon', repo='mastodon', stars=300,
                language='Ruby', sbom_commit_sha=NEW,
                sbom_commit_sha_short=NEW[:7],
            ),
            repo_row(
                id=2, owner='graph-only', repo='app', stars=10,
                language='Ruby', sbom_ref='', sbom_ref_type='',
                sbom_commit_sha='', sbom_commit_sha_short='',
                manifest_sources=[],
            ),
        ]),
        REPOSITORIES.column_names,
    )
    ingest.insert_batch(
        ARTIFACTS.name,
        ARTIFACTS.rows([*JANUARY, *SEPTEMBER, *GRAPH_ONLY]),
        ARTIFACTS.column_names,
    )
    ingest.reload_dictionaries()


@pytest.fixture
def two_scans(ingest: IngestionRepository, query: QueryRepository):
    seed_two_scans(ingest)
    return query


@pytest.fixture
def refreshed(ingest: IngestionRepository, two_scans: QueryRepository):
    ingest.refresh_rollups()
    return two_scans


def rows_of(
    query: QueryRepository,
    sql: str,
    **parameters: Any,
) -> list[tuple[Any, ...]]:
    result = query.client.query(sql, parameters=parameters)
    return sorted(tuple(row) for row in result.result_rows)


# --- the rollups ------------------------------------------------------------

#: The current-state facts, as every current-state reader should count
#: them: mail at 2.9.1, rails once at `~> 7.1`, rack from the repository
#: with no commit, and left-pad nowhere.
TOP = [
    (language, direct_only, rank, name)
    for language in ('', 'ruby')
    for direct_only in (0, 1)
    for rank, name in enumerate(('mail', 'rack', 'rails'), start=1)
]

#: Every current-state rollup, with what it must hold once the fixture is
#: refreshed.
CURRENT_STATE: dict[str, tuple[str, list[tuple[Any, ...]]]] = {
    'mv_package_language': (
        'SELECT name, language, repositories, direct_repositories, records, '
        'syft_records, depgraph_records FROM mv_package_language',
        [
            ('mail', 'ruby', 1, 1, 1, 1, 0),
            ('rack', 'ruby', 1, 1, 1, 0, 1),
            ('rails', 'ruby', 1, 1, 1, 0, 1),
        ],
    ),
    'mv_repository_deps': (
        'SELECT repository_id, packages, direct_packages, records '
        'FROM mv_repository_deps',
        [(1, 2, 2, 2), (2, 1, 1, 1)],
    ),
    'mv_licenses': (
        'SELECT license, repositories, packages FROM mv_licenses',
        [('', 2, 2), ('MIT', 1, 1)],
    ),
    'mv_language_totals': (
        'SELECT language, direct_records, transitive_records, '
        'unknown_records, syft_records, depgraph_records, records '
        'FROM mv_language_totals',
        [('ruby', 3, 0, 0, 1, 2, 3)],
    ),
    'mv_packages': (
        'SELECT name, repositories, direct_repositories FROM mv_packages',
        [('mail', 1, 1), ('rack', 1, 1), ('rails', 1, 1)],
    ),
    'mv_package_type': (
        'SELECT name, type, repositories, direct_repositories '
        'FROM mv_package_type',
        [('mail', 'gem', 1, 1), ('rack', 'gem', 1, 1), ('rails', 'gem', 1, 1)],
    ),
    'mv_package_version': (
        'SELECT name, version_kind, version, repositories '
        'FROM mv_package_version',
        [
            ('mail', 'resolved', '2.9.1', 1),
            ('rack', 'unversioned', '', 1),
            ('rails', 'constraint', '~> 7.1', 1),
        ],
    ),
    'mv_dependency_buckets': (
        'SELECT bucket, repositories FROM mv_dependency_buckets',
        [('1-9', 2)],
    ),
    'mv_version_kinds': (
        'SELECT version_kind, records FROM mv_version_kinds',
        [('constraint', 1), ('resolved', 1), ('unversioned', 1)],
    ),
    'mv_language_coverage': (
        'SELECT language, repositories, with_sbom FROM mv_language_coverage',
        [('ruby', 2, 2)],
    ),
    'mv_totals': (
        'SELECT repositories, dependencies, packages, classified '
        'FROM mv_totals',
        [(2, 3, 3, 3)],
    ),
    'mv_top_packages': (
        'SELECT language, direct_only, rank, name FROM mv_top_packages',
        TOP,
    ),
    'mv_edge_ambiguity': (
        'SELECT names, ambiguous_names, edges, ambiguous_edges, '
        'largest_repository FROM mv_edge_ambiguity',
        [(3, 0, 0, 0, 2)],
    ),
}

#: The rollups that are history by design, with what they must hold:
#: both months, and the package the new scan dropped.
HISTORY: dict[str, tuple[str, list[tuple[Any, ...]]]] = {
    'mv_package_month': (
        'SELECT name, source, month, repositories, direct_repositories '
        'FROM mv_package_month',
        [
            ('left-pad', 'syft', '2026-01', 1, 0),
            ('mail', 'syft', '2026-01', 1, 1),
            ('mail', 'syft', '2026-09', 1, 1),
            ('rack', 'github-depgraph', '2026-09', 1, 1),
            ('rails', 'github-depgraph', '2026-01', 1, 1),
            ('rails', 'github-depgraph', '2026-09', 1, 1),
        ],
    ),
}

#: Read `edges`, which records no scan, so neither of the above applies.
EDGES = ('mv_edges_forward',)


class TestTheRollups:

    def test_every_rollup_is_classified(self) -> None:
        """A rollup added later has to be put in one of the two kinds,
        and so pinned by this fixture, before this passes."""
        classified = set(CURRENT_STATE) | set(HISTORY) | set(EDGES)
        assert {name for name, _ in ROLLUPS} == classified

    def test_current_state_rollups_see_only_the_new_scan(self, refreshed):
        held = {
            name: rows_of(refreshed, sql)
            for name, (sql, _) in CURRENT_STATE.items()
        }
        assert held == {
            name: sorted(rows) for name, (_, rows) in CURRENT_STATE.items()
        }

    def test_the_adoption_series_keeps_both_scans(self, refreshed):
        """The one question the append-only table exists to answer.
        Built on the current scan, the series would erase itself."""
        for name, (sql, rows) in HISTORY.items():
            assert rows_of(refreshed, sql) == sorted(rows), name


# --- the dashboard's lookup -------------------------------------------------

QUERIES_TS = ROOT / 'web' / 'src' / 'clickhouse' / 'queries.ts'


def dashboard_predicates() -> list[str]:
    """The predicates every dependants query starts from, as sent.

    Parsed rather than restated, as `ecosystems_test.py` parses its
    mapping: there is no Node in this run, and a copy here would go on
    passing after the dashboard's own query changed.
    """
    source = QUERIES_TS.read_text(encoding='utf-8')
    body = source[source.index('function dependentFilters'):]
    body = body[body.index('const where = ['):]
    body = body[:body.index('];')]
    code = '\n'.join(line.split('//')[0] for line in body.splitlines())
    return [
        double or single
        for double, single in re.findall(r'"([^"]*)"|\'([^\']*)\'', code)
    ]


def dependants(query: QueryRepository, name: str) -> list[tuple[Any, ...]]:
    """(repository, version) pairs the dependants table would list."""
    where = ' AND '.join(dashboard_predicates())
    return rows_of(
        query,
        'SELECT DISTINCT a.repository_id, a.version '
        f'FROM artifacts AS a WHERE {where}',
        name=name,
    )


def dependant_count(query: QueryRepository, name: str) -> int:
    """The count printed above that table, filtered the same way."""
    where = ' AND '.join(dashboard_predicates())
    [(total,)] = rows_of(
        query,
        f'SELECT uniqExact(a.repository_id) FROM artifacts AS a WHERE {where}',
        name=name,
    )
    return int(total)


class TestTheDashboardLookup:

    def test_the_predicates_parse(self) -> None:
        """If this breaks, the tests below are vacuous rather than
        failing, so it is asserted on its own."""
        predicates = dashboard_predicates()
        assert 'a.name = {name:String}' in predicates
        assert "dictHas('dict_repositories', a.repository_id)" in predicates

    def test_the_dictionary_holds_each_repositorys_current_commit(
        self, two_scans,
    ):
        """Read through `FINAL`, so the unmerged January row does not
        win."""
        assert rows_of(
            two_scans,
            "SELECT dictGet('dict_repositories', 'sbom_commit_sha', "
            'toUInt64(1)), '
            "dictGet('dict_repositories', 'sbom_commit_sha', toUInt64(2))",
        ) == [(NEW, '')]

    def test_a_dependant_is_listed_at_its_current_version(self, two_scans):
        assert dependants(two_scans, 'mail') == [(1, '2.9.1')]
        assert dependants(two_scans, 'rails') == [(1, '~> 7.1')]

    def test_a_package_the_new_scan_dropped_has_no_dependants(
        self, two_scans,
    ):
        assert dependants(two_scans, 'left-pad') == []

    def test_a_repository_with_no_commit_is_current(self, two_scans):
        assert dependants(two_scans, 'rack') == [(2, '')]

    def test_the_count_agrees_with_the_cli(self, two_scans):
        for name in ('mail', 'rails', 'rack', 'left-pad'):
            assert dependant_count(two_scans, name) == (
                two_scans.get_dependent_count(name)
            ), name


# --- the CLI ----------------------------------------------------------------

class TestTheCli:

    def test_a_dependant_is_listed_at_its_current_version(self, two_scans):
        deps = two_scans.get_dependents('mail')
        assert [(d.full_name, d.version, d.stars) for d in deps] == [
            ('mastodon/mastodon', '2.9.1', 300),
        ]

    def test_a_package_the_new_scan_dropped_has_no_dependants(
        self, two_scans,
    ):
        assert two_scans.get_dependent_count('left-pad') == 0
        assert two_scans.search_library_candidates('left') == []

    def test_the_graph_is_read_at_the_current_scan(self, two_scans):
        versions = {d.version for d in two_scans.get_dependents('rails')}
        assert versions == {'~> 7.1'}

    def test_a_repository_with_no_commit_is_current(self, two_scans):
        deps = two_scans.get_dependents('rack')
        assert [d.full_name for d in deps] == ['graph-only/app']

    def test_the_ranking_counts_the_current_scan(self, two_scans):
        top = two_scans.get_top_packages(limit=10)
        assert [(p.name, p.repository_count) for p in top] == [
            ('mail', 1), ('rack', 1), ('rails', 1),
        ]

    def test_frameworks_are_read_from_the_current_scan(self, two_scans):
        """`github classify` takes the first version it is given for the
        framework it picks, so January's `mail 2.7.1` could be reported
        as the version in use."""
        found = two_scans.get_frameworks_for_repositories(
            [1, 2],
            {'mailer': ['mail'], 'js': ['left-pad'], 'web': ['rails']},
        )
        assert set(found) == {1}
        assert set(found[1]) == {('mailer', '2.9.1'), ('web', '~> 7.1')}

    def test_the_type_distribution_counts_the_current_scan(self, two_scans):
        assert list(two_scans.get_dependency_type_distribution()) == [
            ('gem', 2),
        ]


class TestWritingAScanAgain:
    """`db index` forgets a scan before it writes it again.

    That is `IngestionRepository.forget_scans`, keyed on the scan rather
    than the repository, and the views have to compose with it: the
    rewritten scan is current once, and the older scan is still there
    as history.
    """

    def test_the_rewritten_scan_is_current_once(self, ingest, two_scans):
        ingest.forget_scans([(1, NEW)])
        ingest.insert_batch(
            ARTIFACTS.name, ARTIFACTS.rows(SEPTEMBER), ARTIFACTS.column_names,
        )
        assert rows_of(
            two_scans,
            'SELECT name, version FROM current_artifacts '
            'WHERE repository_id = 1',
        ) == [('mail', '2.9.1'), ('rails', '~> 7.1'), ('rails', '~> 7.1')]
        assert rows_of(
            two_scans,
            'SELECT name, version FROM artifacts '
            'WHERE sbom_commit_sha = {commit:String}',
            commit=OLD,
        ) == [('left-pad', '1.3.0'), ('mail', '2.7.1'), ('rails', '~> 7.0')]


# --- the exports ------------------------------------------------------------

def parquet_rows(directory: Path, table: str) -> list[dict[str, Any]]:
    pq = pytest.importorskip('pyarrow.parquet')
    [path] = sorted(directory.glob(f'{table}-*.parquet'))
    return list(pq.read_table(path).to_pylist())


class TestTheExports:

    @pytest.fixture
    def exported(self, two_scans, tmp_path) -> Path:
        pytest.importorskip('pyarrow')
        from chatsbom.export.parquet import export_dataset
        export_dataset(two_scans, tmp_path)
        return tmp_path

    def test_the_artifacts_are_the_current_facts(self, exported):
        rows = parquet_rows(exported, 'artifacts')
        assert [
            (r['repository_id'], r['name'], r['version'], r['source'])
            for r in rows
        ] == [
            (1, 'mail', '2.9.1', 'syft'),
            (2, 'rack', '', DEPGRAPH),
            (1, 'rails', '~> 7.1', DEPGRAPH),
        ]

    def test_the_repositories_count_the_current_scan(self, exported):
        rows = {r['repo']: r for r in parquet_rows(exported, 'repositories')}
        assert rows['mastodon']['total_dependencies'] == 2
        assert rows['mastodon']['direct_dependencies'] == 2
        assert rows['mastodon']['sbom_commit_sha'] == NEW
        assert rows['app']['total_dependencies'] == 1

    def test_a_dropped_licence_is_gone(self, exported):
        licences = {r['license'] for r in parquet_rows(exported, 'licenses')}
        assert licences == {'', 'MIT'}

    def test_the_history_keeps_both_scans(self, exported):
        rows = parquet_rows(exported, 'history')
        assert sorted(
            (r['name'], r['month'], r['repository_count']) for r in rows
        ) == [
            ('left-pad', '2026-01', 1),
            ('mail', '2026-01', 1),
            ('mail', '2026-09', 1),
            ('rack', '2026-09', 1),
            ('rails', '2026-01', 1),
            ('rails', '2026-09', 1),
        ]

    def test_the_d1_database_agrees(self, two_scans, tmp_path):
        result = export_d1(
            two_scans, tmp_path / 'd1', depgraph_root=tmp_path / 'none',
        )
        connection = sqlite3.connect(tmp_path / 'applied.sqlite')
        for name in ('01-schema.sql', '02-data.sql', '03-aggregates.sql'):
            connection.executescript(
                (result.directory / name).read_text(encoding='utf-8'),
            )
        assert connection.execute(
            'SELECT repositories, dependencies, packages FROM agg_totals',
        ).fetchall() == [(2, 3, 3)]
        assert sorted(
            connection.execute(
                'SELECT name, month, source FROM history',
            ).fetchall(),
        ) == [
            ('left-pad', '2026-01', 'syft'),
            ('mail', '2026-01', 'syft'),
            ('mail', '2026-09', 'syft'),
            ('rack', '2026-09', DEPGRAPH),
            ('rails', '2026-01', DEPGRAPH),
            ('rails', '2026-09', DEPGRAPH),
        ]
        connection.close()

    def test_the_csv_export_counts_the_current_scan(self, two_scans):
        rows = [
            dict(zip(COLUMNS, row))
            for row in export_rows(two_scans, FrameworkIndex.build())
        ]
        assert [
            (
                r['repo'], r['commit_sha'], r['direct_dependencies'],
                r['total_dependencies'],
            )
            for r in rows
        ] == [('mastodon', NEW, 2, 2), ('app', '', 1, 1)]


# --- openapi candidates -----------------------------------------------------

def seed_flask_projects(ingest: IngestionRepository) -> None:
    """Four Flask-era projects, each scanned twice.

    `api/current` declares an OpenAPI package now; its January row in
    `repositories` is left unmerged, with fewer stars. `api/moved-on`
    declared one in January only, `api/dropped` has left Flask since,
    and `api/was-fastapi` used to carry the package that excludes a
    project from Flask's list.
    """
    ingest.client.command('SYSTEM STOP MERGES repositories')

    def project(repository_id: int, repo: str, commit: str, stars: int = 1):
        return repo_row(
            id=repository_id, owner='api', repo=repo, stars=stars,
            language='Python', sbom_commit_sha=commit,
            sbom_commit_sha_short=commit[:7],
        )

    def package(repository_id: int, name: str, version: str, commit: str):
        return artifact_row(
            repository_id=repository_id, artifact_id=f'{name}@{version}',
            name=name, version=version, type='python',
            sbom_commit_sha=commit,
        )

    ingest.insert_batch(
        REPOSITORIES.name,
        REPOSITORIES.rows([project(10, 'current', 'c' * 40, stars=5)]),
        REPOSITORIES.column_names,
    )
    ingest.insert_batch(
        REPOSITORIES.name,
        REPOSITORIES.rows([
            project(10, 'current', 'd' * 40, stars=50),
            project(11, 'moved-on', 'f' * 40),
            project(12, 'dropped', '1' * 40),
            project(13, 'was-fastapi', '3' * 40),
        ]),
        REPOSITORIES.column_names,
    )
    ingest.insert_batch(
        ARTIFACTS.name,
        ARTIFACTS.rows([
            package(10, 'flask', '2.3.0', 'c' * 40),
            package(10, 'flask', '3.0.0', 'd' * 40),
            package(10, 'flasgger', '0.9.7', 'd' * 40),
            package(11, 'flask', '2.3.0', 'e' * 40),
            package(11, 'flasgger', '0.9.5', 'e' * 40),
            package(11, 'flask', '3.0.0', 'f' * 40),
            package(12, 'flask', '2.3.0', '0' * 40),
            package(12, 'apispec', '6.3.0', '0' * 40),
            package(12, 'django', '5.0.0', '1' * 40),
            package(13, 'fastapi', '0.100.0', '2' * 40),
            package(13, 'flask', '3.0.0', '3' * 40),
            package(13, 'flask-restx', '1.3.0', '3' * 40),
        ]),
        ARTIFACTS.column_names,
    )


class TestOpenApiCandidates:

    def test_candidates_are_judged_on_the_current_scan(self, ingest, query):
        seed_flask_projects(ingest)
        result = OpenApiService().find_candidates(query.client)
        flask = sorted(
            (c.repo, c.stars, c.framework_version, c.matched_dependencies)
            for c in result.candidates if c.framework == 'flask'
        )
        assert flask == [
            ('current', 50, '3.0.0', 'flasgger'),
            ('was-fastapi', 1, '3.0.0', 'flask-restx'),
        ]
        stats = next(s for s in result.stats if s.framework == 'flask')
        assert (stats.total_projects, stats.matched_projects) == (3, 2)


# --- the direct/transitive verdict ------------------------------------------

CONTENT_ROOT = 'data/06-github-content'
GEMSPEC = """
Gem::Specification.new do |s|
  s.add_dependency 'mini_mime'
end
"""


def land(
    ingest: IngestionRepository,
    kind: str,
    path: str,
    body: str,
    fetched_at: datetime,
) -> None:
    """One document into `raw_documents`, as `db raw` writes it."""
    ingest.client.insert(
        'raw_documents',
        [[
            kind, 4321, path,
            hashlib.sha256(body.encode('utf-8')).hexdigest(),
            fetched_at, body,
        ]],
        column_names=[
            'kind', 'repository_id', 'path', 'sha256', 'fetched_at', 'body',
        ],
    )


class TestTheVerdictReadsTheScansOwnManifests:
    """`RawManifests` returned every manifest a repository ever landed.

    The declared set behind the direct/transitive verdict was then a
    union across commits, so a package only an old commit declared made
    the current scan's copy `direct`.
    """

    def test_a_package_only_an_old_commit_declared_is_not_direct(
        self, ingest,
    ):
        commit_dir = f'{CONTENT_ROOT}/ruby/mikel/mail/v2.9.1/{NEW}'
        record = {
            'id': 4321, 'owner': 'mikel', 'name': 'mail',
            'language': 'Ruby', 'default_branch': 'master',
            'download_target': {
                'ref': 'v2.9.1', 'ref_type': 'release',
                'commit_sha': NEW, 'commit_sha_short': NEW[:7],
            },
            'local_content_path': commit_dir,
        }
        land(ingest, 'repo', 'data/07-sbom/ruby.jsonl', json.dumps(record), SEP)
        land(
            ingest, 'syft', f'data/07-sbom/ruby/mikel/mail/v2.9.1/{NEW}/sbom.json',
            json.dumps({
                'artifacts': [
                    {'name': 'mail', 'version': '2.9.1', 'type': 'gem'},
                    {'name': 'mini_mime', 'version': '1.1.5', 'type': 'gem'},
                ],
            }),
            SEP,
        )
        # January's commit declared mini_mime in its gemspec. The Gemfile
        # did not change, so September landed an identical copy.
        january = f'{CONTENT_ROOT}/ruby/mikel/mail/v2.7.1/{OLD}'
        land(ingest, 'content', f'{january}/Gemfile', "gem 'mail'\n", JAN)
        land(ingest, 'content', f'{january}/mail.gemspec', GEMSPEC, JAN)
        land(ingest, 'content', f'{commit_dir}/Gemfile', "gem 'mail'\n", SEP)

        written = FakeIngestionRepository()
        DbService().ingest_from_list(
            RawRecords(ingest.client), written, 'ruby',
            documents=RawDocuments(ingest.client),
            manifests=RawManifests(ingest.client, CONTENT_ROOT),
        )

        verdicts = {
            row['name']: row['relationship']
            for row in written.rows_for('artifacts')
        }
        assert verdicts == {'mail': DIRECT, 'mini_mime': TRANSITIVE}
        [repository] = written.rows_for('repositories')
        assert repository['manifest_sources'] == ['Gemfile']


# --- scripts/verify_rollups.py ----------------------------------------------

def verify_rollups() -> ModuleType:
    """The operator script, loaded as a module: it is not a package."""
    path = ROOT / 'scripts' / 'verify_rollups.py'
    spec = importlib.util.spec_from_file_location('verify_rollups', path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


#: The deduplicated facts, spelled inline over the whole table, as the
#: rollups spelled them before `facts` existed.
WHOLE_TABLE_FACTS = (
    '(SELECT DISTINCT repository_id, name, version, type, found_by, '
    'relationship, source, version_kind FROM artifacts)'
)


def whole_table(ddl: str) -> str:
    """A rollup as it was before the views: reading every observation."""
    ddl = re.sub(r'\bFROM facts\b', f'FROM {WHOLE_TABLE_FACTS}', ddl)
    return re.sub(r'\bFROM current_artifacts\b', 'FROM artifacts', ddl)


class TestVerifyRollups:
    """The script restated each rollup's SQL, so it shared its bugs.

    Every rollup read the whole table and so did every check: "14 of 14
    agree" on a database where the CLI and the dashboard disagreed.
    """

    @staticmethod
    def run(
        query: QueryRepository,
        monkeypatch: pytest.MonkeyPatch,
        capsys: pytest.CaptureFixture[str],
    ) -> tuple[int, set[str]]:
        script = verify_rollups()
        monkeypatch.setattr(
            script, 'get_container',
            lambda: SimpleNamespace(get_export_repository=lambda: query),
        )
        monkeypatch.setattr(sys, 'argv', ['verify_rollups.py'])
        failures = script.main()
        flagged = {
            line.split()[0]
            for line in capsys.readouterr().out.splitlines()
            if 'DISAGREES' in line
        }
        return failures, flagged

    def test_it_catches_rollups_that_count_every_observation(
        self, ingest, two_scans, monkeypatch, capsys,
    ):
        monkeypatch.setattr(
            'chatsbom.core.repository.ROLLUPS',
            tuple((name, whole_table(ddl)) for name, ddl in ROLLUPS),
        )
        ingest.refresh_rollups(recreate=True)

        failures, flagged = self.run(two_scans, monkeypatch, capsys)
        assert failures > 0
        # The three the issue names, and the history rollup, which reads
        # every observation on purpose and must not be flagged for it.
        assert {
            'mv_package_version', 'mv_repository_deps', 'mv_packages',
        } <= flagged
        assert 'mv_package_month' not in flagged

    def test_the_rollups_as_defined_agree(
        self, refreshed, monkeypatch, capsys,
    ):
        assert self.run(refreshed, monkeypatch, capsys) == (0, set())
