"""The corpus, and rollups keyed by ecosystem (#55 §4.12, PR E).

Two owner decisions on #55 shape what is counted:

- **D2.** Rollups, the dashboard and the exports count only the current
  search snapshot. A repository no longer listed keeps its rows.
- **D7.** GitHub's language is shown as the top twelve and `other`.

And one property of the data replaced the assumption the rollups were
built on: a repository has as many ecosystems as it has manifests for,
so a whole-corpus distinct count is never a sum of per-ecosystem ones.
"""
from __future__ import annotations

import importlib.util
import sys
from datetime import datetime
from pathlib import Path
from types import ModuleType
from types import SimpleNamespace
from typing import Any

import pytest

from chatsbom.core.repository import IngestionRepository
from chatsbom.core.repository import QueryRepository
from chatsbom.core.rollups import OBSOLETE_ROLLUPS
from chatsbom.core.schema import ARTIFACTS
from chatsbom.core.schema import LANGUAGE_BUCKETS
from chatsbom.core.schema import REPOSITORIES
from chatsbom.models.provenance import DEPGRAPH
from chatsbom.models.provenance import MANIFEST
from chatsbom.models.relationship import DIRECT
from chatsbom.models.relationship import TRANSITIVE
from tests.conftest import requires_clickhouse
from tests.repository_query_test import artifact_row
from tests.repository_query_test import CURRENT_SHA
from tests.repository_query_test import repo_row

ROOT = Path(__file__).resolve().parents[1]
SEEN = datetime(2026, 9, 1)
SNAPSHOT = 'all-2026-03-09'


def rows_of(query: QueryRepository, sql: str) -> list[tuple[Any, ...]]:
    return sorted(tuple(r) for r in query.client.query(sql).result_rows)


def insert(
    ingest: IngestionRepository,
    repositories: list[dict[str, Any]],
    artifacts: list[dict[str, Any]],
) -> None:
    ingest.insert_batch(
        REPOSITORIES.name, REPOSITORIES.rows(repositories),
        REPOSITORIES.column_names,
    )
    ingest.insert_batch(
        ARTIFACTS.name, ARTIFACTS.rows(artifacts), ARTIFACTS.column_names,
    )
    ingest.reload_dictionaries()
    ingest.refresh_rollups()


def npm(repository_id: int, name: str, **over: Any) -> dict[str, Any]:
    row = {
        'repository_id': repository_id, 'artifact_id': f'{name}-npm',
        'name': name, 'version': '1.0.0', 'type': 'npm',
        'purl': f'pkg:npm/{name}@1.0.0', 'found_by': 'javascript-lock',
        'relationship': DIRECT, 'licenses': ['MIT'], 'observed_at': SEEN,
    }
    row.update(over)
    return artifact_row(**row)


def maven(repository_id: int, name: str, **over: Any) -> dict[str, Any]:
    row = {
        'repository_id': repository_id, 'artifact_id': f'{name}-maven',
        'name': name, 'version': '3.5.9', 'type': 'java-archive',
        'purl': f'pkg:maven/org.example/{name}@3.5.9',
        'found_by': 'java-pom-cataloger', 'relationship': DIRECT,
        'licenses': ['Apache-2.0'], 'observed_at': SEEN,
    }
    row.update(over)
    return artifact_row(**row)


@pytest.fixture
def polyglot(ingest: IngestionRepository, query: QueryRepository):
    """Four repositories of the snapshot and one it no longer lists.

    1. `stirling/pdf`: GitHub says TypeScript. An npm front end and a
       Maven back end, the Maven side declared only in Gradle (the
       `manifest` source) and in its graph.
    2. `halo/halo`: Java. Maven from its graph.
    3. `left/pad`: JavaScript. npm only; `utils` is a name it shares
       with the Maven artifact below — two packages, one name.
    4. `never/scanned`: in the snapshot, nothing collected.
    5. `gone/away`: below the star cut now, not in the snapshot. It has
       a scan, and must count nowhere.
    """
    repositories = [
        repo_row(
            id=1, owner='stirling', repo='pdf', stars=900,
            language='TypeScript', snapshot=SNAPSHOT,
            ecosystems=['maven', 'npm'], depgraph_observed_at=SEEN,
        ),
        repo_row(
            id=2, owner='halo', repo='halo', stars=800, language='Java',
            snapshot=SNAPSHOT, ecosystems=['maven'],
            depgraph_observed_at=SEEN,
        ),
        repo_row(
            id=3, owner='left', repo='pad', stars=700,
            language='JavaScript', snapshot=SNAPSHOT, ecosystems=['npm'],
            depgraph_observed_at=SEEN,
        ),
        repo_row(
            id=4, owner='never', repo='scanned', stars=600, language='',
            snapshot=SNAPSHOT, sbom_commit_sha='', ecosystems=[],
        ),
        repo_row(
            id=5, owner='gone', repo='away', stars=10, language='Go',
            snapshot='all-2025-01-01', ecosystems=['npm'],
        ),
    ]
    artifacts = [
        npm(1, 'react'),
        npm(1, 'utils', relationship=TRANSITIVE),
        maven(
            1, 'spring-boot-starter-web', source=MANIFEST,
            version_kind='constraint',
        ),
        maven(
            1, 'spring-boot-starter-web', artifact_id='g1',
            source=DEPGRAPH, type='maven',
        ),
        maven(2, 'spring-boot-starter-web', source=DEPGRAPH, type='maven'),
        maven(2, 'utils', source=DEPGRAPH, type='maven'),
        npm(3, 'react'),
        npm(3, 'utils'),
        npm(5, 'react'),
    ]
    insert(ingest, repositories, artifacts)
    return query


@requires_clickhouse
class TestTheCorpusIsTheCurrentSnapshot:

    def test_the_view_keeps_the_newest_all_snapshot(self, polyglot):
        assert rows_of(polyglot, 'SELECT id FROM corpus') == [
            (1,), (2,), (3,), (4,),
        ]

    def test_a_pilot_list_does_not_shrink_it(self, ingest, polyglot):
        """`queue track --snapshot pilot.jsonl` names a newer, smaller
        snapshot. An `all-*` one is the corpus whatever the dates."""
        insert(
            ingest, [
                repo_row(id=9, repo='pilot', snapshot='pilot-2026-10-05'),
            ], [],
        )
        assert (9,) not in rows_of(polyglot, 'SELECT id FROM corpus')
        assert len(rows_of(polyglot, 'SELECT id FROM corpus')) == 4

    def test_with_no_snapshot_recorded_every_repository_is_in_it(
        self, ingest, query,
    ):
        """A database indexed before the column existed reads as before."""
        insert(ingest, [repo_row(id=1), repo_row(id=2)], [])
        assert rows_of(query, 'SELECT id FROM corpus') == [(1,), (2,)]

    def test_a_repository_outside_it_counts_nowhere(self, polyglot):
        """Its rows stay in `artifacts`; nothing current reads them."""
        assert rows_of(
            polyglot, 'SELECT count() FROM artifacts WHERE repository_id = 5',
        ) == [(1,)]
        for sql in (
            'SELECT DISTINCT repository_id FROM current_artifacts',
            'SELECT repository_id FROM mv_repository_deps',
            'SELECT DISTINCT repository_id FROM facts',
        ):
            assert (5,) not in rows_of(polyglot, sql), sql
        # `react` has three dependants with a scan, one of them gone.
        assert rows_of(
            polyglot, "SELECT repositories FROM mv_packages WHERE name = 'react'",
        ) == [(2,)]
        assert polyglot.get_dependent_count('react') == 2

    def test_the_dashboard_cannot_see_it(self, polyglot):
        assert rows_of(
            polyglot,
            "SELECT dictHas('dict_repositories', toUInt64(5)), "
            "dictHas('dict_repositories', toUInt64(4))",
        ) == [(False, True)]

    def test_the_totals_state_their_denominator(self, polyglot):
        """`tracked` is the snapshot, collected or not; `repositories`
        the part of it with dependency data."""
        assert rows_of(
            polyglot, 'SELECT repositories, tracked FROM mv_totals',
        ) == [(3, 4)]


@requires_clickhouse
class TestEcosystemsAreNeverSummed:

    def test_a_package_is_counted_once_per_ecosystem(self, polyglot):
        assert rows_of(
            polyglot,
            'SELECT ecosystem, name, repositories FROM mv_package_ecosystem '
            "WHERE name = 'utils'",
        ) == [('maven', 'utils', 1), ('npm', 'utils', 2)]

    def test_and_once_across_them(self, polyglot):
        """`utils` is in three repositories. Summed over ecosystems it
        would be three here too — but `spring-boot-starter-web` in
        repository 1 comes from two sources under one ecosystem, and a
        repository with npm `utils` and Maven `utils` would be two."""
        assert rows_of(
            polyglot,
            "SELECT repositories FROM mv_packages WHERE name = 'utils'",
        ) == [(3,)]
        assert rows_of(
            polyglot,
            'SELECT repositories FROM mv_packages '
            "WHERE name = 'spring-boot-starter-web'",
        ) == [(2,)]

    def test_a_repository_with_two_ecosystems_is_one_repository(
        self, ingest, polyglot,
    ):
        insert(ingest, [], [maven(3, 'utils', type='maven', source=DEPGRAPH)])
        ecosystems = rows_of(
            polyglot,
            'SELECT ecosystem, repositories FROM mv_package_ecosystem '
            "WHERE name = 'utils'",
        )
        assert ecosystems == [('maven', 2), ('npm', 2)]
        assert sum(n for _, n in ecosystems) == 4
        assert rows_of(
            polyglot,
            "SELECT repositories FROM mv_packages WHERE name = 'utils'",
        ) == [(3,)]
        assert rows_of(
            polyglot,
            "SELECT repositories FROM mv_top_packages WHERE ecosystem = '' "
            "AND direct_only = 0 AND name = 'utils'",
        ) == [(3,)]

    def test_records_partition_by_ecosystem(self, polyglot):
        by_ecosystem = rows_of(
            polyglot,
            'SELECT ecosystem, records, syft_records, depgraph_records, '
            'manifest_records FROM mv_ecosystem_totals',
        )
        assert by_ecosystem == [('maven', 4, 0, 3, 1), ('npm', 4, 4, 0, 0)]
        assert rows_of(polyglot, 'SELECT dependencies FROM mv_totals') == [
            (sum(r[1] for r in by_ecosystem),),
        ]

    def test_the_ranking_is_per_ecosystem(self, polyglot):
        assert rows_of(
            polyglot,
            'SELECT name FROM mv_top_packages '
            "WHERE ecosystem = 'maven' AND direct_only = 1 AND rank = 1",
        ) == [('spring-boot-starter-web',)]


@requires_clickhouse
class TestCoverage:

    def test_by_ecosystem_counts_every_ecosystem_a_repository_has(
        self, polyglot,
    ):
        assert rows_of(
            polyglot,
            'SELECT ecosystem, repositories, with_any, with_syft, '
            'with_depgraph, with_manifest FROM mv_ecosystem_coverage',
        ) == [('maven', 2, 2, 0, 2, 1), ('npm', 2, 2, 2, 0, 0)]

    def test_by_language_is_out_of_the_whole_snapshot(self, polyglot):
        """`never/scanned` has no language and nothing collected, and is
        in the denominator all the same."""
        assert rows_of(
            polyglot,
            'SELECT language, repositories, with_sbom, with_syft, '
            'with_depgraph, with_manifest FROM mv_language_coverage',
        ) == [
            ('java', 1, 1, 0, 1, 0),
            ('javascript', 1, 1, 1, 0, 0),
            ('none', 1, 0, 0, 0, 0),
            ('typescript', 1, 1, 1, 1, 1),
        ]

    def test_the_cli_agrees(self, polyglot):
        coverage = polyglot.get_corpus_coverage()
        assert (
            coverage.snapshot, coverage.tracked, coverage.with_dependencies,
            coverage.with_syft, coverage.with_depgraph, coverage.with_manifest,
        ) == (SNAPSHOT, 4, 3, 2, 2, 1)
        assert sorted(
            (
                e.ecosystem, e.repository_count, e.syft_count,
                e.depgraph_count, e.manifest_count,
            )
            for e in polyglot.get_ecosystem_stats()
        ) == [('maven', 2, 0, 2, 1), ('npm', 2, 2, 0, 0)]


@requires_clickhouse
class TestTheLanguageFold:

    def test_the_top_twelve_and_the_rest(self, ingest, query):
        """Fourteen languages: the twelve largest by name, the other two
        as `other`, and a repository GitHub names none as `none`."""
        languages = [f'Lang{n:02}' for n in range(14)]
        repositories = []
        next_id = 1
        for rank, language in enumerate(languages):
            for _ in range(20 - rank):
                repositories.append(
                    repo_row(
                        id=next_id, language=language, snapshot=SNAPSHOT,
                    ),
                )
                next_id += 1
        repositories.append(
            repo_row(id=next_id, language='', snapshot=SNAPSHOT),
        )
        insert(ingest, repositories, [])

        top = rows_of(query, 'SELECT language FROM language_buckets')
        assert len(top) == LANGUAGE_BUCKETS
        assert ('lang00',) in top and ('lang12',) not in top
        buckets = dict(
            rows_of(
                query, 'SELECT language, repositories FROM mv_language_coverage',
            ),
        )
        assert buckets['other'] == (20 - 12) + (20 - 13)
        assert buckets['none'] == 1
        assert len(buckets) == LANGUAGE_BUCKETS + 2
        # The dashboard's filter matches the same fold.
        assert rows_of(
            query,
            "SELECT dictGet('dict_repositories', 'language_bucket', "
            f'toUInt64({next_id - 1})), '
            "dictGet('dict_repositories', 'language_bucket', toUInt64(1))",
        ) == [('other', 'lang00')]


@requires_clickhouse
class TestFrameworksByEcosystem:

    def test_a_backend_under_another_language_counts(self, polyglot):
        """Stirling-PDF is TypeScript to GitHub and Spring Boot to its
        Gradle build (#51). Counted by ecosystem, it is a Spring Boot
        project; by language it was not."""
        assert polyglot.get_framework_usage(
            'maven', ['spring-boot-starter-web'],
        ) == 2
        assert polyglot.get_framework_usage(
            'npm', ['spring-boot-starter-web'],
        ) == 0
        samples = polyglot.get_top_projects_by_framework(
            'maven', ['spring-boot-starter-web'],
        )
        assert [d.full_name for d in samples] == ['stirling/pdf', 'halo/halo']

    def test_the_cli_filters_by_ecosystem(self, polyglot):
        assert polyglot.get_dependent_count('utils', ecosystem='npm') == 2
        assert polyglot.get_dependent_count('utils', ecosystem='maven') == 1
        assert polyglot.get_dependent_count('utils') == 3
        top = polyglot.get_top_packages(limit=5, ecosystem='maven')
        assert [p.name for p in top][:1] == ['spring-boot-starter-web']


@requires_clickhouse
class TestTheLanguageRollupsAreDropped:

    def test_ensure_schema_drops_them(self, ingest, query):
        for name in OBSOLETE_ROLLUPS:
            ingest.client.command(
                f'CREATE MATERIALIZED VIEW {name} REFRESH EVERY 1 DAY '
                'ENGINE = MergeTree ORDER BY tuple() AS SELECT 1 AS x',
            )
        ingest.ensure_schema()
        assert rows_of(
            query,
            'SELECT name FROM system.tables WHERE database = currentDatabase() '
            f'AND name IN {tuple(OBSOLETE_ROLLUPS)}',
        ) == []


def verify_rollups() -> ModuleType:
    path = ROOT / 'scripts' / 'verify_rollups.py'
    spec = importlib.util.spec_from_file_location('verify_rollups', path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@requires_clickhouse
class TestVerifyRollups:

    def run(self, query, monkeypatch, capsys) -> tuple[int, str]:
        script = verify_rollups()
        monkeypatch.setattr(
            script, 'get_container',
            lambda: SimpleNamespace(get_export_repository=lambda: query),
        )
        monkeypatch.setattr(sys, 'argv', ['verify_rollups.py'])
        return script.main(), capsys.readouterr().out

    def test_every_check_agrees_on_a_polyglot_corpus(
        self, polyglot, monkeypatch, capsys,
    ):
        failures, out = self.run(polyglot, monkeypatch, capsys)
        assert failures == 0, out
        # What the sum would have claimed is reported beside it.
        assert 'summed across ecosystems' in out

    def test_it_catches_a_whole_corpus_count_summed_over_ecosystems(
        self, ingest, polyglot, monkeypatch, capsys,
    ):
        """The rollup as it was keyed by language: exact while every
        repository had one, wrong once one has two ecosystems."""
        insert(ingest, [], [maven(3, 'utils', type='maven', source=DEPGRAPH)])
        ingest.client.command(
            'CREATE OR REPLACE TABLE mv_packages_summed ENGINE = Memory AS '
            'SELECT name, sum(repositories) AS repositories, '
            'sum(direct_repositories) AS direct_repositories '
            'FROM mv_package_ecosystem GROUP BY name',
        )
        ingest.client.command(
            'EXCHANGE TABLES mv_packages_summed AND mv_packages',
        )
        failures, out = self.run(polyglot, monkeypatch, capsys)
        assert failures > 0
        assert any(
            line.startswith('mv_packages') and 'DISAGREES' in line
            for line in out.splitlines()
        ), out

    def test_it_catches_a_corpus_that_is_not_the_snapshot(
        self, ingest, polyglot, monkeypatch, capsys,
    ):
        ingest.client.command(
            'CREATE OR REPLACE VIEW corpus AS '
            'SELECT * FROM repositories FINAL',
        )
        failures, out = self.run(polyglot, monkeypatch, capsys)
        assert failures > 0
        assert any(
            line.startswith('corpus (view)') and 'DISAGREES' in line
            for line in out.splitlines()
        ), out


def test_the_current_sha_is_the_fixtures():
    """The helpers stamp artifacts with the commit `repo_row` records."""
    assert artifact_row()['sbom_commit_sha'] == CURRENT_SHA


@requires_clickhouse
class TestDbStatus:

    def test_it_reports_the_corpus_and_its_ecosystems(
        self, polyglot, monkeypatch,
    ):
        """The denominator first, then ecosystems, then the folded
        languages; frameworks by their ecosystem, not a language."""
        from typer.testing import CliRunner

        from chatsbom.__main__ import app
        container = SimpleNamespace(
            config=SimpleNamespace(get_db_config=lambda _: polyglot.config),
            get_query_repository=lambda: polyglot,
        )
        monkeypatch.setattr(
            'chatsbom.commands.db.status.get_container', lambda: container,
        )
        monkeypatch.setattr(
            'chatsbom.commands.db.status.check_clickhouse_connection',
            lambda **_: None,
        )
        result = CliRunner().invoke(app, ['db', 'status'])
        assert result.exit_code == 0, result.output
        out = result.output
        assert f'Corpus: snapshot {SNAPSHOT}' in out
        assert 'Error' not in out
        corpus = out[
            out.index('Corpus'):out.index(
                'Repositories by Ecosystem',
            )
        ]
        assert 'In the snapshot' in corpus and '4' in corpus
        ecosystems = out[
            out.index('Repositories by Ecosystem'):
            out.index('GitHub Languages')
        ]
        assert 'maven' in ecosystems and 'npm' in ecosystems
        languages = out[out.index('GitHub Languages'):]
        assert 'typescript' in languages and 'none' in languages
        assert 'Framework Usage — maven' in out
        spring = out[out.index('Framework Usage — maven'):]
        assert 'stirling/pdf' in spring
