"""`openapi candidates`, against a real ClickHouse (#47).

It lowercased every owner, repository, branch and tag it found, so a
release URL built from Netflix/zuul's `V3.0.0` named a tag that does not
exist, and every command after it (`clone`, `drift`, `list-paths`,
`stats`) was handed the wrong name. Its query took the framework's
version from whichever of the framework's packages came first, not from
the framework; and it counted `pydantic`, which FastAPI itself depends
on, as a sign that a FastAPI project documents its API, so every FastAPI
project was a candidate.

Run as the command, so the CSV it writes is what is checked.
"""
import csv
from pathlib import Path
from types import SimpleNamespace

import pytest
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.core.config import PathConfig
from chatsbom.core.repository import IngestionRepository
from chatsbom.core.repository import QueryRepository
from chatsbom.core.schema import ARTIFACTS
from chatsbom.core.schema import REPOSITORIES
from chatsbom.models.framework import FastAPI
from chatsbom.models.relationship import DIRECT
from chatsbom.models.relationship import TRANSITIVE
from tests.conftest import requires_clickhouse
from tests.repository_query_test import artifact_row
from tests.repository_query_test import repo_row

pytestmark = requires_clickhouse

runner = CliRunner()

ZUUL = '1' * 40
OLD = '2' * 40
NEW = '3' * 40


def repository(repository_id: int, owner: str, repo: str, **over):
    over.setdefault('sbom_commit_sha', NEW)
    over.setdefault('sbom_commit_sha_short', over['sbom_commit_sha'][:7])
    return repo_row(id=repository_id, owner=owner, repo=repo, **over)


def package(repository_id: int, name: str, version: str, **over):
    over.setdefault('sbom_commit_sha', NEW)
    over.setdefault('artifact_id', f'{name}@{version}')
    return artifact_row(
        repository_id=repository_id, name=name, version=version, **over,
    )


def seed(
    ingest: IngestionRepository,
    repositories: list[dict],
    artifacts: list[dict],
) -> None:
    ingest.insert_batch(
        REPOSITORIES.name, REPOSITORIES.rows(repositories),
        REPOSITORIES.column_names,
    )
    ingest.insert_batch(
        ARTIFACTS.name, ARTIFACTS.rows(artifacts), ARTIFACTS.column_names,
    )


def tree(root: Path, repository_id: int, sha: str, *files: str) -> None:
    """A tree where the tree stage writes it."""
    paths = PathConfig(base_data_dir=root / 'data')
    path = paths.tree_file(repository_id, sha)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(''.join(f'{f}\n' for f in files), encoding='utf-8')


@pytest.fixture
def candidates(query: QueryRepository, tmp_path: Path, monkeypatch):
    """Runs `openapi candidates` in `tmp_path` against the test database,
    and returns the rows of the CSV it wrote, by framework."""
    monkeypatch.chdir(tmp_path)
    container = SimpleNamespace(
        get_query_repository=lambda: QueryRepository(query.config),
    )
    monkeypatch.setattr(
        'chatsbom.commands.openapi.candidates.get_container',
        lambda: container,
    )

    def run() -> dict[str, list[dict[str, str]]]:
        result = runner.invoke(
            app, ['openapi', 'candidates', '--output', 'candidates.csv'],
        )
        assert result.exit_code == 0, result.output
        assert 'Error' not in result.output, result.output
        found: dict[str, list[dict[str, str]]] = {}
        output = tmp_path / 'candidates.csv'
        if output.exists():
            with open(output, encoding='utf-8', newline='') as f:
                for row in csv.DictReader(f):
                    found.setdefault(row['framework'], []).append(row)
        return found

    return run


def test_a_mixed_case_repository_keeps_its_case(ingest, candidates, tmp_path):
    seed(
        ingest,
        [
            repository(
                1, 'Netflix', 'zuul', language='java', stars=13000,
                default_branch='Master', latest_release_tag='V3.0.0',
                sbom_commit_sha=ZUUL,
            ),
        ],
        [
            package(
                1, 'org.springframework.boot:spring-boot-starter-web',
                '3.2.0', type='java-archive', sbom_commit_sha=ZUUL,
            ),
        ],
    )
    tree(tmp_path, 1, ZUUL, 'README.md', 'docs/OpenAPI.yaml')

    [zuul] = candidates()['springboot']

    assert (zuul['owner'], zuul['repo']) == ('Netflix', 'zuul')
    assert (zuul['default_branch'], zuul['latest_release']) == (
        'Master', 'V3.0.0',
    )
    assert zuul['commit_sha'] == ZUUL
    assert zuul['url'] == 'https://github.com/Netflix/zuul/releases/tag/V3.0.0'
    assert zuul['openapi_file'] == 'docs/OpenAPI.yaml'
    assert zuul['openapi_url'] == (
        f'https://github.com/Netflix/zuul/blob/{ZUUL}/docs/OpenAPI.yaml'
    )
    assert zuul['framework_version'] == '3.2.0'


def test_only_the_current_scan_counts(ingest, candidates):
    """`moved-on` declared an OpenAPI package in January only; `current`
    has an unmerged older row in `repositories`, with fewer stars."""
    ingest.client.command('SYSTEM STOP MERGES repositories')
    seed(
        ingest,
        [repository(10, 'api', 'current', stars=5, sbom_commit_sha=OLD)],
        [],
    )
    seed(
        ingest,
        [
            repository(10, 'api', 'current', stars=50),
            repository(11, 'api', 'moved-on'),
        ],
        [
            package(10, 'flask', '2.3.0', sbom_commit_sha=OLD),
            package(10, 'flask', '3.0.0'),
            package(10, 'flasgger', '0.9.7'),
            package(11, 'flask', '2.3.0', sbom_commit_sha=OLD),
            package(11, 'flasgger', '0.9.5', sbom_commit_sha=OLD),
            package(11, 'flask', '3.0.0'),
        ],
    )

    [current] = candidates()['flask']

    assert (current['repo'], current['stars']) == ('current', '50')
    assert current['framework_version'] == '3.0.0'
    assert current['matched_dependencies'] == 'flasgger'


def test_the_version_is_the_frameworks_own(ingest, candidates):
    """Not a sibling's: FastAPI's, not Starlette's, which it is built
    on; and chi v5's, which the project declares, not the chi v1 one of
    its dependencies still pulls in."""
    seed(
        ingest,
        [
            repository(20, 'py', 'service', language='python'),
            repository(21, 'go', 'router', language='go'),
        ],
        [
            package(20, 'starlette', '0.37.2', relationship=TRANSITIVE),
            package(20, 'fastapi', '0.110.0', relationship=DIRECT),
            package(20, 'openapi-spec-validator', '0.7.1'),
            package(
                21, 'github.com/go-chi/chi', 'v1.5.4',
                relationship=TRANSITIVE, type='go-module',
            ),
            package(
                21, 'github.com/go-chi/chi/v5', 'v5.0.12',
                relationship=DIRECT, type='go-module',
            ),
            package(
                21, 'github.com/swaggo/swag', 'v1.16.3', type='go-module',
            ),
        ],
    )

    found = candidates()

    assert [r['framework_version'] for r in found['fastapi']] == ['0.110.0']
    assert [r['framework_version'] for r in found['chi']] == ['v5.0.12']
    assert [r['matched_dependencies'] for r in found['chi']] == [
        'github.com/swaggo/swag',
    ]


def test_pydantic_alone_does_not_make_a_fastapi_candidate(ingest, candidates):
    """FastAPI depends on pydantic, so every FastAPI project has it."""
    seed(
        ingest,
        [repository(30, 'py', 'plain', language='python')],
        [
            package(30, 'fastapi', '0.110.0', relationship=DIRECT),
            package(30, 'pydantic', '2.6.4'),
        ],
    )

    assert candidates().get('fastapi', []) == []
    assert 'pydantic' not in FastAPI().get_openapi_packages()


def test_a_project_with_an_excluded_package_is_left_out(ingest, candidates):
    """Flask's list leaves out projects on FastAPI, in the same scan."""
    seed(
        ingest,
        [
            repository(40, 'py', 'both', language='python'),
            repository(41, 'py', 'was-fastapi', language='python'),
        ],
        [
            package(40, 'flask', '3.0.0'),
            package(40, 'flask-restx', '1.3.0'),
            package(40, 'fastapi', '0.110.0'),
            package(41, 'fastapi', '0.100.0', sbom_commit_sha=OLD),
            package(41, 'flask', '3.0.0'),
            package(41, 'flask-restx', '1.3.0'),
        ],
    )

    assert [r['repo'] for r in candidates()['flask']] == ['was-fastapi']
