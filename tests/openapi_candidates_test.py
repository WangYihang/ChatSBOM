"""`openapi candidates`, against a warehouse (#47, #153).

It lowercased every owner, repository, branch and tag it found, so a
release URL built from Netflix/zuul's `V3.0.0` named a tag that does not
exist, and every command after it (`clone`, `drift`, `list-paths`,
`stats`) was handed the wrong name. Its query took the framework's
version from whichever of the framework's packages came first, not from
the framework; and it counted `pydantic`, which FastAPI itself depends
on, as a sign that a FastAPI project documents its API, so every FastAPI
project was a candidate.

It asked the ClickHouse server until #153, and asks the warehouse now:
its current facts, each repository's newest scan (`warehouse/
frameworks.py`). Run as the command, so the CSV it writes is what is
checked.
"""
import csv
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.core.config import PathConfig
from chatsbom.models.framework import FastAPI
from chatsbom.models.relationship import DIRECT
from chatsbom.models.relationship import TRANSITIVE
from chatsbom.services.openapi_service import OpenApiService
from chatsbom.warehouse import connect
from tests.snapshot.conftest import artifact
from tests.snapshot.conftest import Corpus
from tests.snapshot.conftest import repository
from tests.snapshot.conftest import warehouse

runner = CliRunner()

ZUUL = '1' * 40
OLD = '2' * 40
NEW = '3' * 40

#: When each commit was scanned: OLD before NEW, so NEW is current.
SCANNED = {
    ZUUL: datetime(2026, 9, 1, tzinfo=timezone.utc),
    OLD: datetime(2026, 1, 15, tzinfo=timezone.utc),
    NEW: datetime(2026, 9, 14, tzinfo=timezone.utc),
}


def project(
    repository_id: int,
    owner: str,
    repo: str,
    *,
    stars: int = 1000,
    language: str = 'python',
    **fields: Any,
) -> dict[str, Any]:
    return repository(repository_id, owner, repo, stars, language, **fields)


def package(
    repository_id: int,
    name: str,
    version: str,
    *,
    commit: str = NEW,
    type: str = 'python',
    relationship: str = TRANSITIVE,
) -> dict[str, Any]:
    """A package of the Syft scan of `commit`."""
    return artifact(
        repository_id, name, version, type, observed_at=SCANNED[commit],
        commit=commit, relationship=relationship,
    )


@pytest.fixture
def seed(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    """A warehouse of these rows where the command reads it by default,
    data/warehouse.duckdb, in the directory it runs in."""
    monkeypatch.chdir(tmp_path)

    def made(
        repositories: list[dict[str, Any]],
        artifacts: list[dict[str, Any]],
        corpus: set[int] | None = None,
    ) -> None:
        (tmp_path / 'data').mkdir(exist_ok=True)
        warehouse(
            tmp_path / 'data' / 'warehouse.duckdb',
            Corpus(
                repositories=repositories, artifacts=artifacts,
                corpus=corpus,
            ),
        )

    return made


def tree(root: Path, repository_id: int, sha: str, *files: str) -> None:
    """A tree where the tree stage writes it."""
    paths = PathConfig(base_data_dir=root / 'data')
    path = paths.tree_file(repository_id, sha)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(''.join(f'{f}\n' for f in files), encoding='utf-8')


def candidates(*options: str) -> dict[str, list[dict[str, str]]]:
    """`openapi candidates`, run in the working directory, and the rows
    of the CSV it wrote, by framework."""
    result = runner.invoke(
        app, ['openapi', 'candidates', '--output', 'candidates.csv', *options],
    )
    assert result.exit_code == 0, result.output
    assert 'Error' not in result.output, result.output
    found: dict[str, list[dict[str, str]]] = {}
    output = Path('candidates.csv')
    if output.exists():
        with open(output, encoding='utf-8', newline='') as f:
            for row in csv.DictReader(f):
                found.setdefault(row['framework'], []).append(row)
    return found


def test_a_mixed_case_repository_keeps_its_case(seed, tmp_path):
    seed(
        [
            project(
                1, 'Netflix', 'zuul', language='java', stars=13000,
                default_branch='Master', latest_release_tag='V3.0.0',
            ),
        ],
        [
            package(
                1, 'org.springframework.boot:spring-boot-starter-web',
                '3.2.0', type='java-archive', commit=ZUUL,
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


def test_only_the_current_scan_counts(seed):
    """`moved-on` declared an OpenAPI package in January only."""
    seed(
        [
            project(10, 'api', 'current', stars=50),
            project(11, 'api', 'moved-on'),
        ],
        [
            package(10, 'flask', '2.3.0', commit=OLD),
            package(10, 'flask', '3.0.0'),
            package(10, 'flasgger', '0.9.7'),
            package(11, 'flask', '2.3.0', commit=OLD),
            package(11, 'flasgger', '0.9.5', commit=OLD),
            package(11, 'flask', '3.0.0'),
        ],
    )

    [current] = candidates()['flask']

    assert (current['repo'], current['stars']) == ('current', '50')
    assert current['framework_version'] == '3.0.0'
    assert current['matched_dependencies'] == 'flasgger'
    assert current['commit_sha'] == NEW


def test_the_version_is_the_frameworks_own(seed):
    """Not a sibling's: FastAPI's, not Starlette's, which it is built
    on; and chi v5's, which the project declares, not the chi v1 one of
    its dependencies still pulls in."""
    seed(
        [
            project(20, 'py', 'service'),
            project(21, 'go', 'router', language='go'),
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


def test_pydantic_alone_does_not_make_a_fastapi_candidate(seed):
    """FastAPI depends on pydantic, so every FastAPI project has it."""
    seed(
        [project(30, 'py', 'plain')],
        [
            package(30, 'fastapi', '0.110.0', relationship=DIRECT),
            package(30, 'pydantic', '2.6.4'),
        ],
    )

    assert candidates().get('fastapi', []) == []
    assert 'pydantic' not in FastAPI().get_openapi_packages()


def test_a_project_with_an_excluded_package_is_left_out(seed):
    """Flask's list leaves out projects on FastAPI, in the same scan."""
    seed(
        [
            project(40, 'py', 'both'),
            project(41, 'py', 'was-fastapi'),
        ],
        [
            package(40, 'flask', '3.0.0'),
            package(40, 'flask-restx', '1.3.0'),
            package(40, 'fastapi', '0.110.0'),
            package(41, 'fastapi', '0.100.0', commit=OLD),
            package(41, 'flask', '3.0.0'),
            package(41, 'flask-restx', '1.3.0'),
        ],
    )

    assert [r['repo'] for r in candidates()['flask']] == ['was-fastapi']


def test_only_the_corpus_is_a_candidate(seed):
    """What is current is of the corpus, the newest complete snapshot's
    repositories, as every answer the warehouse gives."""
    seed(
        [
            project(50, 'py', 'listed'),
            project(51, 'py', 'unlisted', snapshot=''),
        ],
        [
            package(50, 'flask', '3.0.0'),
            package(50, 'flasgger', '0.9.7'),
            package(51, 'flask', '3.0.0'),
            package(51, 'flasgger', '0.9.7'),
        ],
        corpus={50},
    )

    assert [r['repo'] for r in candidates()['flask']] == ['listed']


def test_the_candidates_and_their_count_are_of_the_current_scan(tmp_path):
    """Four Flask-era projects, each scanned twice: `current` declares an
    OpenAPI package now; `moved-on` declared one in January only;
    `dropped` has left Flask since; and `was-fastapi` used to carry the
    package that takes a project off Flask's list. Three use Flask now,
    and two of them have the tooling."""
    path = warehouse(
        tmp_path / 'warehouse.duckdb',
        Corpus(
            repositories=[
                project(10, 'api', 'current', stars=50),
                project(11, 'api', 'moved-on', stars=1),
                project(12, 'api', 'dropped', stars=1),
                project(13, 'api', 'was-fastapi', stars=1),
            ],
            artifacts=[
                package(10, 'flask', '2.3.0', commit=OLD),
                package(10, 'flask', '3.0.0'),
                package(10, 'flasgger', '0.9.7'),
                package(11, 'flask', '2.3.0', commit=OLD),
                package(11, 'flasgger', '0.9.5', commit=OLD),
                package(11, 'flask', '3.0.0'),
                package(12, 'flask', '2.3.0', commit=OLD),
                package(12, 'apispec', '6.3.0', commit=OLD),
                package(12, 'django', '5.0.0'),
                package(13, 'fastapi', '0.100.0', commit=OLD),
                package(13, 'flask', '3.0.0'),
                package(13, 'flask-restx', '1.3.0'),
            ],
        ),
    )

    with connect(path, read_only=True) as con:
        result = OpenApiService().find_candidates(con)

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


def test_another_warehouse_is_named_with_warehouse(seed, tmp_path):
    seed(
        [project(60, 'py', 'elsewhere')], [
            package(60, 'flask', '3.0.0'), package(60, 'flasgger', '0.9.7'),
        ],
    )
    moved = tmp_path / 'elsewhere.duckdb'
    (tmp_path / 'data' / 'warehouse.duckdb').rename(moved)

    found = candidates('--warehouse', str(moved))

    assert [r['repo'] for r in found['flask']] == ['elsewhere']


def test_without_a_warehouse_it_says_to_build_one(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)

    result = runner.invoke(app, ['openapi', 'candidates'])

    assert result.exit_code == 1, result.output
    said = ' '.join(result.stderr.split())
    assert 'no warehouse at data/warehouse.duckdb' in said
    assert 'chatsbom warehouse build' in said
    assert not (tmp_path / 'openapi_candidates.csv').exists()
