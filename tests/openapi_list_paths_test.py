"""`openapi list-paths`, and the candidates CSV it reads (#114).

`clone`, `drift` and `stats` read the CSV with `csv.DictReader`, as the
text it holds. `list-paths` read it with pandas, which guesses a type
for each column: a column whose every value looks like a number was
read as numbers, so a release tagged `1.10` became 1.1 (#47), and `07`
became 7. Pandas also reads `null`, `NA` and `None`, among others, as
no value at all. The snapshot it then looked for was one `clone` never
made, and the candidate was skipped without a word.
"""
import csv
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.core.container import Container
from chatsbom.services.openapi_service import OpenApiService

runner = CliRunner()

SHA = 'c' * 40
SPEC = """
openapi: 3.0.0
paths:
  /users:
    get: {}
  /users/{id}:
    delete: {}
"""

#: A candidate, as `openapi candidates` writes one.
CANDIDATE = {
    'language': 'go', 'framework': 'gin', 'owner': 'Acme', 'repo': 'Shop',
    'stars': '10', 'default_branch': 'main', 'latest_release': 'V1.0.0',
    'commit_sha': SHA,
}


@pytest.fixture
def workdir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A working directory of its own, where `.workspaces` is."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr('chatsbom.core.config._config', None)
    monkeypatch.setattr(Container, '_instance', None)
    return tmp_path


def cloned(candidate: dict[str, str]) -> None:
    """The candidates CSV holding `candidate` alone, and the snapshot
    `openapi clone` left for it, with its specification."""
    with open('candidates.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=list(candidate))
        writer.writeheader()
        writer.writerow(candidate)
    snapshot = (
        Path('.workspaces') / candidate['owner'] / candidate['repo']
        / OpenApiService().get_version_path(
            candidate['latest_release'], candidate['commit_sha'],
        )
    )
    snapshot.mkdir(parents=True)
    (snapshot / 'openapi.yaml').write_text(SPEC)


def paths() -> list[tuple[str, ...]]:
    """What `list-paths` wrote: each row's owner, repository, tag,
    method and path."""
    with open('openapi_paths.csv', encoding='utf-8', newline='') as f:
        return [
            (row['owner'], row['repo'], row['tag'], row['method'], row['path'])
            for row in csv.DictReader(f)
        ]


def list_paths() -> Any:
    return runner.invoke(
        app, ['openapi', 'list-paths', '--input', 'candidates.csv'],
    )


@pytest.mark.parametrize(
    'column, value',
    [
        pytest.param('latest_release', '1.10', id='tag 1.10'),
        pytest.param('latest_release', '07', id='tag 07'),
        pytest.param('repo', 'null', id='repository null'),
        pytest.param('owner', 'NA', id='owner NA'),
    ],
)
def test_the_csv_is_read_as_the_text_it_holds(workdir, column, value):
    candidate = {**CANDIDATE, column: value}
    cloned(candidate)

    result = list_paths()

    assert result.exit_code == 0, result.output
    assert 'No OpenAPI paths found' not in result.output
    assert paths() == [
        (
            candidate['owner'], candidate['repo'],
            candidate['latest_release'], method, path,
        )
        # A parameter's name is not compared: `/users/{}`.
        for method, path in (('GET', '/users'), ('DELETE', '/users/{}'))
    ]


def test_a_candidate_with_no_ref_is_still_tagged_head(workdir):
    """An empty cell is read as '' now, where it was a missing value:
    with no release, commit or branch, the tag is `HEAD` still, as the
    snapshot's directory is."""
    cloned({
        **CANDIDATE, 'default_branch': '', 'latest_release': '',
        'commit_sha': '',
    })

    result = list_paths()

    assert result.exit_code == 0, result.output
    assert {tag for _, _, tag, _, _ in paths()} == {'HEAD'}
