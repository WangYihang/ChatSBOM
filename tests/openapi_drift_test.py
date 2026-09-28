"""`openapi drift`, and the chart that never read it (#47).

`drift` opened a database connection it never used, so it failed
wherever there was no database, though it reads nothing but files.

`plot-drift` charted a time series, per repository across releases, from
columns `drift` does not write: `date`, `code_count`, `spec_count`,
`code_commits`, `overlap_pct` and `stale_pct`. `drift` measures one
snapshot per candidate, with no date. Every run failed on `'date'`, and
there is no series in the data to rewrite it against, so it is gone, and
matplotlib, which nothing else used, with it.
"""
import csv
import json
import socket
import tomllib
from pathlib import Path

import pytest
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.core.container import Container

ROOT = Path(__file__).resolve().parents[1]
runner = CliRunner()

SHA = 'c' * 40
SPEC = """
openapi: 3.0.0
paths:
  /users:
    get: {}
    post: {}
  /users/{id}:
    get: {}
"""


@pytest.fixture
def offline(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> list[object]:
    """A working directory of its own, and every connection refused."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(Container, '_instance', None)
    tried: list[object] = []

    def refuse(self: socket.socket, address: object, *args: object) -> None:
        tried.append(address)
        raise OSError('no connections in this test')

    monkeypatch.setattr(socket.socket, 'connect', refuse)
    return tried


def test_drift_needs_no_database(offline, tmp_path):
    with open('candidates.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        writer.writerow([
            'language', 'framework', 'owner', 'repo', 'stars',
            'default_branch', 'latest_release', 'commit_sha',
        ])
        writer.writerow(
            ['go', 'gin', 'Acme', 'Shop', '10', 'main', 'V1.0.0', SHA],
        )
    snapshot = tmp_path / '.workspaces' / 'Acme' / 'Shop' / 'V1.0.0' / SHA
    snapshot.mkdir(parents=True)
    (snapshot / 'openapi.yaml').write_text(SPEC)
    code = tmp_path / 'data' / '08-code-endpoints' / 'Acme' / 'Shop'
    code.mkdir(parents=True)
    (code / 'V1.0.0.json').write_text(
        json.dumps([
            {'method': 'get', 'path': '/users'},
            {'method': 'get', 'path': '/users/:id'},
            {'method': 'delete', 'path': '/users/:id'},
        ]),
    )

    result = runner.invoke(
        app, ['openapi', 'drift', '--input', 'candidates.csv'],
    )

    assert result.exit_code == 0, result.output
    assert offline == []
    with open('openapi_drift_data.csv', encoding='utf-8') as f:
        [row] = list(csv.DictReader(f))
    assert (row['owner'], row['repo'], row['tag']) == (
        'Acme', 'Shop', 'V1.0.0',
    )
    assert row['implemented_endpoints'] == '3'
    assert row['documented_endpoints'] == '3'
    assert (float(row['precision']), float(row['recall'])) == (
        pytest.approx(2 / 3, abs=1e-4), pytest.approx(2 / 3, abs=1e-4),
    )


def test_plot_drift_is_gone_and_matplotlib_with_it():
    result = runner.invoke(app, ['openapi', 'plot-drift', '--help'])
    assert result.exit_code != 0
    assert 'No such command' in result.output

    project = tomllib.loads((ROOT / 'pyproject.toml').read_text())['project']
    declared = list(project['dependencies'])
    for specs in project['optional-dependencies'].values():
        declared += specs
    assert [spec for spec in declared if 'matplotlib' in spec] == []
