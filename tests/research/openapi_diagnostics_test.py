"""What the `openapi` commands say besides their output (#124).

As the `db` commands do since #116 (#114): stdout carries what a
command prints for its reader, its table and its report, and anything
else is said on stderr, where the logs go. A failure exits 1, and with
JSON logs each thing said is one event.

`candidates` printed "Failed to process results" on stdout and exited 0,
so a script took a CSV it never wrote for a success. Every command
printed on stdout that its candidates CSV was missing, and that it
found nothing.
"""
import csv
import json
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest
from typer.testing import CliRunner

from chatsbom.core.container import Container
from chatsbom.core.logging import setup_logging
from chatsbom.research.__main__ import app
from chatsbom.research.models.openapi import FrameworkStats
from chatsbom.research.models.openapi import OpenApiCandidate
from chatsbom.research.models.openapi import OpenApiCandidateResult
from chatsbom.research.services.openapi_service import OpenApiService
from chatsbom.warehouse import connect

runner = CliRunner()

#: A candidate, as `openapi candidates` finds one.
CANDIDATE = OpenApiCandidate(
    language='go', framework='gin', framework_version='1.9.1',
    owner='acme', repo='shop', stars=10, default_branch='main',
    latest_release='v1.0.0', commit_sha='c' * 40,
    url='https://github.com/acme/shop', openapi_file='openapi.yaml',
    openapi_url='', has_openapi_file=True,
)

#: The candidates CSV's columns, as `candidates` writes them.
COLUMNS = [
    'language', 'framework', 'framework_version', 'owner', 'repo',
    'stars', 'default_branch', 'latest_release', 'commit_sha', 'url',
    'openapi_file', 'openapi_url', 'matched_dependencies',
    'has_openapi_file', 'has_openapi_deps', 'generation_command',
]


@pytest.fixture(autouse=True)
def workdir(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> Iterator[Path]:
    """A working directory of its own, logs for a person unless a test
    asks for JSON, and a tokenizer that is never downloaded: `stats`
    loads one before it reads its CSV."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr('chatsbom.core.config._config', None)
    monkeypatch.setattr(Container, '_instance', None)
    for name in ('CHATSBOM_LOG_FORMAT', 'ENV'):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setattr(
        'chatsbom.research.commands.openapi.stats.load_tokenizer',
        lambda cache: SimpleNamespace(encode_ordinary=str.split),
    )
    yield tmp_path
    setup_logging('INFO')


def found(
    monkeypatch: pytest.MonkeyPatch, *candidates: OpenApiCandidate,
) -> None:
    """`candidates`' query, finding `candidates`, of an empty warehouse
    where the command reads one by default."""
    Path('data').mkdir(exist_ok=True)
    connect(Path('data') / 'warehouse.duckdb').close()
    stats = [
        FrameworkStats('gin', 'go', total_projects=4, matched_projects=1),
    ] if candidates else []
    monkeypatch.setattr(
        OpenApiService, 'find_candidates',
        lambda self, con: OpenApiCandidateResult(list(candidates), stats),
    )


def listed(*rows: dict[str, str]) -> None:
    """The candidates CSV, `candidates.csv`, holding `rows`."""
    with open('candidates.csv', 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def words(text: str) -> str:
    """`text` as words: Rich wraps a long line."""
    return ' '.join(text.split())


def events(result: Any) -> list[dict[str, Any]]:
    """Every line on stderr, each read as the JSON object it must be."""
    return [json.loads(line) for line in result.stderr.splitlines()]


# --- a candidates CSV that is not there -------------------------------------

#: Each command that reads the candidates CSV, and what it says of one
#: that is missing.
READING = {
    'clone': 'CSV file not found: missing.csv',
    'drift': 'CSV not found: missing.csv',
    'list-paths': 'CSV not found: missing.csv',
    'stats': 'CSV file not found: missing.csv',
}


@pytest.mark.parametrize(
    'command, said', READING.items(), ids=list(READING),
)
def test_a_missing_csv_is_said_on_stderr(command, said):
    """It was printed on stdout, exiting 1: an error where the output
    goes."""
    result = runner.invoke(app, ['openapi', command, '--input', 'missing.csv'])

    assert result.exit_code == 1, result.output
    assert result.stdout == ''
    assert said in words(result.stderr)


@pytest.mark.parametrize('command', list(READING))
def test_a_missing_csv_is_one_json_event(command, json_logs):
    """A machine reads stderr then, and what it reads is one event."""
    result = runner.invoke(app, ['openapi', command, '--input', 'missing.csv'])

    assert result.exit_code == 1, result.output
    assert result.stdout == ''
    [event] = events(result)
    assert (event['event'], event['level'], event['logger']) == (
        'CSV not found', 'error', f"openapi_{command.replace('-', '_')}",
    )
    assert event['path'] == 'missing.csv'


# --- nothing found --------------------------------------------------------

#: Each command that can find nothing, what it is then given, and what
#: it says.
NOTHING = {
    'candidates': (
        ['openapi', 'candidates'], 'No OpenAPI specs found', 'candidates',
    ),
    'drift': (
        ['openapi', 'drift', '--input', 'candidates.csv'],
        'No drift data collected', 'drift',
    ),
    'list-paths': (
        ['openapi', 'list-paths', '--input', 'candidates.csv'],
        'No OpenAPI paths found', 'list_paths',
    ),
    'stats': (
        ['openapi', 'stats', '--input', 'candidates.csv'],
        'No cloned repositories found to analyze', 'stats',
    ),
}


def nothing(monkeypatch: pytest.MonkeyPatch) -> None:
    """A query that finds no candidate, and a CSV whose one candidate
    was never cloned: there is nothing to read in either."""
    found(monkeypatch)
    listed({
        column: str(value) for column, value
        in zip(COLUMNS, CANDIDATE.to_csv_row())
    })


@pytest.mark.parametrize(
    'command, said, module', NOTHING.values(), ids=list(NOTHING),
)
def test_finding_nothing_is_said_on_stderr_and_is_no_failure(
    monkeypatch, command, said, module,
):
    """An answer, if an empty one, so the status stays 0. It is no
    output either: on stdout, a script read the notice as a result."""
    nothing(monkeypatch)

    result = runner.invoke(app, command)

    assert result.exit_code == 0, result.output
    assert said not in result.stdout
    assert said in words(result.stderr)


@pytest.mark.parametrize(
    'command, said, module', NOTHING.values(), ids=list(NOTHING),
)
def test_finding_nothing_is_one_json_event(
    monkeypatch, json_logs, command, said, module,
):
    nothing(monkeypatch)

    result = runner.invoke(app, command)

    assert result.exit_code == 0, result.output
    assert said not in result.stdout
    [event] = events(result)
    assert (event['event'], event['level'], event['logger']) == (
        said, 'warning', f'openapi_{module}',
    )


# --- candidates: the CSV it could not write ------------------------------------

def test_candidates_that_cannot_be_written_fail_on_stderr(monkeypatch):
    """"Failed to process results" was printed on stdout, and the
    command exited 0 without the CSV it was asked for."""
    found(monkeypatch, CANDIDATE)

    result = runner.invoke(
        app, ['openapi', 'candidates', '--output', 'gone/candidates.csv'],
    )

    assert result.exit_code == 1, result.output
    assert 'Failed' not in result.stdout
    said = words(result.stderr)
    assert 'Failed to process results:' in said
    assert 'gone/candidates.csv' in said


def test_candidates_that_cannot_be_written_are_one_json_event(
    monkeypatch, json_logs,
):
    found(monkeypatch, CANDIDATE)

    result = runner.invoke(
        app, ['openapi', 'candidates', '--output', 'gone/candidates.csv'],
    )

    assert result.exit_code == 1, result.output
    [event] = events(result)
    assert (event['event'], event['level'], event['logger']) == (
        'Failed to process results', 'error', 'openapi_candidates',
    )
    assert event['output'] == 'gone/candidates.csv'
    assert 'No such file or directory' in event['error']


def test_what_candidates_found_is_still_its_output(monkeypatch):
    """The table and the total stay on stdout, and nothing is said."""
    found(monkeypatch, CANDIDATE)
    monkeypatch.setenv('COLUMNS', '200')

    result = runner.invoke(app, ['openapi', 'candidates'])

    assert result.exit_code == 0, result.output
    assert result.stderr == ''
    assert 'OpenAPI Candidate Statistics' in result.stdout
    assert (
        'Total: Found 1 OpenAPI specs across 1 unique projects → '
        'openapi_candidates.csv'
    ) in words(result.stdout)
    with open('openapi_candidates.csv', encoding='utf-8') as f:
        assert [row['repo'] for row in csv.DictReader(f)] == ['shop']
