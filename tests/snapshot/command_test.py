"""`chatsbom snapshot build`: opt-in, one pass at a time (#132).

The command writes a snapshot of `data/warehouse.duckdb` and publishes
it in `data/snapshots/`, or says that the data has not changed and
publishes nothing. What it did is its output, on stdout; anything else
it has to say, on stderr (#114).
"""
from __future__ import annotations

import fcntl
import json
from pathlib import Path

import pytest
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.core.container import Container
from chatsbom.dataset.open import current
from tests.snapshot.conftest import shop
from tests.snapshot.conftest import warehouse

ROOT = Path(__file__).resolve().parents[2]
runner = CliRunner()


@pytest.fixture
def here(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A warehouse where the command looks for one, `data/`, in the
    working directory."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr('chatsbom.core.config._config', None)
    monkeypatch.setattr(Container, '_instance', None)
    (tmp_path / 'data').mkdir()
    warehouse(tmp_path / 'data' / 'warehouse.duckdb', shop())
    return tmp_path


def test_it_publishes_a_snapshot_of_the_warehouse(here: Path) -> None:
    result = runner.invoke(app, ['snapshot', 'build'])

    assert result.exit_code == 0, result.output
    published = current(Path('data') / 'snapshots')
    assert published.is_file()
    snapshot = published.name.removesuffix('.sqlite')
    assert f'Published {snapshot}' in ' '.join(result.stdout.split())
    # What it holds, beside it.
    for said in ('artifacts', 'repositories', 'history'):
        assert said in result.stdout
    assert 'Published' not in result.stderr


def test_a_pass_that_changed_nothing_publishes_nothing(here: Path) -> None:
    assert runner.invoke(app, ['snapshot', 'build']).exit_code == 0
    snapshots = Path('data') / 'snapshots'
    before = {
        path.name: path.stat().st_mtime_ns for path in snapshots.iterdir()
    }
    snapshot = current(snapshots).name.removesuffix('.sqlite')

    result = runner.invoke(app, ['snapshot', 'build'])

    assert result.exit_code == 0, result.output
    said = ' '.join(result.stdout.split())
    assert f'Unchanged: {snapshot} is current' in said
    assert {
        path.name: path.stat().st_mtime_ns for path in snapshots.iterdir()
    } == before


def test_it_reads_and_writes_where_it_is_told(
    here: Path, tmp_path: Path,
) -> None:
    elsewhere = tmp_path / 'elsewhere'
    warehouse(tmp_path / 'other.duckdb', shop())
    result = runner.invoke(
        app, [
            'snapshot', 'build', '--warehouse', str(tmp_path / 'other.duckdb'),
            '--output', str(elsewhere),
        ],
    )
    assert result.exit_code == 0, result.output
    assert current(elsewhere).is_file()
    assert not (Path('data') / 'snapshots').exists()


def test_without_a_warehouse_it_says_so(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr('chatsbom.core.config._config', None)
    monkeypatch.setattr(Container, '_instance', None)

    result = runner.invoke(app, ['snapshot', 'build'])

    assert result.exit_code == 1
    assert result.stdout == ''
    said = ' '.join(result.stderr.split()).lower()
    assert 'no warehouse' in said and 'warehouse build' in said
    assert list(tmp_path.iterdir()) == []


def test_a_second_pass_while_one_runs_is_refused(here: Path) -> None:
    snapshots = Path('data') / 'snapshots'
    snapshots.mkdir()
    with (snapshots / '.lock').open('a') as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = runner.invoke(app, ['snapshot', 'build'])

    assert result.exit_code == 1
    assert result.stdout == ''
    assert 'another pass' in ' '.join(result.stderr.split()).lower()
    assert sorted(p.name for p in snapshots.iterdir()) == ['.lock']


def test_a_refusal_is_one_event_when_logs_are_json(
    here: Path, json_logs: None,
) -> None:
    snapshots = Path('data') / 'snapshots'
    snapshots.mkdir()
    with (snapshots / '.lock').open('a') as held:
        fcntl.flock(held, fcntl.LOCK_EX | fcntl.LOCK_NB)
        result = runner.invoke(app, ['snapshot', 'build'])

    assert result.exit_code == 1
    events = [json.loads(line) for line in result.stderr.splitlines()]
    assert [event['level'] for event in events] == ['error']
    assert events[0]['lock'].endswith('.lock')


def test_what_it_does_not_catch_is_reported_on_stderr(
    here: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    """As `handle_errors` reports it for every command (#124)."""
    def refuse(*args: object, **kwargs: object) -> None:
        raise RuntimeError('unreadable [/dim] warehouse')

    monkeypatch.setattr('chatsbom.snapshot.build.write', refuse)
    result = runner.invoke(app, ['snapshot', 'build'])

    assert isinstance(result.exception, SystemExit), repr(result.exception)
    assert result.exit_code == 1
    assert result.stdout == ''
    assert 'Unexpected Error: unreadable [/dim] warehouse' in result.stderr


def test_nothing_in_the_collector_loop_builds_it() -> None:
    """Opt-in until the cutover (#128): no service, script or unit the
    loop runs asks for a snapshot, and `run` does not either."""
    compose = sorted(ROOT.glob('docker-compose*.yaml'))
    assert ROOT / 'docker-compose.yaml' in compose
    for path in (
        *compose,
        *sorted((ROOT / 'deploy').rglob('*')),
        ROOT / 'chatsbom' / 'commands' / 'run.py',
        ROOT / 'chatsbom' / 'services' / 'run_service.py',
    ):
        if path.is_file():
            text = path.read_text(encoding='utf-8', errors='replace')
            assert 'snapshot build' not in text, path
            assert 'chatsbom.snapshot' not in text, path
