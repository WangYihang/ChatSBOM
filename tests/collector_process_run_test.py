"""`chatsbom collect` for real, end to end (#171): the process on the
clock on the wall, its stages, its child processes, and its signals,
against the stand-ins for GitHub's API, git, raw content and Syft
(tests/collector_scenario.py). What the schedule does, on a virtual
clock, is `collector_process_test`'s.

- **End to end:** a small universe searched, swept, each repository
  collected from its push to its SBOM, the dependency graphs fetched,
  and the index pass, whose warehouse and snapshot are the real ones,
  publishing a snapshot; then SIGTERM, and a clean stop.
- **Stopping:** a SIGTERM in the middle of a collection ends within
  the grace, its Syft killed; collector.sqlite is consistent, the store
  holds no half-written file, and a restart carries on to the end.
- **The command:** its start-up checks, each naming the fix; and the
  command as a process of its own, stopped by a signal, as compose
  stops it, and healthy while it runs.
"""
from __future__ import annotations

import asyncio
import json
import os
import signal
import sqlite3
import subprocess
import sys
import time
from collections.abc import Callable
from collections.abc import Iterator
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest
import structlog
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.collector import process
from chatsbom.collector.depgraph import DepgraphSettings
from chatsbom.collector.health import check
from chatsbom.collector.health import heartbeat_path
from chatsbom.collector.settings import CollectorSettings
from chatsbom.collector.state import CollectorState
from chatsbom.collector.state import state_path
from chatsbom.collector.syftpool import SyftSettings
from chatsbom.collector.tokens import Token
from chatsbom.core.config import PathConfig
from chatsbom.core.container import Container
from tests.collector_scenario import build
from tests.collector_scenario import Scenario
from tests.collector_scenario import TOKEN

ROOT = Path(__file__).resolve().parent.parent

cli = CliRunner()


@pytest.fixture
def workdir(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """Where the process runs, `data/` and `.cache/` in it, as the
    collector's image has them in /app."""
    work = tmp_path / 'work'
    work.mkdir()
    monkeypatch.chdir(work)
    monkeypatch.setattr(Container, '_instance', None)
    return work


def with_syft(scenario: Scenario, monkeypatch: pytest.MonkeyPatch) -> None:
    assert scenario.syft is not None
    monkeypatch.setenv(
        'PATH', f'{scenario.syft.directory}{os.pathsep}{os.environ["PATH"]}',
    )


def settings(**changes: Any) -> CollectorSettings:
    return CollectorSettings(
        tokens=(Token('token 1', TOKEN),), reserve={},
        index_interval=timedelta(seconds=1), **changes,
    )


def signalled_when(
    condition: Callable[[], bool], number: int = signal.SIGTERM,
) -> asyncio.Future[None]:
    """Once `condition` holds, this process sent `number`, as compose
    sends it: the loop's handler takes it."""
    async def watching() -> None:
        deadline = time.monotonic() + 120
        while not condition():
            assert time.monotonic() < deadline, 'it never came to it'
            await asyncio.sleep(0.05)
        os.kill(os.getpid(), number)

    return asyncio.ensure_future(watching())


def stray(data: Path) -> list[Path]:
    """What a write left half done: a file written aside and not
    renamed into place."""
    return sorted(
        path for path in data.rglob('*')
        if path.name.endswith('.tmp') or '.building' in path.name
    )


def consistent(path: Path) -> bool:
    with sqlite3.connect(path) as db:
        [[said]] = db.execute('PRAGMA integrity_check').fetchall()
    db.close()
    return bool(said == 'ok')


class TestEndToEnd:
    def test_collects_a_small_universe_and_publishes_a_snapshot(
        self, tmp_path, workdir, monkeypatch,
    ):
        scenario = build(tmp_path / 'upstream')
        with_syft(scenario, monkeypatch)
        paths = PathConfig()
        current = paths.snapshots_dir / 'CURRENT'
        published: list[float] = []

        def done() -> bool:
            """Every repository collected and both graphs stored, then a
            snapshot published after that."""
            if published:
                return current.exists() and (
                    current.stat().st_mtime > published[0]
                )
            collected = (
                state.observed(3) is not None
                and not state.never_collected() and not state.changed()
            )
            graphs = [
                paths.depgraph_dir / str(key) for key in (1, 3)
            ]
            if collected and all(
                any(graph.rglob('sbom.spdx.json')) for graph in graphs
            ):
                published.append(time.time())
            return False

        with CollectorState.open(state_path(paths.base_data_dir)) as state:
            async def running() -> int:
                watching = signalled_when(done)
                try:
                    return await process.run(
                        paths, state, settings(),
                        SyftSettings(slots=2, timeout=60, memory=0),
                        DepgraphSettings(),
                        upstream=scenario.upstream_for_process(),
                    )
                finally:
                    watching.cancel()
                    await asyncio.gather(watching, return_exceptions=True)

            with structlog.testing.capture_logs() as logs:
                status = asyncio.run(running())
            collected = {
                key: state.observed(key) for key in (1, 2, 3)
            }

        assert status == 0
        events = [log['event'] for log in logs]
        assert 'The universe was searched again' in events
        assert 'The universe was swept' in events
        assert 'Index pass' in events
        assert events[-1] == 'The collector stopped'
        assert sorted(
            log['repository_id'] for log in logs
            if log['event'] == 'Repository collected'
            and log['current']
        ) == [1, 2, 3]
        # Each repository's chain, in the store.
        for key, repository in (
            (1, 'octo/one'), (2, 'octo/two'), (3, 'octo/three'),
        ):
            [sha] = scenario.repositories[repository].files_at
            assert paths.sbom_file(key, sha).is_file(), key
            assert collected[key] is not None
        # The snapshot published serves them.
        [name, *_] = current.read_text().split()
        snapshot = paths.snapshots_dir / f'{name}.sqlite'
        with sqlite3.connect(f'file:{snapshot}?mode=ro', uri=True) as db:
            served = {
                row[0] for row in db.execute(
                    'SELECT id FROM repositories',
                )
            }
        db.close()
        assert {1, 2, 3} <= served
        # The export directory `web` mounts, made at the start.
        assert (paths.base_data_dir / 'export').is_dir()
        assert stray(paths.base_data_dir) == []
        assert 'stopped at' in check(
            heartbeat_path(paths.base_data_dir), datetime.now(timezone.utc),
        )
        assert TOKEN not in json.dumps(logs, default=str)


class TestStopping:
    def test_a_sigterm_mid_collection_ends_in_time_and_a_restart_resumes(
        self, tmp_path, workdir, monkeypatch,
    ):
        """The Syft of the first scan takes a minute: SIGTERM while it
        runs. The scan is killed, the repository is due again, and the
        next start collects it."""
        scenario = build(tmp_path / 'upstream', delay=60)
        assert scenario.syft is not None
        with_syft(scenario, monkeypatch)
        monkeypatch.setattr(process, 'FINISH', 1.0)
        paths = PathConfig()
        syft_log = scenario.syft.directory / 'syft.log'

        def scanning() -> bool:
            return syft_log.exists() and bool(syft_log.read_text().strip())

        async def running(
            state: CollectorState, stop_when: Callable[[], bool],
        ) -> tuple[int, float]:
            watching = signalled_when(stop_when)
            try:
                status = await process.run(
                    paths, state, settings(),
                    SyftSettings(slots=1, timeout=120, memory=0),
                    DepgraphSettings(),
                    upstream=scenario.upstream_for_process(),
                )
            finally:
                watching.cancel()
                await asyncio.gather(watching, return_exceptions=True)
            return status, time.monotonic()

        with CollectorState.open(state_path(paths.base_data_dir)) as state:
            began = time.monotonic()
            status, ended = asyncio.run(running(state, scanning))
            unfinished = state.never_collected() + state.changed()

        assert status == 0
        # Within its grace, and far short of the scan's minute.
        assert ended - began < 30
        scans = scenario.syft.scans
        assert scans
        assert not Path(f'/proc/{scans[0]["pid"]}').exists() or (
            Path(f'/proc/{scans[0]["pid"]}/stat').read_text()
            .rpartition(')')[2].split()[0] == 'Z'
        )
        assert unfinished, 'what was in flight is due again'
        assert stray(paths.base_data_dir) == []
        assert consistent(state_path(paths.base_data_dir))

        # The next start: scans take no time, and it carries on.
        scenario.syft.configure(delay=0)
        with CollectorState.open(state_path(paths.base_data_dir)) as state:
            def all_collected() -> bool:
                return (
                    state.observed(3) is not None
                    and not state.never_collected() and not state.changed()
                )

            status, _ = asyncio.run(running(state, all_collected))
        assert status == 0
        for key, repository in (
            (1, 'octo/one'), (2, 'octo/two'), (3, 'octo/three'),
        ):
            [sha] = scenario.repositories[repository].files_at
            assert paths.sbom_file(key, sha).is_file(), key
        assert stray(paths.base_data_dir) == []


# -- the command ----------------------------------------------------------------


class TestTheCommand:
    def test_says_how_to_run_it_and_the_one_by_hand(self):
        result = cli.invoke(app, ['collect', '--help'])
        assert result.exit_code == 0, result.output
        text = ' '.join(result.output.split())
        assert 'Run the collector until SIGTERM or SIGINT.' in text
        assert 'repo' in text

    def test_refuses_to_start_without_a_token(self, workdir, monkeypatch):
        for name in ('GITHUB_TOKEN', 'CHATSBOM_GITHUB_TOKENS'):
            monkeypatch.delenv(name, raising=False)
        result = cli.invoke(app, ['collect'])
        assert result.exit_code == 1
        said = ' '.join(result.output.split())
        assert 'No GitHub token: set GITHUB_TOKEN' in said
        assert not (workdir / 'data').exists()

    def test_refuses_a_setting_it_cannot_use_naming_it(
        self, workdir, monkeypatch,
    ):
        monkeypatch.setenv('GITHUB_TOKEN', TOKEN)
        monkeypatch.setenv('CHATSBOM_REPOSITORIES_AT_ONCE', '0')
        result = cli.invoke(app, ['collect'])
        assert result.exit_code == 1
        assert 'CHATSBOM_REPOSITORIES_AT_ONCE' in result.output

    def test_refuses_a_data_directory_it_cannot_write_saying_how(
        self, workdir, monkeypatch,
    ):
        if os.geteuid() == 0:
            # Root writes anything: an unwritable directory is one it
            # cannot make, under a file.
            (workdir / 'data').write_text('')
        else:
            (workdir / 'data').mkdir(mode=0o500)
        monkeypatch.setenv('GITHUB_TOKEN', TOKEN)

        result = cli.invoke(app, ['collect'])

        assert result.exit_code == 1
        said = ' '.join(result.output.split())
        # As any command says it (bare_run_test): the mounts for a bare
        # run, and for compose the directories to make, or to give.
        assert f'cannot write data in {workdir} as uid' in said
        assert 'mkdir -p data .cache' in said
        assert f'sudo chown -R {os.getuid()}:{os.getgid()} data .cache' in said
        assert 'docker run --rm --user' in said

    def test_refuses_to_share_collector_sqlite(self, workdir, monkeypatch):
        monkeypatch.setenv('GITHUB_TOKEN', TOKEN)
        with CollectorState.open(state_path(workdir / 'data')):
            result = cli.invoke(app, ['collect'])
        assert result.exit_code == 1
        assert 'is in use: another collector' in ' '.join(
            result.output.split(),
        )


def launched(
    workdir: Path, stand_ins: Path, **environ: str,
) -> subprocess.Popen[str]:
    """`chatsbom collect` in `workdir`, a process of its own, on the
    scenario (tests/collector_scenario.py)."""
    return subprocess.Popen(
        [sys.executable, '-m', 'tests.collector_scenario', str(workdir)],
        cwd=ROOT,
        env={
            **os.environ, 'SCENARIO_STAND_INS': str(stand_ins),
            'CHATSBOM_LOG_FORMAT': 'json', 'PYTHONPATH': str(ROOT),
            **environ,
        },
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
    )


@pytest.fixture
def launch(tmp_path: Path) -> Iterator[Callable[..., subprocess.Popen[str]]]:
    started: list[subprocess.Popen[str]] = []

    def launching(**environ: str) -> subprocess.Popen[str]:
        running = launched(
            tmp_path / 'work', tmp_path / 'stand-ins', **environ,
        )
        started.append(running)
        return running

    (tmp_path / 'work').mkdir()
    yield launching
    for running in started:
        if running.poll() is None:
            running.kill()
        running.communicate()


def test_the_command_stops_on_sigterm_within_the_grace(tmp_path, launch):
    """As compose stops it: SIGTERM, with a scan in flight. It stops
    within the 30 s compose gives it, its Syft with it, with nothing
    half written; and while it ran, its healthcheck passed."""
    running = launch(SCENARIO_SYFT_DELAY='60')
    data = tmp_path / 'work' / 'data'
    syft_log = tmp_path / 'stand-ins' / 'upstream' / 'bin' / 'syft.log'
    deadline = time.monotonic() + 120
    while not (syft_log.exists() and syft_log.read_text().strip()):
        assert running.poll() is None, running.communicate()[0]
        assert time.monotonic() < deadline, 'no scan began'
        time.sleep(0.1)

    health = subprocess.run(
        [sys.executable, '-m', 'chatsbom.collector.health'],
        cwd=tmp_path / 'work', capture_output=True, text=True, check=False,
    )
    signalled = time.monotonic()
    running.send_signal(signal.SIGTERM)
    output, _ = running.communicate(timeout=60)
    took = time.monotonic() - signalled

    assert (health.returncode, health.stdout) == (0, 'healthy\n')
    assert running.returncode == 0, output
    assert took < 30, took
    [scan] = [
        json.loads(line) for line in syft_log.read_text().splitlines()
    ][:1]
    stat = Path(f'/proc/{scan["pid"]}/stat')
    assert not stat.exists() or (
        stat.read_text().rpartition(')')[2].split()[0] == 'Z'
    )
    assert stray(data) == []
    assert consistent(state_path(data))
    lines = [
        json.loads(line) for line in output.splitlines()
        if line.startswith('{')
    ]
    events = [line['event'] for line in lines]
    assert 'The collector is stopping' in events
    assert events[-1] == 'The collector stopped'
    assert TOKEN not in output
    unhealthy = subprocess.run(
        [sys.executable, '-m', 'chatsbom.collector.health'],
        cwd=tmp_path / 'work', capture_output=True, text=True, check=False,
    )
    assert unhealthy.returncode == 1
    assert 'stopped at' in unhealthy.stdout
