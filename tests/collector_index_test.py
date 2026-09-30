"""The index pass of `chatsbom collect` (#171; #128 sections 2.3 and 2.4).

`warehouse build`, `snapshot build`, the weekly export by its
manifest's age (#154), and `data prune`, each a child process: one that
fails is said, and the next run all the same; one that runs past its
time, or whose pass is given up on, is stopped with what it started.

Run here by a CLI of the test's own, which writes down how it was run
and does as it is told; `collector_process_test` runs the real one.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest
import structlog

from chatsbom.collector.index import export_due
from chatsbom.collector.index import EXPORT_EVERY
from chatsbom.collector.index import IndexPass
from chatsbom.collector.index import IndexRun
from chatsbom.core.config import PathConfig

#: The CLI a pass runs, here: it logs its argv and pid, then does what
#: `steps.json` says of its step: exit with a status, sleep first, start
#: a child of its own, write a file aside as it sleeps, which it removes
#: when interrupted as the CLI's writers do, or ignore an interrupt.
FAKE_CLI = r'''
import json, os, signal, subprocess, sys, time
from pathlib import Path

HERE = Path(__file__).resolve().parent
step = ' '.join(sys.argv[1:3])
told = json.loads((HERE / 'steps.json').read_text()).get(step, {})
with open(HERE / 'ran.log', 'a') as log:
    log.write(json.dumps({'argv': sys.argv[1:], 'pid': os.getpid()}) + '\n')
if told.get('ignore_interrupt'):
    signal.signal(signal.SIGINT, signal.SIG_IGN)
if told.get('child'):
    child = subprocess.Popen(['sleep', '60'])
    (HERE / 'child.pid').write_text(str(child.pid))
aside = HERE / '.written.tmp'
if told.get('writes'):
    aside.write_text('half')
try:
    time.sleep(told.get('sleep', 0))
except KeyboardInterrupt:
    aside.unlink(missing_ok=True)
    sys.exit(130)
sys.exit(told.get('exit', 0))
'''


class Stand:
    """The store, and the CLI of the test's own."""

    def __init__(self, root: Path) -> None:
        self.root = root
        self.paths = PathConfig(base_data_dir=root / 'data')
        self.paths.base_data_dir.mkdir()
        self.bin = root / 'bin'
        self.bin.mkdir()
        (self.bin / 'cli.py').write_text(FAKE_CLI)
        self.tell({})

    def tell(self, steps: dict[str, dict[str, Any]]) -> None:
        (self.bin / 'steps.json').write_text(json.dumps(steps))

    def ran(self) -> list[list[str]]:
        log = self.bin / 'ran.log'
        if not log.exists():
            return []
        return [
            json.loads(line)['argv'] for line in log.read_text().splitlines()
        ]

    def pids(self) -> list[int]:
        log = self.bin / 'ran.log'
        return [
            json.loads(line)['pid'] for line in log.read_text().splitlines()
        ]

    def index(self, **options: Any) -> IndexPass:
        return IndexPass(
            self.paths, cli=(sys.executable, str(self.bin / 'cli.py')),
            **options,
        )

    def warehouse(self) -> None:
        self.paths.warehouse_path.write_bytes(b'')

    def exported(self, age: timedelta) -> None:
        manifest = self.paths.base_data_dir / 'export' / 'manifest.json'
        manifest.parent.mkdir(parents=True, exist_ok=True)
        manifest.write_text('{}')
        when = time.time() - age.total_seconds()
        os.utime(manifest, (when, when))


@pytest.fixture
def stand(tmp_path: Path) -> Stand:
    return Stand(tmp_path)


def run(index: IndexPass) -> IndexRun:
    return asyncio.run(index.run())


def alive(pid: int) -> bool:
    """Whether `pid` runs: not gone, and not a zombie left to reap."""
    try:
        stat = Path(f'/proc/{pid}/stat').read_text()
    except OSError:
        return False
    return stat.rpartition(')')[2].split()[0] != 'Z'


def now() -> datetime:
    return datetime.now(timezone.utc)


class TestTheSteps:
    def test_are_the_warehouse_the_snapshot_and_prune_in_order(self, stand):
        """With no warehouse there is nothing to export."""
        done = run(stand.index())

        assert stand.ran() == [
            ['warehouse', 'build'],
            ['snapshot', 'build'],
            ['data', 'prune', '--keep', '2', '--apply'],
        ]
        assert [step.step for step in done.ran] == [
            'warehouse build', 'snapshot build', 'data prune',
        ]
        assert done.failed == []

    def test_export_the_warehouse_when_the_last_export_is_a_week_old(
        self, stand,
    ):
        stand.warehouse()
        stand.exported(EXPORT_EVERY)

        run(stand.index())

        assert stand.ran()[2] == [
            'export', 'parquet', '--output',
            str(stand.paths.base_data_dir / 'export'),
        ]

    def test_run_in_the_process_s_own_environment(self, stand, monkeypatch):
        """CHATSBOM_DUCKDB_MEMORY_LIMIT and the rest reach DuckDB."""
        monkeypatch.setenv('CHATSBOM_DUCKDB_THREADS', '1')
        stand.tell({'warehouse build': {'exit': 0}})
        env_probe = stand.bin / 'cli.py'
        env_probe.write_text(
            FAKE_CLI.replace(
                "'pid': os.getpid()",
                "'pid': os.getpid(), "
                "'threads': os.environ.get('CHATSBOM_DUCKDB_THREADS')",
            ),
        )
        run(stand.index())
        said = [
            json.loads(line)
            for line in (stand.bin / 'ran.log').read_text().splitlines()
        ]
        assert {entry['threads'] for entry in said} == {'1'}


class TestTheExport:
    def test_is_due_with_none_once_there_is_a_warehouse(self, stand):
        assert export_due(stand.paths, now()) is False
        stand.warehouse()
        assert export_due(stand.paths, now()) is True

    def test_is_due_by_the_age_of_its_manifest(self, stand):
        """Not by passes counted since a start: a collector started more
        often than weekly would never export (#154)."""
        stand.warehouse()
        stand.exported(EXPORT_EVERY - timedelta(minutes=1))
        assert export_due(stand.paths, now()) is False
        assert export_due(stand.paths, now() + timedelta(minutes=2)) is True
        assert EXPORT_EVERY == timedelta(days=7)


class TestAStepThatFails:
    def test_is_said_and_the_next_steps_run(self, stand):
        """A warehouse build that crashed leaves the last warehouse, and a
        snapshot of that is the snapshot already published."""
        stand.tell({'warehouse build': {'exit': 1}})

        with structlog.testing.capture_logs() as logs:
            done = run(stand.index())

        assert len(stand.ran()) == 3
        assert done.failed == ['warehouse build']
        [warned] = [
            log for log in logs if log['event'].startswith('An index step')
        ]
        assert warned['step'] == 'warehouse build'
        assert warned['status'] == 1
        [summary] = [log for log in logs if log['event'] == 'Index pass']
        assert summary['steps'] == (
            'warehouse-build:exit-1 snapshot-build:ok data-prune:ok'
        )
        assert summary['failed'] == 1

    def test_is_stopped_past_its_time_with_what_it_started(self, stand):
        stand.tell({
            'snapshot build': {'sleep': 60, 'child': True},
        })

        started = time.monotonic()
        done = run(
            stand.index(step_timeout=timedelta(seconds=1), kill_after=1),
        )

        assert time.monotonic() - started < 20
        assert [(step.step, step.status) for step in done.ran] == [
            ('warehouse build', 0), ('snapshot build', None),
            ('data prune', 0),
        ]
        child = int((stand.bin / 'child.pid').read_text())
        assert [pid for pid in (*stand.pids(), child) if alive(pid)] == []

    def test_is_interrupted_and_leaves_nothing_written_aside(self, stand):
        """As Ctrl-C would: the CLI's writers remove what they wrote
        aside, which a TERM, ending it where it stood, left behind."""
        stand.tell({'export parquet': {'sleep': 60, 'writes': True}})
        stand.warehouse()

        done = run(
            stand.index(step_timeout=timedelta(seconds=1), kill_after=10),
        )

        assert [(step.step, step.status) for step in done.ran][1:3] == [
            ('snapshot build', 0), ('export parquet', None),
        ]
        assert not (stand.bin / '.written.tmp').exists()

    def test_one_that_will_not_stop_is_killed(self, stand):
        stand.tell({
            'warehouse build': {'sleep': 60, 'ignore_interrupt': True},
        })

        started = time.monotonic()
        done = run(
            stand.index(step_timeout=timedelta(seconds=1), kill_after=1),
        )

        assert time.monotonic() - started < 20
        assert done.ran[0].status is None
        assert [pid for pid in stand.pids() if alive(pid)] == []


class TestAPassGivenUpOn:
    def test_stops_its_step_with_what_it_started(self, stand):
        """As the collector stops: the step is told to stop, and killed
        once it has had its time, and the pass goes no further."""
        stand.tell({'warehouse build': {'sleep': 60, 'child': True}})
        child_pid = stand.bin / 'child.pid'

        async def giving_up() -> float:
            index = stand.index(kill_after=1)
            running = asyncio.ensure_future(index.run())
            while not child_pid.exists():
                await asyncio.sleep(0.02)
            started = time.monotonic()
            running.cancel()
            with pytest.raises(asyncio.CancelledError):
                await running
            return time.monotonic() - started

        with structlog.testing.capture_logs() as logs:
            assert asyncio.run(giving_up()) < 5
        assert stand.ran() == [['warehouse', 'build']]
        child = int(child_pid.read_text())
        assert [pid for pid in (*stand.pids(), child) if alive(pid)] == []
        # Its line all the same, which says where it stopped.
        [summary] = [log for log in logs if log['event'] == 'Index pass']
        assert summary['steps'] == 'warehouse-build:stopped'
        assert summary['stopped'] is True


class TestWhenTheLastPassWas:
    def test_is_when_the_warehouse_was_built(self, stand):
        index = stand.index()
        assert index.last_at() is None
        stand.warehouse()
        built = datetime.fromtimestamp(
            stand.paths.warehouse_path.stat().st_mtime, timezone.utc,
        )
        assert index.last_at() == built
