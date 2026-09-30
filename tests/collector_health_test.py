"""Whether `chatsbom collect` is making progress (#171): the heartbeat
it writes, and the check compose's healthcheck runs of it.

The check fails when there is no heartbeat, when the last is old, the
process no longer running its loop, and when a part of it is wedged:
busy past its deadline, or idle past the time it was to wake. It
passes while every part works within its deadline or waits by choice.
"""
from __future__ import annotations

import json
import subprocess
import sys
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

from chatsbom.collector.health import check
from chatsbom.collector.health import HEARTBEAT
from chatsbom.collector.health import Heartbeat
from chatsbom.collector.health import heartbeat_path
from chatsbom.collector.health import LATE
from chatsbom.collector.health import main
from chatsbom.collector.health import STALE
from tests.fake_github_test import FakeClock
from tests.fake_github_test import START

NOW = datetime.fromtimestamp(START, timezone.utc)
HOUR = timedelta(hours=1)


def beat(tmp_path: Path) -> tuple[Heartbeat, FakeClock]:
    clock = FakeClock()
    return Heartbeat(heartbeat_path(tmp_path), clock), clock


def now_of(clock: FakeClock) -> datetime:
    return datetime.fromtimestamp(clock(), timezone.utc)


class TestTheHeartbeat:
    def test_is_in_the_data_directory(self, tmp_path):
        assert heartbeat_path(tmp_path) == tmp_path / HEARTBEAT
        assert HEARTBEAT == 'collector.heartbeat'

    def test_says_the_pid_when_it_was_written_and_each_part(self, tmp_path):
        heartbeat, clock = beat(tmp_path)
        heartbeat.idle('sweep', NOW + HOUR, 'the next sweep')
        heartbeat.busy('collect 7', 3 * HOUR, 'octo/seven')
        heartbeat.write()

        said = json.loads(heartbeat.path.read_text())

        assert said['at'] == '2026-09-21T14:13:20Z'
        assert said['stopped'] is False
        assert said['stalled'] == []
        assert said['parts'] == {
            'collect 7': {
                'state': 'busy', 'since': '2026-09-21T14:13:20Z',
                'until': '2026-09-21T17:13:20Z', 'doing': 'octo/seven',
            },
            'sweep': {
                'state': 'idle', 'since': '2026-09-21T14:13:20Z',
                'until': '2026-09-21T15:13:20Z', 'doing': 'the next sweep',
            },
        }
        assert isinstance(said['pid'], int)
        assert check(heartbeat.path, now_of(clock)) == ''

    def test_leaves_nothing_beside_it(self, tmp_path):
        heartbeat, _ = beat(tmp_path)
        heartbeat.write()
        heartbeat.write()
        assert sorted(path.name for path in tmp_path.iterdir()) == [HEARTBEAT]


class TestTheCheck:
    def test_fails_with_no_heartbeat(self, tmp_path):
        said = check(heartbeat_path(tmp_path), NOW)
        assert 'no heartbeat' in said

    def test_fails_on_one_it_cannot_read(self, tmp_path):
        heartbeat_path(tmp_path).write_text('{')
        assert 'cannot be read' in check(heartbeat_path(tmp_path), NOW)

    def test_fails_once_the_loop_stops_writing_it(self, tmp_path):
        """The process blocked, wedged or gone: nothing writes it."""
        heartbeat, clock = beat(tmp_path)
        heartbeat.idle('sweep', NOW + HOUR)
        heartbeat.write()

        assert check(heartbeat.path, NOW + STALE) == ''
        said = check(heartbeat.path, NOW + STALE + timedelta(seconds=1))
        assert 'is not running its loop' in said

    def test_fails_when_a_part_is_busy_past_its_deadline(self, tmp_path):
        """A collection that hangs: the loop runs on, and writes the
        heartbeat, and makes no progress."""
        heartbeat, clock = beat(tmp_path)
        heartbeat.busy('collect 7', 3 * HOUR, 'octo/seven')
        heartbeat.idle('sweep', NOW + 4 * HOUR)
        clock.advance((3 * HOUR).total_seconds())
        heartbeat.write()
        assert check(heartbeat.path, now_of(clock)) == ''

        clock.advance(1)
        heartbeat.write()
        said = check(heartbeat.path, now_of(clock))
        assert said.startswith('stalled: collect 7: busy since')
        assert 'octo/seven' in said

    def test_fails_when_a_part_does_not_wake(self, tmp_path):
        heartbeat, clock = beat(tmp_path)
        heartbeat.idle('sweep', NOW + HOUR, 'the next sweep')
        clock.advance((HOUR + LATE).total_seconds())
        heartbeat.write()
        assert check(heartbeat.path, now_of(clock)) == ''

        clock.advance(1)
        heartbeat.write()
        assert 'stalled: sweep: idle' in check(heartbeat.path, now_of(clock))

    def test_passes_a_part_waiting_to_be_woken_for_as_long_as_it_takes(
        self, tmp_path,
    ):
        heartbeat, clock = beat(tmp_path)
        heartbeat.idle('index', None, 'something to index')
        clock.advance(30 * 86_400)
        heartbeat.write()
        assert check(heartbeat.path, now_of(clock)) == ''

    def test_fails_once_the_process_has_stopped(self, tmp_path):
        heartbeat, clock = beat(tmp_path)
        heartbeat.write(stopped=True)
        said = check(heartbeat.path, now_of(clock))
        assert said.startswith('the collector (pid ')
        assert said.endswith('stopped at 2026-09-21T14:13:20Z')

    def test_passes_a_part_done(self, tmp_path):
        heartbeat, clock = beat(tmp_path)
        heartbeat.busy('collect 7', HOUR)
        heartbeat.done('collect 7')
        clock.advance(2 * HOUR.total_seconds())
        heartbeat.write()
        assert check(heartbeat.path, now_of(clock)) == ''


class TestTheCommand:
    """`python -m chatsbom.collector.health`, as compose runs it."""

    def test_exits_0_while_healthy(self, tmp_path, capsys):
        heartbeat = Heartbeat(heartbeat_path(tmp_path), __import__('time').time)
        heartbeat.idle('sweep', None)
        heartbeat.write()
        assert main([str(tmp_path)]) == 0
        assert capsys.readouterr().out == 'healthy\n'

    def test_exits_1_and_says_why_when_not(self, tmp_path, capsys):
        assert main([str(tmp_path)]) == 1
        assert capsys.readouterr().out.startswith('unhealthy: no heartbeat')

    def test_reads_data_in_the_working_directory_unless_told(self, tmp_path):
        """As compose's healthcheck runs it: from the image's working
        directory, where data/ is mounted, and with nothing else."""
        data = tmp_path / 'data'
        data.mkdir()
        heartbeat = Heartbeat(heartbeat_path(data), __import__('time').time)
        heartbeat.idle('sweep', None)
        heartbeat.write()

        result = subprocess.run(
            [sys.executable, '-m', 'chatsbom.collector.health'],
            cwd=tmp_path, capture_output=True, text=True, check=False,
        )

        assert (result.returncode, result.stdout) == (0, 'healthy\n')

    def test_starts_without_the_collector_s_libraries(self, tmp_path):
        """A check every minute: it imports the standard library alone."""
        result = subprocess.run(
            [
                sys.executable, '-c',
                'import sys, chatsbom.collector.health; '
                'print(sorted(m for m in sys.modules if m.startswith('
                '("chatsbom", "httpx2", "structlog", "typer", "pydantic"))))',
            ],
            cwd=tmp_path, capture_output=True, text=True, check=True,
        )
        assert result.stdout.strip() == (
            "['chatsbom', 'chatsbom.collector', 'chatsbom.collector.health']"
        )
