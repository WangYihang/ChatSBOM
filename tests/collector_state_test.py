"""collector.sqlite: what the collector keeps between runs, and never
what is done (#156).

It holds what saves requests and what paces them: each repository as it
was last observed, REST validators, `nothing` and failure outcomes with
their backoff (#100 Q5), and the dependency graph's pending reports. The
store is what is done; this file is never read for it, and deleting it
costs requests (collector_client_test, TestADeletedState), not results.

One process writes it, and a second is refused, by a lock the kernel
frees when its holder dies. A later schema brings an earlier file
forward, and a file from a later one is refused, untouched.
"""
import os
import signal
import sqlite3
import subprocess
import sys
import textwrap
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest

from chatsbom.collector.state import APPLICATION_ID
from chatsbom.collector.state import backoff
from chatsbom.collector.state import CollectorState
from chatsbom.collector.state import FAILED
from chatsbom.collector.state import Foreign
from chatsbom.collector.state import InUse
from chatsbom.collector.state import MIGRATIONS
from chatsbom.collector.state import NOTHING
from chatsbom.collector.state import Observed
from chatsbom.collector.state import Outcome
from chatsbom.collector.state import PendingReport
from chatsbom.collector.state import SCHEMA_VERSION
from chatsbom.collector.state import STATE_FILE
from chatsbom.collector.state import state_path
from chatsbom.collector.state import StateError
from chatsbom.collector.state import TooNew
from chatsbom.collector.state import Validators

ROOT = Path(__file__).resolve().parent.parent

NOW = datetime(2026, 9, 30, 12, 0, tzinfo=timezone.utc)


@pytest.fixture
def path(tmp_path: Path) -> Path:
    return tmp_path / 'data' / STATE_FILE


OBSERVED = Observed(
    repository_id=1,
    node_id='R_kgDO00000001',
    full_name='octo/one',
    stars=5_000,
    archived=False,
    pushed_at=datetime(2026, 9, 29, 8, 30, tzinfo=timezone.utc),
    default_branch='main',
    head='a' * 40,
    release_tag='v2.0',
    release_at=datetime(2026, 9, 10, tzinfo=timezone.utc),
    observed_at=NOW,
)


def observed(**changes: Any) -> Observed:
    return replace(OBSERVED, **changes)


def in_another_process(
    code: str, *, timeout: float = 60,
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, '-c', textwrap.dedent(code)],
        capture_output=True, text=True, timeout=timeout, cwd=ROOT,
    )


class TestTheFile:
    def test_is_collector_sqlite_in_data(self):
        assert state_path(Path('data')) == Path('data') / 'collector.sqlite'

    def test_is_made_in_wal_at_the_current_version(self, path):
        with CollectorState.open(path) as state:
            assert state.path == path
            assert state.version == SCHEMA_VERSION == len(MIGRATIONS) == 1
        db = sqlite3.connect(path)
        try:
            assert db.execute('PRAGMA journal_mode').fetchone()[0] == 'wal'
            assert db.execute('PRAGMA user_version').fetchone()[0] == 1
            assert db.execute(
                'PRAGMA application_id',
            ).fetchone()[0] == APPLICATION_ID
        finally:
            db.close()

    def test_a_database_of_something_else_is_refused_untouched(self, path):
        """The ledger, say, named by mistake: nothing is added to it."""
        path.parent.mkdir(parents=True)
        db = sqlite3.connect(path)
        db.execute('CREATE TABLE repository_state (repository_id INTEGER)')
        db.commit()
        db.close()
        before = path.read_bytes()

        with pytest.raises(Foreign) as refused:
            CollectorState.open(path)

        assert str(path) in str(refused.value)
        assert path.read_bytes() == before

    def test_a_file_that_is_not_sqlite_is_refused(self, path):
        path.parent.mkdir(parents=True)
        path.write_bytes(b'not a database, but long enough to be read' * 50)
        with pytest.raises(StateError):
            CollectorState.open(path)


class TestWhatItKeeps:
    def test_a_repository_as_last_observed(self, path):
        with CollectorState.open(path) as state:
            first = observed()
            assert state.observe(first) is None
            later = observed(
                full_name='octo/uno', stars=5_001, head='b' * 40,
                observed_at=NOW + timedelta(hours=1),
            )
            # What it held before, for the caller to compare.
            assert state.observe(later) == first
            assert state.observed(1) == later
            assert state.observed_node('R_kgDO00000001') == later
            assert state.observed(2) is None
            assert list(state.observations()) == [later]

    def test_a_repository_with_nothing_but_its_names(self, path):
        bare = Observed(
            repository_id=7, node_id='R_kgDO00000007', full_name='octo/new',
            stars=None, archived=None, pushed_at=None, default_branch=None,
            head=None, release_tag=None, release_at=None, observed_at=NOW,
        )
        with CollectorState.open(path) as state:
            state.observe(bare)
            assert state.observed(7) == bare

    def test_validators_by_request(self, path):
        request = 'GET /repos/octo/one application/vnd.github+json'
        with CollectorState.open(path) as state:
            assert state.validators(request) is None
            state.keep_validators(request, Validators('W/"1"', None), NOW)
            assert state.validators(request) == Validators('W/"1"', None)
            state.keep_validators(
                request,
                Validators(None, 'Wed, 30 Sep 2026 00:00:00 GMT'),
                NOW,
            )
            assert state.validators(request) == Validators(
                None, 'Wed, 30 Sep 2026 00:00:00 GMT',
            )
            state.drop_validators(request)
            assert state.validators(request) is None

    def test_an_outcome_backs_off_more_each_time(self, path):
        with CollectorState.open(path) as state:
            first = state.record(
                1, 'release', 'P=2026-09-29', NOTHING, now=NOW,
            )
            assert first == Outcome(
                repository_id=1, stage='release', key='P=2026-09-29',
                kind=NOTHING, attempts=1, due_at=NOW + backoff(1),
                detail='', first_at=NOW, last_at=NOW,
            )
            later = NOW + timedelta(hours=1)
            second = state.record(
                1, 'release', 'P=2026-09-29', FAILED, now=later,
                detail='HTTP 502',
            )
            assert second.attempts == 2
            assert second.kind == FAILED
            assert second.due_at == later + backoff(2)
            assert second.first_at == NOW
            assert second.detail == 'HTTP 502'
            assert state.outcome(1, 'release', 'P=2026-09-29') == second
            assert second.backing_off(later) is True
            assert second.backing_off(second.due_at) is False

    def test_an_outcome_may_say_when_it_is_due(self, path):
        with CollectorState.open(path) as state:
            kept = state.record(
                1, 'depgraph', 'head:abc', NOTHING, now=NOW,
                delay=timedelta(days=30),
            )
            assert kept.due_at == NOW + timedelta(days=30)

    def test_outcomes_are_per_repository_stage_and_key(self, path):
        with CollectorState.open(path) as state:
            state.record(1, 'release', 'P=1', NOTHING, now=NOW)
            state.record(1, 'release', 'P=2', FAILED, now=NOW)
            state.record(1, 'tree', 'abc', FAILED, now=NOW)
            state.record(2, 'release', 'P=1', NOTHING, now=NOW)
            assert state.outcome(1, 'release', 'P=3') is None
            assert [
                (o.repository_id, o.key) for o in state.outcomes('release')
            ] == [(1, 'P=1'), (1, 'P=2'), (2, 'P=1')]
            assert len(list(state.outcomes())) == 4
            state.clear(1, 'release', 'P=1')
            assert state.outcome(1, 'release', 'P=1') is None
            state.clear(1, 'release')
            assert [(o.repository_id, o.stage) for o in state.outcomes()] == [
                (1, 'tree'), (2, 'release'),
            ]

    def test_backoff_doubles_from_15_minutes_to_a_week(self):
        assert backoff(1) == timedelta(minutes=15)
        assert backoff(2) == timedelta(minutes=30)
        assert backoff(5) == timedelta(hours=4)
        assert backoff(20) == timedelta(days=7)
        assert backoff(10_000) == timedelta(days=7)

    def test_what_an_outcome_says_is_kept_without_secrets(self, path):
        detail = (
            'GET https://example.com/r.json?X-Amz-Signature=5ec7e7: '
            "header 'Bearer ghp_n0tk3pt0000000000000000'"
        )
        with CollectorState.open(path) as state:
            kept = state.record(
                1, 'depgraph', 'k', FAILED,
                now=NOW, detail=detail,
            )
        assert '5ec7e7' not in kept.detail
        assert 'ghp_n0tk3pt' not in kept.detail
        assert b'ghp_n0tk3pt' not in path.read_bytes()

    def test_a_pending_report_until_it_is_dropped(self, path):
        url = 'https://api.github.com/repos/octo/one/dependency-graph/sbom/fetch-report/u-1'
        with CollectorState.open(path) as state:
            kept = state.pend_report(
                1, url, head='a' * 40, now=NOW,
                due_at=NOW + timedelta(seconds=2),
            )
            assert kept == PendingReport(
                repository_id=1, url=url, head='a' * 40, requested_at=NOW,
                attempts=0, due_at=NOW + timedelta(seconds=2),
            )
            state.pend_report(
                2, url.replace('u-1', 'u-2'), head=None, now=NOW,
                due_at=NOW + timedelta(minutes=5),
            )
            assert state.report(1) == kept
            assert [r.repository_id for r in state.reports_due(NOW)] == []
            soon = NOW + timedelta(seconds=2)
            assert [r.repository_id for r in state.reports_due(soon)] == [1]
            polled = state.polled(1, due_at=soon + timedelta(seconds=4))
            assert polled.attempts == 1
            assert polled.due_at == soon + timedelta(seconds=4)
            state.drop_report(1)
            assert state.report(1) is None
            assert [
                r.repository_id for r in state.reports_due(
                    NOW + timedelta(days=1),
                )
            ] == [2]

    def test_every_pending_report_due_or_not(self, path):
        """What the dependency graph has in flight, which it asks for no
        more of (#162)."""
        url = 'https://api.github.com/repos/octo/one/dependency-graph/sbom/fetch-report/u-1'
        with CollectorState.open(path) as state:
            assert state.reports() == []
            later = state.pend_report(
                1, url, head=None, now=NOW,
                due_at=NOW + timedelta(minutes=5),
            )
            sooner = state.pend_report(
                2, url.replace('u-1', 'u-2'), head='b' * 40, now=NOW,
                due_at=NOW + timedelta(seconds=2),
            )
            assert state.reports_due(NOW) == []
            assert state.reports() == [sooner, later]

    def test_many_writes_as_one(self, path):
        with CollectorState.open(path) as state:
            with pytest.raises(RuntimeError):
                with state.transaction():
                    state.record(1, 'tree', 'k', FAILED, now=NOW)
                    raise RuntimeError('gone before the commit')
            assert state.outcome(1, 'tree', 'k') is None
            with state.transaction():
                state.record(1, 'tree', 'k', FAILED, now=NOW)
                state.observe(observed())
            assert state.outcome(1, 'tree', 'k') is not None


class TestARestart:
    def test_everything_kept_is_there_when_it_opens_again(self, path):
        request = 'GET /repos/octo/one application/vnd.github+json'
        url = 'https://api.github.com/repos/octo/one/dependency-graph/sbom/fetch-report/u-1'
        with CollectorState.open(path) as state:
            state.observe(observed())
            state.keep_validators(request, Validators('W/"1"', None), NOW)
            outcome = state.record(1, 'release', 'P=1', FAILED, now=NOW)
            report = state.pend_report(1, url, head=None, now=NOW, due_at=NOW)

        with CollectorState.open(path) as state:
            assert state.observed(1) == observed()
            assert state.validators(request) == Validators('W/"1"', None)
            assert state.outcome(1, 'release', 'P=1') == outcome
            assert state.report(1) == report


class TestMigrations:
    @staticmethod
    def v2(db: sqlite3.Connection) -> None:
        """A later schema's step: a column added, and filled in from
        what the earlier one kept."""
        db.execute('ALTER TABLE repository ADD COLUMN owner TEXT')
        db.execute(
            'UPDATE repository SET owner = substr(full_name, 1, '
            "instr(full_name, '/') - 1)",
        )

    def test_an_older_file_is_brought_forward(self, path):
        with CollectorState.open(path) as state:
            state.observe(observed())
            state.record(1, 'release', 'P=1', FAILED, now=NOW)

        with CollectorState.open(path, migrations=(*MIGRATIONS, self.v2)) as state:
            assert state.version == 2
            assert state.observed(1) == observed()
            assert state.outcome(1, 'release', 'P=1') is not None
        db = sqlite3.connect(path)
        try:
            assert db.execute(
                'SELECT owner FROM repository',
            ).fetchall() == [('octo',)]
            assert db.execute('PRAGMA user_version').fetchone()[0] == 2
        finally:
            db.close()

    def test_a_newer_file_is_refused_untouched(self, path):
        with CollectorState.open(path, migrations=(*MIGRATIONS, self.v2)):
            pass
        before = path.read_bytes()

        with pytest.raises(TooNew) as refused:
            CollectorState.open(path)

        message = str(refused.value)
        assert 'version 2' in message and 'version 1' in message
        assert path.read_bytes() == before
        # Refused, it holds no lock: the newer collector may open it.
        with CollectorState.open(path, migrations=(*MIGRATIONS, self.v2)):
            pass

    def test_a_step_that_fails_leaves_the_file_as_it_was(self, path):
        with CollectorState.open(path) as state:
            state.observe(observed())

        def broken(db: sqlite3.Connection) -> None:
            db.execute('ALTER TABLE repository ADD COLUMN owner TEXT')
            raise sqlite3.OperationalError('the step fails half-way')

        with pytest.raises(StateError):
            CollectorState.open(path, migrations=(*MIGRATIONS, broken))

        with CollectorState.open(path) as state:
            assert state.version == 1
            assert state.observed(1) == observed()
        db = sqlite3.connect(path)
        try:
            columns = [
                row[1] for row in db.execute(
                    'PRAGMA table_info(repository)',
                )
            ]
        finally:
            db.close()
        assert 'owner' not in columns


class TestTheLock:
    def test_a_second_open_is_refused(self, path):
        with CollectorState.open(path):
            with pytest.raises(InUse) as refused:
                CollectorState.open(path)
        assert str(os.getpid()) in str(refused.value)

    def test_closing_lets_it_go(self, path):
        CollectorState.open(path).close()
        CollectorState.open(path).close()

    def test_a_second_process_is_refused(self, path):
        with CollectorState.open(path):
            other = in_another_process(f"""
                from chatsbom.collector.state import CollectorState, InUse
                try:
                    CollectorState.open({str(path)!r})
                except InUse as refused:
                    print('refused:', refused)
                    raise SystemExit(3)
                """)
        assert other.returncode == 3, other.stderr
        assert f'pid {os.getpid()}' in other.stdout

    def test_the_kernel_lets_it_go_when_its_holder_dies(self, path):
        holder = textwrap.dedent(f"""
            import sys, time
            from datetime import datetime, timezone
            from chatsbom.collector.state import CollectorState, FAILED
            state = CollectorState.open({str(path)!r})
            state.record(
                1, 'tree', 'k', FAILED, now=datetime.now(timezone.utc),
            )
            print('held', flush=True)
            time.sleep(120)
            """)
        with subprocess.Popen(
            [sys.executable, '-c', holder], stdout=subprocess.PIPE,
            text=True, cwd=ROOT,
        ) as child:
            assert child.stdout is not None
            assert child.stdout.readline().strip() == 'held'
            with pytest.raises(InUse):
                CollectorState.open(path)
            child.send_signal(signal.SIGKILL)
            child.wait(timeout=60)

        with CollectorState.open(path) as state:
            # And what it wrote before it died is kept.
            assert state.outcome(1, 'tree', 'k') is not None

    def test_a_process_it_starts_does_not_hold_it(self, path):
        """What the collector runs, Syft among it, may outlive it, and
        must not keep the next collector out."""
        holder = textwrap.dedent(f"""
            import subprocess, sys, time
            from chatsbom.collector.state import CollectorState
            state = CollectorState.open({str(path)!r})
            child = subprocess.Popen(
                [sys.executable, '-c', 'import time; time.sleep(120)'],
            )
            print(child.pid, flush=True)
            time.sleep(120)
            """)
        with subprocess.Popen(
            [sys.executable, '-c', holder], stdout=subprocess.PIPE,
            text=True, cwd=ROOT,
        ) as parent:
            assert parent.stdout is not None
            grandchild = int(parent.stdout.readline())
            try:
                parent.send_signal(signal.SIGKILL)
                parent.wait(timeout=60)
                CollectorState.open(path).close()
            finally:
                os.kill(grandchild, signal.SIGKILL)
