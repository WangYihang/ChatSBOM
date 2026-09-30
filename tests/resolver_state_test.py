"""resolver.sqlite: the resolver's failures, and when each is tried again
(#168).

Kept as collector.sqlite is, on the same machinery
(`collector.state.StateFile`): one process writes it, the resolver; a
second is refused. It is never what says a directory is resolved, which
is a lockfile in the store; so deleting it loses the backoff and nothing
else (resolver_due_test).
"""
import os
import sqlite3
from dataclasses import replace
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path

import pytest

from chatsbom.collector.state import CollectorState
from chatsbom.collector.state import FAILED
from chatsbom.collector.state import Foreign
from chatsbom.collector.state import InUse
from chatsbom.collector.state import NOTHING
from chatsbom.collector.state import TooNew
from chatsbom.core.sandbox import LOCK_RECIPES
from chatsbom.core.sandbox import LockTarget
from chatsbom.resolver import state as resolver_state
from chatsbom.resolver.state import ResolverState

UTC = timezone.utc
NOW = datetime(2026, 9, 30, 12, 0, tzinfo=UTC)
S1 = '1' * 40
S2 = '2' * 40
BACKEND = LockTarget('backend', 'composer', LOCK_RECIPES['composer'])
ROOT = LockTarget('', 'gem', LOCK_RECIPES['gem'])


@pytest.fixture
def path(tmp_path: Path) -> Path:
    return resolver_state.state_path(tmp_path / 'data')


def test_is_resolver_sqlite_in_data():
    assert resolver_state.state_path(Path('data')) == (
        Path('data') / 'resolver.sqlite'
    )


def test_is_made_in_wal_as_a_file_of_its_own_kind(path):
    with ResolverState.open(path) as state:
        assert state.version == len(ResolverState.MIGRATIONS)
    db = sqlite3.connect(path)
    try:
        assert db.execute('PRAGMA journal_mode').fetchone()[0] == 'wal'
        assert db.execute('PRAGMA application_id').fetchone()[0] == (
            ResolverState.APPLICATION_ID
        )
    finally:
        db.close()
    assert ResolverState.APPLICATION_ID != CollectorState.APPLICATION_ID


def test_neither_file_is_taken_for_the_other(tmp_path):
    """collector.sqlite named where resolver.sqlite should be, or the
    other way round: refused, and left as it was."""
    collector = tmp_path / 'collector.sqlite'
    resolver = tmp_path / 'resolver.sqlite'
    CollectorState.open(collector).close()
    ResolverState.open(resolver).close()
    before = collector.read_bytes(), resolver.read_bytes()

    with pytest.raises(Foreign, match='is not a resolver.sqlite'):
        ResolverState.open(collector)
    with pytest.raises(Foreign, match='is not a collector.sqlite'):
        CollectorState.open(resolver)

    assert (collector.read_bytes(), resolver.read_bytes()) == before


def test_one_process_writes_it(path):
    with ResolverState.open(path):
        with pytest.raises(InUse) as refused:
            ResolverState.open(path)
    said = str(refused.value)
    assert 'another resolver' in said
    assert f'pid {os.getpid()}' in said


def test_a_later_file_is_refused_untouched(path):
    def later(db: sqlite3.Connection) -> None:
        db.execute('CREATE TABLE later (one INTEGER)')

    with ResolverState.open(
        path, migrations=(*ResolverState.MIGRATIONS, later),
    ):
        pass
    before = path.read_bytes()

    with pytest.raises(TooNew) as refused:
        ResolverState.open(path)

    said = str(refused.value)
    assert 'a later resolver wrote' in said
    assert 'only the backoff' in said
    assert path.read_bytes() == before


def test_a_failure_backs_off_from_15_minutes_doubling_to_a_week(path):
    with ResolverState.open(path) as state:
        dues = []
        for attempt in range(12):
            now = NOW + timedelta(days=attempt)
            failure = state.failed(1, S1, BACKEND, FAILED, now=now)
            assert failure.attempts == attempt + 1
            dues.append(failure.due_at - now)
    assert dues[:3] == [
        timedelta(minutes=15), timedelta(minutes=30), timedelta(hours=1),
    ]
    assert dues[-1] == timedelta(days=7)
    assert max(dues) == timedelta(days=7)


def test_a_failure_is_the_directorys_of_its_commit_by_its_recipe(path):
    """Another directory, another commit or another recipe is not held
    back by it: a recipe whose image or script moved may resolve what
    the last one could not."""
    with ResolverState.open(path) as state:
        state.failed(1, S1, BACKEND, FAILED, now=NOW, detail='exit 1')

        held = state.failure(1, S1, BACKEND)
        assert held is not None
        assert held.backing_off(NOW)
        assert held.detail == 'exit 1'
        assert state.failure(1, S1, ROOT) is None
        assert state.failure(1, S2, BACKEND) is None
        assert state.failure(2, S1, BACKEND) is None

        composer = LOCK_RECIPES['composer']
        moved = replace(
            BACKEND, recipe=replace(composer, script=composer.script + ' -v'),
        )
        assert state.failure(1, S1, moved) is None


def test_a_directory_it_could_not_resolve_is_one_too(path):
    """A run that went well and left no lockfile: nothing to merge, and
    tried again as a failure is."""
    with ResolverState.open(path) as state:
        nothing = state.failed(1, S1, ROOT, NOTHING, now=NOW)
    assert nothing.kind == NOTHING
    assert nothing.due_at == NOW + timedelta(minutes=15)


def test_what_it_says_of_a_failure_is_kept_without_secrets(path):
    with ResolverState.open(path) as state:
        kept = state.failed(
            1, S1, ROOT, FAILED, now=NOW,
            detail='fetching https://user:hunter2@gems.example/ failed',
        )
    assert 'hunter2' not in kept.detail


def test_a_resolution_clears_the_failure(path):
    with ResolverState.open(path) as state:
        state.failed(1, S1, BACKEND, FAILED, now=NOW)
        state.resolved(1, S1, BACKEND)
        assert state.failure(1, S1, BACKEND) is None
        # And the next failure starts again from 15 minutes.
        again = state.failed(1, S1, BACKEND, FAILED, now=NOW)
    assert again.attempts == 1


def test_it_keeps_what_it_was_told_across_a_restart(path):
    with ResolverState.open(path) as state:
        state.failed(1, S1, BACKEND, FAILED, now=NOW, detail='exit 2')
    with ResolverState.open(path) as state:
        kept = state.failure(1, S1, BACKEND)
    assert kept is not None and kept.detail == 'exit 2'


def test_a_failure_nothing_asked_after_for_two_weeks_is_forgotten(path):
    """Its commit is no longer the repository's, or its directory was
    resolved since: a failure still failing is tried again weekly, and
    each try keeps it."""
    with ResolverState.open(path) as state:
        state.failed(1, S1, BACKEND, FAILED, now=NOW - timedelta(days=15))
        state.failed(1, S2, BACKEND, FAILED, now=NOW - timedelta(days=13))
        assert state.forget(now=NOW) == 1
        assert state.failure(1, S1, BACKEND) is None
        assert state.failure(1, S2, BACKEND) is not None
