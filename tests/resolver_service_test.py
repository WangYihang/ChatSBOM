"""The resolver as a service (#168): a pass over what is due, and the
loop that runs one after another and sleeps while nothing is due.

A pass resolves the directories due (resolver_due_test), `workers` at
once, and keeps what became of each in resolver.sqlite: a failure with
its backoff, a result by forgetting the failure. What the sandbox could
not run, or was told to stop, is not the project's doing, and nothing
is kept of it. These run a pass with the resolution stood in for; the
sandbox's own is sandbox_test's, and SIGTERM on a real process is
sbom_lock_test's.
"""
from __future__ import annotations

import threading
from datetime import timedelta
from pathlib import Path
from typing import Any

import pytest
from structlog.testing import capture_logs

from chatsbom.collector.settings import SettingsError
from chatsbom.collector.state import FAILED
from chatsbom.collector.state import NOTHING
from chatsbom.core.config import PathConfig
from chatsbom.core.fs import atomic_write_text
from chatsbom.core.sandbox import lock_recipe_for
from chatsbom.core.sandbox import LockResult
from chatsbom.core.sandbox import SandboxError
from chatsbom.core.sandbox import SandboxLimits
from chatsbom.resolver import service
from chatsbom.resolver.due import Walk
from chatsbom.resolver.service import Passed
from chatsbom.resolver.state import ResolverState
from chatsbom.resolver.state import state_path
from tests.resolver_due_test import collected
from tests.resolver_due_test import COMPOSER
from tests.resolver_due_test import GEMFILE
from tests.resolver_due_test import NOW
from tests.resolver_due_test import S1
from tests.resolver_due_test import searched
from tests.resolver_due_test import target


@pytest.fixture
def paths(tmp_path: Path) -> PathConfig:
    return PathConfig(base_data_dir=tmp_path / 'data')


@pytest.fixture
def state(paths: PathConfig) -> Any:
    with ResolverState.open(state_path(paths.base_data_dir)) as state:
        yield state


class Sandbox:
    """The sandbox as a pass uses it, stood in for: each resolution's
    result as the test plans it, by repository and directory; and what
    the pass asked of it."""

    def __init__(self) -> None:
        self.plans: dict[str, Any] = {}
        self.resolved: list[str] = []
        self.prepared: list[list[str]] = []
        self.swept = 0
        self.refuse: str | None = None

    def generate_lockfile(
        self, ecosystem: str, project_dir: Path, output_dir: Path,
        limits: SandboxLimits | None = None,
        cancel: threading.Event | None = None,
    ) -> LockResult:
        parts = project_dir.parts
        at = parts.index('06-github-content')
        where = '/'.join([parts[at + 1], *parts[at + 3:]])
        self.resolved.append(f'{where}:{ecosystem}')
        plan = self.plans.get(where, 'ok')
        if callable(plan):
            return plan(output_dir, cancel)
        if plan == 'ok':
            lock = output_dir / lock_recipe_for(ecosystem).produces[0]
            atomic_write_text(lock, 'resolved\n')
            return LockResult(produced=(lock,), returncode=0, stderr='')
        return plan

    def prepare(self, recipes: Any) -> None:
        self.prepared.append(sorted(recipe.manifest for recipe in recipes))
        if self.refuse:
            raise SandboxError(self.refuse)

    def sweep(self) -> None:
        self.swept += 1


@pytest.fixture
def sandbox(monkeypatch) -> Sandbox:
    fake = Sandbox()
    monkeypatch.setattr(service, 'generate_lockfile', fake.generate_lockfile)
    monkeypatch.setattr(service, 'prepare', fake.prepare)
    monkeypatch.setattr(service, 'sweep', fake.sweep)
    return fake


def run_pass(paths: PathConfig, state: ResolverState, **options: Any) -> Passed:
    options.setdefault('stop', threading.Event())
    options.setdefault('limits', SandboxLimits())
    options.setdefault('clock', lambda: NOW)
    return service.run_pass(paths, state, **options)


# -- a pass -------------------------------------------------------------------------


def test_a_pass_resolves_what_is_due_and_keeps_what_became_of_it(
    paths, state, sandbox,
):
    searched(paths, {1: 3000, 2: 2000, 3: 1000})
    for repository_id in (1, 2, 3):
        collected(paths, repository_id, COMPOSER)
    sandbox.plans['2'] = LockResult(
        produced=(), returncode=1, stderr='curl error 7 while downloading',
    )
    sandbox.plans['3'] = LockResult(produced=(), returncode=0, stderr='')

    passed = run_pass(paths, state)

    assert sandbox.resolved == ['1:composer', '2:composer', '3:composer']
    assert (passed.resolved, passed.failed) == (1, 2)
    assert passed.halted is None
    assert state.failure(1, S1, target()) is None
    failed = state.failure(2, S1, target())
    assert failed is not None and failed.kind == FAILED
    assert 'curl error 7' in failed.detail
    nothing = state.failure(3, S1, target())
    assert nothing is not None and nothing.kind == NOTHING
    # And the next pass finds each resolved, or backing off.
    again = run_pass(paths, state)
    assert again.walk.due == []
    assert (again.walk.resolved, again.walk.backing_off) == (1, 2)


def test_a_result_forgets_the_failure_before_it(paths, state, sandbox):
    searched(paths, {1: 3000})
    collected(paths, 1, COMPOSER)
    state.failed(1, S1, target(), FAILED, now=NOW - timedelta(hours=1))

    run_pass(paths, state)

    assert state.failure(1, S1, target()) is None


@pytest.mark.parametrize(
    'result', [
        LockResult((), 130, 'cancelled', cancelled=True),
        LockResult(
            (), 125, 'the egress proxy did not start',
            sandbox_failed=True,
        ),
    ],
    ids=['cancelled', 'the sandbox failed'],
)
def test_what_the_project_did_not_do_is_not_kept_against_it(
    paths, state, sandbox, result,
):
    searched(paths, {1: 3000})
    collected(paths, 1, COMPOSER)
    sandbox.plans['1'] = result

    passed = run_pass(paths, state)

    assert state.failure(1, S1, target()) is None
    assert (passed.resolved, passed.failed) == (0, 0)


def test_a_sandbox_that_fails_stops_the_pass(paths, state, sandbox):
    """Docker, a network or a proxy that cannot be set up: the next
    resolution would fail as this one did, for nothing the project did.
    What had not started is not started."""
    searched(paths, {1: 3000, 2: 2000, 3: 1000})
    for repository_id in (1, 2, 3):
        collected(paths, repository_id, COMPOSER)
    sandbox.plans['1'] = LockResult(
        (), 125, 'could not make a network for the resolution: no pool',
        sandbox_failed=True,
    )

    passed = run_pass(paths, state)

    assert sandbox.resolved == ['1:composer']
    assert passed.halted is not None and 'no pool' in passed.halted


def test_a_sandbox_that_cannot_be_prepared_resolves_nothing(
    paths, state, sandbox,
):
    searched(paths, {1: 3000})
    collected(paths, 1, COMPOSER)
    sandbox.refuse = 'pull access denied'

    passed = run_pass(paths, state)

    assert sandbox.resolved == []
    assert passed.halted == 'pull access denied'
    assert state.failure(1, S1, target()) is None


def test_the_sandbox_is_set_up_once_a_pass_and_only_when_needed(
    paths, state, sandbox,
):
    """Swept of what an earlier resolver left, and prepared for the
    recipes the pass runs; while nothing is due, Docker is not asked."""
    searched(paths, {1: 3000, 2: 2000})
    collected(paths, 1, {**COMPOSER, **GEMFILE})
    collected(paths, 2, COMPOSER)

    run_pass(paths, state)
    run_pass(paths, state)

    assert sandbox.swept == 1
    assert sandbox.prepared == [['Gemfile', 'composer.json']]


def test_what_the_proxy_refused_is_logged_with_who_asked(
    paths, state, sandbox,
):
    searched(paths, {1: 3000})
    collected(paths, 1, {'api/composer.json': '{}'})
    refused = {
        'event': 'refused', 'client': '10.0.0.2:40000', 'reason': 'host',
        'request': 'CONNECT github.com:443 HTTP/1.1',
        'detail': 'github.com is not a registry this resolution may reach',
    }
    sandbox.plans['1/api'] = LockResult(
        produced=(), returncode=1, stderr='', egress=(refused,),
    )

    with capture_logs() as logs:
        run_pass(paths, state)

    [logged] = [e for e in logs if e['event'] == 'Egress refused']
    assert logged['log_level'] == 'warning'
    assert (logged['repository'], logged['directory']) == ('octo/r1', 'api')
    assert logged['reason'] == 'host'
    assert logged['request'] == 'CONNECT github.com:443 HTTP/1.1'


def test_a_failed_resolution_leaves_no_directory_behind(
    paths, state, sandbox,
):
    searched(paths, {1: 3000})
    collected(paths, 1, {'api/composer.json': '{}'})

    def made_and_failed(output: Path, cancel: Any) -> LockResult:
        output.mkdir(parents=True)
        return LockResult(produced=(), returncode=1, stderr='')

    sandbox.plans['1/api'] = made_and_failed

    run_pass(paths, state)

    assert not (paths.generated_lock_path(1, S1) / 'api').exists()


def test_a_stop_resolves_nothing_more(paths, state, sandbox):
    """A resolution in flight is told (`cancel`), and ends as one cut
    short does, its container removed (sandbox_test); none after it
    starts, and nothing is kept of what was stopped."""
    searched(paths, {1: 3000, 2: 2000})
    for repository_id in (1, 2):
        collected(paths, repository_id, COMPOSER)
    stop = threading.Event()

    def stopped_while_running(output: Path, cancel: Any) -> LockResult:
        stop.set()
        assert cancel is stop
        return LockResult((), 130, 'cancelled', cancelled=True)

    sandbox.plans['1'] = stopped_while_running

    passed = run_pass(paths, state, stop=stop)

    assert sandbox.resolved == ['1:composer']
    assert state.failure(1, S1, target()) is None
    assert (passed.resolved, passed.failed) == (0, 0)


def test_failures_long_untried_are_forgotten(paths, state, sandbox):
    searched(paths, {1: 3000})
    state.failed(7, S1, target(), FAILED, now=NOW - timedelta(days=30))

    run_pass(paths, state)

    assert state.failure(7, S1, target()) is None


# -- the loop -------------------------------------------------------------------------


class Stopping(threading.Event):
    """A stop that never sleeps: it records each wait's length, and
    sets itself at the `last`."""

    def __init__(self, last: int) -> None:
        super().__init__()
        self.waits: list[float] = []
        self.last = last

    def wait(self, timeout: float | None = None) -> bool:
        self.waits.append(timeout or 0.0)
        if len(self.waits) >= self.last:
            self.set()
        return self.is_set()


def passes(*done: tuple[int, int, str | None]) -> Any:
    """Passes that each resolved and failed this many, or halted."""
    ran: list[int] = []
    planned = list(done)

    def run() -> Passed:
        ran.append(len(ran))
        resolved, failed, halted = planned.pop(0) if planned else (0, 0, None)
        return Passed(Walk(), resolved=resolved, failed=failed, halted=halted)

    run.ran = ran  # type: ignore[attr-defined]
    return run


HOUR = timedelta(hours=1)


def test_while_nothing_is_due_it_sleeps_the_interval(paths):
    run = passes()
    stop = Stopping(last=3)

    service.serve(run, interval=HOUR, stop=stop)

    assert stop.waits == [3600.0] * 3
    assert len(run.ran) == 3


def test_after_a_pass_that_did_something_it_goes_on_at_once(paths):
    """What it did may have been cut by `--limit`, or more may have
    become due meanwhile: the next pass says, and sleeps if nothing
    is."""
    run = passes((3, 1, None), (1, 0, None), (0, 0, None))
    stop = Stopping(last=1)

    service.serve(run, interval=HOUR, stop=stop)

    assert len(run.ran) == 3
    assert stop.waits == [3600.0]


def test_after_a_pass_the_sandbox_stopped_it_sleeps(paths):
    """The daemon, a network or a proxy failing would fail the next pass
    too: it tries again after the interval, not at once."""
    run = passes((0, 0, 'the daemon is gone'), (0, 0, 'the daemon is gone'))
    stop = Stopping(last=2)

    service.serve(run, interval=HOUR, stop=stop)

    assert stop.waits == [3600.0, 3600.0]
    assert len(run.ran) == 2


def test_once_it_runs_one_pass(paths):
    run = passes((3, 0, None))
    stop = Stopping(last=10)

    service.serve(run, interval=HOUR, stop=stop, once=True)

    assert len(run.ran) == 1
    assert stop.waits == []


def test_a_stop_ends_the_loop_after_its_pass(paths):
    stop = threading.Event()
    ran: list[int] = []

    def run() -> Passed:
        ran.append(1)
        stop.set()
        return Passed(Walk(), resolved=1)

    service.serve(run, interval=HOUR, stop=stop)

    assert ran == [1]


# -- how often ---------------------------------------------------------------------


def test_the_interval_is_an_hour_unless_said():
    assert service.resolve_interval({}) == HOUR
    assert service.resolve_interval({'CHATSBOM_RESOLVE_INTERVAL': ''}) == HOUR


@pytest.mark.parametrize(
    'value, interval', [
        ('90m', timedelta(minutes=90)), ('6h', timedelta(hours=6)),
        ('1d', timedelta(days=1)),
    ],
)
def test_the_interval_is_said_as_the_collectors_are(value, interval):
    assert service.resolve_interval(
        {'CHATSBOM_RESOLVE_INTERVAL': value},
    ) == interval


@pytest.mark.parametrize('value', ['0h', 'an hour', '1.5h', '-1h'])
def test_an_interval_it_cannot_read_is_refused(value):
    with pytest.raises(SettingsError) as refused:
        service.resolve_interval({'CHATSBOM_RESOLVE_INTERVAL': value})
    assert refused.value.setting == 'CHATSBOM_RESOLVE_INTERVAL'
