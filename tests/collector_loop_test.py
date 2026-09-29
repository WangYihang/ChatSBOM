"""deploy/collector-loop.sh, run for real against a fake `chatsbom` (#20).

The collector's shell ran as PID 1 with no trap for TERM, so every stop
waited out Docker's grace period and ended in SIGKILL, the slice in
flight included. It was compose, not the loop, that refused to go
without a token, which stopped `up`, `ps` and `down` for everyone
else. And the loop took its bind mounts to be writable, where on a
fresh clone Docker creates them, owned by root. These start the loop
with a `chatsbom` and a `sleep` that record what happens to them,
signal it as Docker and a terminal would, and watch what it does.
"""
import os
import re
import shutil
import signal
import subprocess
import tempfile
import time
from collections.abc import Callable
from collections.abc import Iterator
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
LOOP = ROOT / 'deploy' / 'collector-loop.sh'

#: The bind mounts the loop writes, in its working directory: compose
#: mounts them under /app, the image's WORKDIR.
MOUNTS = ('data', '.cache', '.requests-cache')

#: Compose runs the loop as the invoking user, never as root — and root
#: may write any directory whatever its mode, so as root the check for
#: one the loop cannot write would pass every time. A suite running as
#: root starts the loop as nobody instead.
AS_ROOT = os.geteuid() == 0
LOOP_UID, LOOP_GID = (65534, 65534) if AS_ROOT else (os.getuid(), os.getgid())

#: The real one, for the fakes: `sleep` on the loop's PATH is a fake.
REAL_SLEEP = shutil.which('sleep') or '/bin/sleep'

#: Records each call. `queue sync` then exits SLICE_STATUS, `run`
#: RUN_STATUS and `sbom generate` GENERATE_STATUS; with SLICE_SECONDS,
#: RUN_SECONDS or GENERATE_SECONDS set, that one runs so long instead,
#: noting a TERM that comes first. It says it is running only once its
#: trap is set, so a signal sent after that cannot beat the trap.
FAKE_CHATSBOM = """#!/bin/sh
printf '%s\\n' "$*" >> "$RECORD/calls"
case "$1 ${2:-}" in
    'queue sync') seconds="${SLICE_SECONDS:-}" status="${SLICE_STATUS:-0}" name=slice ;;
    'run '*) seconds="${RUN_SECONDS:-}" status="${RUN_STATUS:-0}" name=run ;;
    'sbom generate') seconds="${GENERATE_SECONDS:-}" status="${GENERATE_STATUS:-0}" name=generate ;;
    *) exit 0 ;;
esac
if [ -n "$seconds" ]; then
    "$REAL_SLEEP" "$seconds" &
    trap 'echo TERM >> "$RECORD/signals"; kill $!; exit 143' TERM
    echo $$ > "$RECORD/$name.new" && mv "$RECORD/$name.new" "$RECORD/$name"
    wait
fi
exit "$status"
"""

#: What one slice runs, as the fake records it: revalidate, then collect
#: what that made due.
SYNC = 'queue sync --slice 500 --quota 250'
RUN = 'run --limit 50 --quota 400 --no-depgraph'

#: What an index pass runs: regenerate the SBOMs no longer current, then
#: land the documents, then index them.
INDEX_PASS = ['sbom generate', 'db raw --apply', 'db index']

#: The loop's wait between slices: says it has begun, then sleeps.
FAKE_SLEEP = """#!/bin/sh
echo $$ > "$RECORD/sleep.new" && mv "$RECORD/sleep.new" "$RECORD/sleep"
exec "$REAL_SLEEP" "$@"
"""

#: How long a stop may take. Docker waits ten seconds before SIGKILL,
#: and the loop's waits here are minutes, so a loop that sat out either
#: one fails by a wide margin.
PROMPTLY = 5

#: How long to wait for the loop to reach the point a test is about.
DEADLINE = 10


def eventually(condition: Callable[[], bool], what: str) -> None:
    deadline = time.monotonic() + DEADLINE
    while not condition():
        if time.monotonic() > deadline:
            pytest.fail(f'{what}: not within {DEADLINE}s')
        time.sleep(0.02)


def gone(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    return False


def default_signals() -> None:
    """Start the loop as compose does, with no signal ignored.

    A background job of a non-interactive shell starts with SIGINT
    ignored, so a suite run with `&` passed that on to the loop, and a
    shell cannot trap a signal that was ignored when it started: the
    loop's INT trap did nothing, and the test of it failed. Runs in the
    child, between fork and exec.
    """
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)


class Loop:
    def __init__(self, base: Path) -> None:
        # A copy, which the uid it runs as can read wherever the
        # checkout is.
        self.script = base / 'collector-loop.sh'
        shutil.copyfile(LOOP, self.script)
        self.script.chmod(0o644)
        self.bin = base / 'bin'
        self.bin.mkdir()
        for name, script in (('chatsbom', FAKE_CHATSBOM), ('sleep', FAKE_SLEEP)):
            fake = self.bin / name
            fake.write_text(script)
            fake.chmod(0o755)
        self.record = self.writable(base / 'record')
        # The image's WORKDIR, with the mounts compose puts there.
        self.workdir = base / 'app'
        self.workdir.mkdir()
        for mount in MOUNTS:
            self.writable(self.workdir / mount)
        self.stdout = base / 'stdout'
        self.stderr = base / 'stderr'
        self.process: subprocess.Popen[bytes] | None = None

    @staticmethod
    def writable(directory: Path) -> Path:
        """A new directory, which the loop's uid can write."""
        directory.mkdir()
        if AS_ROOT:
            os.chown(directory, LOOP_UID, LOOP_GID)
        return directory

    def spoil(self, mount: str, how: str) -> None:
        """Leave a mount missing, or unwritable to the loop — as one
        Docker made is to anyone but root."""
        directory = self.workdir / mount
        if how == 'missing':
            directory.rmdir()
        else:
            directory.chmod(0o555)

    def start(self, **env: str | None) -> None:
        """Start it as compose does; a None in `env` leaves that unset."""
        environment: dict[str, str | None] = {
            'PATH': f'{self.bin}{os.pathsep}{os.environ["PATH"]}',
            'RECORD': str(self.record),
            'REAL_SLEEP': REAL_SLEEP,
            # Nothing here talks to GitHub; any non-empty value will do.
            'GITHUB_TOKEN': 'ghp_not-a-real-token',
            'SYNC_INTERVAL_SECONDS': '300',
            **env,
        }
        with self.stdout.open('wb') as out, self.stderr.open('wb') as err:
            self.process = subprocess.Popen(
                ['/bin/sh', str(self.script)],
                cwd=self.workdir,
                env={k: v for k, v in environment.items() if v is not None},
                stdout=out,
                stderr=err,
                # A process group of its own, so that whatever it leaves
                # running can be found and stopped afterwards.
                start_new_session=True,
                preexec_fn=default_signals,
                user=LOOP_UID if AS_ROOT else None,
                group=LOOP_GID if AS_ROOT else None,
                extra_groups=[] if AS_ROOT else None,
            )

    def signal(self, signum: int) -> None:
        assert self.process is not None
        self.process.send_signal(signum)

    def exit_status(self) -> int:
        """How it exited; a failure if it has not, promptly."""
        assert self.process is not None
        try:
            return self.process.wait(timeout=PROMPTLY)
        except subprocess.TimeoutExpired:
            raise AssertionError(f'still running {PROMPTLY}s later') from None

    def pid_of(self, name: str) -> int:
        """The pid a fake wrote, once it has."""
        eventually((self.record / name).exists, f'no {name}')
        return int((self.record / name).read_text())

    def calls(self) -> list[str]:
        calls = self.record / 'calls'
        return calls.read_text().splitlines() if calls.exists() else []

    def stop_everything(self) -> None:
        if self.process is None:
            return
        try:
            os.killpg(self.process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        self.process.wait()


@pytest.fixture
def base(tmp_path: Path) -> Iterator[Path]:
    """Where the loop, its fakes and its working directory live.

    tmp_path, except as root: pytest keeps that below a directory only
    its owner may enter, and the loop then runs as nobody.
    """
    if not AS_ROOT:
        yield tmp_path
        return
    shared = Path(tempfile.mkdtemp(prefix='collector-loop-'))
    shared.chmod(0o755)
    try:
        yield shared
    finally:
        shutil.rmtree(shared)


@pytest.fixture
def loop(base: Path) -> Iterator[Loop]:
    started = Loop(base)
    yield started
    started.stop_everything()


@pytest.mark.parametrize('token', [None, ''], ids=['unset', 'empty'])
def test_without_a_token_it_stops_before_it_starts(loop, token):
    """It used to be compose's `${GITHUB_TOKEN:?}` that refused, and
    compose interpolates the whole file for every command, profile or
    not — so without a token `up`, `ps` and `down` refused for every
    service. The check belongs here, where the token is used."""
    loop.start(GITHUB_TOKEN=token)

    assert loop.exit_status() != 0
    assert 'GITHUB_TOKEN' in loop.stderr.read_text()
    assert loop.calls() == []


@pytest.mark.parametrize('how', ['missing', 'unwritable'])
@pytest.mark.parametrize('mount', MOUNTS)
def test_a_mount_it_cannot_write_stops_it_before_it_starts(loop, mount, how):
    """None of the three is in a fresh clone, and Docker creates a
    missing bind-mount source owned by root, which the loop — running as
    the invoking user — cannot write. The first sign was a read-only
    ledger, deep in the first slice. It checks before anything else, and
    says which, as whom, and what to do about it."""
    loop.spoil(mount, how)

    loop.start()

    assert loop.exit_status() != 0
    stderr = loop.stderr.read_text()
    named = [line for line in stderr.splitlines() if f' {mount}/ ' in line]
    assert len(named) == 1, stderr
    assert f'uid {LOOP_UID}' in named[0]
    assert 'mkdir -p data .cache .requests-cache' in stderr
    assert f'sudo chown -R {LOOP_UID}:{LOOP_GID} ' in stderr
    assert loop.calls() == []


def test_term_while_it_waits_ends_it_at_once(loop):
    """`sleep` in the foreground held a TERM back until it finished:
    fifteen minutes, against a ten-second grace period."""
    loop.start()
    sleeper = loop.pid_of('sleep')
    assert loop.calls() == ['queue track', SYNC, RUN]

    loop.signal(signal.SIGTERM)

    assert loop.exit_status() == 0
    eventually(lambda: gone(sleeper), 'the wait outlived the loop')


@pytest.mark.parametrize('step', ['slice', 'run', 'generate'])
@pytest.mark.parametrize('signum', [signal.SIGTERM, signal.SIGINT])
def test_a_stop_during_a_slice_is_passed_on_to_it(loop, signum, step):
    """The step in flight gets TERM and is waited for, so it ends on the
    signal, not on SIGKILL when the grace period runs out. INT — Ctrl-C,
    for a loop run by hand — is passed on as TERM: a background command
    of a non-interactive shell starts with INT ignored, so it would reach
    nothing. `run`, which collects, is the longest step of a slice, and
    is stopped as `queue sync` is. So is `sbom generate`, the longest of
    all the day after a Syft upgrade, when it rescans every root. An
    index pass after each slice, here, so that it is reached."""
    loop.start(**{f'{step.upper()}_SECONDS': '60'}, INDEX_EVERY_SLICES='1')
    in_flight = loop.pid_of(step)

    loop.signal(signum)

    assert loop.exit_status() == 0
    assert (loop.record / 'signals').read_text() == 'TERM\n'
    assert gone(in_flight)


def test_a_failing_slice_is_stepped_over(loop):
    """The ledger records the failure and backs that repository off;
    the loop goes on to the rest of the slice and the next one. The index
    pass comes after every INDEX_EVERY_SLICES of them, regenerating the
    SBOMs no longer current, then landing the documents and indexing
    them, and then the retention pass after every PRUNE_EVERY_SLICES."""
    loop.start(
        SLICE_STATUS='1', RUN_STATUS='1', SYNC_INTERVAL_SECONDS='0',
        INDEX_EVERY_SLICES='2', PRUNE_EVERY_SLICES='2',
    )
    eventually(lambda: loop.calls().count(SYNC) >= 3, 'no third slice')

    loop.signal(signal.SIGTERM)

    assert loop.exit_status() == 0
    assert loop.calls()[:9] == [
        'queue track',
        SYNC, RUN,
        SYNC, RUN, *INDEX_PASS,
        'data prune --keep 2 --apply',
    ]
    stdout = loop.stdout.read_text()
    assert 'collector: slice 1 failed' in stdout
    assert 'collector: run 1 failed' in stdout


def test_the_index_pass_regenerates_stale_sboms_before_landing_them(loop):
    """After a Syft upgrade every stored SBOM is another Syft's, and so
    not current. `run` regenerates only those of the repositories it
    walks, which are the ones due for other reasons, so on its own the
    loop would have left most of the corpus on the old Syft for months.
    Each index pass runs `sbom generate` first, and lands and indexes
    what it regenerated in the same pass."""
    loop.start(SYNC_INTERVAL_SECONDS='0', INDEX_EVERY_SLICES='2')
    eventually(lambda: 'db index' in loop.calls(), 'no index pass')

    loop.signal(signal.SIGTERM)

    assert loop.exit_status() == 0
    assert loop.calls()[:8] == [
        'queue track', SYNC, RUN, SYNC, RUN, *INDEX_PASS,
    ]


def test_a_failing_rescan_does_not_hold_back_the_index(loop):
    """A `sbom generate` that fails is said and stepped over as any other
    step is: the documents there are landed and indexed all the same,
    and the next slice starts. Here it fails as GENERATE_LIMIT=0 makes
    it: 0 is passed on as it is, not taken for all, and `--limit 0` is a
    usage error, status 2 (sbom_generate_test)."""
    loop.start(
        GENERATE_LIMIT='0', GENERATE_STATUS='2',
        SYNC_INTERVAL_SECONDS='0', INDEX_EVERY_SLICES='1',
    )
    eventually(lambda: loop.calls().count(SYNC) >= 2, 'no second slice')

    loop.signal(signal.SIGTERM)

    assert loop.exit_status() == 0
    assert loop.calls()[:7] == [
        'queue track', SYNC, RUN,
        'sbom generate --limit 0', 'db raw --apply', 'db index',
        SYNC,
    ]
    assert 'collector: sbom generate failed' in loop.stdout.read_text()


@pytest.mark.parametrize(
    'limit,generate',
    [
        (None, 'sbom generate'),
        ('all', 'sbom generate'),
        ('4000', 'sbom generate --limit 4000'),
    ],
    ids=['unset', 'all', 'a-number'],
)
def test_generate_limit_can_spread_a_rescan_over_days(loop, limit, generate):
    """The pass after a Syft upgrade rescans every stored root, and no
    slice runs until it ends: about seven hours for 28,000 roots on the
    collector's two CPUs. GENERATE_LIMIT bounds each pass, so that the
    rescan takes a few days of shorter passes instead; each takes up
    where the last stopped, since what it regenerated is current."""
    loop.start(GENERATE_LIMIT=limit, INDEX_EVERY_SLICES='1')
    eventually(lambda: 'db index' in loop.calls(), 'no index pass')

    loop.signal(signal.SIGTERM)

    assert loop.exit_status() == 0
    assert loop.calls()[3] == generate


def test_every_command_is_a_step():
    """A shell runs a trap only once the foreground command returns, so
    a command the loop runs other than through `step` holds a stop back
    until it is done: minutes, for a `run` or an index pass, against a
    ten-second grace period."""
    commands = [
        line.strip() for line in LOOP.read_text().splitlines()
        if re.match(r'\s*(step\s+)?(chatsbom|sleep)\b', line)
    ]
    assert any('chatsbom run ' in c for c in commands), commands
    assert [c for c in commands if not c.startswith('step ')] == []
