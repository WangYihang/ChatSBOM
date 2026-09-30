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

#: Records each call, and the DuckDB limits it was given. `queue sync`
#: then exits SLICE_STATUS, `run` RUN_STATUS, `sbom generate`
#: GENERATE_STATUS, `warehouse build` WAREHOUSE_STATUS, `snapshot build`
#: SNAPSHOT_STATUS and `export parquet` EXPORT_STATUS; with the same
#: name's _SECONDS set, that one runs so long instead, noting a TERM
#: that comes first. It says it is running only once its trap is set,
#: so a signal sent after that cannot beat the trap. As the real ones
#: do, a `warehouse build` that succeeds leaves a warehouse, and an
#: `export parquet` a manifest, in the directory the loop made for it.
FAKE_CHATSBOM = """#!/bin/sh
printf '%s\\n' "$*" >> "$RECORD/calls"
printf '%s|%s|%s\\n' "$1 ${2:-}" "${CHATSBOM_DUCKDB_MEMORY_LIMIT-unset}" \\
    "${CHATSBOM_DUCKDB_THREADS-unset}" >> "$RECORD/limits"
made=''
case "$1 ${2:-}" in
    'queue sync') seconds="${SLICE_SECONDS:-}" status="${SLICE_STATUS:-0}" name=slice ;;
    'run '*) seconds="${RUN_SECONDS:-}" status="${RUN_STATUS:-0}" name=run ;;
    'sbom generate') seconds="${GENERATE_SECONDS:-}" status="${GENERATE_STATUS:-0}" name=generate ;;
    'warehouse build') seconds="${WAREHOUSE_SECONDS:-}" status="${WAREHOUSE_STATUS:-0}" name=warehouse made=data/warehouse.duckdb ;;
    'snapshot build') seconds="${SNAPSHOT_SECONDS:-}" status="${SNAPSHOT_STATUS:-0}" name=snapshot ;;
    'export parquet') seconds="${EXPORT_SECONDS:-}" status="${EXPORT_STATUS:-0}" name=export made=data/export/manifest.json ;;
    *) exit 0 ;;
esac
if [ -n "$seconds" ]; then
    "$REAL_SLEEP" "$seconds" &
    trap 'echo TERM >> "$RECORD/signals"; kill $!; exit 143' TERM
    echo $$ > "$RECORD/$name.new" && mv "$RECORD/$name.new" "$RECORD/$name"
    wait
fi
if [ "$status" -eq 0 ] && [ -n "$made" ]; then
    : > "$made"
fi
exit "$status"
"""

#: What one slice runs, as the fake records it: revalidate, then collect
#: what that made due.
SYNC = 'queue sync --slice 500 --quota 250'
RUN = 'run --limit 50 --quota 400 --no-depgraph'

#: What an index pass runs: regenerate the SBOMs no longer current, then
#: land the documents, then index them; then build the warehouse from
#: the store, and publish a snapshot of it if what it serves changed.
INDEX_PASS = [
    'sbom generate', 'db raw --apply', 'db index',
    'warehouse build', 'snapshot build',
]

#: The weekly export, of the warehouse the last index pass built, into
#: the data volume, where `web` serves it.
EXPORT = 'export parquet --from warehouse --output data/export'

#: A week, in seconds: how old the last export's manifest is before the
#: loop exports again, by default.
WEEK = 7 * 24 * 3600

#: The retention pass, as it runs by default.
PRUNE = 'data prune --keep 2 --apply'

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


def asleep(pid: int) -> bool:
    """Whether the process is blocked: `S`, its state in /proc.

    Between the commands they start, the collector loop and the web
    watchdog run only builtins, so the one place either blocks is
    `wait`: found asleep while a command it started runs, it is waiting
    on that command.
    """
    stat = Path(f'/proc/{pid}/stat').read_text()
    # The state follows the command's name, in parentheses that the name
    # may itself contain.
    return stat[stat.rindex(')') + 2] == 'S'


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

    def built(self) -> None:
        """A warehouse, as an index pass before this start left one."""
        self.writable_file(self.workdir / 'data' / 'warehouse.duckdb')

    def exported(self, age: float) -> Path:
        """The last export's manifest, `age` seconds old."""
        export = self.workdir / 'data' / 'export'
        if not export.exists():
            self.writable(export)
        manifest = self.writable_file(export / 'manifest.json')
        then = time.time() - age
        os.utime(manifest, (then, then))
        return manifest

    def writable_file(self, path: Path) -> Path:
        """A new, empty file, which the loop's uid can write."""
        path.write_bytes(b'')
        if AS_ROOT:
            os.chown(path, LOOP_UID, LOOP_GID)
        return path

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

    def waiting_on(self, name: str) -> int:
        """The pid a fake wrote, once the loop is waiting on it.

        That the step has begun is not enough. The loop forks a step and
        records its pid after, and the step starts out as a copy of the
        loop, TERM still trapped, until it sets TERM back to its default.
        On a loaded machine a stop can come in between: the loop exits
        with no pid to pass TERM on to, and leaves the step running; or
        it passes TERM on to a step that drops it, and waits the step
        out, the five minutes of `sleep 300` here. So the stop is sent
        once the loop is asleep in `wait`, which it reaches only after
        recording the pid.
        """
        pid = self.pid_of(name)
        process = self.process
        assert process is not None
        eventually(
            lambda: asleep(process.pid), f'the loop never waited on {name}',
        )
        return pid

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


@pytest.mark.parametrize(
    'step', ['slice', 'run', 'generate', 'warehouse', 'snapshot', 'export'],
)
@pytest.mark.parametrize('signum', [signal.SIGTERM, signal.SIGINT])
def test_a_stop_during_a_slice_is_passed_on_to_it(loop, signum, step):
    """The step in flight gets TERM and is waited for, so it ends on the
    signal, not on SIGKILL when the grace period runs out. INT — Ctrl-C,
    for a loop run by hand — is passed on as TERM: a background command
    of a non-interactive shell starts with INT ignored, so it would reach
    nothing. `run`, which collects, is the longest step of a slice, and
    is stopped as `queue sync` is. So is `sbom generate`, the longest of
    all the day after a Syft upgrade, when it rescans every root, and so
    are the warehouse, the snapshot and the export, minutes each. An
    index pass after each slice, here, and so a warehouse, and with no
    export yet, an export, so that they are reached."""
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
    them, then building the warehouse and publishing a snapshot of it,
    and then the retention pass after every PRUNE_EVERY_SLICES."""
    # The last export a minute old: none is due here.
    loop.exported(60)
    loop.start(
        SLICE_STATUS='1', RUN_STATUS='1', SYNC_INTERVAL_SECONDS='0',
        INDEX_EVERY_SLICES='2', PRUNE_EVERY_SLICES='2',
    )
    eventually(lambda: loop.calls().count(SYNC) >= 3, 'no third slice')

    loop.signal(signal.SIGTERM)

    assert loop.exit_status() == 0
    expected = [
        'queue track',
        SYNC, RUN,
        SYNC, RUN, *INDEX_PASS,
        PRUNE,
        SYNC,
    ]
    assert loop.calls()[:len(expected)] == expected
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
    eventually(lambda: 'snapshot build' in loop.calls(), 'no index pass')

    loop.signal(signal.SIGTERM)

    assert loop.exit_status() == 0
    expected = ['queue track', SYNC, RUN, SYNC, RUN, *INDEX_PASS]
    assert loop.calls()[:len(expected)] == expected


def test_a_failing_rescan_does_not_hold_back_the_index(loop):
    """A `sbom generate` that fails is said and stepped over as any other
    step is: the documents there are landed and indexed all the same,
    and the next slice starts. Here it fails as GENERATE_LIMIT=0 makes
    it: 0 is passed on as it is, not taken for all, and `--limit 0` is a
    usage error, status 2 (sbom_generate_test)."""
    # The last export a minute old: none is due here.
    loop.exported(60)
    loop.start(
        GENERATE_LIMIT='0', GENERATE_STATUS='2',
        SYNC_INTERVAL_SECONDS='0', INDEX_EVERY_SLICES='1',
    )
    eventually(lambda: loop.calls().count(SYNC) >= 2, 'no second slice')

    loop.signal(signal.SIGTERM)

    assert loop.exit_status() == 0
    expected = [
        'queue track', SYNC, RUN,
        'sbom generate --limit 0', *INDEX_PASS[1:],
        SYNC,
    ]
    assert loop.calls()[:len(expected)] == expected
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
    # The index pass over, and the wait for the next slice begun.
    loop.waiting_on('sleep')

    loop.signal(signal.SIGTERM)

    assert loop.exit_status() == 0
    assert loop.calls()[3] == generate


# -- the warehouse, the snapshot and the export (#150) ----------------------


@pytest.mark.parametrize('warehouse', [None, 'on'], ids=['unset', 'on'])
def test_the_index_pass_builds_the_warehouse_and_publishes_a_snapshot(
    loop, warehouse,
):
    """#128, Q11: once `db index` has landed what the pass collected,
    `warehouse build` makes the DuckDB warehouse from the store, and
    `snapshot build` publishes a snapshot of it into data/snapshots for
    `web` to serve, if what it serves changed; `snapshot build` decides
    that, and publishes nothing when it has not. Then the next slice."""
    # The last export a minute old: none is due here.
    loop.exported(60)
    loop.start(
        WAREHOUSE=warehouse, SYNC_INTERVAL_SECONDS='0',
        INDEX_EVERY_SLICES='1',
    )
    eventually(lambda: loop.calls().count(SYNC) >= 2, 'no second slice')

    loop.signal(signal.SIGTERM)

    assert loop.exit_status() == 0
    expected = ['queue track', SYNC, RUN, *INDEX_PASS, SYNC]
    assert loop.calls()[:len(expected)] == expected


@pytest.mark.parametrize('warehouse', ['on', 'off'])
def test_it_makes_the_exports_directory_as_it_starts(loop, warehouse):
    """data/export, which `web` mounts, and is not made without (#154):
    made as the collector starts, rather than at its first export, a
    week or a day away, so that `web` can start before it. Whether or
    not the loop exports: `web` mounts it all the same."""
    loop.start(WAREHOUSE=warehouse)
    loop.waiting_on('sleep')

    loop.signal(signal.SIGTERM)

    assert loop.exit_status() == 0
    assert (loop.workdir / 'data' / 'export').is_dir()
    assert loop.calls() == ['queue track', SYNC, RUN]


def test_the_first_export_follows_the_first_warehouse(loop):
    """With no export yet, one is due; but there is nothing to export
    until an index pass has built a warehouse, and until then the loop
    says nothing of it. The public Parquet export (Q11) then follows
    the pass that built it, into data/export, where `web` serves it,
    and not again the slice after: its manifest is new."""
    loop.start(SYNC_INTERVAL_SECONDS='0', INDEX_EVERY_SLICES='2')
    eventually(lambda: loop.calls().count(SYNC) >= 5, 'no fifth slice')

    loop.signal(signal.SIGTERM)

    assert loop.exit_status() == 0
    expected = [
        'queue track', SYNC, RUN, SYNC, RUN, *INDEX_PASS, EXPORT, SYNC, RUN,
        SYNC, RUN, *INDEX_PASS, SYNC,
    ]
    assert loop.calls()[:len(expected)] == expected
    stdout = loop.stdout.read_text()
    assert stdout.count('collector: export pass') == 1
    assert 'export failed' not in stdout


@pytest.mark.parametrize(
    'age,exported',
    [
        (None, True),
        (WEEK + 60, True),
        (WEEK - 3600, False),
        (60, False),
    ],
    ids=['none', 'a-week-old', 'younger', 'new'],
)
def test_the_export_is_due_by_the_manifests_age(loop, age, exported):
    """When data/export/manifest.json is missing or a week old, and not
    otherwise, counted from the export and not from the container's
    start: a slice count started again with each start, and a collector
    started again more often than weekly never exported (#154). So one
    started a week after the last export exports at its first slice,
    whether an index pass is due then or not; and one started the day
    after, not before the week is out."""
    loop.built()
    if age is not None:
        loop.exported(age)
    loop.start(SYNC_INTERVAL_SECONDS='0')
    eventually(lambda: loop.calls().count(SYNC) >= 3, 'no third slice')

    loop.signal(signal.SIGTERM)

    assert loop.exit_status() == 0
    calls = loop.calls()
    if exported:
        expected = ['queue track', SYNC, RUN, EXPORT, SYNC, RUN, SYNC]
        assert calls[:len(expected)] == expected
        assert 'collector: export pass' in loop.stdout.read_text()
    else:
        assert EXPORT not in calls
        assert 'collector: export pass' not in loop.stdout.read_text()


@pytest.mark.parametrize('hours,exported', [(2, True), (0.5, False)])
def test_export_interval_seconds_is_how_old(loop, hours, exported):
    """How old the last export may be, in seconds: a week unless set."""
    loop.built()
    loop.exported(hours * 3600)
    loop.start(SYNC_INTERVAL_SECONDS='0', EXPORT_INTERVAL_SECONDS='3600')
    eventually(lambda: loop.calls().count(SYNC) >= 2, 'no second slice')

    loop.signal(signal.SIGTERM)

    assert loop.exit_status() == 0
    assert (EXPORT in loop.calls()) is exported


def test_an_export_that_fails_is_tried_again_at_the_next_slice(loop):
    """It leaves the last export as it was, manifest and all, which is
    still as old as it was: so the next slice tries again, rather than
    a week later."""
    loop.built()
    loop.start(EXPORT_STATUS='1', SYNC_INTERVAL_SECONDS='0')
    eventually(lambda: loop.calls().count(SYNC) >= 3, 'no third slice')

    loop.signal(signal.SIGTERM)

    assert loop.exit_status() == 0
    expected = [
        'queue track', SYNC, RUN, EXPORT, SYNC, RUN, EXPORT, SYNC,
    ]
    assert loop.calls()[:len(expected)] == expected
    assert loop.stdout.read_text().count('collector: export failed') >= 2


def fallbacks() -> dict[str, str]:
    """Each setting the loop reads, and what it takes when it is unset."""
    return dict(re.findall(r'"\$\{(\w+):-([^}]*)\}"', LOOP.read_text()))


def test_by_default_the_export_is_weekly():
    """A week after the last, however often the collector started in
    between; and the index pass is daily, at the default interval, so
    the export reads a warehouse a day old at most."""
    given = fallbacks()
    interval = int(given['SYNC_INTERVAL_SECONDS'])
    index = int(given['INDEX_EVERY_SLICES'])
    assert int(given['EXPORT_INTERVAL_SECONDS']) == WEEK
    assert index * interval == 24 * 3600
    assert 'EXPORT_EVERY_SLICES' not in given


@pytest.mark.parametrize(
    'step,said',
    [
        ('WAREHOUSE', 'collector: warehouse build failed'),
        ('SNAPSHOT', 'collector: snapshot build failed'),
        ('EXPORT', 'collector: export failed'),
    ],
    ids=['warehouse', 'snapshot', 'export'],
)
def test_a_failing_warehouse_step_is_stepped_over(loop, step, said):
    """Said, as any step's failure is, and the rest of the pass and the
    next slice go on. A warehouse build that fails leaves the last
    warehouse in place (#141), and a snapshot of it is the same content,
    which publishes nothing (#146); a snapshot that fails leaves
    `CURRENT` naming the last one, which `web` goes on serving; and an
    export that fails leaves the last export's manifest naming its own
    files. With the last pass's warehouse there, and no export yet, the
    export is due."""
    loop.built()
    loop.start(
        **{f'{step}_STATUS': '1'},
        SYNC_INTERVAL_SECONDS='0', INDEX_EVERY_SLICES='1',
    )
    eventually(lambda: loop.calls().count(SYNC) >= 2, 'no second slice')

    loop.signal(signal.SIGTERM)

    assert loop.exit_status() == 0
    expected = ['queue track', SYNC, RUN, *INDEX_PASS, EXPORT, SYNC]
    assert loop.calls()[:len(expected)] == expected
    assert said in loop.stdout.read_text()


def test_warehouse_off_builds_publishes_and_exports_nothing(loop):
    """For a host without the disk they take (DEPLOY.md): the index pass
    is what it was before them, no export runs, not even of a warehouse
    left from before, and the loop says so as it starts."""
    loop.built()
    loop.start(
        WAREHOUSE='off', SYNC_INTERVAL_SECONDS='0',
        INDEX_EVERY_SLICES='1', PRUNE_EVERY_SLICES='1',
    )
    eventually(lambda: loop.calls().count(SYNC) >= 2, 'no second slice')

    loop.signal(signal.SIGTERM)

    assert loop.exit_status() == 0
    expected = [
        'queue track', SYNC, RUN, 'sbom generate', 'db raw --apply',
        'db index', PRUNE, SYNC,
    ]
    assert loop.calls()[:len(expected)] == expected
    assert 'WAREHOUSE=off' in loop.stdout.read_text()


@pytest.mark.parametrize('value', ['yes', 'OFF', 'false', '0'])
def test_warehouse_is_on_or_off_and_nothing_else(loop, value):
    """Another value stops it before it starts, naming the setting and
    what it takes: taken as on, a mistyped `of` would fill the disk it
    was set to spare."""
    loop.start(WAREHOUSE=value)

    assert loop.exit_status() != 0
    stderr = loop.stderr.read_text()
    assert f"WAREHOUSE is '{value}'" in stderr
    assert 'on or off' in stderr
    assert loop.calls() == []


def test_duckdbs_limits_reach_what_opens_duckdb(loop):
    """Compose gives the collector DuckDB's memory limit and threads
    (compose_test), which the warehouse, the snapshot and the export are
    each given as they are."""
    loop.start(
        CHATSBOM_DUCKDB_MEMORY_LIMIT='1GiB', CHATSBOM_DUCKDB_THREADS='1',
        INDEX_EVERY_SLICES='1',
    )
    loop.waiting_on('sleep')

    loop.signal(signal.SIGTERM)

    assert loop.exit_status() == 0
    given = {
        command: (memory, threads)
        for command, memory, threads in (
            line.split('|')
            for line in (loop.record / 'limits').read_text().splitlines()
        )
    }
    for command in ('warehouse build', 'snapshot build', 'export parquet'):
        assert given[command] == ('1GiB', '1'), command


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
