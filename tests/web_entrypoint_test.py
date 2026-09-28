"""deploy/web-entrypoint.sh, run for real against a fake wrangler (#18).

The dashboard's secrets reached the Worker as `--var` arguments, and a
process's arguments are readable by every user on the host: uid 65534
read the ClickHouse password and the Anthropic key off wrangler's
/proc/<pid>/cmdline. These run the script with a wrangler, where the
script looks for it in the project's node_modules, that records how it
was called, so what reaches the command line is observed rather than
read off the source.

The script starts wrangler one of two ways: under its watchdog, which
probes the Worker and exits when it wedges, or, with WATCHDOG_DISABLED,
as a bare `exec`. What reaches wrangler, and a stop, are tested both
ways, the watchdog on the timings of a test and with a `node` of its own
for the probe; what the watchdog itself does, under the watchdog alone.

Under the watchdog the script spends nearly all its time waiting, and a
shell runs a trap only once the command in the foreground has returned:
a stop that came during the minute's grace, between probes or during a
probe was held back until that wait was over, past Docker's ten seconds
and into SIGKILL (#53). The tests of a stop run the watchdog on the
timings the container does, with a `sleep` and a `node` that say when
they have begun.
"""
import os
import re
import shutil
import signal
import stat
import subprocess
import time
from collections.abc import Callable
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
ENTRYPOINT = ROOT / 'deploy' / 'web-entrypoint.sh'

#: Distinctive enough that finding one anywhere is not a coincidence.
SECRETS = {
    'CLICKHOUSE_PASSWORD': 'clickhouse-pw-7f3a',
    'ANTHROPIC_API_KEY': 'sk-ant-test-9c1e',
    'TURNSTILE_SECRET': '0x4AAAAAAA-turnstile-2d8b',
}

CLICKHOUSE_URL = 'http://clickhouse:8123'

#: wrangler, in the web directory's node_modules/.bin: records its name
#: and arguments, NUL-separated, and where it ran; starts nothing, and
#: exits WRANGLER_STATUS, 0 unless told otherwise. With WRANGLER_SECONDS
#: set it runs that long first, noting a TERM that comes first, and says
#: it is running only once its trap is set.
FAKE_WRANGLER = """#!/bin/sh
printf '%s\\0' "${0##*/}" "$@" > "$RECORD/argv"
pwd > "$RECORD/cwd"
if [ -n "${WRANGLER_SECONDS:-}" ]; then
    "$REAL_SLEEP" "$WRANGLER_SECONDS" &
    trap 'echo TERM >> "$RECORD/signals"; kill $!; exit 143' TERM
    echo $$ > "$RECORD/wrangler.new" && mv "$RECORD/wrangler.new" "$RECORD/wrangler"
    wait
fi
exit "${WRANGLER_STATUS:-0}"
"""

#: `npx wrangler`, as far as a stop is concerned: npm ran the bin under a
#: `sh -c` of its own, which forked it rather than exec it, and passed a
#: TERM on to that shell alone, which died of it and left wrangler
#: running (measured, npm 10.9.7 and dash 0.5.12). The script runs no
#: npx; this is here so that one which went back to it fails the tests
#: of a stop, whatever npx the machine has.
FAKE_NPX = """#!/bin/sh
bin="node_modules/.bin/$1"
shift
sh -c '"$0" "$@"; exit' "$bin" "$@" &
trap 'kill -TERM $!; exit 143' TERM INT
wait "$!"
"""

#: The watchdog's probe: `node -e` asking the Worker which backend it
#: is. This one asks nothing, so no test depends on what listens on
#: 8787; it answers PROBE_STATUS, 0 (healthy) unless told otherwise.
#: With PROBE_HANGS set it asks a wedged Worker instead, which takes the
#: connection and never answers: it says it has begun, then waits out
#: the timeout it was given.
FAKE_NODE = """#!/bin/sh
echo probe >> "$RECORD/probes"
if [ -n "${PROBE_HANGS:-}" ]; then
    echo $$ > "$RECORD/node.new" && mv "$RECORD/node.new" "$RECORD/node"
    exec "$REAL_SLEEP" "$WATCHDOG_TIMEOUT_SECONDS"
fi
exit "${PROBE_STATUS:-0}"
"""

#: The watchdog's waits: says it has begun, under a name that gives its
#: length (`sleep-60`), then sleeps.
FAKE_SLEEP = """#!/bin/sh
echo $$ > "$RECORD/sleep-$1.new" && mv "$RECORD/sleep-$1.new" "$RECORD/sleep-$1"
exec "$REAL_SLEEP" "$@"
"""

#: The real one, for the fakes: `sleep` on the script's PATH is a fake.
REAL_SLEEP = shutil.which('sleep') or '/bin/sleep'

#: How the script is started: under its watchdog, which checks at once,
#: then as fast as it can, and gives up on no test's timescale; or bare.
MODES = {
    'supervised': {
        'WATCHDOG_GRACE_SECONDS': '0',
        'WATCHDOG_INTERVAL_SECONDS': '0',
        'WATCHDOG_TIMEOUT_SECONDS': '1',
        'WATCHDOG_FAILURES': '1000',
    },
    'bare': {'WATCHDOG_DISABLED': '1'},
}

#: The watchdog as the container runs it, on compose's defaults: a
#: minute's grace after the start, then a probe every thirty seconds
#: that waits up to twenty for an answer.
IN_PRODUCTION = {
    'WATCHDOG_GRACE_SECONDS': '60',
    'WATCHDOG_INTERVAL_SECONDS': '30',
    'WATCHDOG_TIMEOUT_SECONDS': '20',
    'WATCHDOG_FAILURES': '4',
}

#: Where it waits on those timings: what else to set to reach it, and
#: the fake that says it has. The later two skip the grace.
WAITS = {
    'grace': ({}, 'sleep-60'),
    'interval': ({'WATCHDOG_GRACE_SECONDS': '0'}, 'sleep-30'),
    'probe': ({'WATCHDOG_GRACE_SECONDS': '0', 'PROBE_HANGS': '1'}, 'node'),
}

#: How long a stop may take: Docker waits ten seconds before SIGKILL.
PROMPTLY = 5

#: How long a stop that comes while the watchdog waits may take. Passed
#: on, it takes milliseconds; held back, it takes the rest of that wait,
#: twenty seconds or more on the timings the container runs.
AT_ONCE = 2

#: How long to wait for the script to reach the point a test is about.
DEADLINE = 10

#: The line pattern of dotenv 16.3.1 — the parser wrangler bundles and
#: reads `.dev.vars` with, so these tests read the file the way it will.
DOTENV_LINE = re.compile(
    r"""(?:^|^)\s*(?:export\s+)?([\w.-]+)(?:\s*=\s*?|:\s+?)"""
    r"""(\s*'(?:\\'|[^'])*'|\s*"(?:\\"|[^"])*"|\s*`(?:\\`|[^`])*`|[^#\r\n]+)?"""
    r"""\s*(?:#.*)?(?:$|$)""",
    re.MULTILINE,
)


def read_dev_vars(text: str) -> dict[str, str]:
    """dotenv's `parse`, ported line for line."""
    values = {}
    for match in DOTENV_LINE.finditer(re.sub(r'\r\n?', '\n', text)):
        value = (match[2] or '').strip()
        quote = value[:1]
        value = re.sub(r"""^(['"`])([\s\S]*)\1$""", r'\2', value, flags=re.M)
        if quote == '"':
            value = value.replace('\\n', '\n').replace('\\r', '\r')
        values[match[1]] = value
    return values


def vars_on(argv: list[str]) -> dict[str, str]:
    """The `--var NAME:value` pairs in a command line."""
    pairs = {}
    for flag, value in zip(argv, argv[1:]):
        if flag == '--var':
            name, _, rest = value.partition(':')
            pairs[name] = rest
    return pairs


def eventually(condition: Callable[[], bool], what: str) -> None:
    deadline = time.monotonic() + DEADLINE
    while not condition():
        if time.monotonic() > deadline:
            pytest.fail(f'{what}: not within {DEADLINE}s')
        time.sleep(0.02)


def default_signals() -> None:
    """Start the script as compose does, with no signal ignored.

    A background job of a non-interactive shell starts with SIGINT
    ignored, so a suite run with `&` would pass that on to the script,
    and a shell cannot trap a signal that was ignored when it started.
    Runs in the child, between fork and exec.
    """
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)


def exit_status(process: subprocess.Popen[str], within: float) -> int:
    """How it exited; a failure if it has not, within that long."""
    try:
        return process.wait(timeout=within)
    except subprocess.TimeoutExpired:
        raise AssertionError(f'still running {within}s later') from None


@dataclass
class Started:
    returncode: int
    stderr: str
    #: None when wrangler never ran.
    argv: list[str] | None
    cwd: Path | None


class Entrypoint:
    def __init__(self, tmp_path: Path, mode: str) -> None:
        self.bin = tmp_path / 'bin'
        self.bin.mkdir()
        self.web = tmp_path / 'web'
        self.web.mkdir()
        # Where `npm ci` puts it, in the image as in a checkout.
        self.wrangler = self.web / 'node_modules' / '.bin' / 'wrangler'
        self.wrangler.parent.mkdir(parents=True)
        fakes = (
            (self.wrangler, FAKE_WRANGLER),
            (self.bin / 'npx', FAKE_NPX),
            (self.bin / 'node', FAKE_NODE),
            (self.bin / 'sleep', FAKE_SLEEP),
        )
        for fake, script in fakes:
            fake.write_text(script)
            fake.chmod(0o755)
        self.record = tmp_path / 'record'
        self.record.mkdir()
        self.elsewhere = tmp_path
        self.mode = mode
        self.processes: list[subprocess.Popen[str]] = []

    @property
    def dev_vars(self) -> Path:
        return self.web / '.dev.vars'

    def environment(self, env: dict[str, str]) -> dict[str, str]:
        return {
            'PATH': f'{self.bin}{os.pathsep}{os.environ["PATH"]}',
            'RECORD': str(self.record),
            'REAL_SLEEP': REAL_SLEEP,
            'WEB_DIR': str(self.web),
            **MODES[self.mode],
            **env,
        }

    def spawn(self, **env: str) -> subprocess.Popen[str]:
        """Start it and leave it running, as a container does."""
        process = subprocess.Popen(
            [str(ENTRYPOINT)],
            env=self.environment(env),
            cwd=self.elsewhere,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            # A process group of its own, so that whatever it leaves
            # running can be found and stopped afterwards.
            start_new_session=True,
            preexec_fn=default_signals,
        )
        self.processes.append(process)
        return process

    def stop_everything(self) -> None:
        """Kill what it started and left running, the script included."""
        for process in self.processes:
            try:
                os.killpg(process.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            process.wait()

    def pid_of(self, name: str) -> int:
        """The pid a fake wrote, once it has."""
        eventually((self.record / name).exists, f'no {name}')
        return int((self.record / name).read_text())

    def signals(self) -> str:
        """What wrangler was sent, a line a signal; empty for nothing."""
        signals = self.record / 'signals'
        return signals.read_text() if signals.exists() else ''

    def start(self, **env: str) -> Started:
        result = subprocess.run(
            [str(ENTRYPOINT)],
            env=self.environment(env),
            # Not the web directory: the script must find its own way.
            cwd=self.elsewhere,
            capture_output=True,
            text=True,
            timeout=30,
        )
        argv = self.record / 'argv'
        cwd = self.record / 'cwd'
        return Started(
            result.returncode,
            result.stderr,
            argv.read_text().split('\0')[:-1] if argv.exists() else None,
            Path(cwd.read_text().strip()) if cwd.exists() else None,
        )


@pytest.fixture(params=sorted(MODES))
def entrypoint(request, tmp_path: Path) -> Iterator[Entrypoint]:
    started = Entrypoint(tmp_path, request.param)
    yield started
    started.stop_everything()


def test_no_secret_reaches_the_command_line(entrypoint):
    started = entrypoint.start(CLICKHOUSE_URL=CLICKHOUSE_URL, **SECRETS)

    assert started.returncode == 0, started.stderr
    assert started.argv is not None, 'wrangler never ran'
    for name, value in SECRETS.items():
        assert not any(value in arg for arg in started.argv), name
    assert not set(SECRETS) & set(vars_on(started.argv))


def test_secrets_reach_the_worker_through_dev_vars(entrypoint):
    """wrangler reads `.dev.vars` from beside wrangler.jsonc and binds
    each entry as a secret, which the Worker reads like any other var."""
    entrypoint.start(CLICKHOUSE_URL=CLICKHOUSE_URL, **SECRETS)

    assert read_dev_vars(entrypoint.dev_vars.read_text()) == SECRETS


def test_dev_vars_is_readable_by_its_owner_alone(entrypoint):
    entrypoint.start(CLICKHOUSE_URL=CLICKHOUSE_URL, **SECRETS)

    mode = stat.S_IMODE(entrypoint.dev_vars.stat().st_mode)
    assert mode == 0o600, oct(mode)


def test_a_dev_vars_from_an_earlier_start_is_replaced(entrypoint):
    """A restart runs the script again in the same container, so the
    file is already there. `>` would keep its mode, and anything it held
    that the environment no longer does would still reach the Worker."""
    entrypoint.dev_vars.write_text("ANTHROPIC_API_KEY='sk-ant-stale'\n")
    entrypoint.dev_vars.chmod(0o644)

    entrypoint.start(CLICKHOUSE_URL=CLICKHOUSE_URL)

    assert stat.S_IMODE(entrypoint.dev_vars.stat().st_mode) == 0o600
    assert 'ANTHROPIC_API_KEY' not in read_dev_vars(
        entrypoint.dev_vars.read_text(),
    )


def test_an_empty_key_does_not_count_as_configured(entrypoint):
    """Compose passes `${ANTHROPIC_API_KEY:-}`, so unset arrives empty.
    Empty must mean absent — /api/chat then says AI answers are not set
    up — and the same for Turnstile. The password keeps its default."""
    started = entrypoint.start(
        CLICKHOUSE_URL=CLICKHOUSE_URL, ANTHROPIC_API_KEY='', TURNSTILE_SECRET='',
    )

    assert started.returncode == 0, started.stderr
    assert read_dev_vars(entrypoint.dev_vars.read_text()) == {
        'CLICKHOUSE_PASSWORD': 'guest',
    }
    assert started.argv is not None
    assert not set(SECRETS) & set(vars_on(started.argv))


@pytest.mark.parametrize(
    'password',
    [
        # Unquoted, dotenv ends a value at `#`; double-quoted, it turns
        # `\n` into a newline. `$` would be expanded by a `.env` file.
        'p#ss w"rd $HOME \\n',
        # A trailing backslash beside the closing quote.
        'ends-in-a-backslash\\',
        # A single quote, which single quotes cannot carry.
        "it's-a-password",
    ],
)
def test_a_value_dotenv_would_misread_arrives_intact(entrypoint, password):
    started = entrypoint.start(
        CLICKHOUSE_URL=CLICKHOUSE_URL,
        CLICKHOUSE_PASSWORD=password,
        ANTHROPIC_API_KEY=SECRETS['ANTHROPIC_API_KEY'],
    )

    assert started.returncode == 0, started.stderr
    assert read_dev_vars(entrypoint.dev_vars.read_text()) == {
        'CLICKHOUSE_PASSWORD': password,
        'ANTHROPIC_API_KEY': SECRETS['ANTHROPIC_API_KEY'],
    }


def test_a_value_dotenv_cannot_carry_is_refused(entrypoint):
    """dotenv has no escape inside quotes, so a value holding both kinds
    it reads literally cannot be written. Refusing beats a Worker that
    holds a password which is subtly not the password — and the refusal
    names the variable without printing it."""
    password = "it's `both`"
    started = entrypoint.start(
        CLICKHOUSE_URL=CLICKHOUSE_URL, CLICKHOUSE_PASSWORD=password,
    )

    assert started.returncode != 0
    assert started.argv is None
    assert 'CLICKHOUSE_PASSWORD' in started.stderr
    assert password not in started.stderr


@pytest.mark.parametrize('url', [None, ''], ids=['unset', 'empty'])
def test_it_still_refuses_to_start_without_clickhouse(entrypoint, url):
    """Without a live backend the Worker would serve whatever stale
    snapshot it could find, from a container that reports healthy."""
    env = dict(SECRETS)
    if url is not None:
        env['CLICKHOUSE_URL'] = url

    started = entrypoint.start(**env)

    assert started.returncode != 0
    assert started.argv is None
    assert 'CLICKHOUSE_URL' in started.stderr
    assert not entrypoint.dev_vars.exists()


def test_what_is_not_secret_stays_on_the_command_line(entrypoint):
    """Where the Worker points is worth being able to read off `ps`."""
    started = entrypoint.start(
        CLICKHOUSE_URL=CLICKHOUSE_URL,
        CLICKHOUSE_DB='chatsbom',
        CLICKHOUSE_USER='guest',
        GENERATOR='chatsbom/0.5.4 clickhouse',
        DAILY_SPEND_CAP_USD='5',
        **SECRETS,
    )

    assert started.argv is not None
    assert started.argv[:3] == ['wrangler', 'dev', '--local']
    assert vars_on(started.argv) == {
        'CLICKHOUSE_URL': CLICKHOUSE_URL,
        'CLICKHOUSE_DB': 'chatsbom',
        'CLICKHOUSE_USER': 'guest',
        'GENERATOR': 'chatsbom/0.5.4 clickhouse',
        'DAILY_SPEND_CAP_USD': '5',
    }


def test_wrangler_runs_beside_the_dev_vars_it_reads(entrypoint):
    """wrangler finds wrangler.jsonc from where it runs, and `.dev.vars`
    beside that — so it has to run in the directory the file went to."""
    started = entrypoint.start(CLICKHOUSE_URL=CLICKHOUSE_URL)

    assert started.cwd == entrypoint.web


def gone(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return True
    return False


Spawn = Callable[..., 'subprocess.Popen[str]']


@pytest.fixture
def supervised(tmp_path: Path) -> Iterator[tuple[Entrypoint, Spawn]]:
    """The script under its watchdog, killed with whatever it started."""
    started = Entrypoint(tmp_path, 'supervised')
    yield started, started.spawn
    started.stop_everything()


def test_a_stop_reaches_wrangler_under_the_watchdog(supervised):
    """Under the watchdog wrangler is the script's background job, not
    what it execs: `docker stop` must still reach wrangler, and be
    waited for, rather than be waited out and end in SIGKILL."""
    entrypoint, spawn = supervised
    process = spawn(CLICKHOUSE_URL=CLICKHOUSE_URL, WRANGLER_SECONDS='60')
    wrangler = entrypoint.pid_of('wrangler')

    process.send_signal(signal.SIGTERM)

    assert process.wait(timeout=PROMPTLY) != 0
    assert entrypoint.signals() == 'TERM\n'
    assert gone(wrangler)


def test_a_stop_reaches_wrangler_itself(entrypoint):
    """`npx wrangler` ran wrangler under npm's own `sh -c`, and npm
    passed a TERM on to that shell alone: a stop ended npx, and left
    wrangler — its CLI, and workerd under that — running until the
    container's PID 1 exited, when all of it went by SIGKILL. Under the
    watchdog and with WATCHDOG_DISABLED alike (measured). The script
    runs wrangler itself now: the pid it holds, or execs, is wrangler's
    own, which hands the TERM on to its CLI (bin/wrangler.js)."""
    process = entrypoint.spawn(
        CLICKHOUSE_URL=CLICKHOUSE_URL, WRANGLER_SECONDS='600',
    )
    wrangler = entrypoint.pid_of('wrangler')

    process.send_signal(signal.SIGTERM)

    assert exit_status(process, within=AT_ONCE) == 143
    assert entrypoint.signals() == 'TERM\n'
    assert gone(wrangler)


def test_a_wedged_worker_is_stopped_so_the_container_restarts(supervised):
    """A Worker that stops answering does not exit, and a restart
    policy acts only on an exit. After WATCHDOG_FAILURES failed probes in
    a row, wrangler is stopped and the script exits non-zero."""
    entrypoint, spawn = supervised
    process = spawn(
        CLICKHOUSE_URL=CLICKHOUSE_URL, WRANGLER_SECONDS='60',
        PROBE_STATUS='1', WATCHDOG_FAILURES='2',
    )
    wrangler = entrypoint.pid_of('wrangler')

    # TERM, then five seconds before a KILL.
    assert process.wait(timeout=PROMPTLY + 5) == 1
    assert (entrypoint.record / 'probes').read_text() == 'probe\nprobe\n'
    assert entrypoint.signals() == 'TERM\n'
    assert gone(wrangler)
    assert process.stderr is not None
    assert 'wedged' in process.stderr.read()


def test_a_wrangler_that_fails_is_followed_and_said_to_have(supervised):
    """wrangler that exits on its own takes the script with it, with its
    status, for the restart policy to act on; and the watchdog says so.
    It did not when that status was not 0: set -e ended the script at
    the `wait` that returned it, before the line in the log."""
    entrypoint, _ = supervised

    started = entrypoint.start(
        CLICKHOUSE_URL=CLICKHOUSE_URL, WRANGLER_STATUS='3',
    )

    assert started.returncode == 3
    assert 'watchdog: wrangler exited (3)' in started.stderr


@pytest.mark.parametrize('signum', [signal.SIGTERM, signal.SIGINT])
@pytest.mark.parametrize('wait', list(WAITS))
def test_a_stop_while_the_watchdog_waits_is_passed_on_at_once(
    supervised, wait, signum,
):
    """The watchdog's waits ran in the foreground, and a shell runs a
    trap only once the foreground command has returned: a stop during
    the grace period, between probes or during a probe of a Worker that
    never answers was held back until that wait was over — up to a
    minute, against Docker's ten seconds — and ended in SIGKILL,
    wrangler and all.

    It reaches wrangler at once now, as TERM whichever signal came; the
    wait in flight goes with the script rather than outliving it; and
    the script exits as wrangler did, with its status, as it does with
    WATCHDOG_DISABLED, where wrangler is what the script execs.
    """
    entrypoint, spawn = supervised
    settings, waiting = WAITS[wait]
    process = spawn(
        CLICKHOUSE_URL=CLICKHOUSE_URL, WRANGLER_SECONDS='600',
        **{**IN_PRODUCTION, **settings},
    )
    wrangler = entrypoint.pid_of('wrangler')
    in_flight = entrypoint.pid_of(waiting)
    assert not gone(in_flight)

    process.send_signal(signum)

    # 143 is what the fake wrangler exits with on TERM.
    assert exit_status(process, within=AT_ONCE) == 143
    assert entrypoint.signals() == 'TERM\n'
    assert gone(wrangler)
    eventually(lambda: gone(in_flight), f'{waiting} outlived the script')


def test_a_stop_after_wrangler_has_exited_ends_with_its_status(supervised):
    """wrangler can exit during a wait, before the watchdog looks again,
    and a stop then finds nothing to pass TERM on to. The script still
    ends the wait in flight and exits with the status wrangler left,
    rather than being ended by the `kill` that found it gone: set -e
    holds in a trap too."""
    entrypoint, spawn = supervised
    process = spawn(
        CLICKHOUSE_URL=CLICKHOUSE_URL, WRANGLER_SECONDS='0', **IN_PRODUCTION,
    )
    wrangler = entrypoint.pid_of('wrangler')
    sleeper = entrypoint.pid_of('sleep-60')
    eventually(lambda: gone(wrangler), 'wrangler never exited')

    process.send_signal(signal.SIGTERM)

    assert exit_status(process, within=AT_ONCE) == 0
    assert entrypoint.signals() == ''
    eventually(lambda: gone(sleeper), 'sleep-60 outlived the script')


def test_every_wait_is_a_step():
    """A shell runs a trap only once the foreground command returns, so
    a `sleep` or a probe the watchdog runs other than through `step`
    holds a stop back until it is done: the wait before a wedged
    Worker's KILL as much as those above."""
    commands = [
        line.strip() for line in ENTRYPOINT.read_text().splitlines()
        if re.search(r'\b(sleep|node)\s', line)
        and not line.lstrip().startswith('#')
    ]
    assert any(' node -e ' in c for c in commands), commands
    assert [c for c in commands if not c.startswith('step ')] == []


def test_no_kill_can_end_the_script():
    """`kill` fails once its target has gone, and set -e, which holds in
    a trap too, would end the script there, with kill's status rather
    than the one it meant: each `kill` says `|| true`."""
    kills = [
        line.strip() for line in ENTRYPOINT.read_text().splitlines()
        if re.match(r'\s*kill\s', line)
    ]
    assert kills, 'no kill found'
    assert [k for k in kills if not k.endswith('|| true')] == []
