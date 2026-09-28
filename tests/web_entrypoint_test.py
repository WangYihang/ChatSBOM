"""deploy/web-entrypoint.sh, run for real against a fake `npx` (#18).

The dashboard's secrets reached the Worker as `--var` arguments, and a
process's arguments are readable by every user on the host: uid 65534
read the ClickHouse password and the Anthropic key off wrangler's
/proc/<pid>/cmdline. These run the script with an `npx` that records
how it was called and exits, so what reaches the command line is
observed rather than read off the source.

The script starts wrangler one of two ways: under its watchdog, which
probes the Worker and exits when it wedges, or, with WATCHDOG_DISABLED,
as a bare `exec`. Every test here runs both ways, the watchdog on the
timings of a test and with a `node` of its own for the probe.
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

#: Records its arguments, NUL-separated, and where it ran; starts nothing.
#: With NPX_SECONDS set it runs that long instead, noting a TERM that
#: comes first, and says it is running only once its trap is set.
FAKE_NPX = """#!/bin/sh
printf '%s\\0' "$@" > "$RECORD/argv"
pwd > "$RECORD/cwd"
if [ -n "${NPX_SECONDS:-}" ]; then
    "$REAL_SLEEP" "$NPX_SECONDS" &
    trap 'echo TERM >> "$RECORD/signals"; kill $!; exit 143' TERM
    echo $$ > "$RECORD/npx.new" && mv "$RECORD/npx.new" "$RECORD/npx"
    wait
fi
"""

#: The watchdog's probe: `node -e` asking the Worker which backend it
#: is. This one asks nothing, so no test depends on what listens on
#: 8787; it answers PROBE_STATUS, 0 (healthy) unless told otherwise.
FAKE_NODE = """#!/bin/sh
echo probe >> "$RECORD/probes"
exit "${PROBE_STATUS:-0}"
"""

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

#: How long a stop may take: Docker waits ten seconds before SIGKILL.
PROMPTLY = 5

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


@dataclass
class Started:
    returncode: int
    stderr: str
    #: None when `npx` never ran.
    argv: list[str] | None
    cwd: Path | None


class Entrypoint:
    def __init__(self, tmp_path: Path, mode: str) -> None:
        self.bin = tmp_path / 'bin'
        self.bin.mkdir()
        for name, script in (('npx', FAKE_NPX), ('node', FAKE_NODE)):
            fake = self.bin / name
            fake.write_text(script)
            fake.chmod(0o755)
        self.web = tmp_path / 'web'
        self.web.mkdir()
        self.record = tmp_path / 'record'
        self.record.mkdir()
        self.elsewhere = tmp_path
        self.mode = mode

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
        return subprocess.Popen(
            [str(ENTRYPOINT)],
            env=self.environment(env),
            cwd=self.elsewhere,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.PIPE,
            text=True,
            # A process group of its own, so that whatever it leaves
            # running can be found and stopped afterwards.
            start_new_session=True,
        )

    def pid_of(self, name: str) -> int:
        """The pid a fake wrote, once it has."""
        deadline = time.monotonic() + 10
        while not (self.record / name).exists():
            if time.monotonic() > deadline:
                pytest.fail(f'no {name} within 10s')
            time.sleep(0.02)
        return int((self.record / name).read_text())

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
def entrypoint(request, tmp_path: Path) -> Entrypoint:
    return Entrypoint(tmp_path, request.param)


def test_no_secret_reaches_the_command_line(entrypoint):
    started = entrypoint.start(CLICKHOUSE_URL=CLICKHOUSE_URL, **SECRETS)

    assert started.returncode == 0, started.stderr
    assert started.argv is not None, 'npx never ran'
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
    processes: list[subprocess.Popen[str]] = []

    def spawn(**env: str) -> subprocess.Popen[str]:
        process = started.spawn(**env)
        processes.append(process)
        return process

    yield started, spawn
    for process in processes:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()


def test_a_stop_reaches_wrangler_under_the_watchdog(supervised):
    """Under the watchdog wrangler is the script's background job, not
    what it execs: `docker stop` must still reach wrangler, and be
    waited for, rather than be waited out and end in SIGKILL."""
    entrypoint, spawn = supervised
    process = spawn(CLICKHOUSE_URL=CLICKHOUSE_URL, NPX_SECONDS='60')
    wrangler = entrypoint.pid_of('npx')

    process.send_signal(signal.SIGTERM)

    assert process.wait(timeout=PROMPTLY) != 0
    assert (entrypoint.record / 'signals').read_text() == 'TERM\n'
    assert gone(wrangler)


def test_a_wedged_worker_is_stopped_so_the_container_restarts(supervised):
    """A Worker that stops answering does not exit, and a restart
    policy acts only on an exit. After WATCHDOG_FAILURES failed probes in
    a row, wrangler is stopped and the script exits non-zero."""
    entrypoint, spawn = supervised
    process = spawn(
        CLICKHOUSE_URL=CLICKHOUSE_URL, NPX_SECONDS='60',
        PROBE_STATUS='1', WATCHDOG_FAILURES='2',
    )
    wrangler = entrypoint.pid_of('npx')

    # TERM, then five seconds before a KILL.
    assert process.wait(timeout=PROMPTLY + 5) == 1
    assert (entrypoint.record / 'probes').read_text() == 'probe\nprobe\n'
    assert (entrypoint.record / 'signals').read_text() == 'TERM\n'
    assert gone(wrangler)
    assert process.stderr is not None
    assert 'wedged' in process.stderr.read()
