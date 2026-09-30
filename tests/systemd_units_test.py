"""deploy/systemd, read as systemd reads it (#20).

Nothing here runs systemd. The units could not have run anywhere: they
named a checkout under the project's old name, started chatsbom through
`uv run`, which cannot start under `ProtectHome=read-only`, and wrote
where their sandbox made read-only. These check each of those from the
text, with the paths the CLI writes taken from the CLI itself.

The units were two timers, a slice of the old pipeline every quarter of
an hour and a retention pass a day. The collector is one process now
(#171), `chatsbom collect`, which keeps its own time and prunes in its
own index pass: one service, which runs until it is stopped.
"""
import re
import shlex
from pathlib import Path

import pytest

from chatsbom.collector.index import KILL_AFTER
from chatsbom.collector.process import FINISH
from chatsbom.core.config import PathConfig

ROOT = Path(__file__).resolve().parent.parent
UNITS = ROOT / 'deploy' / 'systemd'
SERVICES = sorted(UNITS.glob('*.service'))
TIMERS = sorted(UNITS.glob('*.timer'))

#: What may lead an Exec*= command: `-` ignores its failure, `+` runs it
#: outside the sandbox, and so on. Not part of the path.
EXEC_PREFIXES = '@-:+!|'

#: flock options that take a value, so the lock file is not mistaken
#: for one.
FLOCK_VALUED = {'-w', '--timeout', '-E', '--conflict-exit-code'}

Unit = dict[str, list[tuple[str, str]]]


def read_unit(path: Path) -> Unit:
    """Each section's settings, in order, as systemd reads them.

    A backslash at the end of a line continues it, comment lines are
    dropped, and a setting given twice stays twice: systemd adds to a
    list setting such as ReadWritePaths= rather than replacing it.
    """
    sections: Unit = {}
    current: list[tuple[str, str]] | None = None
    pending = ''
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line[0] in '#;':
            continue
        if line.endswith('\\'):
            pending += line[:-1] + ' '
            continue
        line, pending = pending + line, ''
        if line.startswith('[') and line.endswith(']'):
            current = sections.setdefault(line[1:-1], [])
            continue
        assert current is not None, f'{path.name}: {line!r} is in no section'
        key, _, value = line.partition('=')
        current.append((key.strip(), value.strip()))
    return sections


def values(unit: Unit, section: str, key: str) -> list[str]:
    return [value for name, value in unit.get(section, []) if name == key]


def one(unit: Unit, section: str, key: str) -> str:
    found = values(unit, section, key)
    assert len(found) == 1, f'{key}= is set {len(found)} times'
    return found[0]


def command(value: str) -> list[str]:
    """An Exec*= line as words, the prefixes off its executable."""
    words = shlex.split(value)
    words[0] = words[0].lstrip(EXEC_PREFIXES)
    return words


def under_flock(words: list[str]) -> tuple[str | None, list[str]]:
    """(the lock file, the command) for `flock [options] file command`;
    (None, the words) for a command run without it."""
    if Path(words[0]).name != 'flock':
        return None, words
    rest = iter(words[1:])
    for word in rest:
        if word in FLOCK_VALUED:
            next(rest)
        elif not word.startswith('-'):
            return word, list(rest)
    raise AssertionError(f'flock with no command: {words}')


def invocation(unit: Unit) -> tuple[str | None, list[str]]:
    return under_flock(command(one(unit, 'Service', 'ExecStart')))


def writable(unit: Unit) -> list[str]:
    """Every ReadWritePaths= entry, `-` and all."""
    return [
        path
        for value in values(unit, 'Service', 'ReadWritePaths')
        for path in shlex.split(value)
    ]


def chatsbom_arguments(run: list[str]) -> list[str]:
    """What follows `chatsbom` in a command, however it is launched."""
    for index, word in enumerate(run):
        if Path(word).name == 'chatsbom':
            return run[index + 1:]
    raise AssertionError(f'no chatsbom in {run}')


def writes(arguments: list[str]) -> set[Path]:
    """The directories a chatsbom command writes, relative to where it
    runs — from the CLI's own defaults, so a path it moves is followed.
    """
    paths = PathConfig()
    known: dict[tuple[str, ...], set[Path]] = {
        # The store and collector.sqlite, and the Syft cache; its index
        # pass writes under data/ too.
        ('collect',): {paths.base_data_dir, paths.cache_dir},
    }
    subcommand = tuple(arguments[:1])
    assert subcommand in known, (
        f'what does `chatsbom {" ".join(subcommand)}` write?'
    )
    return known[subcommand]


@pytest.mark.parametrize('path', SERVICES, ids=lambda path: path.name)
def test_the_checkout_is_the_instance_not_a_path_in_the_file(path):
    """The units named `%h/<the old name>` in four places each, a
    checkout that no longer exists under that name.

    As templates, the checkout is the instance — `systemd-escape --path`
    of it, so `chatsbom-collect@home-alice-ChatSBOM.service` runs in
    /home/alice/ChatSBOM — and no path is written into them at all.
    """
    assert path.name.endswith('@.service'), 'not a template'
    unit = read_unit(path)
    checkout = one(unit, 'Service', 'WorkingDirectory')
    assert checkout == '%f'
    for value in values(unit, 'Service', 'EnvironmentFile'):
        assert value.lstrip('-') == f'{checkout}/.env'
    for entry in writable(unit):
        assert entry.lstrip('-').startswith(f'{checkout}/'), entry


@pytest.mark.parametrize('path', SERVICES, ids=lambda path: path.name)
def test_it_does_not_go_through_uv(path):
    """`uv run` writes its cache under ~/.cache before it runs anything,
    and ProtectHome=read-only forbids that: it exits 2, `--frozen
    --no-sync` or not, and chatsbom never starts."""
    for key in ('ExecStartPre', 'ExecStart'):
        for value in values(read_unit(path), 'Service', key):
            assert 'uv' not in {Path(word).name for word in command(value)}


@pytest.mark.parametrize('path', SERVICES, ids=lambda path: path.name)
def test_it_runs_the_checkouts_own_chatsbom(path):
    """The one `uv sync` put in the checkout's .venv, which starts
    without writing anything."""
    unit = read_unit(path)
    checkout = one(unit, 'Service', 'WorkingDirectory')
    _, run = invocation(unit)
    assert run[0] == f'{checkout}/.venv/bin/chatsbom'


@pytest.mark.parametrize('path', SERVICES, ids=lambda path: path.name)
def test_every_command_is_an_absolute_path(path):
    """systemd looks a bare name up on a search path of its own, never
    in the working directory."""
    unit = read_unit(path)
    for key in ('ExecStartPre', 'ExecStart'):
        for value in values(unit, 'Service', key):
            _, run = under_flock(command(value))
            for executable in {command(value)[0], run[0]}:
                assert executable.startswith(('/', '%h/', '%f/')), value


@pytest.mark.parametrize('path', SERVICES, ids=lambda path: path.name)
def test_every_directory_it_writes_is_writable(path):
    """ProtectHome=read-only leaves the checkout read-only but for its
    ReadWritePaths=. `.requests-cache` was not among them, and the old
    pipeline opened its HTTP cache there before its first request."""
    unit = read_unit(path)
    checkout = one(unit, 'Service', 'WorkingDirectory')
    _, run = invocation(unit)
    needed = {
        f'{checkout}/{directory}'
        for directory in writes(chatsbom_arguments(run))
    }
    assert needed - set(writable(unit)) == set()


@pytest.mark.parametrize('path', SERVICES, ids=lambda path: path.name)
def test_it_needs_no_lock_of_its_own(path):
    """The sync unit ran its slice under flock, so that one run by hand
    could not overlap it. The collector holds collector.sqlite's lock
    for as long as it runs, in data/, and a second is refused whoever
    started it (collector_state_test)."""
    lock, run = invocation(read_unit(path))
    assert lock is None
    assert chatsbom_arguments(run) == ['collect']


@pytest.mark.parametrize('path', SERVICES, ids=lambda path: path.name)
def test_it_runs_until_stopped_and_comes_back_after_a_failure(path):
    """A process that runs for good, not a slice a timer starts: it
    exits 0 when stopped and 1 on what no pause mends, a refused token
    say, and comes back after a while if it failed."""
    unit = read_unit(path)
    assert one(unit, 'Service', 'Type') == 'exec'
    assert one(unit, 'Service', 'Restart') == 'on-failure'
    assert 'RestartSec' in dict(unit['Service'])
    assert values(unit, 'Service', 'TimeoutStartSec') == []


@pytest.mark.parametrize('path', SERVICES, ids=lambda path: path.name)
def test_a_stop_ends_it_within_its_time(path):
    """SIGTERM, systemd's own, gives a collection in flight `FINISH`
    seconds and an index step `KILL_AFTER` more; the unit waits for
    both, with room, before it kills what is left."""
    unit = read_unit(path)
    assert values(unit, 'Service', 'KillSignal') in ([], ['SIGTERM'])
    stop = re.fullmatch(r'(\d+)s?', one(unit, 'Service', 'TimeoutStopSec'))
    assert stop and int(stop[1]) >= FINISH + KILL_AFTER + 5
    # The whole cgroup at the end, Syft and git and the index pass's
    # steps with it.
    assert values(unit, 'Service', 'KillMode') in ([], ['control-group'])


@pytest.mark.parametrize('path', SERVICES, ids=lambda path: path.name)
def test_a_fresh_checkout_does_not_stop_it(path):
    """A ReadWritePaths= directory must exist when the sandbox is built,
    or the unit fails before it starts (226/NAMESPACE), and nothing
    inside the sandbox could create it. `.requests-cache` appears on
    first use, so a fresh checkout has none; the unit makes each one
    first, outside the sandbox (`+`)."""
    unit = read_unit(path)
    made: set[str] = set()
    for value in values(unit, 'Service', 'ExecStartPre'):
        words = command(value)
        if value.startswith('+') and Path(words[0]).name == 'mkdir':
            assert '-p' in words, value
            made |= {word for word in words[1:] if not word.startswith('-')}
    required = {entry for entry in writable(unit) if not entry.startswith('-')}
    assert required - made == set()


@pytest.mark.parametrize('path', SERVICES, ids=lambda path: path.name)
def test_a_checkout_without_an_env_file_is_not_an_error(path):
    """Without the `-`, a missing `.env` failed the unit outright; the
    CLI runs on its defaults without one."""
    for value in values(read_unit(path), 'Service', 'EnvironmentFile'):
        assert value.startswith('-'), value


def test_the_collector_is_the_one_unit():
    """No timer: the collector keeps its own time."""
    assert [path.name for path in SERVICES] == ['chatsbom-collect@.service']
    assert TIMERS == []
