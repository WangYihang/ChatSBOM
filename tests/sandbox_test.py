"""Container isolation for lockfile generation.

Resolving dependencies means executing project-controlled code: `mvn`
runs build plugins, a Gemfile *is* Ruby, `composer` runs scripts. None of
that may touch the host, so the command is built here and asserted on
without ever running Docker.

What happens around the command — the deadline, the output cap, the
container removed when either is hit — runs against a fake `docker` on
PATH (`FakeDocker`), which records its calls and plays the part a test
gives it. Nothing here needs a daemon or the network.
"""
import io
import json
import os
import re
import sys
import tarfile
import threading
import time
from collections.abc import Callable
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from structlog.testing import capture_logs

from chatsbom.core import sandbox
from chatsbom.core.sandbox import build_docker_command
from chatsbom.core.sandbox import lock_recipe_for
from chatsbom.core.sandbox import LOCK_RECIPES
from chatsbom.core.sandbox import recipes_for
from chatsbom.core.sandbox import SandboxLimits

#: What `generate_lockfile` would name a container; any name will do.
NAME = 'chatsbom-lock-0123456789abcdef0123456789abcdef'


@pytest.fixture
def command(tmp_path):
    (tmp_path / 'in').mkdir()
    return build_docker_command(
        recipe=lock_recipe_for('gem'),
        project_dir=tmp_path / 'in',
        limits=SandboxLimits(),
        name=NAME,
    )


def joined(command: list[str]) -> str:
    return ' '.join(command)


def mounts(command: list[str]) -> list[str]:
    return [command[i + 1] for i, a in enumerate(command) if a == '--mount']


# --- isolation ------------------------------------------------------------

def test_runs_through_docker(command):
    assert command[:2] == ['docker', 'run']


def test_container_is_removed_after_the_run(command):
    assert '--rm' in command


def test_the_container_is_named_so_it_can_be_removed(command):
    """Killing `docker run` does not stop the container it started.

    The deadline killed the client and nothing else: the container ran
    on, with its 2 GB and 2 CPUs, until the resolver exited by itself.
    A name is what `docker rm -f` can find it by.
    """
    assert '--name' in command
    assert command[command.index('--name') + 1] == NAME


def test_project_is_mounted_read_only(command, tmp_path):
    project = next(m for m in mounts(command) if 'target=/project' in m)
    assert 'readonly' in project, 'resolvers must not modify the source tree'


def test_no_host_path_is_writable(command):
    """The lockfile comes back on stdout, under a cap.

    It used to be written into a host directory mounted writable at
    /out, with no bound on its size, by project-controlled code: this
    test said that directory was the one writable path. There is none
    now.
    """
    assert mounts(command), 'the project is mounted'
    assert all('readonly' in m for m in mounts(command)), mounts(command)
    assert '--volume' not in command and '-v' not in command
    assert '/out' not in joined(command)


def test_the_project_is_the_only_host_path(command, tmp_path):
    """Nothing else from the host filesystem may be visible."""
    [mount] = mounts(command)
    assert f'source={tmp_path / "in"},' in mount
    assert 'target=/project' in mount


def test_docker_socket_is_never_mounted(command):
    assert 'docker.sock' not in joined(command)


def test_runs_as_a_non_root_user(command):
    assert '--user' in command
    user = command[command.index('--user') + 1]
    assert not user.startswith(
        '0:',
    ), 'root in the container is root on a mount'


def test_privileges_are_dropped(command):
    text = joined(command)
    assert '--cap-drop ALL' in text
    assert '--security-opt no-new-privileges' in text


def test_root_filesystem_is_read_only_with_a_scratch_tmpfs(command):
    text = joined(command)
    assert '--read-only' in text
    assert '--tmpfs /tmp' in text


def test_resources_are_bounded(command):
    text = joined(command)
    assert '--memory' in text
    assert '--cpus' in text
    assert '--pids-limit' in text


def test_network_is_available_because_resolvers_need_it(command):
    """The one thing we cannot take away: resolution fetches metadata."""
    assert '--network none' not in joined(command)


def test_the_resolver_runs_on_a_network_of_its_own(command):
    """Not the daemon's default bridge, where every container it runs
    can reach every other, and whatever else is on it."""
    assert '--network' in command
    assert command[command.index('--network') + 1] == sandbox.LOCK_NETWORK


def test_the_container_ends_itself_at_the_deadline(command):
    """A backstop for when nothing on this side is left to remove it:
    `docker compose down`, or a SIGKILL, ends this process before any
    `finally` runs, and the container would run on without it."""
    image = lock_recipe_for('gem').image
    after = command[command.index(image) + 1:]
    assert after[:4] == ['timeout', '-s', 'KILL', '300']
    assert after[4:6] == ['sh', '-c']
    assert after[6] == sandbox.container_script(lock_recipe_for('gem'))


def test_the_image_entrypoint_is_not_run(command):
    """composer's entrypoint asks `composer help` about the first word
    of the command, which is `timeout` now, not the `sh` it lets by."""
    assert command[command.index('--entrypoint') + 1] == ''
    assert command.index('--entrypoint') < command.index(
        lock_recipe_for('gem').image,
    )


def test_limits_are_configurable(tmp_path):
    (tmp_path / 'in').mkdir()
    command = build_docker_command(
        recipe=lock_recipe_for('gem'),
        project_dir=tmp_path / 'in',
        limits=SandboxLimits(memory='512m', cpus='0.5', pids=64, timeout=60),
        name=NAME,
    )
    text = joined(command)
    assert '--memory 512m' in text
    assert '--cpus 0.5' in text
    assert '--pids-limit 64' in text
    assert 'timeout -s KILL 60 sh -c' in text


# --- recipes --------------------------------------------------------------

@pytest.mark.parametrize('ecosystem', ['composer', 'gem'])
def test_supported_ecosystems_have_a_recipe(ecosystem):
    recipe = lock_recipe_for(ecosystem)
    assert recipe.image
    assert recipe.produces
    assert recipe.script


def test_unsupported_ecosystem_is_an_error():
    with pytest.raises(ValueError, match='no lockfile recipe'):
        lock_recipe_for('go')


def test_recipes_are_keyed_by_ecosystem_not_language():
    """A recipe runs wherever its manifest is, whatever the repository
    is labelled: the keys are `core/ecosystems.py`'s names."""
    from chatsbom.core.ecosystems import MEMBERS
    assert set(LOCK_RECIPES) <= set(MEMBERS)


def test_every_recipe_writes_a_file_syft_reads():
    """A lockfile Syft does not read changes nothing but the cache key.

    Java wrote `dependency-tree.txt` and Python `requirements.lock`.
    Syft 1.41.2 finds no package in either, and finds them all in the
    same text named `requirements.txt`: its Python cataloger reads
    `*requirements*.txt`, and its Java one `pom.xml`, `gradle.lockfile*`
    and archives.
    """
    from chatsbom.services.sbom_service import MANIFEST_NAMES
    for ecosystem, recipe in LOCK_RECIPES.items():
        unread = sorted(set(recipe.produces) - MANIFEST_NAMES)
        assert not unread, f'{ecosystem}: Syft never reads {unread}'
        assert recipe.manifest in MANIFEST_NAMES, ecosystem


@pytest.mark.parametrize(
    'ecosystem,unread', [
        ('maven', 'dependency-tree.txt'),
        ('pypi', 'requirements.lock'),
    ],
)
def test_maven_and_pypi_have_no_recipe_and_say_why(ecosystem, unread):
    """Withdrawn with a reason rather than dropped without a word:
    whoever runs `sbom lock --ecosystem maven` should learn why nothing
    happens."""
    with pytest.raises(ValueError, match='no lockfile recipe') as error:
        lock_recipe_for(ecosystem)
    assert unread in str(error.value)
    assert ecosystem not in LOCK_RECIPES


def test_every_recipe_pins_its_image_by_digest():
    """A tag moves with every rebuild of its image, so `composer:2.8`
    resolved the same project with another composer or PHP from one
    week to the next, which README called reproducible. This test
    accepted any tag but `latest`. The tag stays, for whoever reads it;
    Docker pulls by the digest."""
    for recipe in LOCK_RECIPES.values():
        assert re.fullmatch(
            r'[a-z0-9._/-]+:[\w.-]+@sha256:[0-9a-f]{64}', recipe.image,
        ), f'{recipe.image} is not pinned by digest'


def test_recipe_scripts_resolve_in_the_scratch_directory():
    """/project is read-only and nothing else from the host is mounted:
    every recipe copies the project into the tmpfs and resolves there.
    This test asked for a copy to /out, the writable mount there was."""
    for recipe in LOCK_RECIPES.values():
        assert f'cd {sandbox.WORKDIR}' in recipe.script, recipe.image
        assert '/out' not in recipe.script, recipe.image


def test_lockfiles_come_back_as_a_tar_on_stdout():
    """Everything the recipe prints goes to stderr, so that stdout holds
    the tar and nothing else, and a recipe that fails sends nothing."""
    for recipe in LOCK_RECIPES.values():
        script = sandbox.container_script(recipe)
        assert script.startswith('set -e; ')
        assert f'{{ {recipe.script}; }} >&2; ' in script
        assert script.endswith(
            f'tar -cf - -C {sandbox.WORKDIR} ' + ' '.join(recipe.produces),
        )


# --- container identity ---------------------------------------------------

def test_container_runs_as_the_invoking_user_so_it_can_read_the_project(
    command,
):
    """The project is a host directory the caller owns, and may be
    readable by no one else."""
    user = command[command.index('--user') + 1]
    if os.getuid() != 0:
        assert user == f'{os.getuid()}:{os.getgid()}'


def test_a_root_caller_falls_back_to_nobody(tmp_path, monkeypatch):
    from chatsbom.core.sandbox import NOBODY
    from chatsbom.core.sandbox import SandboxLimits
    monkeypatch.setattr('os.getuid', lambda: 0)
    assert SandboxLimits().resolved_user() == NOBODY


def test_explicit_user_wins(tmp_path):
    from chatsbom.core.sandbox import SandboxLimits
    assert SandboxLimits(user='1234:5678').resolved_user() == '1234:5678'


# --- rootless daemons invert the --user decision --------------------------

def test_a_rootless_daemon_omits_the_user_flag(tmp_path):
    """Under rootless Docker, `--user` is what broke the output write.

    A rootful daemon maps container uid 1000 to host uid 1000, so passing
    the invoking uid is what let the container write the bind-mounted
    output directory. A rootless daemon maps container *root* to the
    unprivileged host user instead, and an explicit `--user 1000` lands
    on a subuid that owns nothing — the resolver ran, and then
    `cp: /out/Gemfile.lock: Permission denied`. There is no output
    mount now, and that subuid could read only what anyone may.

    Verified against a real rootless daemon, `docker:27-dind-rootless`
    at the time, before the move to 29: container-root wrote a file
    owned by the host user, and the hostile Gemfile still could not
    touch /project or /etc.
    """
    (tmp_path / 'in').mkdir()
    command = build_docker_command(
        recipe=lock_recipe_for('gem'),
        project_dir=tmp_path / 'in',
        limits=SandboxLimits(),
        name=NAME,
        rootless_daemon=True,
    )
    assert '--user' not in command


def test_a_rootful_daemon_still_pins_the_user(command):
    assert '--user' in command


def test_rootless_keeps_every_other_restriction(tmp_path):
    """Dropping --user must not quietly drop the rest of the sandbox."""
    (tmp_path / 'in').mkdir()
    text = ' '.join(
        build_docker_command(
            recipe=lock_recipe_for('gem'),
            project_dir=tmp_path / 'in',
            limits=SandboxLimits(),
            name=NAME,
            rootless_daemon=True,
        ),
    )
    assert '--cap-drop ALL' in text
    assert '--security-opt no-new-privileges' in text
    assert '--read-only' in text
    assert 'readonly' in text
    assert '--pids-limit' in text
    assert f'--name {NAME}' in text
    assert f'--network {sandbox.LOCK_NETWORK}' in text


def test_daemon_rootlessness_is_detected_from_security_options():
    from chatsbom.core.sandbox import is_rootless_daemon_output
    rootless = 'name=seccomp,profile=builtin name=rootless name=cgroupns'
    rootful = 'name=apparmor,profile=default name=seccomp,profile=builtin'
    assert is_rootless_daemon_output(rootless)
    assert not is_rootless_daemon_output(rootful)
    assert not is_rootless_daemon_output('')


# --- choosing where to resolve ------------------------------------------------

def _targets(paths):
    return [(t.directory, t.ecosystem) for t in recipes_for(paths)]


def test_a_recipe_runs_where_its_manifest_is_not_at_the_root_only():
    """`sbom lock` resolved the root of a PHP- or Ruby-labelled
    repository. A Composer project under `backend/` of a repository
    labelled TypeScript was never resolved."""
    assert _targets([
        'package.json', 'package-lock.json',
        'backend/composer.json',
        'tools/docs/Gemfile',
    ]) == [('backend', 'composer'), ('tools/docs', 'gem')]


def test_a_directory_that_ships_its_lockfile_is_not_a_target():
    assert _targets([
        'composer.json', 'composer.lock',
        'api/composer.json',
        'site/Gemfile', 'site/Gemfile.lock',
    ]) == [('api', 'composer')]


def test_a_lockfile_without_its_manifest_is_nothing_to_resolve():
    assert _targets(['composer.lock', 'a/Gemfile.lock', 'go.mod']) == []


def test_targets_are_ordered_shallowest_first_and_capped():
    paths = [f'p{i:02d}/composer.json' for i in range(20)] + ['composer.json']
    targets = _targets(paths)
    assert len(targets) == 10
    assert targets[0] == ('', 'composer')
    assert targets[1:] == [(f'p{i:02d}', 'composer') for i in range(9)]


def test_both_recipes_can_run_in_one_directory():
    assert _targets(['Gemfile', 'composer.json']) == [
        ('', 'composer'), ('', 'gem'),
    ]


# --- a fake docker ------------------------------------------------------------

#: A `docker` that runs nothing. It records each call as a JSON array, one
#: a line, and plays the part the test planned: `info` prints security
#: options, `network` keeps one network's inter-container setting in a
#: file, `run` writes what it was given to stdout and stderr, sleeps, and
#: exits. Anything else, `rm -f` included, is only recorded.
FAKE_DOCKER = r'''
import json
import os
import sys
import time

here = os.environ['FAKE_DOCKER_DIR']
args = sys.argv[1:]
with open(os.path.join(here, 'calls.jsonl'), 'a') as calls:
    calls.write(json.dumps(args) + '\n')
with open(os.path.join(here, 'plan.json')) as handle:
    plan = json.load(handle)
network = os.path.join(here, 'network')

if args[:1] == ['info']:
    print(plan.get('security_options', 'name=seccomp,profile=builtin'))
elif args[:2] == ['network', 'inspect']:
    if not os.path.exists(network):
        sys.exit('Error response from daemon: network not found')
    with open(network) as handle:
        print(handle.read())
elif args[:2] == ['network', 'create']:
    setting = 'true'
    for at, arg in enumerate(args):
        if arg == '--opt':
            key, _, value = args[at + 1].partition('=')
            if key == 'com.docker.network.bridge.enable_icc':
                setting = value
    with open(network, 'w') as handle:
        handle.write(setting)
elif args[:1] == ['run']:
    run = plan.get('run', {})
    sys.stderr.buffer.write(b'e' * run.get('stderr_bytes', 0))
    sys.stderr.buffer.write(run.get('stderr', '').encode())
    sys.stderr.flush()
    if run.get('stdout_file'):
        with open(run['stdout_file'], 'rb') as handle:
            sys.stdout.buffer.write(handle.read())
    sys.stdout.buffer.write(b'x' * run.get('stdout_bytes', 0))
    sys.stdout.flush()
    time.sleep(run.get('sleep', 0))
    sys.exit(run.get('exit', 0))
'''


@dataclass
class FakeDocker:
    """The fake `docker`'s plan, and what it was asked to do."""

    directory: Path

    def plan(self, **plan: Any) -> None:
        (self.directory / 'plan.json').write_text(json.dumps(plan))

    def network(self, setting: str) -> None:
        """A network that exists already, with this ICC setting."""
        (self.directory / 'network').write_text(setting)

    def calls(self) -> list[list[str]]:
        path = self.directory / 'calls.jsonl'
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text().splitlines()]

    def runs(self) -> list[list[str]]:
        return [call for call in self.calls() if call[:1] == ['run']]

    def removed(self) -> list[str]:
        return [call[2] for call in self.calls() if call[:2] == ['rm', '-f']]


def _forget_the_daemon() -> None:
    """Drop what the sandbox caches about its daemon for the process —
    whether it is rootless, the resolvers' network — so that each test
    asks its own docker."""
    sandbox.daemon_is_rootless.cache_clear()
    sandbox.lock_network.cache_clear()


@pytest.fixture
def docker(tmp_path, monkeypatch) -> Iterator[FakeDocker]:
    directory = tmp_path / 'fake-docker'
    (directory / 'bin').mkdir(parents=True)
    executable = directory / 'bin' / 'docker'
    executable.write_text(f'#!{sys.executable}\n{FAKE_DOCKER}')
    executable.chmod(0o755)
    monkeypatch.setenv(
        'PATH', f'{directory / "bin"}{os.pathsep}{os.environ["PATH"]}',
    )
    monkeypatch.setenv('FAKE_DOCKER_DIR', str(directory))
    fake = FakeDocker(directory)
    fake.plan()
    _forget_the_daemon()
    yield fake
    _forget_the_daemon()


def name_of(run: list[str]) -> str | None:
    """The `--name` a `docker run` was given, if any."""
    return run[run.index('--name') + 1] if '--name' in run else None


Member = tuple[tarfile.TarInfo, bytes | None]


def regular(name: str, data: bytes) -> Member:
    info = tarfile.TarInfo(name)
    info.size = len(data)
    return info, data


def link(name: str, target: str, kind: bytes = tarfile.SYMTYPE) -> Member:
    info = tarfile.TarInfo(name)
    info.type = kind
    info.linkname = target
    return info, None


def directory(name: str) -> Member:
    info = tarfile.TarInfo(name)
    info.type = tarfile.DIRTYPE
    return info, None


def archive(*members: Member) -> bytes:
    """A tar of `members`, as a resolver's stdout would carry it."""
    buffer = io.BytesIO()
    with tarfile.open(fileobj=buffer, mode='w') as tar:
        for info, data in members:
            tar.addfile(info, io.BytesIO(data) if data is not None else None)
    return buffer.getvalue()


LOCKFILE = b'GEM\n  remote: https://rubygems.org/\n  specs:\n    rack (3.1.7)\n'


@pytest.fixture
def project(tmp_path) -> Path:
    project = tmp_path / 'in'
    project.mkdir()
    (project / 'Gemfile').write_text("source 'https://rubygems.org'\n")
    return project


@pytest.fixture
def out(tmp_path) -> Path:
    return tmp_path / 'out'


@pytest.fixture
def sends(tmp_path, docker) -> Callable[..., None]:
    """Plan a run whose stdout is `data`."""
    def plan(data: bytes, **run: Any) -> None:
        stdout = tmp_path / 'stdout.bin'
        stdout.write_bytes(data)
        docker.plan(run={'stdout_file': str(stdout), **run})
    return plan


def generate(project: Path, out: Path, **limits: Any) -> sandbox.LockResult:
    return sandbox.generate_lockfile(
        'gem', project, out, SandboxLimits(user='1000:1000', **limits),
    )


def test_a_resolved_lockfile_is_written_whole(docker, sends, project, out):
    sends(archive(regular('Gemfile.lock', LOCKFILE)))

    result = generate(project, out)

    assert result.ok, result
    assert result.produced == (out / 'Gemfile.lock',)
    assert (out / 'Gemfile.lock').read_bytes() == LOCKFILE
    assert sorted(p.name for p in out.iterdir()) == ['Gemfile.lock']
    [run] = docker.runs()
    name = name_of(run)
    assert name is not None and name.startswith(sandbox.CONTAINER_PREFIX)
    assert run[run.index('--network') + 1] == sandbox.LOCK_NETWORK
    # It ran to a clean end, so `--rm` has it: nothing to remove.
    assert docker.removed() == []


def test_every_run_gets_a_name_of_its_own(docker, sends, project, out):
    sends(archive(regular('Gemfile.lock', LOCKFILE)))

    generate(project, out)
    generate(project, out)

    first, second = (name_of(run) for run in docker.runs())
    assert first and second and first != second


def test_a_run_past_its_deadline_has_its_container_removed(
    docker, project, out,
):
    """The deadline killed the `docker` client, and SIGKILL is not
    passed on: the container ran on, unnamed, until the resolver was
    done, whenever that was."""
    docker.plan(run={'sleep': 60})

    started = time.monotonic()
    result = generate(project, out, timeout=1)
    elapsed = time.monotonic() - started

    [run] = docker.runs()
    assert name_of(run), 'no --name: nothing can remove the container'
    assert docker.removed() == [name_of(run)]
    assert elapsed < 30, 'the client was left to finish on its own'
    assert not result.ok
    assert result.returncode == sandbox.TIMED_OUT
    assert 'timed out after 1s' in result.stderr
    assert not out.exists()


@pytest.mark.parametrize(
    'error', [RuntimeError('a bug'), KeyboardInterrupt()],
    ids=['exception', 'interrupt'],
)
def test_an_error_while_waiting_removes_the_container(
    docker, project, out, monkeypatch, error,
):
    """Ctrl-C, or anything raised while the output is read, leaves the
    container running just as a timeout did: removed, then raised."""
    docker.plan(run={'sleep': 60})
    clients: list[Any] = []

    def fails(process: Any, *args: Any, **kwargs: Any) -> Any:
        clients.append(process)
        deadline = time.monotonic() + 10
        while not docker.runs() and time.monotonic() < deadline:
            time.sleep(0.02)
        raise error

    monkeypatch.setattr(sandbox, '_collect', fails)

    with pytest.raises(type(error)):
        generate(project, out)

    [run] = docker.runs()
    assert docker.removed() == [name_of(run)]
    [client] = clients
    assert client.poll() is not None, 'the docker client is still running'


def test_a_cancelled_run_has_its_container_removed(docker, project, out):
    """What `sbom lock --workers` tells every resolution in flight when
    one of them is interrupted: KeyboardInterrupt reaches the main
    thread only."""
    docker.plan(run={'sleep': 60})
    cancel = threading.Event()
    threading.Timer(0.5, cancel.set).start()

    result = sandbox.generate_lockfile(
        'gem', project, out, SandboxLimits(user='1000:1000'), cancel=cancel,
    )

    assert result.returncode == sandbox.INTERRUPTED
    [run] = docker.runs()
    assert docker.removed() == [name_of(run)]


def test_a_run_cancelled_before_it_starts_runs_nothing(docker, project, out):
    cancel = threading.Event()
    cancel.set()

    result = sandbox.generate_lockfile(
        'gem', project, out, SandboxLimits(user='1000:1000'), cancel=cancel,
    )

    assert not result.ok
    assert docker.runs() == []


def test_output_over_the_cap_is_refused_and_the_container_removed(
    docker, project, out,
):
    """/out was a host directory with no bound on what the resolver,
    running project-controlled code, wrote into it."""
    docker.plan(run={'stdout_bytes': 1024 * 1024, 'sleep': 60})

    started = time.monotonic()
    result = generate(project, out, output_bytes=64 * 1024)

    assert time.monotonic() - started < 30, 'it was left to run'
    assert not result.ok
    assert result.returncode == sandbox.KILLED
    assert result.produced == ()
    [run] = docker.runs()
    assert docker.removed() == [name_of(run)]
    assert not out.exists()


def test_a_lockfile_over_the_cap_is_refused_even_from_a_clean_exit(
    docker, sends, project, out,
):
    sends(archive(regular('Gemfile.lock', b'x' * 128 * 1024)))

    result = generate(project, out, output_bytes=64 * 1024)

    assert not result.ok
    assert result.produced == ()
    assert not out.exists()


def test_only_the_tail_of_stderr_is_kept(docker, sends, project, out):
    """Resolvers print their progress there, and a hostile one can
    print forever. The error is at the end."""
    sends(
        archive(regular('Gemfile.lock', LOCKFILE)),
        stderr_bytes=4 * 1024 * 1024, stderr='the error, at the end',
    )

    result = generate(project, out)

    assert result.ok
    assert len(result.stderr) <= sandbox.STDERR_TAIL
    assert result.stderr.endswith('the error, at the end')


@pytest.mark.parametrize(
    'member', [
        link('Gemfile.lock', '/etc/passwd'),
        link('Gemfile.lock', 'Gemfile', tarfile.LNKTYPE),
        directory('Gemfile.lock'),
        regular('../Gemfile.lock', LOCKFILE),
        regular('/tmp/Gemfile.lock', LOCKFILE),
        regular('sub/Gemfile.lock', LOCKFILE),
        regular('composer.lock', LOCKFILE),
    ],
    ids=[
        'symlink', 'hardlink', 'directory', 'parent', 'absolute',
        'subdirectory', 'another-recipes-name',
    ],
)
def test_nothing_but_a_regular_file_named_in_produces_is_written(
    docker, sends, project, out, tmp_path, member,
):
    """The tar is the resolver's to write: project code runs as the same
    user as the script that makes it. A link would have `sbom generate`
    read whatever it points at on the host, a path would land outside
    the directory, and another name would put another ecosystem's
    lockfile into the scan."""
    sends(archive(member))

    with capture_logs() as logs:
        result = generate(project, out)

    assert result.produced == ()
    assert not result.ok
    assert not out.exists() or list(out.iterdir()) == []
    assert not (tmp_path / 'Gemfile.lock').exists()
    warned = [e for e in logs if e['log_level'] == 'warning']
    assert [e['name'] for e in warned] == [member[0].name], warned


def test_a_lockfile_is_kept_and_what_came_with_it_dropped(
    docker, sends, project, out, tmp_path,
):
    sends(
        archive(
            regular('../x', b'escaped\n'),
            regular('Gemfile.lock', LOCKFILE),
            regular('package-lock.json', b'{"packages": {}}\n'),
            link('composer.lock', '/etc/passwd'),
            regular('Gemfile.lock', b'a second copy\n'),
        ),
    )

    with capture_logs() as logs:
        result = generate(project, out)

    assert result.ok
    assert result.produced == (out / 'Gemfile.lock',)
    assert sorted(p.name for p in out.iterdir()) == ['Gemfile.lock']
    assert (out / 'Gemfile.lock').read_bytes() == LOCKFILE
    assert not (tmp_path / 'x').exists()
    warned = sorted(e['name'] for e in logs if e['log_level'] == 'warning')
    assert warned == [
        '../x', 'Gemfile.lock', 'composer.lock', 'package-lock.json',
    ]


def test_a_failed_resolution_writes_nothing(docker, sends, project, out):
    """Whatever it sent: the exit status says the resolution failed."""
    sends(archive(regular('Gemfile.lock', LOCKFILE)), exit=1)

    result = generate(project, out)

    assert not result.ok
    assert result.returncode == 1
    assert not out.exists()


def test_output_that_is_not_a_tar_writes_nothing(docker, sends, project, out):
    sends(b'resolving... done\n')

    with capture_logs() as logs:
        result = generate(project, out)

    assert not result.ok
    assert not out.exists()
    assert any(e['log_level'] == 'warning' for e in logs)


# --- the resolvers' network ---------------------------------------------------

def test_the_lock_network_is_made_with_inter_container_traffic_off(docker):
    """Resolvers run at once under `--workers`, each project-controlled,
    and none should reach another. They still reach the registries, so
    it is not `--internal`."""
    assert sandbox.lock_network() == sandbox.LOCK_NETWORK

    [create] = [c for c in docker.calls() if c[:2] == ['network', 'create']]
    assert create[-1] == sandbox.LOCK_NETWORK
    assert 'com.docker.network.bridge.enable_icc=false' in create
    assert '--internal' not in create


def test_an_existing_lock_network_is_used_as_it_is(docker):
    docker.network('false')

    assert sandbox.lock_network() == sandbox.LOCK_NETWORK

    assert [c for c in docker.calls() if c[:2] == ['network', 'create']] == []


@pytest.mark.parametrize('setting', ['true', ''])
def test_a_lock_network_that_lets_containers_talk_is_refused(docker, setting):
    """Made by hand, or by something else: not the network this relies
    on, and so not used."""
    docker.network(setting)

    with pytest.raises(sandbox.SandboxError, match='docker network rm'):
        sandbox.lock_network()
