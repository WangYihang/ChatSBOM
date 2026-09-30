"""Container isolation for lockfile generation.

Resolving dependencies means executing project-controlled code: `mvn`
runs build plugins, a Gemfile *is* Ruby, `composer` runs scripts. None of
that may touch the host, so the command is built here and asserted on
without ever running Docker.

What happens around the command — the deadline, the output cap, the
container removed when either is hit, and the network and the proxy
each resolution gets (#168) — runs against a fake `docker` on PATH
(`FakeDocker`), which records its calls and plays the part a test gives
it. Nothing here needs a daemon or the network.
"""
import io
import json
import os
import re
import shlex
import sys
import tarfile
import threading
import time
from collections.abc import Callable
from collections.abc import Iterator
from dataclasses import dataclass
from dataclasses import replace
from pathlib import Path
from typing import Any

import pytest
from structlog.testing import capture_logs

from chatsbom.core import egress
from chatsbom.core import sandbox
from chatsbom.core.sandbox import build_docker_command
from chatsbom.core.sandbox import lock_recipe_for
from chatsbom.core.sandbox import LOCK_RECIPES
from chatsbom.core.sandbox import recipes_for
from chatsbom.core.sandbox import SandboxLimits

#: What `generate_lockfile` would name a container, and the network it
#: makes the resolution; any names will do.
NAME = 'chatsbom-lock-0123456789abcdef0123456789abcdef'
NETWORK = 'chatsbom-resolution-0123456789abcdef0123456789abcdef'


@pytest.fixture
def command(tmp_path):
    (tmp_path / 'in').mkdir()
    return build_docker_command(
        recipe=lock_recipe_for('gem'),
        project_dir=tmp_path / 'in',
        limits=SandboxLimits(),
        name=NAME,
        network=NETWORK,
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


def environment(command: list[str]) -> dict[str, str]:
    """What a `docker run` sets in its container's environment."""
    return dict(
        command[i + 1].partition('=')[::2]
        for i, a in enumerate(command) if a == '--env'
    )


def test_the_registries_are_reached_through_the_proxy(command):
    """Resolution fetches metadata: the one thing the network cannot be
    taken away for. What a resolver reaches is its proxy, named in its
    environment for each tool's reading of it: curl's, Composer's and
    Ruby's (#168)."""
    assert '--network none' not in joined(command)
    assert {
        name: value for name, value in environment(command).items()
        if name.lower() in ('http_proxy', 'https_proxy', 'no_proxy')
    } == {
        'http_proxy': sandbox.PROXY_URL, 'https_proxy': sandbox.PROXY_URL,
        'HTTP_PROXY': sandbox.PROXY_URL, 'HTTPS_PROXY': sandbox.PROXY_URL,
        'no_proxy': '', 'NO_PROXY': '',
    }
    assert sandbox.PROXY_URL == (
        f'http://{sandbox.PROXY_ALIAS}:{sandbox.PROXY_PORT}'
    )


def test_the_resolver_runs_on_a_network_of_its_own(command):
    """Not the daemon's default bridge, where every container it runs
    can reach every other, and whatever else is on it; and on that one
    network alone."""
    assert [
        command[i + 1] for i, a in enumerate(command) if a == '--network'
    ] == [NETWORK]


def test_the_image_is_never_pulled_by_the_run(command):
    """Every image is pulled before the pass resolves anything
    (`sandbox.prepare`), over the daemon's own network: a run finds its
    image, or fails, and pulls nothing while a resolver is running."""
    assert command[command.index('--pull') + 1] == 'never'


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
        network=NETWORK,
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
    Syft finds no package in either, on 1.41.2 or on 1.52.0, and finds
    them all in the same text named `requirements.txt`: its Python
    cataloger reads `*requirements*.txt`, and its Java one `pom.xml`,
    `gradle.lockfile*` and archives.
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


def test_composer_resolves_what_the_constraints_admit():
    """Composer 2.9 leaves out of `update` every version a security
    advisory names, and 2.10 every version a malware list names, as a
    project's composer.json may also ask. On composer:2.10 a project
    whose constraints admit only such versions got no lockfile: "found
    guzzlehttp/guzzle[6.3.0, ..., 6.3.3] but these were not loaded,
    because they are affected by security advisories". Every policy is
    off, over what composer.json says, and nothing is audited."""
    words = lock_recipe_for('composer').script.split()
    at = words.index('update')
    assert words[at - 2:at] == ['COMPOSER_POLICY=0', 'composer']
    assert '--no-audit' in words[at:]


def test_bundler_has_a_home_it_can_use() -> None:
    """The container runs as the invoking user, or as nobody, whose home
    is /nonexistent, on a read-only root: Bundler warned "`/nonexistent`
    is not a directory" on every resolution (#118), and made a home of
    its own under /tmp. HOME is on the tmpfs, as everything the recipe
    writes is."""
    exported: dict[str, str] = {}
    for statement in lock_recipe_for('gem').script.split(';'):
        words = shlex.split(statement)
        if words[:1] == ['bundle']:
            break
        if words[:1] == ['export']:
            exported.update(word.partition('=')[::2] for word in words[1:])
    else:
        pytest.fail('the recipe never runs bundle')
    assert exported.get('HOME') == '/tmp'


def test_deploy_checks_a_resolvers_view_from_the_composer_image():
    """DEPLOY.md shows what a resolver can reach from the image the
    composer recipe runs: a pin moved in one place alone would show
    another image's."""
    deploy = (Path(__file__).resolve().parents[1] / 'DEPLOY.md').read_text()
    images = set(re.findall(r'\bcomposer:[\w.-]+@sha256:[0-9a-f]{64}', deploy))
    assert images == {lock_recipe_for('composer').image}


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
        network=NETWORK,
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
            network=NETWORK,
            rootless_daemon=True,
        ),
    )
    assert '--cap-drop ALL' in text
    assert '--security-opt no-new-privileges' in text
    assert '--read-only' in text
    assert 'readonly' in text
    assert '--pids-limit' in text
    assert f'--name {NAME}' in text
    assert f'--network {NETWORK}' in text


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
#: options; `network` makes, shows and removes networks, each a JSON file
#: as `network inspect` prints one; `image inspect` finds an image the
#: plan names or one pulled, and `pull` pulls one; `ps` lists the
#: leftovers the plan names; the proxy's `run` says it listens, then
#: says the lines the plan gives it, and runs until `rm -f` removes it;
#: a resolver's `run` writes what it was given to stdout and stderr,
#: sleeps, and exits. Anything else is only recorded.
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
networks = os.path.join(here, 'networks')
removed = os.path.join(here, 'removed')
pulled = os.path.join(here, 'pulled')
for directory in (networks, removed, pulled):
    os.makedirs(directory, exist_ok=True)


def option(flag):
    return [args[at + 1] for at, arg in enumerate(args[:-1]) if arg == flag]


def named(prefix):
    return any(name.startswith(prefix) for name in option('--name'))


if args[:1] == ['info']:
    print(plan.get('security_options', 'name=seccomp,profile=builtin'))
elif args[:2] == ['network', 'inspect']:
    path = os.path.join(networks, args[-1] + '.json')
    if not os.path.exists(path):
        sys.exit('Error response from daemon: network not found')
    with open(path) as handle:
        print(handle.read())
elif args[:2] == ['network', 'create']:
    if plan.get('network_create_exit'):
        sys.stderr.write('could not find an available address pool\n')
        sys.exit(plan['network_create_exit'])
    made = {
        'Name': args[-1],
        'Driver': (option('--driver') or ['bridge'])[0],
        'Internal': '--internal' in args,
        'Options': dict(o.partition('=')[::2] for o in option('--opt')),
        'Labels': dict(o.partition('=')[::2] for o in option('--label')),
    }
    with open(os.path.join(networks, args[-1] + '.json'), 'w') as handle:
        json.dump(made, handle)
elif args[:2] == ['network', 'rm']:
    for name in args[2:]:
        if os.path.exists(os.path.join(networks, name + '.json')):
            os.remove(os.path.join(networks, name + '.json'))
elif args[:2] == ['image', 'inspect']:
    image = args[-1]
    if image not in plan.get('images', []) and not os.path.exists(
        os.path.join(pulled, image.replace('/', '_')),
    ):
        sys.exit('Error: No such image: ' + image)
    print('sha256:' + '0' * 64)
elif args[:1] == ['pull']:
    if plan.get('pull_exit'):
        sys.stderr.write('pull access denied\n')
        sys.exit(plan['pull_exit'])
    open(os.path.join(pulled, args[-1].replace('/', '_')), 'w').close()
elif args[:1] == ['ps']:
    for name in plan.get('leftovers', []):
        print(name)
elif args[:1] == ['rm']:
    for name in args[2:]:
        open(os.path.join(removed, name), 'w').close()
elif args[:1] == ['run'] and named('chatsbom-proxy-'):
    proxy = plan.get('proxy', {})
    if proxy.get('exit') is not None:
        sys.stderr.write('docker: Error response from daemon\n')
        sys.exit(proxy['exit'])
    [name] = option('--name')
    time.sleep(proxy.get('delay', 0))
    with open(os.path.join(here, 'calls.jsonl'), 'a') as calls:
        calls.write(json.dumps(['listening', name]) + '\n')
    print(json.dumps({
        'event': 'listening', 'address': '0.0.0.0:3128',
        'allow': sorted(option('--allow')),
    }), flush=True)
    for line in proxy.get('lines', []):
        print(json.dumps(line), flush=True)
    deadline = time.monotonic() + 60
    while not os.path.exists(os.path.join(removed, name)):
        if time.monotonic() > deadline:
            sys.exit('the proxy was never removed')
        time.sleep(0.02)
    sys.exit(137)
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

    def network(self, name: str, **made: Any) -> None:
        """A network that exists already, as `network inspect` prints
        it."""
        networks = self.directory / 'networks'
        networks.mkdir(exist_ok=True)
        (networks / f'{name}.json').write_text(
            json.dumps({'Name': name, **made}),
        )

    def networks(self) -> dict[str, Any]:
        """The networks there are now, by name."""
        return {
            path.stem: json.loads(path.read_text())
            for path in (self.directory / 'networks').glob('*.json')
        }

    def calls(self) -> list[list[str]]:
        path = self.directory / 'calls.jsonl'
        if not path.exists():
            return []
        return [json.loads(line) for line in path.read_text().splitlines()]

    def runs(self) -> list[list[str]]:
        """The resolvers' `docker run`s."""
        return [
            call for call in self.calls() if call[:1] == ['run']
            and (name_of(call) or '').startswith(sandbox.CONTAINER_PREFIX)
        ]

    def proxies(self) -> list[list[str]]:
        """The proxies' `docker run`s."""
        return [
            call for call in self.calls() if call[:1] == ['run']
            and (name_of(call) or '').startswith(sandbox.PROXY_PREFIX)
        ]

    def created(self) -> list[list[str]]:
        return [c for c in self.calls() if c[:2] == ['network', 'create']]

    def removed(self) -> list[str]:
        return [call[2] for call in self.calls() if call[:2] == ['rm', '-f']]

    def networks_removed(self) -> list[str]:
        return [
            name for call in self.calls() if call[:2] == ['network', 'rm']
            for name in call[2:]
        ]


def _forget_the_daemon() -> None:
    """Drop what the sandbox caches about its daemon for the process —
    whether it is rootless, the proxies' network — so that each test
    asks its own docker."""
    sandbox.daemon_is_rootless.cache_clear()
    sandbox.egress_network.cache_clear()


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


def ours(run: list[str]) -> tuple[str, str, str]:
    """The names of a resolution's resolver, proxy and network, by the
    resolver's `docker run`."""
    name = name_of(run)
    assert name is not None and name.startswith(sandbox.CONTAINER_PREFIX)
    key = name.removeprefix(sandbox.CONTAINER_PREFIX)
    return (
        name, f'{sandbox.PROXY_PREFIX}{key}', f'{sandbox.NETWORK_PREFIX}{key}',
    )


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
    def plan(data: bytes, proxy: Any = None, **run: Any) -> None:
        stdout = tmp_path / 'stdout.bin'
        stdout.write_bytes(data)
        docker.plan(run={'stdout_file': str(stdout), **run}, proxy=proxy or {})
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
    name, proxy, network = ours(run)
    assert run[run.index('--network') + 1] == network
    # It ran to a clean end, so `--rm` has it: nothing to remove but its
    # proxy, and then its network.
    assert docker.removed() == [proxy]
    assert docker.networks_removed() == [network]


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
    name, proxy, network = ours(run)
    assert docker.removed() == [name, proxy]
    assert docker.networks_removed() == [network]
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
    name, proxy, network = ours(run)
    assert docker.removed() == [name, proxy]
    assert docker.networks_removed() == [network]
    [client] = clients
    assert client.poll() is not None, 'the docker client is still running'


def test_a_cancelled_run_has_its_container_removed(docker, project, out):
    """What `sbom lock --workers` tells every resolution in flight when
    one of them is interrupted: KeyboardInterrupt reaches the main
    thread only."""
    docker.plan(run={'sleep': 60})
    cancel = threading.Event()

    def once_it_runs() -> None:
        # Once the run is in flight, which is the case here. Half a
        # second in, as it was, a loaded machine had not yet started it,
        # and a run cancelled before it starts runs nothing (below).
        deadline = time.monotonic() + 10
        while not docker.runs() and time.monotonic() < deadline:
            time.sleep(0.02)
        cancel.set()

    canceller = threading.Thread(target=once_it_runs)
    canceller.start()

    result = sandbox.generate_lockfile(
        'gem', project, out, SandboxLimits(user='1000:1000'), cancel=cancel,
    )
    canceller.join()

    assert result.returncode == sandbox.INTERRUPTED
    assert result.cancelled and not result.sandbox_failed
    [run] = docker.runs()
    name, proxy, network = ours(run)
    assert docker.removed() == [name, proxy]
    assert docker.networks_removed() == [network]


def test_a_run_cancelled_before_it_starts_runs_nothing(docker, project, out):
    cancel = threading.Event()
    cancel.set()

    result = sandbox.generate_lockfile(
        'gem', project, out, SandboxLimits(user='1000:1000'), cancel=cancel,
    )

    assert not result.ok
    assert result.cancelled
    assert docker.calls() == [], 'no network, no proxy, no resolver'


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
    name, proxy, _ = ours(run)
    assert docker.removed() == [name, proxy]
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


#: What Composer printed on stderr, from the recipe's own command and
#: flags, for a download that failed: its progress, the exception, and
#: then the command's usage synopsis, 677 characters of it. #118's was
#: `curl error 60`, from composer 2.10.3, whose synopsis was about 580;
#: this is composer 2.8.12 with its registry unreachable, on the same
#: path (a TransportException from its curl downloader). Captured as it
#: came, trailing spaces and all, but for a line it printed only because
#: it ran as root, which the sandbox never does.
COMPOSER_TRANSPORT_ERROR = (
    'Composer could not detect the root package (p16/guzzle-63) v'
    "ersion, defaulting to '1.0.0'. See https://getcomposer.org/r"
    'oot-version\n'
    'Loading composer repositories with package information\n'
    '\n'
    'In CurlDownloader.php line 394:\n'
    '                                                            '
    '                   \n'
    '  curl error 7 while downloading https://repo.packagist.org/'
    'packages.json: Fa  \n'
    "  iled to connect to 127.0.0.1 port 9 after 0 ms: Couldn't c"
    'onnect to server   \n'
    '                                                            '
    '                   \n'
    '\n'
    'update [--with WITH] [--prefer-source] [--prefer-dist] [--pr'
    'efer-install PREFER-INSTALL] [--dry-run] [--dev] [--no-dev] '
    '[--lock] [--no-install] [--no-audit] [--audit-format AUDIT-F'
    'ORMAT] [--no-autoloader] [--no-suggest] [--no-progress] [-w|'
    '--with-dependencies] [-W|--with-all-dependencies] [-v|vv|vvv'
    '|--verbose] [-o|--optimize-autoloader] [-a|--classmap-author'
    'itative] [--apcu-autoloader] [--apcu-autoloader-prefix APCU-'
    'AUTOLOADER-PREFIX] [--ignore-platform-req IGNORE-PLATFORM-RE'
    'Q] [--ignore-platform-reqs] [--prefer-stable] [--prefer-lowe'
    'st] [-m|--minimal-changes] [--patch-only] [-i|--interactive]'
    ' [--root-reqs] [--bump-after-update [BUMP-AFTER-UPDATE]] [--'
    '] [<packages>...]\n'
    '\n'
)


def logged_stderr(logs: list[dict[str, Any]]) -> str:
    """What the log says a resolution that produced nothing printed."""
    [failed] = [e for e in logs if e['event'] == 'No lockfile produced']
    return str(failed['stderr'])


def test_the_log_keeps_the_error_a_synopsis_follows(docker, project, out):
    """After an exception Composer prints the command's usage synopsis,
    longer than the 400 characters of the end the log kept: #118's log
    showed the synopsis and never the `curl error 60` before it."""
    docker.plan(run={'stderr': COMPOSER_TRANSPORT_ERROR, 'exit': 100})

    with capture_logs() as logs:
        result = generate(project, out)

    assert not result.ok
    assert result.stderr == COMPOSER_TRANSPORT_ERROR
    assert (
        'curl error 7 while downloading https://repo.packagist.org/'
        'packages.json'
    ) in logged_stderr(logs)


def test_the_log_keeps_both_ends_of_a_long_stderr(docker, project, out):
    """An error a resolver prints first, before a trailer longer than
    what the log keeps of the end, and one it prints last, after its
    progress: both reach the log, which says how much it left out
    between them and stays bounded."""
    docker.plan(
        run={
            'stderr_bytes': 4 * 1024 * 1024, 'stderr': 'the error, last',
            'exit': 1,
        },
    )
    with capture_logs() as logs:
        generate(project, out)
    last = logged_stderr(logs)

    trailer = 's' * 16 * 1024
    docker.plan(run={'stderr': f'the error, first\n{trailer}', 'exit': 1})
    with capture_logs() as logs:
        generate(project, out)
    first = logged_stderr(logs)

    assert last.endswith('the error, last')
    assert first.startswith('the error, first\n')
    for logged in (first, last):
        assert re.search(
            r'\n\[\.\.\. [\d,]+ characters left out \.\.\.\]\n', logged,
        )
        assert len(logged) < 2 * sandbox.STDERR_LOGGED + 100


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


# --- the resolution's network, and its proxy (#168) --------------------------
#
# A resolution runs project-controlled code: a Gemfile is Ruby. On a
# shared bridge with traffic between its containers off, resolvers could
# not reach each other, and could reach anything on the internet. Now
# each resolution gets a network of its own, internal, which only its
# resolver and its proxy are on: the proxy is on the proxies' network
# too, the one with a route out, and lets through CONNECT to its
# recipe's registries alone (core/egress.py).


def test_each_resolution_gets_an_internal_network_of_its_own(
    docker, sends, project, out,
):
    """No route out: internal, and isolated, so that its bridge has no
    address on the daemon's side either, where the daemon's own API
    listens. Traffic between its containers is on, as it must be for
    the resolver to reach its proxy, and there are no others: resolvers
    are kept apart by being on networks of their own."""
    sends(archive(regular('Gemfile.lock', LOCKFILE)))

    generate(project, out)
    generate(project, out)

    first, second = docker.runs()
    networks = [ours(run)[2] for run in (first, second)]
    assert networks[0] != networks[1]
    created = [
        call for call in docker.created()
        if call[-1].startswith(sandbox.NETWORK_PREFIX)
    ]
    assert [call[-1] for call in created] == networks
    for call in created:
        assert '--internal' in call
        options = {
            call[i + 1] for i, a in enumerate(call) if a == '--opt'
        }
        assert options == {
            'com.docker.network.bridge.enable_icc=true',
            'com.docker.network.bridge.gateway_mode_ipv4=isolated',
            'com.docker.network.bridge.gateway_mode_ipv6=isolated',
        }
    # Made fresh for each, and gone with it.
    assert docker.networks_removed() == networks


def test_the_proxy_is_the_one_way_out(docker, sends, project, out):
    """The resolution's network holds its resolver and its proxy, and
    nothing else; the proxy alone is on a network with a route out, the
    proxies', on which traffic between containers is off."""
    sends(archive(regular('Gemfile.lock', LOCKFILE)))

    generate(project, out)

    [run] = docker.runs()
    [proxy] = docker.proxies()
    name, proxy_name, network = ours(run)
    assert name_of(proxy) == proxy_name
    on = [proxy[i + 1] for i, a in enumerate(proxy) if a == '--network']
    assert on == [
        sandbox.EGRESS_NETWORK, f'name={network},alias={sandbox.PROXY_ALIAS}',
    ]
    everyone = [
        call for call in docker.calls() if call[:1] == ['run']
        and any(network in arg for arg in call)
    ]
    assert everyone == [proxy, run]
    [way_out] = [
        call for call in docker.created()
        if call[-1] == sandbox.EGRESS_NETWORK
    ]
    assert '--internal' not in way_out
    assert 'com.docker.network.bridge.enable_icc=false' in way_out


def test_the_resolver_runs_once_its_proxy_listens(
    docker, sends, project, out,
):
    """The resolver reaches for its registry as it starts: its proxy is
    started first, and waited for until it says it listens, however long
    it takes to."""
    sends(archive(regular('Gemfile.lock', LOCKFILE)), proxy={'delay': 0.5})

    generate(project, out)

    [proxy] = docker.proxies()
    [run] = docker.runs()
    order = [
        call for call in docker.calls()
        if call[:1] in (['run'], ['listening'])
    ]
    assert order == [proxy, ['listening', name_of(proxy)], run]


def _run_options(run: list[str]) -> list[str]:
    """What a `docker run` sets for its container, before the image."""
    return run[:run.index(sandbox.PROXY_IMAGE)]


def test_the_proxy_runs_without_privileges(docker, sends, project, out):
    """Non-root, read-only, with no capability and no way to gain one,
    bounded, and with nothing of the host's: no mount at all."""
    sends(archive(regular('Gemfile.lock', LOCKFILE)))

    generate(project, out)

    [proxy] = docker.proxies()
    options = _run_options(proxy)
    text = ' '.join(options)
    user = options[options.index('--user') + 1]
    assert user.split(':')[0] not in ('0', 'root', '')
    assert '--read-only' in options
    assert '--cap-drop ALL' in text
    assert '--security-opt no-new-privileges' in text
    for bound in ('--memory', '--cpus', '--pids-limit'):
        assert bound in options, bound
    assert '--mount' not in options and '--volume' not in options
    assert '-v' not in options and '--tmpfs' not in options
    assert options[options.index('--pull') + 1] == 'never'
    assert '--rm' in options


def test_the_proxy_image_is_pinned_by_digest():
    assert re.fullmatch(
        r'[a-z0-9._/-]+:[\w.-]+@sha256:[0-9a-f]{64}', sandbox.PROXY_IMAGE,
    )


def test_the_proxy_runs_our_proxy_from_its_source_alone(
    docker, sends, project, out,
):
    """Its configuration reaches it on its command line, and its code as
    its source, since the nested daemon sees its own filesystem and not
    the resolver's: nothing is mounted or copied in. It lets through its
    recipe's registries, and ends itself at the resolution's deadline
    should nothing be left to remove it."""
    sends(archive(regular('Gemfile.lock', LOCKFILE)))

    generate(project, out, timeout=45)

    [proxy] = docker.proxies()
    after = proxy[proxy.index(sandbox.PROXY_IMAGE) + 1:]
    assert after[:3] == ['timeout', '-s', 'KILL']
    assert int(after[3]) > 45
    assert after[4:8] == ['python3', '-I', '-B', '-c']
    assert after[8] == egress.source()
    listen = after[after.index('--listen') + 1]
    assert listen == f'0.0.0.0:{sandbox.PROXY_PORT}'
    allowed = [after[i + 1] for i, a in enumerate(after) if a == '--allow']
    assert allowed == list(lock_recipe_for('gem').hosts)


def test_each_recipe_declares_the_registries_it_reaches():
    """Packagist for Composer and RubyGems for Bundler, as #168 lists
    them. A real resolution of each asked for no other host: composer
    2.8.12 opened one tunnel, to repo.packagist.org, and Bundler 4.0.9
    four, to index.rubygems.org."""
    assert lock_recipe_for('composer').hosts == (
        'repo.packagist.org', 'packagist.org',
    )
    assert lock_recipe_for('gem').hosts == (
        'rubygems.org', 'index.rubygems.org',
    )


def test_the_allowlist_is_the_union_of_the_recipes_hosts():
    """What any resolution may reach; each resolution's proxy lets
    through its own recipe's alone."""
    assert sandbox.egress_hosts() == sorted({
        host for recipe in LOCK_RECIPES.values() for host in recipe.hosts
    })
    assert sandbox.egress_hosts() == [
        'index.rubygems.org', 'packagist.org', 'repo.packagist.org',
        'rubygems.org',
    ]


def test_a_recipe_whose_hosts_moved_is_another_recipe():
    """A failure is kept by the recipe's fingerprint: a host added may
    let through what failed without it."""
    recipe = lock_recipe_for('gem')
    moved = replace(recipe, hosts=(*recipe.hosts, 'gems.example'))
    assert moved.fingerprint != recipe.fingerprint


def test_what_the_proxy_said_comes_back_with_the_result(
    docker, sends, project, out,
):
    """Each tunnel it opened and each request it refused, for `sbom
    lock` to log with the directory that asked."""
    refused = {
        'event': 'refused', 'client': '10.0.0.2:40000', 'reason': 'host',
        'request': 'CONNECT github.com:443 HTTP/1.1',
        'detail': 'github.com is not a registry this resolution may reach',
    }
    tunnel = {
        'event': 'tunnel', 'client': '10.0.0.2:40002',
        'host': 'index.rubygems.org', 'address': '151.101.1.227:443',
    }
    docker.plan(
        run={'stdout_file': None, 'exit': 11},
        proxy={'lines': [tunnel, refused]},
    )

    result = generate(project, out)

    assert not result.ok
    assert result.egress == (tunnel, refused)
    assert result.refused == (refused,)


def test_a_proxy_that_does_not_start_fails_the_sandbox_not_the_project(
    docker, project, out,
):
    """Nothing of the project ran: not a failure to keep against it,
    and its network goes."""
    docker.plan(proxy={'exit': 125})

    result = generate(project, out)

    assert not result.ok
    assert result.sandbox_failed
    assert 'proxy' in result.stderr
    assert docker.runs() == []
    [created] = [
        call for call in docker.created()
        if call[-1].startswith(sandbox.NETWORK_PREFIX)
    ]
    assert docker.networks_removed() == [created[-1]]


def test_a_network_that_cannot_be_made_fails_the_sandbox(
    docker, project, out,
):
    docker.plan(network_create_exit=1)

    result = generate(project, out)

    assert result.sandbox_failed
    assert 'address pool' in result.stderr
    assert docker.runs() == [] and docker.proxies() == []


def test_a_failed_resolution_is_the_projects(docker, sends, project, out):
    sends(b'', exit=1)

    result = generate(project, out)

    assert not result.ok
    assert not result.sandbox_failed and not result.cancelled


# --- what a pass needs first ------------------------------------------------------


def test_every_image_is_pulled_before_anything_runs(docker):
    """The proxy's and each recipe's, once, over the daemon's own
    network, before any resolution's network is made; each is then run
    with `--pull never`."""
    docker.plan(images=[lock_recipe_for('gem').image])

    sandbox.prepare(LOCK_RECIPES.values())

    pulled = [call[-1] for call in docker.calls() if call[:1] == ['pull']]
    assert pulled == [sandbox.PROXY_IMAGE, lock_recipe_for('composer').image]
    assert not [c for c in docker.calls() if c[:1] == ['run']]


def test_an_image_there_already_is_not_pulled_again(docker):
    docker.plan(
        images=[sandbox.PROXY_IMAGE] + [
            recipe.image for recipe in LOCK_RECIPES.values()
        ],
    )

    sandbox.prepare(LOCK_RECIPES.values())

    assert [c for c in docker.calls() if c[:1] == ['pull']] == []


def test_an_image_that_cannot_be_pulled_stops_the_pass(docker):
    docker.plan(pull_exit=1)

    with pytest.raises(sandbox.SandboxError, match='pull access denied'):
        sandbox.prepare(LOCK_RECIPES.values())


def test_the_proxies_network_has_a_route_out_and_no_traffic_within(docker):
    """The proxies' way out, which each resolution's proxy is on beside
    its resolution's network: several at once under `--workers`, and
    none reaches another."""
    sandbox.prepare([])

    [create] = docker.created()
    assert create[-1] == sandbox.EGRESS_NETWORK
    assert 'com.docker.network.bridge.enable_icc=false' in create
    assert '--internal' not in create


def test_the_proxies_network_is_used_as_it_is(docker):
    docker.network(
        sandbox.EGRESS_NETWORK, Internal=False,
        Options={'com.docker.network.bridge.enable_icc': 'false'},
    )

    sandbox.prepare([])

    assert docker.created() == []


@pytest.mark.parametrize(
    'internal, icc', [(False, 'true'), (False, ''), (True, 'false')],
    ids=['traffic within', 'traffic within, by default', 'no route out'],
)
def test_a_proxies_network_made_otherwise_is_refused(docker, internal, icc):
    """Made by hand, or by something else: not the network this relies
    on, and so not used."""
    options = {'com.docker.network.bridge.enable_icc': icc} if icc else {}
    docker.network(sandbox.EGRESS_NETWORK, Internal=internal, Options=options)

    with pytest.raises(sandbox.SandboxError, match='docker network rm'):
        sandbox.prepare([])


def test_what_a_resolver_left_behind_is_swept(docker):
    """A resolver that ended without cleaning up, killed say, left its
    containers to their deadline and its networks for good: each takes
    a subnet from the daemon's pools, which run out. Swept by their
    label, while nothing of this resolver's runs."""
    docker.plan(leftovers=['chatsbom-proxy-1', 'chatsbom-lock-1'])

    sandbox.sweep()

    calls = docker.calls()
    assert ['ps', '-aq', '--filter', f'label={sandbox.LABEL}'] in calls
    assert ['rm', '-f', 'chatsbom-proxy-1', 'chatsbom-lock-1'] in calls
    assert [
        'network', 'prune', '--force', '--filter',
        f'label={sandbox.LABEL}=resolution',
    ] in calls


def test_every_container_and_network_it_makes_is_labelled(
    docker, sends, project, out,
):
    """So that the sweep finds what is the resolver's, and nothing
    else."""
    sends(archive(regular('Gemfile.lock', LOCKFILE)))

    generate(project, out)

    for call in docker.created() + docker.runs() + docker.proxies():
        labels = [call[i + 1] for i, a in enumerate(call) if a == '--label']
        assert any(label.startswith(f'{sandbox.LABEL}=') for label in labels)
