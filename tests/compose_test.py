"""The compose setup, checked without starting anything.

These guard the properties that were wrong on the first attempt: a bare
`up` must not start collecting, the collector must run as the invoking
user, and the host Docker socket must never be mounted.
"""
import fnmatch
import itertools
import json
import os
import re
import shlex
import shutil
import subprocess
from collections.abc import Iterator
from dataclasses import dataclass
from pathlib import Path

import pytest
import yaml

from chatsbom.core.clickhouse import START_CLICKHOUSE
from tests.extras_test import NEEDS

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope='module')
def compose() -> dict:
    return yaml.safe_load((ROOT / 'docker-compose.yaml').read_text())


@pytest.fixture(scope='module')
def dockerfile() -> str:
    return (ROOT / 'Dockerfile').read_text()


@pytest.fixture(scope='module')
def web_dockerfile() -> str:
    return (ROOT / 'Dockerfile.web').read_text()


def _instructions(dockerfile: str) -> list[tuple[str, str]]:
    """(INSTRUCTION, arguments) in order, continuation lines joined.

    Comments go first, as Docker drops them: one ending in a backslash
    would otherwise swallow the instruction after it.
    """
    lines = (
        line for line in dockerfile.splitlines()
        if not line.lstrip().startswith('#')
    )
    instructions = []
    for line in re.sub(r'\\\n', ' ', '\n'.join(lines)).splitlines():
        if line.strip():
            keyword, _, arguments = line.strip().partition(' ')
            instructions.append((keyword.upper(), arguments.strip()))
    return instructions


def _image_env(dockerfile: str) -> dict[str, str]:
    """Every `ENV KEY=value` the image sets."""
    env = {}
    for keyword, arguments in _instructions(dockerfile):
        if keyword == 'ENV':
            for word in shlex.split(arguments):
                key, _, value = word.partition('=')
                env[key] = value
    return env


def _workdir(dockerfile: str) -> str:
    return [a for k, a in _instructions(dockerfile) if k == 'WORKDIR'][-1]


@dataclass
class Stage:
    """A `FROM`, and the instructions after it up to the next one."""
    #: Its `AS` name, lowercased as Docker compares them; None if unnamed.
    name: str | None
    #: What it is built on: an image, or the name of an earlier stage.
    base: str
    instructions: list[tuple[str, str]]


def _stages(dockerfile: str) -> list[Stage]:
    stages: list[Stage] = []
    for keyword, arguments in _instructions(dockerfile):
        if keyword == 'FROM':
            words = [w for w in arguments.split() if not w.startswith('--')]
            named = len(words) == 3 and words[1].upper() == 'AS'
            name = words[2].lower() if named else None
            stages.append(Stage(name, words[0], []))
        elif stages:
            stages[-1].instructions.append((keyword, arguments))
    return stages


def _copied_from(stage: Stage) -> list[str]:
    """What each `COPY --from=` in a stage copies out of."""
    return [
        word.removeprefix('--from=')
        for keyword, arguments in stage.instructions if keyword == 'COPY'
        for word in arguments.split() if word.startswith('--from=')
    ]


def _lineage(stages: list[Stage], target: str | None) -> list[str | None]:
    """The stages an image built for `target` is made of, its own first.

    None is what a `docker build` without `--target` makes: the last.
    """
    by_name = {stage.name: stage for stage in stages if stage.name}
    stage = by_name[target.lower()] if target else stages[-1]
    lineage = [stage]
    while stage.base.lower() in by_name and by_name[stage.base.lower()] not in lineage:
        stage = by_name[stage.base.lower()]
        lineage.append(stage)
    return [stage.name for stage in lineage]


def _adds_a_docker_client(keyword: str, arguments: str) -> bool:
    """A package that is one, or a binary copied from the docker image."""
    if keyword == 'RUN':
        return bool(re.search(r'\bdocker(-ce)?-cli\b|\bdocker\.io\b', arguments))
    if keyword in ('COPY', 'ADD'):
        return (
            '--from=docker:' in arguments
            or bool(re.search(r'/bin/docker\b', arguments))
        )
    return False


def _split_reference(reference: str) -> tuple[str, str]:
    """An image reference as (repository, tag or digest)."""
    name, _, digest = reference.partition('@')
    if ':' in name.rsplit('/', 1)[-1]:
        repository, _, tag = name.rpartition(':')
        return repository, digest or tag
    return name, digest


def _strings(node: object) -> Iterator[str]:
    """Every string in a parsed YAML document."""
    if isinstance(node, dict):
        for key, value in node.items():
            yield from _strings(key)
            yield from _strings(value)
    elif isinstance(node, list):
        for item in node:
            yield from _strings(item)
    elif isinstance(node, str):
        yield node


def _dockerignored(path: str, patterns: list[str]) -> bool:
    """Whether `.dockerignore` keeps `path` out of the build context.

    Docker's rules, near enough for plain patterns: a pattern that
    matches a directory excludes what is in it, and a later `!` line
    takes a match back.
    """
    parts = path.split('/')
    ignored = False
    for line in patterns:
        line = line.strip()
        if not line or line.startswith('#'):
            continue
        negated = line.startswith('!')
        pattern = line.removeprefix('!').strip().strip('/')
        if any(
            fnmatch.fnmatchcase('/'.join(parts[:depth]), pattern)
            for depth in range(1, len(parts) + 1)
        ):
            ignored = not negated
    return ignored


#: Services that must never start without being asked for, and why.
#:
#: The reason is what this guards, not the length of the default list:
#: `web` was added to the defaults and broke an assertion that said
#: `== ['clickhouse']` while satisfying every word of its docstring.
#: Serving a page is not spending anything.
COSTLY = {
    'collector': 'spends GitHub rate budget',
    'depgraph': 'spends GitHub dependency-graph rate budget',
    'lock': 'runs a container per repository',
    'dind': 'runs a privileged Docker daemon',
    'cli': 'a one-shot tool, not a service',
}


def test_nothing_costly_starts_without_being_asked(compose):
    """Spending budget, running containers or publishing to the
    internet must each be a decision, not a side effect of
    `docker compose up`."""
    default = {
        name for name, svc in compose['services'].items()
        if not svc.get('profiles')
    }
    for name, why in COSTLY.items():
        assert name not in default, f'{name} starts by default and {why}'


def test_the_database_starts_by_default(compose):
    """Everything else needs it, and it costs nothing to have up."""
    assert not compose['services']['clickhouse'].get('profiles')


def test_every_service_is_either_default_or_accounted_for(compose):
    """A new service must be a deliberate choice on this question.

    Without this the guard above only covers the names it already
    knows, so the next costly service would start by default and no
    test would notice.
    """
    known = set(COSTLY) | {'clickhouse', 'web'}
    assert set(compose['services']) == known


def test_the_collector_is_behind_a_profile(compose):
    assert 'collect' in compose['services']['collector']['profiles']


def test_the_collector_runs_as_the_invoking_user(compose):
    """A baked-in uid cannot write the bind-mounted ledger."""
    user = compose['services']['collector']['user']
    assert '${UID' in user and '${GID' in user


def test_the_cli_service_shares_the_collector_mounts(compose):
    """A manual stage and the loop must see identical state."""
    collector = set(compose['services']['collector']['volumes'])
    cli = set(compose['services']['cli']['volumes'])
    data_mounts = {v for v in collector if v.startswith('./data')}
    assert data_mounts and data_mounts <= cli


def test_the_docker_socket_is_never_mounted(compose):
    """`sbom lock` runs containers; the socket would be an escape hatch."""
    for name, service in compose['services'].items():
        for volume in service.get('volumes', []):
            assert 'docker.sock' not in volume, name


def test_the_image_has_no_docker_cli(dockerfile):
    assert 'docker-ce-cli' not in dockerfile
    assert 'docker.io' not in dockerfile.replace('ghcr.io', '')


def test_syft_is_pinned(dockerfile):
    """The version keys the SBOM cache; `latest` would repartition it.

    The installer is pinned as well: the one at the release's tag,
    checked against a digest before it runs. get.anchore.io served
    whatever the installer was the day of the build, piped to `sh`.
    """
    instructions = _instructions(dockerfile)
    arguments = dict(
        argument.partition('=')[::2]
        for keyword, argument in instructions if keyword == 'ARG'
    )
    assert re.fullmatch(r'\d+\.\d+\.\d+', arguments['SYFT_VERSION'])
    assert re.fullmatch(r'[0-9a-f]{64}', arguments['SYFT_INSTALLER_SHA256'])
    runs = [argument for keyword, argument in instructions if keyword == 'RUN']
    assert not any('get.anchore.io' in run for run in runs)
    [install] = [run for run in runs if 'install-syft.sh' in run]
    fetch = install.index(
        'https://raw.githubusercontent.com/anchore/syft/v${SYFT_VERSION}/'
        'install.sh',
    )
    check = install.index(
        'echo "${SYFT_INSTALLER_SHA256}  /tmp/install-syft.sh"',
    )
    run = install.index(
        'sh /tmp/install-syft.sh -b /usr/local/bin "v${SYFT_VERSION}"',
    )
    assert fetch < check < run
    assert 'sha256sum --check --strict' in install[check:run]


def test_the_dataset_is_mounted_not_baked_in(dockerfile):
    ignore = (ROOT / '.dockerignore').read_text().splitlines()
    assert 'data/' in ignore
    assert '.cache/' in ignore
    assert 'COPY data' not in dockerfile


def test_the_collector_is_resource_bounded(compose):
    collector = compose['services']['collector']
    assert 'mem_limit' in collector
    assert 'cpus' in collector


def test_the_healthcheck_does_not_use_localhost(compose):
    """Inside the image localhost resolves to ::1, where it is not listening."""
    test = compose['services']['clickhouse']['healthcheck']['test']
    assert not any('localhost' in part for part in test)
    assert any('127.0.0.1' in part for part in test)


def test_the_collector_waits_for_a_healthy_database(compose):
    depends = compose['services']['collector']['depends_on']
    assert depends['clickhouse']['condition'] == 'service_healthy'


def test_no_variable_is_required_to_read_the_file(compose):
    """Compose interpolates the whole file for every command, whichever
    profiles are active.

    `${GITHUB_TOKEN:?...}` on the collector, which is behind a profile,
    made `up`, `config`, `ps` and `down` all refuse without a token —
    the README's first step among them. A check for what one service
    needs belongs to that service's own start: the collector's loop
    refuses to begin without a token (collector_loop_test).
    """
    required = re.compile(r'\$\{\w+:?\?')
    offending = [
        text for text in _strings(compose)
        if required.search(text.replace('$$', ''))
    ]
    assert offending == []


def test_the_collector_is_handed_the_token_as_it_is(compose):
    """Empty when unset, for the loop to refuse, rather than a refusal
    of compose's own."""
    token = compose['services']['collector']['environment']['GITHUB_TOKEN']
    assert token == '${GITHUB_TOKEN:-}'


@pytest.mark.parametrize('service', ['collector', 'cli', 'depgraph'])
def test_the_dependency_graph_endpoint_choice_reaches_the_container(
    compose, service,
):
    """Which of GitHub's two SBOM flows to use: the synchronous endpoint
    closes on 2026-11-13. Set in `.env` and not handed on, the choice
    would change the CLI on the host and not the collector, which runs
    `chatsbom run` every slice."""
    environment = compose['services'][service]['environment']
    assert environment['CHATSBOM_DEPGRAPH_API'] == (
        '${CHATSBOM_DEPGRAPH_API:-auto}'
    )


@pytest.mark.parametrize('service', ['collector', 'depgraph'])
def test_what_runs_unattended_logs_json(compose, service):
    """One object per line, on stderr, for whatever collects the logs.

    A literal, not `${CHATSBOM_LOG_FORMAT:-json}`: `.env` sets the CLI's
    format on the host, a person's, and must not turn these to it.
    """
    environment = compose['services'][service]['environment']
    assert environment['CHATSBOM_LOG_FORMAT'] == 'json'


@pytest.mark.parametrize('service', ['cli', 'lock'])
def test_what_a_person_runs_logs_for_a_person(compose, service):
    """`run --rm cli` and `run --rm lock` are read at a terminal."""
    environment = compose['services'][service].get('environment') or {}
    assert 'CHATSBOM_LOG_FORMAT' not in environment
    assert 'ENV' not in environment


def _compose_cli() -> bool:
    if shutil.which('docker') is None:
        return False
    version = subprocess.run(
        ['docker', 'compose', 'version'], capture_output=True, timeout=60,
    )
    return version.returncode == 0


@pytest.mark.skipif(
    not _compose_cli(),
    reason='needs the docker compose CLI (not a daemon)',
)
@pytest.mark.parametrize(
    'profiles',
    [(), ('collect',), ('lock',), ('tools',), ('collect', 'lock', 'tools')],
    ids=lambda profiles: '+'.join(profiles) or 'default',
)
def test_compose_reads_the_file_with_nothing_set(profiles, tmp_path):
    """No token, no `.env`, no daemon: the file is interpolated before
    `up`, `ps` or `down` does anything else, and `config` is that step
    alone."""
    empty = tmp_path / 'empty.env'
    empty.write_text('')
    command = [
        'docker', 'compose', '--env-file', str(empty),
        '--file', str(ROOT / 'docker-compose.yaml'),
    ]
    for profile in profiles:
        command += ['--profile', profile]
    # Where docker keeps its config and plugins, and nothing else: not
    # GITHUB_TOKEN, nor anything else compose would interpolate.
    kept = ('PATH', 'HOME', 'DOCKER_CONFIG')
    result = subprocess.run(
        [*command, 'config', '--quiet'],
        env={name: os.environ[name] for name in kept if name in os.environ},
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.parametrize(
    'name', ['collector', 'depgraph', 'cli', 'lock', 'web'],
)
def test_what_runs_our_code_runs_under_an_init(compose, name):
    """docker-init as PID 1 hands on the SIGTERM a stop sends.

    The kernel ignores a signal sent to PID 1 that has no handler for
    it. The collector's shell had no trap, and chatsbom — PID 1 in `cli`
    and `lock` — has no handler for TERM, so each stop waited out the
    grace period and ended in SIGKILL, the work in flight with it. Under
    an init neither is PID 1, and TERM does what it would anywhere
    else; the loop traps it besides (collector_loop_test). The web
    entrypoint traps it too and passes it on to wrangler
    (web_entrypoint_test); under an init neither it nor wrangler, which
    it execs with WATCHDOG_DISABLED, is PID 1 either.
    """
    assert compose['services'][name].get('init') is True


#: A ClickHouse account a compose service may be given, and the default
#: the CLI takes when it is not set (chatsbom/core/config.py).
ACCOUNTS = {
    'CLICKHOUSE_ADMIN_USER': 'admin',
    'CLICKHOUSE_ADMIN_PASSWORD': 'admin',
    'CLICKHOUSE_GUEST_USER': 'guest',
    'CLICKHOUSE_GUEST_PASSWORD': 'guest',
}


def test_every_account_comes_from_the_environment(compose):
    """As the CLI's does, with the same default.

    Written into the file, a password changed in database/config/users.d
    and `.env` reached the CLI on the host but never the collector or
    `cli`, which went on sending the old one.
    """
    for name, service in compose['services'].items():
        environment = service.get('environment') or {}
        for key, default in ACCOUNTS.items():
            if key in environment:
                value = str(environment[key])
                assert value == '${' + key + ':-' + default + '}', (
                    f'{name}: {key}={value}'
                )


def test_the_cli_service_is_given_both_accounts(compose):
    """`db index` connects as admin and `db query` as guest, so a stage
    run by hand needs both, as the CLI on the host has them. Without
    the guest account `run --rm cli db query` sent the CLI's default
    password, whatever `.env` said."""
    environment = compose['services']['cli']['environment']
    assert set(ACCOUNTS) <= set(environment)


def test_lock_is_given_no_account(compose):
    """`sbom lock` reads the content lists under data/ and never
    connects to ClickHouse. The service that drives project-controlled
    resolvers is given no credentials it does not use."""
    environment = compose['services']['lock'].get('environment') or {}
    assert [key for key in environment if key in ACCOUNTS] == []


@pytest.mark.parametrize('path', ['.env', 'web/.env', 'web/.dev.vars'])
def test_env_files_never_reach_an_image(path):
    """`COPY web/ ./` copies whatever is there, so a developer's own
    `web/.env` was baked into a layer of the web image, where anyone
    holding the image can read it, whether or not wrangler loads it."""
    patterns = (ROOT / '.dockerignore').read_text().splitlines()
    assert _dockerignored(path, patterns)


def test_the_loop_survives_a_failing_slice():
    loop = (ROOT / 'deploy' / 'collector-loop.sh').read_text()
    # A slice that fails must be recorded and stepped over, not fatal.
    assert 'queue sync' in loop
    assert '||' in loop
    assert 'set -eu' in loop


# --- what the image installs (#27) ------------------------------------------

def _uv_syncs(stage: Stage) -> list[list[str]]:
    """The arguments of each `uv sync` a stage runs."""
    syncs = []
    for keyword, arguments in stage.instructions:
        if keyword == 'RUN':
            for command in re.split(r'&&|;', arguments):
                words = shlex.split(command)
                if words[:2] == ['uv', 'sync']:
                    syncs.append(words[2:])
    return syncs


def _collector_stage(dockerfile: str) -> Stage:
    return next(s for s in _stages(dockerfile) if s.name == 'collector')


def _extras(sync: list[str]) -> set[str]:
    """What `--extra` names, in either spelling, and `--all-extras`."""
    extras = {
        sync[at + 1] for at, word in enumerate(sync[:-1]) if word == '--extra'
    }
    extras |= {
        word.removeprefix('--extra=') for word in sync
        if word.startswith('--extra=')
    }
    if '--all-extras' in sync:
        extras.add('all')
    return extras


def _loop_commands() -> set[tuple[str, ...]]:
    """The commands deploy/collector-loop.sh runs, without their options."""
    loop = (ROOT / 'deploy' / 'collector-loop.sh').read_text()
    return {
        tuple(words.split())
        for words in re.findall(r'^\s*step chatsbom((?: [a-z][a-z-]*)+)', loop, re.M)
    }


def test_the_image_installs_no_development_dependencies(dockerfile):
    """The dev group brings pytest, and every extra, so that a plain `uv
    sync` makes an environment the whole suite runs in. The image synced
    without `--no-dev`, and so took the lot."""
    syncs = _uv_syncs(_collector_stage(dockerfile))
    assert syncs
    for sync in syncs:
        assert '--no-dev' in sync, sync
        assert '--frozen' in sync, sync


def test_the_image_has_the_extras_the_collector_loop_needs_and_no_more(
    dockerfile,
):
    """What the loop runs — `queue`, `run`, `sbom generate`, `db raw` and
    `db index`, `data prune`, and the `depgraph` worker — needs no extra,
    so the image has none.

    None beyond that on purpose. clickhouse-connect imports pandas and
    pyarrow on every command's first connection when they are there,
    and they were: every command the loop ran paid for libraries only
    `export parquet` and the `openapi` analyses use. The `cli` service
    shares this image, and a command that needs an extra says so there.
    """
    ran = _loop_commands()
    assert {('queue', 'sync'), ('run',), ('db', 'index')} <= ran
    needed = {
        extra for argv, extra in NEEDS
        if tuple(itertools.takewhile(lambda w: w[0] != '-', argv)) in ran
    }
    installed = set().union(
        *(_extras(sync) for sync in _uv_syncs(_collector_stage(dockerfile))),
    )
    assert installed == needed


def test_the_image_has_ps_for_gits_timeouts(dockerfile):
    """GitPython stops a git that outlives `kill_after_timeout` by running
    `ps --ppid <pid>` first, for its children. With no `ps` that raises
    in the timer's thread, nothing is killed, and the timeout never
    fires. The release stage's `ls-remote` relies on it, and
    python:*-slim has no procps, so a stalled `ls-remote` could hold the
    collector loop for as long as the network did (#75).
    """
    installed = [
        word
        for keyword, arguments in _collector_stage(dockerfile).instructions
        if keyword == 'RUN' and 'apt-get install' in arguments
        for word in arguments.split()
    ]
    assert 'procps' in installed


def test_the_image_is_byte_compiled(dockerfile):
    """The container runs as a uid that cannot write /app, so whatever the
    build left uncompiled, each start compiled again, and threw away:
    `chatsbom --help` took 1.55 s so, and 0.63 s compiled. The collector
    starts the CLI for every step of every slice (#28).

    The project is installed, not linked back to /app: uv compiles what
    it installs, and an editable project's own modules stayed source.
    """
    compiling = _image_env(dockerfile).get('UV_COMPILE_BYTECODE') == '1'
    syncs = _uv_syncs(_collector_stage(dockerfile))
    for sync in syncs:
        assert compiling or '--compile-bytecode' in sync, sync
    [project] = [sync for sync in syncs if '--no-install-project' not in sync]
    assert '--no-editable' in project, project


def test_the_installed_project_carries_its_licence(dockerfile):
    """pyproject.toml names its licence file, and hatchling, building the
    wheel `uv sync` installs, leaves the file and its `License-File`
    metadata out without a word when it is not there. The image copied
    pyproject.toml, uv.lock, README.md and the package, and so shipped
    chatsbom without its licence (#28).
    """
    import tomllib
    declared = tomllib.loads(
        (ROOT / 'pyproject.toml').read_text(),
    )['project']['license-files']
    ignored = (ROOT / '.dockerignore').read_text().splitlines()

    copied: set[str] = set()
    for keyword, arguments in _collector_stage(dockerfile).instructions:
        if keyword == 'COPY' and '--from=' not in arguments:
            *sources, _ = shlex.split(arguments)
            copied.update(sources)
        elif keyword == 'RUN' and any(
            '--no-install-project' not in sync
            for sync in _uv_syncs(Stage(None, '', [(keyword, arguments)]))
        ):
            break
    else:
        pytest.fail('the collector stage never installs the project')

    assert declared and set(declared) <= copied, copied
    assert not any(_dockerignored(path, ignored) for path in declared)


# --- the nested daemon for `sbom lock` ------------------------------------

def test_lock_is_behind_its_own_profile(compose):
    """Resolving lockfiles runs project-controlled code; it is a decision."""
    assert compose['services']['lock']['profiles'] == ['lock']
    assert compose['services']['dind']['profiles'] == ['lock']


def test_the_nested_daemon_is_rootless(compose):
    """Where an escape lands is the whole question.

    The host socket would put it on the host daemon, which is host root.
    A rootless nested daemon maps its own root to an unprivileged host
    uid, and `compose down` destroys it.
    """
    assert 'rootless' in compose['services']['dind']['image']


def test_the_nested_daemon_is_not_reachable_from_outside(compose):
    """No published port: only the compose network can talk to it."""
    assert 'ports' not in compose['services']['dind']


def test_lock_talks_to_the_nested_daemon_not_the_host(compose):
    """Over TLS, on 2376: this said `tcp://dind:2375`, the daemon's API
    in plain TCP with no authentication at all (#30)."""
    host = compose['services']['lock']['environment']['DOCKER_HOST']
    assert host == 'tcp://dind:2376'


def _data_mount(compose: dict, service: str) -> str:
    return next(
        v for v in compose['services'][service]['volumes']
        if v.startswith('./data')
    )


def test_the_data_path_is_mounted_on_both_lock_and_the_daemon(compose):
    """A container the daemon starts resolves bind mounts against *its*
    filesystem, so a path only `lock` can see would mount nothing."""
    def source_and_target(service: str) -> list[str]:
        return _data_mount(compose, service).split(':')[:2]
    assert source_and_target('lock') == source_and_target('dind')


def test_the_daemon_mounts_the_data_read_only(compose):
    """What it runs only reads the project: the lockfile comes back on
    the resolver's stdout, and `lock` writes it (#30). Nothing the
    daemon starts, or anything that takes the daemon over, can write
    to data/ through it."""
    assert _data_mount(compose, 'dind').split(':')[2:] == ['ro']


def test_only_the_lock_stage_carries_a_docker_client(compose, dockerfile):
    """An image with a Docker client and a reachable socket is one
    mistake from being an escape, so the split is a build property.

    The client is added in the `lock` stage alone, and nothing the
    collector or `cli` runs is built on that stage — nor is the image a
    `docker build` makes when no target is named.
    """
    stages = _stages(dockerfile)
    adding = {
        stage.name for stage in stages
        if any(_adds_a_docker_client(*i) for i in stage.instructions)
    }
    assert adding == {'lock'}

    images: dict[str, str | None] = {
        name: compose['services'][name]['build'].get('target')
        for name in ('collector', 'cli')
    }
    images['a bare `docker build`'] = None
    for name, target in images.items():
        assert 'lock' not in _lineage(stages, target), (
            f'{name} is built on the lock stage'
        )


def test_the_lock_image_is_a_stage_of_the_collectors_dockerfile(
    compose, dockerfile,
):
    """Built from these sources each time, rather than on whatever image
    of a given name the machine happens to hold."""
    build = compose['services']['lock']['build']
    assert build.get('dockerfile', 'Dockerfile') == 'Dockerfile'
    assert build.get('target') == 'lock'
    assert 'lock' in {stage.name for stage in _stages(dockerfile)}


def test_the_collector_and_cli_are_one_image(compose, dockerfile):
    """Built once, under one name.

    Named for their services, they were two images of one Dockerfile,
    each built on its own: `run --rm cli` after `up` built again what
    the collector had just built. Sharing a name, whichever is built
    first serves the other.
    """
    collector = compose['services']['collector']
    cli = compose['services']['cli']
    assert collector.get('image'), 'the collector has no image name'
    assert cli.get('image') == collector['image']
    assert cli['build'] == collector['build']
    target = collector['build'].get('target')
    assert target in {stage.name for stage in _stages(dockerfile)}


@pytest.mark.parametrize(
    'path', sorted(ROOT.glob('Dockerfile*')), ids=lambda path: path.name,
)
def test_every_image_a_dockerfile_names_comes_from_a_registry(path, compose):
    """`FROM` and `COPY --from=` name an earlier stage of the same file,
    or an image a registry serves, pinned by digest.

    Dockerfile.lock was `FROM` an image compose had built under the
    project's old name, `:latest`. Nothing built that any more: on a
    fresh clone the lock profile could not build, and on a machine
    that still held the old image it built on that, silently stale.

    A tag moves with every rebuild of its image, so each is pinned to
    the digest of the multi-platform index the registry serves for it,
    as the recipes' images are (#30). The tag stays, to say which image
    the digest is of, and for Dependabot to move both.
    """
    built = {
        name for name, service in compose['services'].items()
        if 'build' in service
    }
    stages = _stages(path.read_text())
    earlier: set[str] = set()
    for index, stage in enumerate(stages):
        for reference in [stage.base, *_copied_from(stage)]:
            if reference.lower() in earlier or (
                reference.isdigit() and int(reference) < index
            ):
                continue
            repository, _ = _split_reference(reference)
            assert re.fullmatch(
                r'[a-z0-9._/-]+:[\w.-]+@sha256:[0-9a-f]{64}', reference,
            ), f'{path.name}: {reference} is not pinned by digest'
            assert ':latest@' not in reference, reference
            assert not any(
                repository.endswith(f'-{service}') for service in built
            ), f'{path.name}: {reference} is an image compose builds'
        if stage.name:
            earlier.add(stage.name)


def test_the_nested_daemon_storage_is_a_named_volume(compose):
    """overlay2 layers need a real filesystem, not a bind mount."""
    storage = next(
        v for v in compose['services']['dind']['volumes']
        if 'docker' in v and not v.startswith('./')
    )
    assert storage.startswith('dind-storage:')
    assert 'dind-storage' in compose['volumes']


# --- who can reach the nested daemon (#30) ----------------------------------
#
# It ran on the default network, beside ClickHouse, `web`, the collector
# and the dependency-graph worker, and served its API in plain TCP on
# 2375 to anything that asked. A resolver runs project-controlled code
# and reaches what the daemon reaches: ClickHouse's `admin` among it. And
# every service there, the internet-facing `web` included, could start
# a container on it.

def _networks(service: dict) -> set[str]:
    """The networks a compose service is on: `default` unless it names
    some, as a list or as a mapping."""
    networks = service.get('networks')
    return {'default'} if networks is None else set(networks)


def _sandbox_networks(compose: dict) -> set[str]:
    """What the daemon and `lock` share."""
    services = compose['services']
    return _networks(services['dind']) & _networks(services['lock'])


def test_the_daemon_and_lock_share_a_network_the_database_is_not_on(compose):
    shared = _sandbox_networks(compose)
    assert shared, 'lock cannot reach the daemon'
    assert not shared & _networks(compose['services']['clickhouse'])


@pytest.mark.parametrize(
    'name', ['clickhouse', 'web', 'collector', 'depgraph', 'cli'],
)
def test_nothing_but_lock_can_reach_the_daemon(compose, name):
    service = compose['services'][name]
    assert not _networks(service) & _networks(compose['services']['dind'])
    assert 'dind' not in str(service.get('network_mode', ''))


def test_every_service_is_kept_from_the_daemon_but_lock(compose):
    """The next service added to the file included."""
    dind = _networks(compose['services']['dind'])
    reach = {
        name for name, service in compose['services'].items()
        if name != 'dind' and _networks(service) & dind
    }
    assert reach == {'lock'}


def test_the_resolvers_can_still_reach_the_registries(compose):
    """Not `internal`: the daemon pulls the recipe images, and a
    resolver fetches metadata from its registry. Resolution is that."""
    declared = compose.get('networks') or {}
    for network in _sandbox_networks(compose):
        assert not (declared.get(network) or {}).get('internal'), network


def test_lock_is_on_the_daemons_network_alone(compose):
    """`sbom lock` reads data/ and never connects to ClickHouse, so it
    is not on the database's network, and is handed no way to find it."""
    lock = compose['services']['lock']
    assert _networks(lock) == _sandbox_networks(compose)
    environment = lock.get('environment') or {}
    assert [key for key in environment if key.startswith('CLICKHOUSE')] == []


def test_the_docker_api_is_never_plain_tcp(compose):
    """TLS, both ways: the daemon verifies a client certificate, which
    only `lock` has, and `lock` verifies the daemon's."""
    assert not [s for s in _strings(compose) if '2375' in s]
    dind = compose['services']['dind']['environment']
    assert dind.get('DOCKER_TLS_CERTDIR'), 'the image makes no certificates'
    lock = compose['services']['lock']['environment']
    assert lock['DOCKER_HOST'].endswith(':2376')
    assert str(lock.get('DOCKER_TLS_VERIFY')) == '1'


def _mounts(service: dict) -> list[tuple[str, str, list[str]]]:
    """(source, target, options) for each volume of a service."""
    mounts = []
    for volume in service.get('volumes', []):
        source, target, *options = volume.split(':')
        mounts.append((source, target, options))
    return mounts


def test_the_client_certificates_reach_lock_alone(compose):
    """Whoever holds them can start any container on the daemon: a
    named volume the daemon writes them to, read-only in `lock`, and
    mounted nowhere else. The CA's key is not in it."""
    services = compose['services']
    certificates = services['dind']['environment']['DOCKER_TLS_CERTDIR']
    client = f'{certificates}/client'
    [volume] = [
        source for source, target, _ in _mounts(services['dind'])
        if target == client
    ]
    assert volume in (compose.get('volumes') or {}), 'not a named volume'

    holders = {
        name for name, service in services.items()
        for source, _, _ in _mounts(service) if source == volume
    }
    assert holders == {'dind', 'lock'}

    [(target, options)] = [
        (target, options) for source, target, options
        in _mounts(services['lock']) if source == volume
    ]
    assert options == ['ro']
    assert services['lock']['environment']['DOCKER_CERT_PATH'] == target

    for name, service in services.items():
        for _, target, _ in _mounts(service):
            assert target != certificates, f'{name} mounts the CA key'


def test_the_daemon_certificate_names_the_host_lock_dials(compose):
    """`lock` verifies the daemon's certificate against the name in
    DOCKER_HOST. The image names its certificate after the container's
    hostname, `docker` and `localhost`, and compose leaves the hostname
    a container id: `dind` has to be asked for."""
    host = compose['services']['lock']['environment']['DOCKER_HOST']
    name = host.removeprefix('tcp://').rsplit(':', 1)[0]
    extra = compose['services']['dind']['environment'].get('DOCKER_TLS_SAN')
    assert f'DNS:{name}' in re.split(r'[\s,]+', extra or '')


def test_the_daemon_healthcheck_speaks_tls(compose):
    """Healthy means `lock` can connect, the way `lock` connects."""
    test = ' '.join(compose['services']['dind']['healthcheck']['test'])
    assert '--tlsverify' in test
    assert 'tcp://127.0.0.1:2376' in test


def test_every_image_compose_pulls_is_pinned_by_digest(compose):
    """A tag moves with every rebuild of its image, and a digest does
    not: pinned as the recipes' images are (sandbox_test), and the
    Dockerfiles'. The tag stays, to say which image the digest is of.

    Every image, not the daemon's alone: ClickHouse's was the one left
    on a tag after #45, and so ran whatever the tag served on the day.
    What compose builds it names, and does not pull.
    """
    pulled = {
        name: service['image']
        for name, service in compose['services'].items()
        if 'build' not in service
    }
    assert {'clickhouse', 'dind'} <= set(pulled)
    for name, image in pulled.items():
        assert re.fullmatch(
            r'[a-z0-9._/-]+:[\w.-]+@sha256:[0-9a-f]{64}', image,
        ), f'{name}: {image} is not pinned by digest'
        assert ':latest@' not in image, image


def _docker_release(reference: str) -> str:
    """The Docker major release an image of `docker` is: 29-cli, 29."""
    _, tag = _split_reference(reference.partition('@')[0])
    return tag.split('-')[0].split('.')[0]


def test_the_lock_image_has_the_daemons_docker_release(compose, dockerfile):
    """The client `sbom lock` drives the nested daemon with is the
    daemon's own release.

    Dependabot moves the daemon, which compose names, and not the
    client, which `COPY --from=` names (dependabot.yml): #80 moved the
    daemon to 29 and would have left the client at 27, past its end of
    life.
    """
    [lock] = [stage for stage in _stages(dockerfile) if stage.name == 'lock']
    [client] = [
        reference for reference in _copied_from(lock)
        if _split_reference(reference)[0] == 'docker'
    ]
    daemon = compose['services']['dind']['image']
    assert _docker_release(client) == _docker_release(daemon), (
        client, daemon,
    )


def test_long_running_services_restart_themselves(compose):
    """A service others depend on must come back on its own.

    `clickhouse` had no restart policy while `web` and `collector` both
    did. A Docker daemon restart therefore returned the dashboard
    without its database: the site stayed up and answered every query
    with a 500, and the container read `Exited (0)` — a clean
    shutdown, so nothing in `docker ps -a`, the logs, or the disk
    looked wrong. Only the page did.

    Scoped to the services that are meant to keep running. `cli` and
    `lock` are one-shot commands, and restarting those would loop.
    """
    persistent = {'clickhouse', 'web', 'collector', 'depgraph'}
    for name in persistent:
        policy = compose['services'][name].get('restart')
        assert policy == 'unless-stopped', f'{name} has restart={policy!r}'


def test_one_shot_services_do_not_restart(compose):
    """`unless-stopped` on a command that exits is a restart loop."""
    for name in ('cli', 'lock'):
        assert not compose['services'][name].get('restart')


# --- the dashboard ----------------------------------------------------------
#
# `wrangler dev` is a development server, and it behaves like one: it
# serves tools under /cdn-cgi/, trusts request headers a proxy would
# normally set, and keeps its state beside the project. These pin what
# the container does about each (#18).

#: Where wrangler keeps local state — Durable Objects, KV, D1, R2 —
#: relative to the directory holding wrangler.jsonc.
WRANGLER_STATE = '.wrangler/state'


def test_the_image_turns_off_the_local_explorer(web_dockerfile):
    """wrangler serves miniflare's local explorer unless told not to.

    It is a UI and API under /cdn-cgi/local/explorer that reads and
    writes every binding — the spend counter's storage, raw SQL on D1 — and
    miniflare admits a /cdn-cgi/ request on its Host header alone, which
    anyone who reaches 8787 can set to `localhost`. wrangler reads
    exactly this variable, and exactly `true` or `false`.
    """
    assert _image_env(web_dockerfile).get('X_LOCAL_EXPLORER') == 'false'


def test_compose_turns_off_the_local_explorer_too(compose):
    """So an image built before the ENV existed starts with it off.

    The string `'false'`: a bare YAML `false` is a boolean, which is not
    what wrangler compares against.
    """
    env = compose['services']['web']['environment']
    assert env.get('X_LOCAL_EXPLORER') == 'false'


def test_the_image_keeps_no_local_traces(web_dockerfile):
    """wrangler also records a trace of every Worker invocation into
    `.wrangler/state`, unless told not to, in a store with no retention
    whose only reader is the explorer.

    Once that directory is a volume, nothing ever clears it: 500
    `/api/q` calls wrote 4.8 MB, about 10 KB each, where with this off
    they wrote nothing.
    """
    assert _image_env(web_dockerfile).get('X_LOCAL_OBSERVABILITY') == 'false'


def test_compose_keeps_no_local_traces_either(compose):
    """The volume comes from compose, so an older image must not fill it."""
    env = compose['services']['web']['environment']
    assert env.get('X_LOCAL_OBSERVABILITY') == 'false'


def test_the_image_installs_the_wrangler_the_entrypoint_runs(web_dockerfile):
    """The entrypoint runs wrangler from node_modules/.bin rather than
    through npx, which passed a stop on to a shell of its own and not to
    wrangler (web_entrypoint_test). wrangler is a devDependency, and
    `npm ci` installs those only without --omit=dev and while NODE_ENV
    is not `production`, which the image sets — after the install.
    """
    package = json.loads((ROOT / 'web' / 'package.json').read_text())
    declared = {
        **package.get('dependencies', {}),
        **package.get('devDependencies', {}),
    }
    assert 'wrangler' in declared

    instructions = _instructions(web_dockerfile)
    install = next(
        at for at, (keyword, arguments) in enumerate(instructions)
        if keyword == 'RUN' and 'npm ci' in arguments
    )
    omits = re.compile(r'--omit[= ]dev|--production|--only[= ]prod')
    assert not omits.search(instructions[install][1])
    assert 'NODE_ENV=production' not in instructions[install][1]
    for keyword, arguments in instructions[:install]:
        if keyword == 'ENV':
            assert 'NODE_ENV=production' not in shlex.split(arguments)


def test_the_spend_counter_survives_a_recreate(compose, web_dockerfile):
    """The daily cap's counter is a Durable Object, which wrangler runs
    locally (#33), keeping its storage in `.wrangler/state` beside
    wrangler.jsonc, as the KV counter before it did. Left in the
    container layer it went with every recreate — `up --build`
    included — and the day's cap reset with it.
    """
    state = f'{_workdir(web_dockerfile)}/{WRANGLER_STATE}'
    mounts = {}
    for volume in compose['services']['web'].get('volumes', []):
        source, target = volume.split(':')[:2]
        mounts[target] = source
    assert state in mounts, f'nothing is mounted at {state}'

    # Named, not a host directory: a bind mount is owned by whoever
    # created it on the host, and the Worker runs as uid 10002.
    source = mounts[state]
    assert not source.startswith(('.', '/', '~')), source
    assert source in (compose.get('volumes') or {})


def test_the_state_volume_starts_out_writable(web_dockerfile):
    """Docker fills an empty named volume from the image, ownership
    included, so the mount point has to exist there and belong to the
    uid the Worker runs as. One the image lacks is created root-owned,
    and the counter could not be written."""
    workdir = _workdir(web_dockerfile)
    state = f'{workdir}/{WRANGLER_STATE}'
    instructions = _instructions(web_dockerfile)
    user_at = max(
        i for i, (keyword, _) in enumerate(instructions) if keyword == 'USER'
    )
    assert instructions[user_at][1] == '10002'

    runs = [a for k, a in instructions[:user_at] if k == 'RUN']
    setup = next((run for run in runs if f'mkdir -p {state}' in run), None)
    assert setup is not None, f'the image never creates {state}'
    chown = setup.find(f'chown -R 10002:10002 {workdir}')
    assert chown > setup.find('mkdir'), f'{state} is not chowned after mkdir'


def test_the_turnstile_secret_reaches_the_container(compose):
    """The Worker verifies Turnstile only when TURNSTILE_SECRET is set,
    and compose never passed it, so it could not be.

    Optional rather than required: a dashboard on a private URL has no
    one to keep out.
    """
    secret = compose['services']['web']['environment'].get('TURNSTILE_SECRET')
    assert secret is not None and '${TURNSTILE_SECRET' in secret
    assert ':?' not in secret


def test_the_turnstile_site_key_reaches_the_container(compose):
    """The secret is half of it (#32). The page renders the widget with
    the site key, which the Worker hands it, so without it here a
    deployment that set the secret could never pass its own check."""
    site_key = compose['services']['web']['environment'].get(
        'TURNSTILE_SITE_KEY',
    )
    assert site_key == '${TURNSTILE_SITE_KEY:-}'


def test_the_edge_secret_reaches_the_container(compose):
    """With EDGE_SECRET set, the Worker believes `CF-Connecting-IP` only
    on a request carrying it, which a Cloudflare Transform Rule adds; a
    client that reaches 8787 directly lands in one shared rate-limit
    bucket, whatever address it claims (#31). Compose has to pass it for
    the entrypoint to hand it on.

    Optional: unset, the Worker takes the header on trust, as it did.
    """
    secret = compose['services']['web']['environment'].get('EDGE_SECRET')
    assert secret is not None and '${EDGE_SECRET' in secret
    assert ':?' not in secret


def test_the_dashboard_bind_address_is_configurable(compose):
    """Every interface includes the LAN.

    A client reaching 8787 directly, rather than through the tunnel,
    chooses the headers the tunnel would otherwise have set —
    `CF-Connecting-IP`, which the chat rate limiter keys on, among them.
    Narrowing it, to the docker bridge say, must not mean editing this
    file.
    """
    ports = compose['services']['web']['ports']
    assert len(ports) == 1
    parts = ports[0].rsplit(':', 2)
    assert len(parts) == 3, f'{ports[0]!r} leaves the address to Docker'
    address, published, target = parts
    assert (published, target) == ('8787', '8787')
    assert address.startswith('${WEB_BIND'), ports[0]


# --- the dependency-graph worker --------------------------------------------

def test_the_depgraph_worker_is_behind_the_collect_profile(compose):
    """It spends a token's dependency-graph budget all day."""
    assert compose['services']['depgraph']['profiles'] == ['collect']


def test_the_depgraph_worker_runs_the_loop_in_its_own_mode(compose):
    """The collector's loop, its checks and stop handling, in `depgraph`
    mode: a loop of its own, paced by its own bucket."""
    service = compose['services']['depgraph']
    assert service['entrypoint'][-1] == 'depgraph'
    assert service['entrypoint'][1] == '/app/collector-loop.sh'
    assert service['image'] == compose['services']['collector']['image']
    assert set(service['volumes']) == set(
        compose['services']['collector']['volumes'],
    )


@pytest.mark.parametrize('service', ['depgraph', 'cli'])
def test_the_extra_depgraph_tokens_reach_the_container(compose, service):
    """Empty when unset: then the worker has GITHUB_TOKEN's alone."""
    environment = compose['services'][service]['environment']
    assert environment['CHATSBOM_DEPGRAPH_TOKENS'] == (
        '${CHATSBOM_DEPGRAPH_TOKENS:-}'
    )


# --- the database -----------------------------------------------------------

def test_the_database_is_a_long_term_support_release(compose):
    """ClickHouse keeps an LTS release, each year's .3 and .8, in
    security support for a year, and a monthly one only while it is
    among the three newest. Dependabot proposes the newest, whatever it
    is: 26.6 (#81), out of support by the time it was looked at, as the
    25.12 it would have replaced was.
    """
    repository, tag = _split_reference(
        compose['services']['clickhouse']['image'].partition('@')[0],
    )
    assert repository == 'clickhouse/clickhouse-server'
    release = re.match(r'(\d+)\.(\d+)\b', tag)
    assert release, tag
    assert int(release[2]) in (3, 8), f'{tag} is not an LTS release'


def test_every_recipe_for_the_database_runs_the_release_compose_runs(
    compose,
):
    """README's `docker run`, and the one the CLI prints when no server
    answers (core/clickhouse.py), start the database on the same
    database/data as compose does.

    A release they named alone would be a downgrade there, and
    ClickHouse does not go back: 25.12 detaches every part 26.x wrote
    (DEPLOY.md). Dependabot moves compose's image and nothing else (#81).
    The release, not the build: compose pins its build by digest, which
    each patch release moves, and a patch release is not a downgrade.
    """
    release = compose['services']['clickhouse']['image'].partition('@')[0]
    repository, _ = _split_reference(release)
    named = re.compile(rf'{re.escape(repository)}[:@][\w.:@-]*')
    recipes = {
        'README.md': (ROOT / 'README.md').read_text(),
        'chatsbom/core/clickhouse.py': START_CLICKHOUSE,
    }
    for where, text in recipes.items():
        runs = {found.partition('@')[0] for found in named.findall(text)}
        assert runs == {release}, where
