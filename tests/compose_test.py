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
import xml.etree.ElementTree as ET
from collections.abc import Iterator
from dataclasses import dataclass
from ipaddress import ip_network
from pathlib import Path

import pytest
import yaml

from chatsbom.core.clickhouse import START_CLICKHOUSE
from chatsbom.core.config import PathConfig
from chatsbom.server.settings import edge_subnets
from tests.env_example_test import server_reads
from tests.env_example_test import shell_reads
from tests.extras_test import NEEDS

ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope='module')
def compose() -> dict:
    return yaml.safe_load((ROOT / 'docker-compose.yaml').read_text())


@pytest.fixture(scope='module')
def dockerfile() -> str:
    return (ROOT / 'Dockerfile').read_text()


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


def _exec_form(arguments: str) -> list[str]:
    """A JSON array of an ENTRYPOINT, CMD or HEALTHCHECK: exec form, run
    as it is written, where a string would be run by a shell."""
    words = json.loads(arguments)
    assert isinstance(words, list), arguments
    return [str(word) for word in words]


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
    'cloudflared': 'puts the dashboard on the internet',
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
        for source, target, _ in _mounts(service):
            assert 'docker.sock' not in source + target, name


def test_the_image_has_no_docker_cli(dockerfile):
    assert 'docker-ce-cli' not in dockerfile
    assert 'docker.io' not in dockerfile.replace('ghcr.io', '')


#: The architectures the collector's image is built for, as BuildKit
#: names them in TARGETARCH and Syft's release names its archives.
SYFT_ARCHITECTURES = ('amd64', 'arm64')


def _arguments(dockerfile: str) -> dict[str, str]:
    """Each `ARG` of the file, by name: its default, '' if it has none."""
    return dict(
        argument.partition('=')[::2]
        for keyword, argument in _instructions(dockerfile) if keyword == 'ARG'
    )


def _syft_step(dockerfile: str) -> str:
    """The one RUN that installs Syft."""
    [step] = [
        arguments for keyword, arguments in _instructions(dockerfile)
        if keyword == 'RUN' and 'syft' in arguments
    ]
    return step


def test_syft_is_pinned(dockerfile):
    """The version keys the SBOM cache; `latest` would repartition it.

    The archive is pinned too: the release's own, for the architecture
    the image is built for, checked against the digest pinned here for
    it before anything is taken out of it. Syft's install.sh, which did
    this before, checked the archive against the release's checksums
    file and only logged a mismatch: given a wrong checksum it said "did
    not verify", installed the archive all the same and exited 0 (#118),
    so nothing checked what the image ran.
    """
    arguments = _arguments(dockerfile)
    assert re.fullmatch(r'\d+\.\d+\.\d+', arguments['SYFT_VERSION'])
    step = _syft_step(dockerfile)
    fetch = step.index(
        'https://github.com/anchore/syft/releases/download/v${SYFT_VERSION}/'
        'syft_${SYFT_VERSION}_linux_${TARGETARCH}.tar.gz',
    )
    check = step.index('| sha256sum --check --strict')
    extract = step.index('tar -xzf')
    assert fetch < check < extract
    # Each architecture's archive against its own digest, and anything
    # else refused: an empty TARGETARCH (a builder without BuildKit)
    # included.
    for architecture in SYFT_ARCHITECTURES:
        name = f'SYFT_SHA256_{architecture.upper()}'
        assert re.fullmatch(r'[0-9a-f]{64}', arguments[name]), name
        [digest] = re.findall(
            rf'\b{architecture}\)\s*(\w+)="\$\{{{name}\}}"\s*;;', step,
        )
        assert f'echo "${{{digest}}}  ' in step[:check]
    assert re.search(r'\*\)[^;]*;\s*exit 1\s*;;', step)
    # TARGETARCH is BuildKit's, and a stage sees an ARG it names.
    assert 'TARGETARCH' in arguments


def test_no_installer_is_run(dockerfile):
    """The archive is fetched and checked here, and nothing else runs.

    install.sh ignored a mismatched archive (above), and asked
    github.com's releases page for the tag first, which this
    environment's egress refuses (#118). Before it was pinned,
    get.anchore.io served whatever the installer was the day of the
    build, piped to `sh`.
    """
    for keyword, arguments in _instructions(dockerfile):
        assert 'install.sh' not in arguments, keyword
        assert 'get.anchore.io' not in arguments, keyword
        if keyword == 'RUN':
            assert not re.search(r'\|\s*(sudo\s+)?(ba)?sh\b', arguments)


def test_the_image_s_syft_is_root_s(dockerfile):
    """The archive's `syft` belongs to uid 1001, the release runner's,
    and tar run as root gives a file the archive's owner. The collector
    runs as the invoking user, often 1001, who could then replace the
    binary every scan runs. install.sh copied it in as root's."""
    assert '--no-same-owner' in _syft_step(dockerfile).split()


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


def _removed_accounts() -> set[str]:
    """The accounts database/config/users.d removes from the image's."""
    return {
        user.tag
        for path in (ROOT / 'database' / 'config' / 'users.d').glob('*.xml')
        for user in ET.parse(path).getroot().findall('users/*')
        if user.get('remove') is not None
    }


def test_the_first_start_asks_nothing_of_the_removed_default_user(compose):
    """The image's entrypoint does two things on an empty data directory,
    both as its `default` user: it creates CLICKHOUSE_DB, and it runs
    what /docker-entrypoint-initdb.d holds. admin.xml removes `default`.

    With CLICKHOUSE_DB set, a fresh clone's first start failed (#79),
    measured on the pinned 26.8 image: `create database 'chatsbom'`,
    then `Code: 516 ... default: Authentication failed`, and the
    entrypoint exited. `up --wait` reported the container unhealthy
    after 4 s; the restart policy brought it back, and the second
    start, finding a data directory, skipped the step and came up
    healthy with no `chatsbom` database, RestartCount 1. The database
    is made by `db index` and `db raw --apply`, as admin.
    """
    assert 'default' in _removed_accounts()
    service = compose['services']['clickhouse']
    environment = service.get('environment') or {}
    if isinstance(environment, list):
        environment = dict(item.partition('=')[::2] for item in environment)
    assert 'CLICKHOUSE_DB' not in environment
    assert not [
        volume for volume in service.get('volumes', [])
        if '/docker-entrypoint-initdb.d' in volume
    ]


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


@pytest.mark.parametrize('service', ['collector', 'depgraph', 'web'])
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
    [
        (), ('collect',), ('lock',), ('tools',), ('tunnel',),
        ('collect', 'lock', 'tools', 'tunnel'),
    ],
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
    else; the loop traps it besides (collector_loop_test).
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


@pytest.mark.parametrize('path', ['.env', 'web/.env'])
def test_env_files_never_reach_an_image(path):
    """`COPY web/ ./` copies whatever is there, so a developer's own
    `web/.env` was baked into a layer of the web image, where anyone
    holding the image can read it."""
    patterns = (ROOT / '.dockerignore').read_text().splitlines()
    assert _dockerignored(path, patterns)


def test_the_loop_survives_a_failing_slice():
    loop = (ROOT / 'deploy' / 'collector-loop.sh').read_text()
    # A slice that fails must be recorded and stepped over, not fatal.
    assert 'queue sync' in loop
    assert '||' in loop
    assert 'set -eu' in loop


def test_every_setting_the_loop_reads_reaches_its_container(compose):
    """A container is given only what its `environment` names, so a
    setting the loop reads that neither of its services is given is one
    `.env` cannot change, and nothing says so: the loop quietly takes
    its own fallback."""
    reads = shell_reads((ROOT / 'deploy' / 'collector-loop.sh').read_text())
    given = {
        name
        for service in ('collector', 'depgraph')
        for name in compose['services'][service]['environment']
    }
    assert sorted(reads - given) == []


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
    'name', ['clickhouse', 'collector', 'depgraph', 'cli', 'web'],
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
    """(source, target, options) for each volume of a service, in the
    short syntax's terms whichever syntax it is written in: the long
    one's `read_only` is `ro`."""
    mounts = []
    for volume in service.get('volumes', []):
        if isinstance(volume, dict):
            options = ['ro'] if volume.get('read_only') else []
            mounts.append((volume['source'], volume['target'], options))
        else:
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
    assert {'clickhouse', 'dind', 'cloudflared'} <= set(pulled)
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
    persistent = {
        'clickhouse', 'collector', 'depgraph', 'cloudflared', 'web',
    }
    for name in persistent:
        policy = compose['services'][name].get('restart')
        assert policy == 'unless-stopped', f'{name} has restart={policy!r}'


def test_one_shot_services_do_not_restart(compose):
    """`unless-stopped` on a command that exits is a restart loop."""
    for name in ('cli', 'lock'):
        assert not compose['services'][name].get('restart')


# --- the tunnel (#130) ------------------------------------------------------
#
# `cloudflared` as a service of this project, on a network it shares with
# `web` alone, which publishes no port: the tunnel is the only way to the
# site from off the machine, and the CF-Connecting-IP it hands on is
# Cloudflare's by construction.

#: The tunnel mode, which compose reads on top of docker-compose.yaml.
TUNNEL_FILE = ROOT / 'docker-compose.tunnel.yaml'


@dataclass
class Tagged:
    """A value under one of compose's merge tags, `!reset` or
    `!override`: it replaces what the files before it set, where a plain
    value would be merged with it."""
    tag: str
    value: object


class ComposeLoader(yaml.SafeLoader):
    """A safe loader that reads compose's merge tags, which
    `yaml.safe_load` refuses, as what they say."""


def _tagged(loader: yaml.SafeLoader, node: yaml.Node) -> Tagged:
    value: object
    if isinstance(node, yaml.SequenceNode):
        value = loader.construct_sequence(node, deep=True)
    elif isinstance(node, yaml.MappingNode):
        value = loader.construct_mapping(node, deep=True)
    else:
        assert isinstance(node, yaml.ScalarNode), node
        value = loader.construct_scalar(node)
    return Tagged(node.tag, value)


for _tag in ('!reset', '!override'):
    ComposeLoader.add_constructor(_tag, _tagged)


@pytest.fixture(scope='module')
def tunnel_file() -> dict:
    return yaml.load(TUNNEL_FILE.read_text(), Loader=ComposeLoader)


def _declared_networks(compose: dict) -> dict:
    """The networks the file declares, each's settings, {} for none."""
    return {
        name: settings or {}
        for name, settings in (compose.get('networks') or {}).items()
    }


def _metrics(service: dict) -> str:
    """The address a command serves or asks for cloudflared's metrics on."""
    command = service['command']
    return command[command.index('--metrics') + 1]


def test_the_tunnel_is_off_unless_asked_for(compose):
    """It puts the site on the internet, so it is a decision (COSTLY),
    under the name the docs give it."""
    assert compose['services']['cloudflared']['profiles'] == ['tunnel']


def test_the_tunnel_mode_switches_the_tunnel_on(tunnel_file):
    """docker-compose.tunnel.yaml on top of docker-compose.yaml is the
    tunnel mode, and it changes one thing and nothing else: `up` starts
    `cloudflared`. `!reset` is compose's way to take a list away: an
    empty list would be merged with the profile, which would stay."""
    assert tunnel_file == {
        'services': {
            'cloudflared': {'profiles': Tagged('!reset', [])},
        },
    }


def test_the_tunnel_image_is_a_cloudflared_release(compose):
    """Cloudflare's image, at a release's tag, pinned by digest as every
    image here is (test_every_image_compose_pulls_is_pinned_by_digest)."""
    repository, tag = _split_reference(
        compose['services']['cloudflared']['image'].partition('@')[0],
    )
    assert repository == 'cloudflare/cloudflared'
    assert re.fullmatch(r'\d{4}\.\d{1,2}\.\d+', tag), tag


def test_the_tunnel_runs_the_remotely_managed_tunnel_its_token_names(
    compose,
):
    """`tunnel run` with no name, config file, credentials or `--url`:
    the token names the tunnel, and the Cloudflare dashboard holds its
    routes.

    The token comes from `.env`, and is empty when unset: cloudflared
    then refuses to start, and says why, where a `${TUNNEL_TOKEN:?...}`
    would refuse every compose command
    (test_no_variable_is_required_to_read_the_file). It is in the
    environment and never on the command line, which any user on the
    host can read. And it is all the service is given: no account, no
    key.
    """
    service = compose['services']['cloudflared']
    command = service['command']
    assert isinstance(command, list), 'a shell would run it'
    assert (command[0], command[-1]) == ('tunnel', 'run')
    for flag in ('--token', '--config', '--cred', '--url', '--hello-world'):
        assert not [word for word in command if word.startswith(flag)], flag
    assert service['environment'] == {'TUNNEL_TOKEN': '${TUNNEL_TOKEN:-}'}


def test_the_tunnel_serves_its_metrics_on_its_own_loopback(compose):
    """The image binds its metrics server to every interface, which here
    would be `edge` and the way out: /metrics, the tunnel's routes at
    /config and /debug/pprof, to the site and to whatever else is there.
    The healthcheck asks from inside the container, where the loopback
    is enough. And the tunnel publishes nothing: it dials out."""
    service = compose['services']['cloudflared']
    host, _, port = _metrics(service).rpartition(':')
    assert host == '127.0.0.1'
    assert port.isdigit()
    assert 'ports' not in service
    assert 'expose' not in service


def test_the_tunnel_healthcheck_asks_the_ready_endpoint(compose):
    """/ready answers 200 while at least one connection to Cloudflare's
    edge is up, and 503 otherwise. The image has no shell and no curl:
    `cloudflared tunnel ready` asks the metrics server it is given, the
    one the service runs, and exits non-zero on anything but a 200."""
    service = compose['services']['cloudflared']
    assert service['healthcheck']['test'] == [
        'CMD', 'cloudflared', 'tunnel', '--metrics', _metrics(service),
        'ready',
    ]


def test_the_edge_is_internal_and_isolated(compose):
    """Internal, so nothing on it reaches beyond it; isolated, so its
    bridge has no address on the host either.

    An internal network is otherwise the host's too: a process there
    reaches the site from an address in the edge's subnet, where only
    the tunnel should be, and the two containers reach whatever the host
    serves on every interface. Docker refuses `isolated` on a network
    that is not internal.
    """
    edge = _declared_networks(compose)['edge']
    assert edge.get('internal') is True
    options = edge.get('driver_opts') or {}
    for family in ('ipv4', 'ipv6'):
        mode = options.get(f'com.docker.network.bridge.gateway_mode_{family}')
        assert mode == 'isolated', family


def test_the_edge_holds_the_tunnel_and_the_site_alone(compose):
    """The next service added to the file included."""
    on_edge = {
        name for name, service in compose['services'].items()
        if 'edge' in _networks(service)
    }
    assert on_edge == {'cloudflared', 'web'}


def test_the_tunnel_is_on_the_edge_and_its_own_way_out_alone(compose):
    """`edge` to reach the site, and a network of its own to reach
    Cloudflare. Not `default`: the tunnel has no business with
    ClickHouse, nor has anything there with the tunnel."""
    services = compose['services']
    assert _networks(services['cloudflared']) == {'edge', 'cloudflared-egress'}
    way_out = {
        name for name, service in services.items()
        if 'cloudflared-egress' in _networks(service)
    }
    assert way_out == {'cloudflared'}
    assert not _declared_networks(compose)['cloudflared-egress'].get(
        'internal',
    )


def _compose_config(
    tmp_path: Path, *files: Path, profiles: tuple[str, ...] = (),
    env: str = '',
) -> dict:
    """What compose makes of the files, as JSON, with nothing set but
    what `env` sets, as the lines of a `.env`."""
    env_file = tmp_path / 'compose.env'
    env_file.write_text(env)
    command = ['docker', 'compose', '--env-file', str(env_file)]
    for path in files:
        command += ['--file', str(path)]
    for profile in profiles:
        command += ['--profile', profile]
    kept = ('PATH', 'HOME', 'DOCKER_CONFIG')
    result = subprocess.run(
        [*command, 'config', '--format', 'json'],
        env={name: os.environ[name] for name in kept if name in os.environ},
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert result.returncode == 0, result.stderr
    config: dict = json.loads(result.stdout)
    return config


@pytest.mark.skipif(
    not _compose_cli(),
    reason='needs the docker compose CLI (not a daemon)',
)
def test_the_tunnel_mode_as_compose_reads_it(tmp_path):
    """As compose merges the files, with nothing set: the tunnel mode
    runs `cloudflared`, on `edge` and its own way out, as `--profile
    tunnel` does for one command. Without either, the tunnel is off."""
    base = ROOT / 'docker-compose.yaml'

    default = _compose_config(tmp_path, base)
    assert 'cloudflared' not in default['services']

    tunnel = _compose_config(tmp_path, base, TUNNEL_FILE)
    cloudflared = tunnel['services']['cloudflared']
    assert 'profiles' not in cloudflared
    assert set(cloudflared['networks']) == {'edge', 'cloudflared-egress'}
    assert tunnel['networks']['edge']['internal'] is True

    asked = _compose_config(tmp_path, base, profiles=('tunnel',))
    assert asked['services']['cloudflared'] == {
        **cloudflared, 'profiles': ['tunnel'],
    }


def test_the_env_example_names_the_tunnel_mode():
    """Uncommenting `.env.example`'s COMPOSE_FILE is the tunnel mode:
    this file, then the tunnel's, in the order compose merges them."""
    text = (ROOT / '.env.example').read_text()
    [files] = re.findall(r'^# COMPOSE_FILE=(\S+)$', text, re.M)
    assert files.split(':') == ['docker-compose.yaml', TUNNEL_FILE.name]


# --- the web service (#145) -------------------------------------------------
#
# `chatsbom web serve`, in the image Dockerfile.web builds
# (web_image_test): the site. Started by a bare `up`; on `edge`, where
# the tunnel reaches it, and on `default`, its way out to the model's
# API; publishing no port; and with nothing it does not need: it writes
# one file, web.sqlite, in a volume of its own.

#: The web service's image.
WEB_DOCKERFILE = ROOT / 'Dockerfile.web'

#: The pools Docker takes a network's subnet from when it is given none
#: (moby's libnetwork/ipamutils): the local ones, 172.17 to 172.31 as
#: /16s and 192.168 as /20s, and the global one, 10/8 as /24s, which
#: swarm's overlay networks take theirs from.
DOCKER_POOLS = tuple(
    ip_network(pool) for pool in (
        '172.17.0.0/16', '172.18.0.0/16', '172.19.0.0/16', '172.20.0.0/14',
        '172.24.0.0/14', '172.28.0.0/14', '192.168.0.0/16', '10.0.0.0/8',
    )
)

#: What compose sets for the service itself, rather than taking from
#: `.env`: where its state volume and the snapshots are mounted, and the
#: edge's subnet, which the network is given as well.
WEB_FIXED = {'WEB_STATE_DIR', 'WEB_SNAPSHOT', 'EDGE_SUBNET'}


def _web(compose: dict) -> dict:
    return compose['services']['web']


def _fallback(text: str) -> tuple[str, str]:
    """`${NAME:-fallback}` as (NAME, fallback)."""
    match = re.fullmatch(r'\$\{(\w+):-([^}]*)\}', text)
    assert match, text
    return match[1], match[2]


def _edge_subnet(compose: dict) -> str:
    """What the file gives `edge` as its subnet, as it is written."""
    [config] = _declared_networks(compose)['edge']['ipam']['config']
    return str(config['subnet'])


def _web_command(dockerfile: str) -> list[str]:
    """What the image runs: its last stage's ENTRYPOINT, then its CMD."""
    last = _stages(dockerfile)[-1].instructions
    [entrypoint] = [a for k, a in last if k == 'ENTRYPOINT']
    [command] = [a for k, a in last if k == 'CMD']
    return _exec_form(entrypoint) + _exec_form(command)


def _web_port() -> int:
    """The port the image serves on, which its command names."""
    command = _web_command(WEB_DOCKERFILE.read_text())
    return int(command[command.index('--port') + 1])


def test_the_site_starts_with_a_bare_up(compose):
    """No profile: it is the site, and serving a page spends nothing
    (COSTLY). Without ALTCHA_HMAC_KEY or a published snapshot it says
    which in its log, and the restart policy tries it again."""
    assert 'profiles' not in _web(compose)


def test_the_site_is_built_from_its_own_dockerfile(compose):
    """The last stage of Dockerfile.web, named: what a `docker build`
    of it makes with no target too."""
    build = _web(compose)['build']
    assert build['context'] == '.'
    assert build['dockerfile'] == WEB_DOCKERFILE.name
    stages = _stages(WEB_DOCKERFILE.read_text())
    assert build['target'] == stages[-1].name


def test_the_site_is_on_the_edge_and_its_way_out(compose):
    """`edge`, where cloudflared reaches it, and `default`, since `edge`
    leads nowhere and the chat's model is on the internet. Not the
    daemon's `sandbox`, nor the tunnel's own way out."""
    assert _networks(_web(compose)) == {'default', 'edge'}


def test_the_site_publishes_no_port(compose):
    """In either mode the tunnel is its way in from off this machine,
    over `edge`, and nothing else is."""
    assert 'ports' not in _web(compose)


def test_the_site_runs_with_nothing_it_does_not_need(compose):
    """A web process that writes one file, in its own volume: a
    read-only root, and a tmpfs, bounded, for what Python and SQLite
    put in /tmp; no capability, and no way to gain one; docker-init as
    PID 1, to hand it the stop signal."""
    web = _web(compose)
    assert web.get('read_only') is True
    assert web.get('cap_drop') == ['ALL']
    assert not web.get('cap_add')
    assert not web.get('privileged')
    assert 'no-new-privileges:true' in web.get('security_opt', [])
    assert web.get('init') is True
    tmpfs = dict(entry.partition(':')[::2] for entry in web.get('tmpfs', []))
    assert set(tmpfs) == {'/tmp'}
    assert re.search(r'(^|,)size=\d+[kmg]?(,|$)', tmpfs['/tmp']), tmpfs


def test_the_site_is_resource_bounded(compose):
    web = _web(compose)
    assert web.get('mem_limit')
    assert float(web.get('cpus', 0)) > 0


def test_a_stop_lets_a_question_in_flight_finish(compose):
    """uvicorn stops on SIGTERM once what is in flight has been
    answered (Dockerfile.web), and an answer streams for as long as its
    turns take. Docker's 10 s would cut most of them off; 30 s is what
    cloudflared gives the requests it is carrying when it stops."""
    grace = re.fullmatch(r'(\d+)s', str(_web(compose)['stop_grace_period']))
    assert grace and int(grace[1]) >= 30, _web(compose)['stop_grace_period']


def test_the_site_checks_itself_as_its_image_does(compose):
    """The image's own healthcheck, which asks /healthz from inside the
    container (web_image_test): a bare `docker run` has it too, and
    there is one of it to keep right."""
    assert 'healthcheck' not in _web(compose)


def test_the_sites_state_is_a_named_volume(compose):
    """web.sqlite: the day's spend and the challenges used, which a
    recreate must keep, or the day's cap would start again with each
    `up --build`. Named, so that Docker fills it from the image's
    directory, owned by the uid the service runs as (web_image_test).
    Written to, so not read-only."""
    web = _web(compose)
    state = web['environment']['WEB_STATE_DIR']
    assert state.startswith('/'), state
    [(source, options)] = [
        (source, options) for source, target, options in _mounts(web)
        if target == state
    ]
    assert source in (compose.get('volumes') or {}), 'not a named volume'
    assert options == []


def test_the_sites_state_volume_starts_out_its_to_write(compose):
    """Docker fills an empty named volume from the image's directory,
    ownership included. So the image makes the directory the volume is
    mounted on, and gives it to the uid it runs as: missing there, the
    volume would be root's, and the service could not open the spend
    ledger."""
    state = _web(compose)['environment']['WEB_STATE_DIR']
    image = _stages(WEB_DOCKERFILE.read_text())[-1].instructions
    [user] = [arguments for keyword, arguments in image if keyword == 'USER']
    uid = user.partition(':')[0]
    [setup] = [
        arguments for keyword, arguments in image
        if keyword == 'RUN' and f'mkdir {state}' in arguments
    ]
    assert re.search(rf'chown {uid}(:\d+)? {re.escape(state)}\b', setup)


def test_the_site_reads_the_published_snapshots_read_only(compose):
    """The directory the CLI publishes snapshots in, where the service
    reads CURRENT as each question starts, so that a new snapshot is
    served without a restart. Read-only: what serves the page cannot
    change what it serves.

    And never made by Docker. A missing bind source is made, owned by
    root, and then the CLI, run as the user, could publish nothing in
    it. `create_host_path: false` refuses the start instead, naming the
    path.
    """
    web = _web(compose)
    snapshots = web['environment']['WEB_SNAPSHOT']
    [volume] = [
        volume for volume in web['volumes']
        if isinstance(volume, dict) and volume['target'] == snapshots
    ]
    assert volume['type'] == 'bind'
    assert volume['source'] == f'./{PathConfig().snapshots_dir}'
    assert volume.get('read_only') is True
    assert (volume.get('bind') or {}).get('create_host_path') is False


def test_every_setting_the_site_reads_reaches_its_container(compose):
    """From `.env`, empty when unset: the service takes its own default
    for a setting that is empty, and does not start without one it
    needs, ALTCHA_HMAC_KEY, saying which. A `${NAME:?...}` would refuse
    every compose command instead, the other services' included
    (test_no_variable_is_required_to_read_the_file).

    Three are compose's to set: where the state volume and the
    snapshots are mounted, and the edge's subnet, which the network is
    given too (test_the_edge_has_the_subnet_the_site_believes)."""
    environment = _web(compose)['environment']
    reads = server_reads()
    assert WEB_FIXED <= reads
    for name in sorted(reads - WEB_FIXED):
        assert environment.get(name) == '${' + name + ':-}', name
    assert WEB_FIXED <= set(environment)


def test_the_site_is_given_nothing_it_does_not_read(compose):
    """No setting of the Worker's, no ClickHouse account, no token: the
    service reads none of them. What it logs in is compose's to say, as
    for everything that runs unattended."""
    environment = _web(compose)['environment']
    assert set(environment) - server_reads() == {'CHATSBOM_LOG_FORMAT'}


def test_the_edge_has_the_subnet_the_site_believes(compose):
    """EDGE_SUBNET names the network only cloudflared is on (#139):
    CF-Connecting-IP is believed from a peer there, and from no other.
    Docker chose the network's subnet, and it could not be named: one on
    this machine, another on the next, and another after a `down`.

    One setting, for both the network and the service, so that the two
    cannot differ. Were they to, every visitor through the tunnel would
    be keyed as cloudflared, all in one rate-limit bucket, and /healthz
    would answer the public. Set in `.env`, it moves both.
    """
    name, _ = _fallback(_edge_subnet(compose))
    assert name == 'EDGE_SUBNET'
    assert _web(compose)['environment']['EDGE_SUBNET'] == (
        _edge_subnet(compose)
    )


def test_the_edges_subnet_is_private_and_none_docker_would_choose(compose):
    """Private, as the service requires of it (`edge_subnets`), and IPv4,
    as the network is. And outside every pool Docker gives a network
    its subnet from (DOCKER_POOLS): there, a network another project
    made first could hold it, and `up` would fail on the overlap. Of the
    private ranges, that leaves 172.16.0.0/16."""
    _, fallback = _fallback(_edge_subnet(compose))
    subnet = ip_network(fallback)
    assert subnet.version == 4
    assert edge_subnets(fallback) == (subnet,)
    assert [pool for pool in DOCKER_POOLS if subnet.overlaps(pool)] == []


@pytest.mark.skipif(
    not _compose_cli(),
    reason='needs the docker compose CLI (not a daemon)',
)
def test_the_site_as_compose_reads_it(compose, tmp_path):
    """As compose merges the files, with nothing set: in either mode,
    on `default` and `edge`, publishing nothing, and told the subnet the
    network has. EDGE_SUBNET in `.env` moves both."""
    base = ROOT / 'docker-compose.yaml'
    for files in ((base,), (base, TUNNEL_FILE)):
        for env, subnet in (
            ('', _fallback(_edge_subnet(compose))[1]),
            ('EDGE_SUBNET=172.16.129.0/24\n', '172.16.129.0/24'),
        ):
            config = _compose_config(tmp_path, *files, env=env)
            web = config['services']['web']
            assert set(web['networks']) == {'default', 'edge'}
            assert 'ports' not in web
            assert config['networks']['edge']['ipam']['config'] == [
                {'subnet': subnet},
            ]
            assert web['environment']['EDGE_SUBNET'] == subnet


def test_the_route_the_docs_give_the_site_is_the_port_it_serves():
    """The site's hostname goes to `http://web:<port>` (DEPLOY.md):
    the service's name, which Docker's DNS answers on `edge`, and the
    port its image serves on. Wherever it is named, it is that one."""
    route = f'http://web:{_web_port()}'
    named = {
        doc: set(re.findall(r'http://web:\d+', (ROOT / doc).read_text()))
        for doc in (
            'DEPLOY.md', 'README.md', '.env.example', 'docker-compose.yaml',
        )
    }
    assert route in named['DEPLOY.md']
    assert {doc: routes - {route} for doc, routes in named.items()} == {
        doc: set() for doc in named
    }


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
