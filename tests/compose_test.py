"""The compose setup, checked without starting anything.

These guard the properties that were wrong on the first attempt: a bare
`up` must not start collecting, the collector must run as the invoking
user, and the host Docker socket must never be mounted.
"""
import re
import shlex
from pathlib import Path

import pytest
import yaml

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


#: Services that must never start without being asked for, and why.
#:
#: The reason is what this guards, not the length of the default list:
#: `web` was added to the defaults and broke an assertion that said
#: `== ['clickhouse']` while satisfying every word of its docstring.
#: Serving a page is not spending anything.
COSTLY = {
    'collector': 'spends GitHub rate budget',
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
    """The version keys the SBOM cache; `latest` would repartition it."""
    assert 'SYFT_VERSION=' in dockerfile
    assert 'get.anchore.io/syft' in dockerfile
    assert 'sh -s -- -b /usr/local/bin "v${SYFT_VERSION}"' in dockerfile


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


def test_a_missing_token_fails_fast(compose):
    """Better a refusal at start than a container looping on 401s."""
    token = compose['services']['collector']['environment']['GITHUB_TOKEN']
    assert ':?' in token


def test_the_loop_survives_a_failing_slice():
    loop = (ROOT / 'deploy' / 'collector-loop.sh').read_text()
    # A slice that fails must be recorded and stepped over, not fatal.
    assert 'queue sync' in loop
    assert '||' in loop
    assert 'set -eu' in loop


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
    host = compose['services']['lock']['environment']['DOCKER_HOST']
    assert host == 'tcp://dind:2375'


def test_the_data_path_is_mounted_on_both_lock_and_the_daemon(compose):
    """A container the daemon starts resolves bind mounts against *its*
    filesystem, so a path only `lock` can see would mount nothing."""
    def data_mount(service: str) -> str:
        return next(
            v for v in compose['services'][service]['volumes']
            if v.startswith('./data')
        )
    assert data_mount('lock') == data_mount('dind')


def test_only_the_lock_image_carries_a_docker_client(dockerfile):
    """An image with a Docker client and a reachable socket is one
    mistake from being an escape, so the split is a build property."""
    lock_image = (ROOT / 'Dockerfile.lock').read_text()
    assert 'docker:27-cli' in lock_image
    assert 'docker:' not in dockerfile.replace('dockerfile', '')


def test_the_lock_service_builds_from_the_lock_image(compose):
    assert compose['services']['lock']['build']['dockerfile'] == 'Dockerfile.lock'
    assert compose['services']['collector']['build']['dockerfile'] == 'Dockerfile'


def test_the_nested_daemon_storage_is_a_named_volume(compose):
    """overlay2 layers need a real filesystem, not a bind mount."""
    storage = next(
        v for v in compose['services']['dind']['volumes']
        if 'docker' in v and not v.startswith('./')
    )
    assert storage.startswith('dind-storage:')
    assert 'dind-storage' in compose['volumes']


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
    persistent = {'clickhouse', 'web', 'collector'}
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

#: Where wrangler keeps local state — KV, D1, R2 — relative to the
#: directory holding wrangler.jsonc.
WRANGLER_STATE = '.wrangler/state'


def test_the_image_turns_off_the_local_explorer(web_dockerfile):
    """wrangler serves miniflare's local explorer unless told not to.

    It is a UI and API under /cdn-cgi/local/explorer that reads and
    writes every binding — the spend counter in KV, raw SQL on D1 — and
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


def test_the_spend_counter_survives_a_recreate(compose, web_dockerfile):
    """The daily cap is a counter in wrangler's local KV.

    That lives in `.wrangler/state` beside wrangler.jsonc. Left in the
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

    Optional rather than required: until the page sends a token (#32),
    setting it refuses every chat request.
    """
    secret = compose['services']['web']['environment'].get('TURNSTILE_SECRET')
    assert secret is not None and '${TURNSTILE_SECRET' in secret
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
