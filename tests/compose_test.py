"""The compose setup, checked without starting anything.

These guard the properties that were wrong on the first attempt: a bare
`up` must not start collecting, the collector must run as the invoking
user, and the host Docker socket must never be mounted.
"""
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
