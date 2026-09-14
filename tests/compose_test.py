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


def test_a_bare_up_starts_only_the_database(compose):
    """Spending GitHub rate budget must be a decision, not a side effect."""
    default = [
        name for name, svc in compose['services'].items()
        if not svc.get('profiles')
    ]
    assert default == ['clickhouse']


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
