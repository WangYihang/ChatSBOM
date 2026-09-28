"""The suite's own rules: what it may skip, and when a skip fails (#45).

Locally, a test that needs something absent, ClickHouse or syft, skips
and says why. In CI, where everything it needs is provided, a skip means
a test did not run, and a green run would hide it: so there it fails.
The guard used to be one test file, grepped for "skipped" and run again.

Each case is a pytest of its own, over a test file written for it, with
this suite's conftest and settings. So what is counted is that run's
outcome, and the environment is only what the case sets: whether CI is
set, where ClickHouse is, and what is on PATH.
"""
from __future__ import annotations

import os
import shutil
import socket
import subprocess
import sys
from collections.abc import Iterator
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

NEEDS_CLICKHOUSE = """
from tests.conftest import requires_clickhouse


@requires_clickhouse
def test_needs_the_database():
    pass
"""

SKIPS = """
import pytest


def test_runs():
    pass


def test_skips():
    pytest.skip('not today')
"""

SKIPPED_AT_COLLECTION = """
import pytest

pytest.importorskip('chatsbom_no_such_module')


def test_never_collected():
    pass
"""


def environment(ci: str | None, **overrides: str) -> dict[str, str]:
    """This process's environment, with CI as given: unset for None."""
    kept = {
        name: value for name, value in os.environ.items()
        if name not in ('CI', 'PYTEST_ADDOPTS')
    }
    if ci is not None:
        kept['CI'] = ci
    return {**kept, **overrides}


def run(
    command: list[str], env: dict[str, str],
) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        [sys.executable, '-m', 'pytest', '-p', 'no:cacheprovider', *command],
        cwd=ROOT, env=env, capture_output=True, text=True, timeout=300,
    )


def run_sample(
    tmp_path: Path, source: str, ci: str | None, **overrides: str,
) -> subprocess.CompletedProcess[str]:
    """pytest over one file holding `source`, as this suite runs it:
    pyproject.toml's settings, `--strict-markers` among them, and
    tests/conftest.py loaded."""
    sample = tmp_path / 'sample_test.py'
    sample.write_text(source)
    return run(
        [
            '-c', str(ROOT / 'pyproject.toml'), '-p', 'tests.conftest',
            str(sample),
        ],
        environment(ci, **overrides),
    )


def said(result: subprocess.CompletedProcess[str]) -> str:
    return result.stdout + result.stderr


@pytest.fixture
def no_clickhouse() -> Iterator[dict[str, str]]:
    """Where ClickHouse is said to be: a port that refuses connections.

    Bound here, so that nothing else takes it while the case runs, and
    not listening, so that a connection to it is refused at once.
    """
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        yield {
            'CLICKHOUSE_TEST_HOST': '127.0.0.1',
            'CLICKHOUSE_TEST_PORT': str(sock.getsockname()[1]),
        }


# --- a test that needs ClickHouse -------------------------------------------

@pytest.mark.parametrize('ci', [None, '', 'false', '0'])
def test_without_clickhouse_a_local_run_skips(tmp_path, no_clickhouse, ci):
    """What `docker compose up -d` is for, said rather than failed."""
    result = run_sample(tmp_path, NEEDS_CLICKHOUSE, ci, **no_clickhouse)

    assert result.returncode == 0, said(result)
    assert '1 skipped' in result.stdout, said(result)
    assert 'ClickHouse not reachable' in result.stdout, said(result)


@pytest.mark.parametrize('ci', ['true', '1'])
def test_without_clickhouse_a_ci_run_fails(tmp_path, no_clickhouse, ci):
    """CI starts a server. A test that cannot reach it did not run, and
    skipped, it looked the same as one that passed."""
    result = run_sample(tmp_path, NEEDS_CLICKHOUSE, ci, **no_clickhouse)

    assert result.returncode == pytest.ExitCode.TESTS_FAILED, said(result)
    assert 'skipped' not in result.stdout, said(result)
    assert 'ClickHouse not reachable' in result.stdout, said(result)


# --- any skip ---------------------------------------------------------------

def test_a_skip_is_reported_locally(tmp_path):
    result = run_sample(tmp_path, SKIPS, None)

    assert result.returncode == 0, said(result)
    assert '1 passed, 1 skipped' in result.stdout, said(result)


@pytest.mark.parametrize(
    'source', [SKIPS, SKIPPED_AT_COLLECTION],
    ids=['a-test-skips', 'a-module-is-skipped'],
)
def test_a_skip_fails_a_ci_run(tmp_path, source):
    """Whether a test skips itself, or its module is skipped whole."""
    result = run_sample(tmp_path, source, 'true')

    assert result.returncode == pytest.ExitCode.TESTS_FAILED, said(result)
    assert 'every test must run' in result.stdout, said(result)


def test_a_ci_run_with_nothing_skipped_passes(tmp_path):
    result = run_sample(tmp_path, 'def test_runs():\n    pass\n', 'true')

    assert result.returncode == 0, said(result)


# --- a test that needs syft -------------------------------------------------

def test_the_sbom_tests_skip_without_syft():
    """They construct the service, which stops the command when syft is
    missing. As an error in a fixture, that read as a broken test."""
    path = os.pathsep.join(
        directory for directory in os.environ['PATH'].split(os.pathsep)
        if directory and shutil.which('syft', path=directory) is None
    )
    result = run(
        ['-q', 'tests/sbom_test.py'], environment(None, PATH=path),
    )

    assert result.returncode == 0, said(result)
    assert 'error' not in result.stdout, said(result)
    assert 'skipped' in result.stdout, said(result)
    assert 'syft is not installed' in result.stdout, said(result)
