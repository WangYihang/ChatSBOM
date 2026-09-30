"""The GitHub workflows and Dependabot's config, read without running.

Actions is disabled on this repository, so none of this runs on GitHub
for now. actionlint checks each workflow's syntax, expressions and
scripts (pre-commit); these hold them to what #45 asked, which it does
not know about.
"""
from __future__ import annotations

import re
import tomllib
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import pytest
import yaml

ROOT = Path(__file__).resolve().parents[1]
WORKFLOWS = sorted((ROOT / '.github' / 'workflows').glob('*.y*ml'))
RELEASE = ROOT / '.github' / 'workflows' / 'release.yaml'
TESTS = ROOT / '.github' / 'workflows' / 'test.yml'


def load(path: Path) -> dict[str, Any]:
    workflow = yaml.safe_load(path.read_text())
    # YAML 1.1 reads the key `on` as True.
    workflow['on'] = workflow.pop(True, None) or workflow.get('on')
    return workflow


def jobs() -> Iterator[tuple[str, str, dict[str, Any]]]:
    for path in WORKFLOWS:
        for name, job in load(path)['jobs'].items():
            yield path.name, name, job


def scripts(job: dict[str, Any]) -> list[str]:
    return [step['run'] for step in job.get('steps', []) if 'run' in step]


def test_there_are_workflows_to_check():
    assert {path.name for path in WORKFLOWS} >= {'test.yml', 'release.yaml'}


@pytest.mark.parametrize('path', WORKFLOWS, ids=lambda path: path.name)
def test_every_action_is_pinned_to_a_commit(path):
    """A tag can be moved to other code, a commit cannot. The comment
    names the release the commit is, for a reader and for Dependabot."""
    used = re.findall(
        r'^\s*(?:-\s+)?uses:\s*(\S+)(.*)$', path.read_text(), re.M,
    )
    assert used
    for action, comment in used:
        if action.startswith('./'):
            continue
        assert re.fullmatch(r'[\w-]+/[\w./-]+@[0-9a-f]{40}', action), action
        assert re.fullmatch(r'\s*# v\d+(\.\d+)*', comment), (action, comment)


@pytest.mark.parametrize('path', WORKFLOWS, ids=lambda path: path.name)
def test_the_token_is_read_only_unless_a_job_says_otherwise(path):
    assert load(path).get('permissions') in ({}, {'contents': 'read'})


def test_every_job_names_its_permissions_and_a_time_limit():
    for workflow, name, job in jobs():
        assert 'permissions' in job, f'{workflow}: {name}'
        # A job that calls a workflow cannot set one; the called
        # workflow's jobs do.
        if 'uses' not in job:
            assert 'timeout-minutes' in job, f'{workflow}: {name}'


def test_every_sync_is_locked():
    """Without `--locked`, a stale uv.lock was quietly re-resolved."""
    synced = [
        line for _, _, job in jobs() for script in scripts(job)
        for line in script.splitlines() if 'uv sync' in line
    ]
    assert synced
    for line in synced:
        assert '--locked' in line, line


def test_the_release_needs_the_tests():
    """They ran on pushes and pull requests only, and the release
    published whatever the tag pointed at."""
    assert 'workflow_call' in load(TESTS)['on']
    release = load(RELEASE)['jobs']
    callers = {
        name for name, job in release.items()
        if job.get('uses') == './.github/workflows/test.yml'
    }
    assert callers
    publish = release['pypi']
    needs = publish.get('needs', [])
    assert callers <= set([needs] if isinstance(needs, str) else needs)


def test_the_release_asks_only_for_what_publishing_needs():
    """Trusted publishing needs the OIDC token; nothing else is written."""
    publish = load(RELEASE)['jobs']['pypi']
    assert publish['permissions'] == {'contents': 'read', 'id-token': 'write'}


def test_the_release_checks_the_tag_and_the_sdist_before_it_publishes():
    """A tag that is not the version, or `workflow_dispatch` from a
    branch, published anyway; and an sdist of the whole repository was
    published once (#28)."""
    publish = scripts(load(RELEASE)['jobs']['pypi'])
    [at] = [i for i, script in enumerate(publish) if 'uv publish' in script]
    before = '\n'.join(publish[:at])
    assert 'GITHUB_REF_TYPE' in before and 'GITHUB_REF_NAME' in before
    assert 'scripts/check_sdist.py' in before


def test_ci_installs_the_syft_the_image_has():
    """The SBOM tests run against a real syft in CI, the image's."""
    [image] = re.findall(
        r'^ARG SYFT_VERSION=(\S+)$', (ROOT / 'Dockerfile').read_text(), re.M,
    )
    [ci] = re.findall(r'SYFT_VERSION: (\S+)', TESTS.read_text())
    assert ci.strip('\'"') == image


def test_ci_checks_syft_against_the_digest_the_image_does():
    """CI installs the image's amd64 archive, and checks it against the
    same digest: moved in one place alone, one of them would be checking
    an archive the other does not install (DEPLOY.md, "Upgrading
    Syft")."""
    [image] = re.findall(
        r'^ARG SYFT_SHA256_AMD64=([0-9a-f]{64})$',
        (ROOT / 'Dockerfile').read_text(), re.M,
    )
    [ci] = re.findall(r'SYFT_SHA256: (\S+)', TESTS.read_text())
    assert ci.strip('\'"') == image


def test_ci_runs_the_uv_the_image_has():
    """Every setup-uv step, in every workflow, names the uv the
    collector's image copies. Without a version it took the newest
    release, so CI locked and synced with another uv than the image
    installs with, and a uv release could turn CI red with no change
    here. Nothing moves either pin by itself (dependabot.yml)."""
    [image] = re.findall(
        r'^COPY --from=ghcr\.io/astral-sh/uv:([^@\s]+)@sha256:[0-9a-f]{64} ',
        (ROOT / 'Dockerfile').read_text(), re.M,
    )
    steps = [
        (workflow, name, step) for workflow, name, job in jobs()
        for step in job.get('steps', [])
        if step.get('uses', '').startswith('astral-sh/setup-uv@')
    ]
    assert {workflow for workflow, _, _ in steps} >= {
        TESTS.name, RELEASE.name,
    }
    for workflow, name, step in steps:
        version = (step.get('with') or {}).get('version')
        assert str(version) == image, (workflow, name, version, image)


def test_ci_starts_no_database_server():
    """No test needs one since the ClickHouse server went (#153). In
    CI, where a skip fails the run, one that did would fail: nothing
    starts a server, and nothing waits for one."""
    for workflow, name, job in jobs():
        assert 'services' not in job, (workflow, name)
        assert [
            key for key in job.get('env') or {}
            if key.startswith('CLICKHOUSE')
        ] == [], (workflow, name)
        assert [
            script for script in scripts(job)
            if 'clickhouse' in script.lower()
        ] == [], (workflow, name)


def test_ci_runs_only_commands_there_are():
    """The compose job went on running `chatsbom db index` after the
    `db` group was deleted (#153): nothing but Actions reads a
    workflow's scripts, and Actions is disabled here."""
    from typer.main import get_command

    from chatsbom.__main__ import app

    root: Any = get_command(app)
    commands = set(root.commands)
    for workflow, name, job in jobs():
        for script in scripts(job):
            for group in re.findall(r'\bchatsbom\s+([a-z][\w-]*)', script):
                assert group in commands, (workflow, name, group)


def test_the_suite_runs_on_the_oldest_python_and_on_the_images():
    """On the oldest Python pyproject.toml declares, which a user of
    the package may have, and on the collector image's (Dockerfile),
    which runs it unattended: 3.12, and 3.14 since #95.

    Each leg has uv make its environment with its own Python, which
    setup-uv's `python-version` sets for every uv command after it, in
    place of .python-version's.
    """
    declared = tomllib.loads(
        (ROOT / 'pyproject.toml').read_text(),
    )['project']['requires-python']
    floor = re.fullmatch(r'>=\s*(\d+\.\d+)', declared)
    assert floor, declared
    [image] = re.findall(
        r'^FROM python:(\d+\.\d+)', (ROOT / 'Dockerfile').read_text(), re.M,
    )

    test = load(TESTS)['jobs']['test']
    matrix = (test.get('strategy') or {}).get('matrix') or {}
    versions = {str(version) for version in matrix.get('python', [])}
    assert {floor[1], image} <= versions, versions
    [uv] = [
        step for step in test['steps']
        if step.get('uses', '').startswith('astral-sh/setup-uv@')
    ]
    assert uv['with'].get('python-version') == '${{ matrix.python }}'


def test_ci_builds_the_dashboard_on_the_node_its_image_runs():
    """The web job type-checks, tests and builds with the Node that
    the web service's image builds the page on.

    Dependabot moves the image alone (#82), and a Node major is not a
    detail: from 25, Node has a `localStorage` of its own. Under vitest 4
    it hid jsdom's and failed 50 of the dashboard's tests, until the test
    workers were started with it off (#106); vitest 5 puts jsdom's in its
    place.
    """
    [image] = re.findall(
        r'^FROM node:(\d+)\b', (ROOT / 'Dockerfile.web').read_text(), re.M,
    )
    [node] = [
        str(step['with']['node-version'])
        for step in load(TESTS)['jobs']['web']['steps']
        if step.get('uses', '').startswith('actions/setup-node@')
    ]
    assert node.split('.')[0] == image, (node, image)


def test_dependabot_moves_every_pin():
    """Pins that nothing moves go stale: the base images, the actions,
    the lockfiles."""
    config = yaml.safe_load((ROOT / '.github' / 'dependabot.yml').read_text())
    covered = {
        (update['package-ecosystem'], update['directory'])
        for update in config['updates']
    }
    assert {
        ('uv', '/'), ('npm', '/web'), ('github-actions', '/'),
        ('docker', '/'), ('docker-compose', '/'),
    } <= covered
