"""The GitHub workflows and Dependabot's config, read without running.

Actions is disabled on this repository, so none of this runs on GitHub
for now. actionlint checks each workflow's syntax, expressions and
scripts (pre-commit); these hold them to what #45 asked, which it does
not know about.
"""
from __future__ import annotations

import re
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


def test_ci_runs_clickhouse_with_the_repository_accounts():
    """Without users.d, guest's grants and limits were never tested."""
    test = load(TESTS)['jobs']['test']
    assert 'services' not in test
    assert any(
        'database/config/users.d:/etc/clickhouse-server/users.d' in script
        for script in scripts(test)
    )


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
