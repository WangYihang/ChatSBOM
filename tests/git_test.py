"""Resolving a ref with `git ls-remote`, against repositories on disk.

These asserted the commits of `vuejs/core` on the real github.com, so
they needed the network and a third party's history to stay put, and
were skipped whenever either was in doubt.

`GitService` names github.com itself. git is told, through its
environment, to read `https://github.com/` from a directory of bare
repositories instead (`url.<base>.insteadOf`), and to use no protocol
but `file`: a URL the rewrite missed fails rather than reaching out.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path

import pytest

from chatsbom.services.git_service import GitService


def git(cwd: Path, *args: str) -> str:
    env = {
        **os.environ,
        'GIT_AUTHOR_NAME': 'a', 'GIT_AUTHOR_EMAIL': 'a@example.com',
        'GIT_COMMITTER_NAME': 'a', 'GIT_COMMITTER_EMAIL': 'a@example.com',
        'GIT_CONFIG_GLOBAL': os.devnull, 'GIT_CONFIG_NOSYSTEM': '1',
    }
    return subprocess.run(
        ['git', *args], cwd=cwd, env=env, check=True,
        capture_output=True, text=True,
    ).stdout.strip()


@pytest.fixture
def github(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> Path:
    """A github.com of bare repositories, `<owner>/<repo>.git`.

    Every `https://github.com/` URL git is given in this test reads from
    it; any other URL, and any other protocol, fails.
    """
    root = tmp_path / 'github.com'
    root.mkdir()
    monkeypatch.setenv('GIT_CONFIG_COUNT', '1')
    monkeypatch.setenv('GIT_CONFIG_KEY_0', f'url.file://{root}/.insteadOf')
    monkeypatch.setenv('GIT_CONFIG_VALUE_0', 'https://github.com/')
    monkeypatch.setenv('GIT_ALLOW_PROTOCOL', 'file')
    return root


def publish(github: Path, name: str) -> dict[str, str]:
    """`name` on `github`: a lightweight tag, an annotated one, and a
    branch past both; the commit each ref resolves to."""
    work = github.parent / 'work' / name
    work.mkdir(parents=True)
    git(work, 'init', '--quiet', '-b', 'main')
    (work / 'a.txt').write_text('one')
    git(work, 'add', 'a.txt')
    git(work, 'commit', '--quiet', '-m', 'one')
    git(work, 'tag', 'v2.0.0')
    first = git(work, 'rev-parse', 'HEAD')

    (work / 'a.txt').write_text('two')
    git(work, 'commit', '--quiet', '-am', 'two')
    git(work, 'tag', '-a', 'v3.0.0', '-m', 'v3')
    second = git(work, 'rev-parse', 'HEAD')

    (work / 'a.txt').write_text('three')
    git(work, 'commit', '--quiet', '-am', 'three')
    head = git(work, 'rev-parse', 'HEAD')

    git(
        github, 'clone', '--quiet', '--bare', str(work),
        str(github / f'{name}.git'),
    )
    return {'v2.0.0': first, 'v3.0.0': second, 'main': head}


def test_git_service_resolve_ref(github):
    """An annotated tag resolves to its commit, not to the tag object."""
    commits = publish(github, 'vuejs/core')

    sha, is_cached, num_refs = GitService().resolve_ref(
        'vuejs', 'core', 'v3.0.0',
    )

    assert sha == commits['v3.0.0']
    assert is_cached is False
    assert num_refs > 0


def test_git_service_resolve_lightweight_tag(github):
    commits = publish(github, 'vuejs/core')

    sha, _, _ = GitService().resolve_ref('vuejs', 'core', 'v2.0.0')

    assert sha == commits['v2.0.0']


def test_git_service_resolve_branch(github):
    commits = publish(github, 'vuejs/core')

    sha, is_cached, num_refs = GitService().resolve_ref(
        'vuejs', 'core', 'main',
    )

    assert sha == commits['main']
    assert is_cached is False
    assert num_refs > 0


def test_git_service_invalid_repo(github):
    service = GitService()
    sha, is_cached, num_refs = service.resolve_ref(
        'nonexistent_user_12345', 'nonexistent_repo_12345', 'main',
    )
    assert sha is None
    assert is_cached is False
    assert num_refs == 0
