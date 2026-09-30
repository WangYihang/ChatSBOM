"""The git `openapi clone` starts (#47): quiet, bare and blobless.

Watched as the collector's is, by the harness of
tests/git_subprocess_test.py (its `github` fixture, which conftest.py
borrows): a `git` of the test's own first on PATH, which writes down
how it was started and runs the real one, with github.com replaced by
repositories on disk. These moved here with the research tools (#167).
They hold each git to what the collector's are held to, and the clones
to more: they are bare and blobless, since a checkout of an untrusted
repository runs whatever filters git is configured with (git-lfs's, for
one) on its files.
"""
import contextlib
import time
from pathlib import Path

import pytest

from chatsbom.research.services import openapi_service
from chatsbom.research.services.openapi_service import OpenApiService
from tests.git_subprocess_test import assert_quiet
from tests.git_subprocess_test import git

SPEC = 'openapi: 3.0.0\npaths:\n  /users:\n    get: {}\n'


@pytest.fixture
def snapshots(github, tmp_path) -> Path:
    """Where `openapi clone` puts the snapshots: `.workspaces`."""
    return tmp_path / 'workspaces'


def cache_of(owner: str, repo: str) -> Path:
    return OpenApiService().config.paths.global_repos_dir / owner / repo


def test_a_snapshot_is_cut_from_a_bare_blobless_clone(github, snapshots):
    github.repository('Acme', 'Shop', **{'api__openapi.yaml': SPEC})

    owner, repo, ok, message, _ = OpenApiService().clone_repo(
        'Acme', 'Shop', snapshots, 'V3.0.0', None,
    )

    assert (owner, repo, ok) == ('Acme', 'Shop', True), message
    snapshot = snapshots / 'Acme' / 'Shop' / 'V3.0.0' / 'HEAD'
    assert (snapshot / 'api' / 'openapi.yaml').read_text() == SPEC
    cache = cache_of('Acme', 'Shop')
    assert git(cache, 'rev-parse', '--is-bare-repository') == 'true'
    blobless = git(cache, 'config', 'remote.origin.partialclonefilter')
    assert blobless == 'blob:none'
    assert_quiet(github.calls())


def test_no_filter_runs_on_an_untrusted_repository(github, snapshots, tmp_path):
    """git-lfs installs its filter in the user's config, and a checkout
    runs it on every file the repository's `.gitattributes` names."""
    marker = tmp_path / 'filtered'
    smudge = tmp_path / 'smudge'
    smudge.write_text(f'#!/bin/sh\ntouch {marker}\nexec cat\n')
    smudge.chmod(0o755)
    config = tmp_path / 'home' / '.gitconfig'
    config.parent.mkdir(parents=True)
    config.write_text(f'[filter "lfs"]\n\tsmudge = {smudge}\n')
    github.repository(
        'acme', 'shop', **{
            '.gitattributes': '*.yaml filter=lfs\n',
            'openapi.yaml': SPEC,
        },
    )

    _, _, ok, message, _ = OpenApiService().clone_repo(
        'acme', 'shop', snapshots, 'V3.0.0', None,
    )

    assert ok, message
    assert not marker.exists()
    snapshot = snapshots / 'acme' / 'shop' / 'V3.0.0' / 'HEAD'
    assert (snapshot / 'openapi.yaml').read_text() == SPEC


def test_a_tag_spelled_like_an_option_is_not_one(github, snapshots, tmp_path):
    """A candidate's tag comes from the CSV, and `git archive
    --output=<path>` writes the archive wherever it says."""
    github.repository('acme', 'shop', **{'openapi.yaml': SPEC})
    written = tmp_path / 'written.tar'

    _, _, ok, _, _ = OpenApiService().clone_repo(
        'acme', 'shop', snapshots, f'--output={written}', None,
    )

    assert not ok
    assert not written.exists()
    for call in github.calls():
        if call.command in ('archive', 'fetch') and f'--output={written}' in call.argv:
            assert call.after_end_of_options(f'--output={written}'), call.argv


def test_a_link_in_a_repository_is_not_followed_out_of_it(github, snapshots):
    work = github.repository('acme', 'shop', **{'openapi.yaml': SPEC})
    (work / 'passwd.py').symlink_to('/etc/passwd')
    sha = github.commit(work)

    _, _, ok, message, _ = OpenApiService().clone_repo(
        'acme', 'shop', snapshots, None, sha,
    )

    assert ok, message
    snapshot = snapshots / 'acme' / 'shop' / 'HEAD' / sha
    assert (snapshot / 'openapi.yaml').is_file()
    assert not (snapshot / 'passwd.py').exists()
    assert not (snapshot / 'passwd.py').is_symlink()


def test_a_stale_cache_fetches_what_it_lacks(github, snapshots):
    work = github.repository('acme', 'shop', **{'openapi.yaml': SPEC})
    service = OpenApiService()
    assert service.clone_repo('acme', 'shop', snapshots, 'V3.0.0', None)[2]
    orders = SPEC + '  /orders:\n    get: {}\n'
    tagged = github.commit(work, **{'openapi.yaml': orders})
    git(work, 'tag', 'V4.0.0')
    # And a commit no tag names, which only a fetch of it brings.
    newest = github.commit(work, **{'README.md': 'shop\n'})

    for tag, sha in (('V4.0.0', None), (None, tagged), (None, newest)):
        _, _, ok, message, _ = service.clone_repo(
            'acme', 'shop', snapshots, tag, sha,
        )
        assert ok, message
        snapshot = snapshots / 'acme' / 'shop' / service.get_version_path(
            tag, sha,
        )
        assert '/orders' in (snapshot / 'openapi.yaml').read_text()


@pytest.mark.parametrize(
    'owner,repo', [('..', 'keep'), ('acme', '..'), ('a/b', 'c')],
)
def test_a_name_that_is_not_a_repository_is_refused(
    github, snapshots, tmp_path, owner, repo,
):
    """A cache directory that is not a repository is removed, and cloned
    again: `../keep` was the home directory's `keep`, and `acme/..` the
    whole cache."""
    home = tmp_path / 'home'
    kept = [home / 'keep', home / '.repositories' / 'other' / 'repo']
    for directory in kept:
        directory.mkdir(parents=True)

    outcome = None
    with contextlib.suppress(Exception):
        outcome = OpenApiService().clone_repo(
            owner, repo, snapshots, 'V3.0.0', None,
        )

    assert all(directory.is_dir() for directory in kept)
    assert outcome is not None and not outcome[2]
    assert github.calls() == []


@pytest.mark.parametrize('command', ['clone', 'archive'])
def test_a_clone_that_hangs_is_stopped(github, snapshots, monkeypatch, command):
    github.repository('acme', 'shop', **{'openapi.yaml': SPEC})
    monkeypatch.setattr(openapi_service, 'GIT_TIMEOUT', 1)
    monkeypatch.setenv('FAKE_GIT_HANG', command)

    started = time.monotonic()
    _, _, ok, _, _ = OpenApiService().clone_repo(
        'acme', 'shop', snapshots, 'V3.0.0', None,
    )

    assert time.monotonic() - started < 20
    assert not ok
