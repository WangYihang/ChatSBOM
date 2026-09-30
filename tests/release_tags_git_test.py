"""Dating tags with git, against a real repository on disk.

The release stage dates a tag that has no GitHub release by its commit.
`GitService.get_tag_dates` does that over the git protocol, so it spends
no REST quota (PR F of #55). These run real `git` against a local
repository reached as `file://`, so they need no network.
"""
import base64
import os
import subprocess
from datetime import datetime
from pathlib import Path

import pytest

from chatsbom.services.git_service import git_auth_env
from chatsbom.services.git_service import GitService
from chatsbom.services.git_service import parse_tag_listing

LIGHT_DATE = '2021-03-04T05:06:07+02:00'
ANNOTATED_COMMIT_DATE = '2022-01-02T03:04:05Z'
TAGGER_DATE = '2023-06-07T08:09:10Z'


def instant(date: str) -> datetime:
    """The moment an ISO 8601 date names, whichever way git wrote it.

    git writes UTC as `Z` in its newer releases, and as `+00:00` in
    older ones (2.43, for one): the same moment, and the release stage
    reads either (`collector/releases.parse_date`).
    """
    return datetime.fromisoformat(date)


def git(cwd: Path, *args: str, date: str = '2020-01-01T00:00:00+00:00') -> str:
    env = {
        **os.environ,
        'GIT_AUTHOR_NAME': 'a', 'GIT_AUTHOR_EMAIL': 'a@example.com',
        'GIT_COMMITTER_NAME': 'a', 'GIT_COMMITTER_EMAIL': 'a@example.com',
        'GIT_AUTHOR_DATE': date, 'GIT_COMMITTER_DATE': date,
        'GIT_CONFIG_GLOBAL': os.devnull, 'GIT_CONFIG_NOSYSTEM': '1',
    }
    return subprocess.run(
        ['git', *args], cwd=cwd, env=env, check=True,
        capture_output=True, text=True,
    ).stdout.strip()


def make_upstream(tmp_path: Path, *, tree_tag: bool) -> tuple[str, dict[str, str]]:
    """A repository with a lightweight tag, annotated tags, a branch
    and, if asked, a tag of a tree; its `file://` URL, and each tag's
    commit."""
    work = tmp_path / 'work'
    work.mkdir()
    git(work, 'init', '--quiet', '-b', 'main')
    # The fetch asks for `--filter=tree:0`; a local server must allow it.
    git(work, 'config', 'uploadpack.allowFilter', 'true')
    (work / 'a.txt').write_text('one')
    git(work, 'add', 'a.txt')
    git(work, 'commit', '--quiet', '-m', 'one', date=LIGHT_DATE)
    light = git(work, 'rev-parse', 'HEAD')
    git(work, 'tag', 'v1.0.0')

    (work / 'a.txt').write_text('two')
    git(work, 'commit', '--quiet', '-am', 'two', date=ANNOTATED_COMMIT_DATE)
    annotated = git(work, 'rev-parse', 'HEAD')
    git(work, 'tag', '-a', 'v2.0.0', '-m', 'v2', date=TAGGER_DATE)

    if tree_tag:
        git(work, 'tag', 'a-tree', git(work, 'rev-parse', 'HEAD^{tree}'))
    git(work, 'tag', 'nested/v3', annotated)

    (work / 'a.txt').write_text('three')
    git(work, 'commit', '--quiet', '-am', 'on a branch')
    return f'file://{work}', {
        'v1.0.0': light, 'v2.0.0': annotated, 'nested/v3': annotated,
    }


@pytest.fixture
def upstream(tmp_path) -> tuple[str, dict[str, str]]:
    return make_upstream(tmp_path, tree_tag=False)


def test_every_tag_is_dated_by_its_commit(upstream):
    url, commits = upstream
    dates = GitService().get_tag_dates('o', 'r', url=url)

    assert dates is not None
    assert dates['v1.0.0'].sha == commits['v1.0.0']
    assert instant(dates['v1.0.0'].date) == instant(LIGHT_DATE)
    # An annotated tag: its commit's date, not the tagger's, which is
    # what `/commits/{sha}` gave.
    assert dates['v2.0.0'].sha == commits['v2.0.0']
    assert instant(dates['v2.0.0'].date) == instant(ANNOTATED_COMMIT_DATE)
    assert dates['nested/v3'].sha == commits['nested/v3']


def test_only_tags_are_listed(upstream):
    url, _ = upstream
    dates = GitService().get_tag_dates('o', 'r', url=url)
    assert dates is not None
    assert set(dates) == {'v1.0.0', 'v2.0.0', 'nested/v3'}


def test_a_tag_of_a_tree_does_not_lose_the_others(tmp_path):
    """Linux has one (`v2.6.11-tree`). A server may refuse to send the
    tree under `--filter=tree:0`, as git's own does; the fetch is then
    made again with `blob:none`. The tree tag gets no commit date, and
    its sha is not what `ls-remote` lists, so the release stage asks the
    API about it instead."""
    url, commits = make_upstream(tmp_path, tree_tag=True)
    dates = GitService().get_tag_dates('o', 'r', url=url)
    assert dates is not None
    assert instant(dates['v1.0.0'].date) == instant(LIGHT_DATE)
    assert dates['a-tree'].sha not in commits.values()


def test_no_trees_or_blobs_are_fetched(upstream, tmp_path, monkeypatch):
    """Commit and tag objects only: the scratch repository is left
    behind here so that what it holds can be counted."""
    url, _ = upstream
    kept = tmp_path / 'kept'

    import tempfile

    class Kept:
        def __init__(self, prefix=''):
            kept.mkdir()

        def __enter__(self):
            return str(kept)

        def __exit__(self, *exc):
            return False

    monkeypatch.setattr(tempfile, 'TemporaryDirectory', Kept)
    assert GitService().get_tag_dates('o', 'r', url=url)
    listing = git(
        kept, 'cat-file', '--batch-all-objects',
        '--batch-check=%(objecttype)',
    ).split()
    assert set(listing) <= {'commit', 'tag'}
    # The branch's newer commit is not fetched either.
    assert listing.count('commit') == 2


def test_a_failed_fetch_is_none(tmp_path):
    assert GitService().get_tag_dates(
        'o', 'r', url=f'file://{tmp_path}/missing',
    ) is None


def test_the_token_is_never_on_the_command_line(upstream, monkeypatch):
    url, _ = upstream
    seen: list[list[str]] = []
    real_run = subprocess.run

    def spy(args, **kwargs):
        seen.append(list(args))
        return real_run(args, **kwargs)

    monkeypatch.setattr(subprocess, 'run', spy)
    GitService(token='ghp_secret').get_tag_dates('o', 'r', url=url)
    assert seen
    assert not any('ghp_secret' in ' '.join(args) for args in seen)


def test_the_token_is_a_header_for_github_only(monkeypatch):
    # The first entry, with none in the environment before it.
    monkeypatch.delenv('GIT_CONFIG_COUNT', raising=False)
    env = git_auth_env('ghp_secret')
    assert env['GIT_CONFIG_KEY_0'] == 'http.https://github.com/.extraheader'
    header = env['GIT_CONFIG_VALUE_0'].removeprefix('Authorization: Basic ')
    assert base64.b64decode(header).decode() == 'x-access-token:ghp_secret'
    assert git_auth_env(None) == {}


#: The header `git_auth_env` gives git for `ghp_secret`.
HEADER = (
    'Authorization: Basic '
    + base64.b64encode(b'x-access-token:ghp_secret').decode()
)


@pytest.mark.parametrize(
    'count, index',
    [
        # No config in the environment, or a count git reads as none.
        (None, 0), ('', 0),
        # Config there already, a proxy or a URL rewrite, which git
        # reads as it reads a count: from a leading space, and a sign.
        ('1', 1), ('3', 3), (' 2', 2), ('+2', 2),
        # A count git refuses, and runs no command over: the token's
        # entry replaces it, as it always did, and git runs.
        ('abc', 0), ('-1', 0), ('2 ', 0), ('1.5', 0), ('1_0', 0),
        ('１', 0), ('99999999999', 0),
    ],
    ids=repr,
)
def test_the_token_is_git_config_after_what_the_environment_has(
    count, index, monkeypatch,
):
    """It set `GIT_CONFIG_COUNT=1` and wrote entry 0, which dropped any
    entry after it and replaced the first (#113)."""
    if count is None:
        monkeypatch.delenv('GIT_CONFIG_COUNT', raising=False)
    else:
        monkeypatch.setenv('GIT_CONFIG_COUNT', count)

    assert git_auth_env('ghp_secret') == {
        'GIT_CONFIG_COUNT': str(index + 1),
        f'GIT_CONFIG_KEY_{index}': 'http.https://github.com/.extraheader',
        f'GIT_CONFIG_VALUE_{index}': HEADER,
    }


def test_a_malformed_listing_line_is_skipped():
    assert parse_tag_listing('garbage\n') == {}
