"""`github tree` end to end, over a faked git.

The stage lists each repository's files at the commit the commit stage
resolved, and stores them one path per line under
`data/05-github-tree/<lang>/<owner>/<repo>/<ref>/<sha>/tree.txt`. The
file was written in place, and a repository already in the ledger was
skipped whenever that file existed. So a write cut short was trusted for
good (#13), and `openapi` searched a partial list of files.
"""
import json
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.commands.github import tree
from chatsbom.core.container import Container

SHA = '0123456789abcdef0123456789abcdef01234567'

COMMITS = Path('data/04-github-commit/python.jsonl')
TREE = Path(f'data/05-github-tree/python/o/a/main/{SHA}/tree.txt')

#: What `git ls-tree -r --name-only` lists at that commit.
FILES = ['README.md', 'pyproject.toml', 'src/widget/__init__.py', 'uv.lock']
LISTING = ''.join(f'{path}\n' for path in FILES)

runner = CliRunner()


class FakeGit:
    """The one call the tree stage makes, answered without a clone."""

    def __init__(self) -> None:
        self.listed: list[str] = []

    def get_repository_tree(
        self, owner: str, repo: str, sha: str, cache_path: Any = None,
    ) -> list[str]:
        self.listed.append(repo)
        return list(FILES)


@pytest.fixture
def git(tmp_path, monkeypatch) -> FakeGit:
    """A fresh working directory and container, one repository in the
    commit ledger, and git answered without the network."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(Container, '_instance', None)
    # The token check asks GitHub whose token it is.
    monkeypatch.setattr(tree, 'verify_github_token', lambda *a, **k: 'octocat')
    fake = FakeGit()
    monkeypatch.setattr(
        Container, 'get_git_service', lambda self, token=None: fake,
    )

    COMMITS.parent.mkdir(parents=True)
    COMMITS.write_text(
        json.dumps({
            'id': 1,
            'owner': 'o',
            'name': 'a',
            'download_target': {
                'ref': 'main', 'ref_type': 'branch',
                'commit_sha': SHA, 'commit_sha_short': SHA[:7],
            },
        }) + '\n',
        encoding='utf-8',
    )
    return fake


def fetch_trees(*args: str):
    return runner.invoke(
        app,
        [
            'github', 'tree', '--token', 'test-token',
            '--language', 'python', *args,
        ],
    )


def test_a_whole_tree_is_not_listed_again(git):
    assert fetch_trees().exit_code == 0
    assert fetch_trees().exit_code == 0

    assert git.listed == ['a']
    assert TREE.read_text(encoding='utf-8') == LISTING


def test_a_rewrite_cut_short_keeps_the_tree_already_stored(git, full_disk):
    """`--force` lists the files again. If the disk fills up midway, the
    stored tree must survive. It used to be replaced by a prefix that the
    next run skipped over."""
    assert fetch_trees().exit_code == 0
    full_disk.fill(TREE.parent)

    result = fetch_trees('--force')

    assert result.exit_code == 0, result.output
    assert git.listed == ['a', 'a']
    assert TREE.read_text(encoding='utf-8') == LISTING
    assert sorted(p.name for p in TREE.parent.iterdir()) == ['tree.txt']


@pytest.mark.parametrize(
    'left',
    [LISTING[:len(LISTING) // 2], ''],
    ids=['cut-mid-path', 'killed-before-the-first-flush'],
)
def test_a_tree_left_cut_short_is_listed_again(git, left):
    """What an in-place write left when it was killed, before writes were
    atomic. It was buffered, so a kill before the first flush left an
    empty file, and one after it left a line cut short. The repository
    was already in the ledger, so either was skipped for good."""
    assert fetch_trees().exit_code == 0
    TREE.write_text(left, encoding='utf-8')

    result = fetch_trees()

    assert result.exit_code == 0, result.output
    assert git.listed == ['a', 'a']
    assert TREE.read_text(encoding='utf-8') == LISTING
