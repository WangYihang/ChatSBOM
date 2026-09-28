"""`github tree` end to end, over a faked git.

The stage lists each repository's files at the commit the commit stage
resolved, and stores them one path per line under
`data/05-github-tree/<repository_id>/<sha>/tree.txt`. The
file was written in place, and a repository already in the ledger was
skipped whenever that file existed. So a write cut short was trusted for
good (#13), and `openapi` searched a partial list of files.

The command is the tree stage of `chatsbom run`, on its own: what is due
comes from the ledger, whatever a repository's language, and the
release and commit stages are walked for the commit.
"""
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.commands.github import tree
from chatsbom.core.container import Container
from chatsbom.core.ledger import Ledger

SHA = '0123456789abcdef0123456789abcdef01234567'

TREE = Path(f'data/05-github-tree/1/{SHA}/tree.txt')

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


class FakeRelease:
    """No release: the default branch is the target."""

    def process_repo(self, repository: Any, stats: Any, language: str):
        return None


class FakeCommit:
    """The commit the commit stage resolves, from its cache."""

    def process_repo(self, repository: Any, stats: Any, language: str):
        return {
            'download_target': {
                'ref': 'main', 'ref_type': 'branch',
                'commit_sha': SHA, 'commit_sha_short': SHA[:7],
            },
        }


@pytest.fixture
def git(tmp_path, monkeypatch) -> FakeGit:
    """A fresh working directory and container, one repository in the
    queue with no language at all, and git answered without the
    network."""
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(Container, '_instance', None)
    # The token check asks GitHub whose token it is.
    monkeypatch.setattr(tree, 'verify_github_token', lambda *a, **k: 'octocat')
    fake = FakeGit()
    monkeypatch.setattr(
        Container, 'get_git_service', lambda self, token=None: fake,
    )
    monkeypatch.setattr(
        Container, 'get_release_service', lambda self, token=None: FakeRelease(),
    )
    monkeypatch.setattr(
        Container, 'get_commit_service', lambda self, token=None: FakeCommit(),
    )

    Path('data').mkdir()
    with Ledger(Path('data/ledger.sqlite3')) as ledger:
        ledger.seed(1, 'o', 'a', snapshot='all')
        ledger.record_push(
            1, datetime(2026, 9, 1, tzinfo=timezone.utc),
            datetime(2026, 9, 2, tzinfo=timezone.utc),
        )
    return fake


def fetch_trees(*args: str):
    return runner.invoke(
        app, ['github', 'tree', '--token', 'test-token', *args],
    )


def test_a_whole_tree_is_not_listed_again(git):
    first = fetch_trees()
    assert first.exit_code == 0, first.output
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
