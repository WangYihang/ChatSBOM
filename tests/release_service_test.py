"""Only tags are tags.

`GitService.get_repo_refs` names every ref twice — full and short — and
the release stage used to keep every short name, so each branch was
dated with an API call and the newest branch became the scan target.
"""
import json
from unittest.mock import MagicMock

import pytest

from chatsbom.models.repository import Repository
from chatsbom.services.release_service import ReleaseService
from chatsbom.services.release_service import ReleaseStats
from chatsbom.services.release_service import tags_from_refs

REFS = {
    'HEAD': 'h0',
    'refs/heads/develop': 'b1',
    'develop': 'b1',
    'refs/heads/github-repo-stats': 'b2',
    'github-repo-stats': 'b2',
    'refs/tags/0.1.0': 'peeled',
    '0.1.0': 'peeled',
    'refs/tags/0.2.0': 't2',
    '0.2.0': 't2',
    'refs/pull/9/head': 'p9',
}


def test_only_tag_refs_are_tags():
    assert tags_from_refs(REFS) == {'0.1.0': 'peeled', '0.2.0': 't2'}


def test_a_bare_prefix_is_not_a_tag():
    assert tags_from_refs({'refs/tags/': 'x'}) == {}


@pytest.fixture
def service(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    github = MagicMock()
    github.get_repository_releases.return_value = [
        {
            'id': 1, 'tag_name': '0.2.0', 'name': '0.2.0',
            'published_at': '2024-01-01T00:00:00Z',
            'created_at': '2024-01-01T00:00:00Z',
            'prerelease': False, 'draft': False,
        },
    ]
    github.get_commit_date.return_value = '2023-01-01T00:00:00Z'
    git = MagicMock()
    git.get_repo_refs.return_value = (REFS, False)
    return ReleaseService(github, git)


def _repo() -> Repository:
    return Repository.model_validate(
        {'id': 1, 'owner': 'o', 'name': 'r', 'language': 'Svelte'},
    )


def test_branches_are_neither_dated_nor_released(service):
    result = service.process_repo(_repo(), ReleaseStats(), 'other')

    # One lookup, for the one tag without a release — not one per branch.
    dated = [c.args[2] for c in service.service.get_commit_date.call_args_list]
    assert dated == ['peeled']
    assert {
        r['tag_name']
        for r in result['all_releases']
    } == {'0.1.0', '0.2.0'}
    assert result['latest_stable_release']['tag_name'] == '0.2.0'


def test_a_cache_written_before_the_fix_is_refetched(service, tmp_path):
    """Its `tags` hold branch names that cannot be told apart from tags."""
    path = service.config.paths.get_release_cache_path('o', 'r')
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps({
            'releases': [],
            'tags': {'github-repo-stats': 'b2'},
        }),
    )

    result = service.process_repo(_repo(), ReleaseStats(), 'other')

    service.service.get_repository_releases.assert_called_once()
    assert 'github-repo-stats' not in {
        r['tag_name'] for r in result['all_releases']
    }
    assert json.loads(path.read_text())['tags_from_tag_refs'] is True


def test_a_fixed_cache_is_used(service, tmp_path):
    path = service.config.paths.get_release_cache_path('o', 'r')
    path.parent.mkdir(parents=True)
    path.write_text(
        json.dumps({
            'releases': [],
            'tags': {'0.1.0': 'peeled'},
            'tags_from_tag_refs': True,
        }),
    )

    result = service.process_repo(_repo(), ReleaseStats(), 'other')

    service.service.get_repository_releases.assert_not_called()
    assert result['total_releases'] == 1
