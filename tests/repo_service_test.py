"""The repository stage, over a fake `GET /repos/{owner}/{repo}`.

`RepoService.process_repo` parses GitHub's payload through `Repository`
and merges what it read onto the record the search stage wrote. The
licence was on the list and always came back empty, because the model
never read GitHub's `license` object (#11). Nothing tested this service.
"""
import pytest

from chatsbom.models.repository import Repository
from chatsbom.services.repo_service import RepoService
from chatsbom.services.repo_service import RepoStats
from tests.repository_model_test import MIRROR_URL
from tests.repository_model_test import OTHER
from tests.repository_model_test import repos_payload


class FakeGitHub:
    """The one API call the repository stage makes."""

    def __init__(self, payload: dict | None) -> None:
        self.payload = payload
        self.fetches = 0

    def get_repository_metadata(self, owner: str, repo: str) -> dict | None:
        self.fetches += 1
        return self.payload


@pytest.fixture(autouse=True)
def working_directory(tmp_path, monkeypatch):
    """The repository cache lives under `.cache/` in the working directory."""
    monkeypatch.chdir(tmp_path)


def enrich(api, searched=None):
    """Run the repository stage once on octocat/Hello-World."""
    repository = searched if searched is not None else Repository(
        id=1296269, owner='octocat', repo='Hello-World',
    )
    record = RepoService(api).process_repo(repository, RepoStats(), 'ruby')
    assert record is not None
    assert api.fetches == 1
    return record


def test_the_record_carries_the_licence():
    record = enrich(FakeGitHub(repos_payload()))
    assert record['license_spdx_id'] == 'MIT'
    assert record['license_name'] == 'MIT License'


def test_the_record_carries_a_mirror():
    record = enrich(FakeGitHub(repos_payload(mirror_url=MIRROR_URL)))
    assert record['is_mirror'] is True


@pytest.mark.parametrize(
    'now, expected',
    [(OTHER, (None, 'Other')), (None, (None, None))],
    ids=['relicensed-to-other', 'licence-removed'],
)
def test_a_licence_changed_since_the_search_replaces_the_old_one(now, expected):
    """`null`, and a licence GitHub cannot identify, are answers too.

    The merge keeps the old value wherever the new one is None, which
    is right for a field the API can leave out and wrong for this one:
    `/repos` always sends `license`. Merged field by field, a repository
    relicensed since the search to something GitHub reports as "Other"
    would keep its old `MIT` beside that name.
    """
    searched = Repository.model_validate(repos_payload())
    record = enrich(FakeGitHub(repos_payload(license=now)), searched)
    assert (record['license_spdx_id'], record['license_name']) == expected

    # And it stays so once `Storage.save` has read it back, rather than
    # being refilled from the search stage's object on the same record.
    stored = Repository.model_validate(record)
    assert (stored.license_spdx_id, stored.license_name) == expected
