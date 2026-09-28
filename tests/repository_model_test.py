"""`Repository` reads a repository as GitHub's REST API writes it.

The API sends the licence as an object and says "mirror" only as a URL:

    "license": {"key": "mit", "name": "MIT License", "spdx_id": "MIT", ...}
    "mirror_url": null

The model has `license_spdx_id`, `license_name` and `is_mirror`, and
nothing mapped one onto the other. `extra='allow'` kept GitHub's keys in
`model_extra` without a word, so every repository reached ClickHouse,
Parquet and D1 unlicensed and not a mirror (#11).
"""
import json
from typing import Any

import pytest

from chatsbom.models.repository import Repository

OCTOCAT = {
    'login': 'octocat',
    'id': 583231,
    'node_id': 'MDQ6VXNlcjU4MzIzMQ==',
    'avatar_url': 'https://avatars.githubusercontent.com/u/583231?v=4',
    'url': 'https://api.github.com/users/octocat',
    'html_url': 'https://github.com/octocat',
    'type': 'User',
    'site_admin': False,
}

#: `license`, for a licence GitHub identified.
MIT = {
    'key': 'mit',
    'name': 'MIT License',
    'spdx_id': 'MIT',
    'url': 'https://api.github.com/licenses/mit',
    'node_id': 'MDc6TGljZW5zZTEz',
}

APACHE = {
    'key': 'apache-2.0',
    'name': 'Apache License 2.0',
    'spdx_id': 'Apache-2.0',
    'url': 'https://api.github.com/licenses/apache-2.0',
    'node_id': 'MDc6TGljZW5zZTI=',
}

#: `license`, for a licence file GitHub found and could not identify.
OTHER = {
    'key': 'other',
    'name': 'Other',
    'spdx_id': 'NOASSERTION',
    'url': None,
    'node_id': 'MDc6TGljZW5zZTA=',
}

MIRROR_URL = 'git:git.example.com/octocat/Hello-World'


def repos_payload(**overrides: Any) -> dict[str, Any]:
    """`GET /repos/{owner}/{repo}` as GitHub sends it, less the thirty
    `*_url` templates nothing here reads. The search API's items are
    much the same object, with a `score`."""
    payload: dict[str, Any] = {
        'id': 1296269,
        'node_id': 'MDEwOlJlcG9zaXRvcnkxMjk2MjY5',
        'name': 'Hello-World',
        'full_name': 'octocat/Hello-World',
        'private': False,
        'owner': OCTOCAT,
        'html_url': 'https://github.com/octocat/Hello-World',
        'description': 'My first repository on GitHub!',
        'fork': False,
        'url': 'https://api.github.com/repos/octocat/Hello-World',
        'created_at': '2011-01-26T19:01:12Z',
        'updated_at': '2026-09-20T08:14:31Z',
        'pushed_at': '2026-09-18T17:42:05Z',
        'clone_url': 'https://github.com/octocat/Hello-World.git',
        'homepage': 'https://github.com',
        'size': 108,
        'stargazers_count': 2984,
        'watchers_count': 2984,
        'language': 'Ruby',
        'has_issues': True,
        'has_projects': True,
        'has_downloads': True,
        'has_wiki': True,
        'has_pages': False,
        'has_discussions': False,
        'forks_count': 3047,
        'mirror_url': None,
        'archived': False,
        'disabled': False,
        'open_issues_count': 1564,
        'license': MIT,
        'allow_forking': True,
        'is_template': False,
        'web_commit_signoff_required': False,
        'topics': ['api', 'octocat'],
        'visibility': 'public',
        'forks': 3047,
        'open_issues': 1564,
        'watchers': 2984,
        'default_branch': 'master',
        'network_count': 3047,
        'subscribers_count': 1731,
    }
    payload.update(overrides)
    return payload


def test_a_licence_is_read_from_githubs_object():
    repository = Repository.model_validate(repos_payload())
    assert repository.license_spdx_id == 'MIT'
    assert repository.license_name == 'MIT License'


def test_no_licence_is_empty_rather_than_an_error():
    """`license: null` is a repository with no licence file GitHub found."""
    repository = Repository.model_validate(repos_payload(license=None))
    assert repository.license_spdx_id is None
    assert repository.license_name is None


@pytest.mark.parametrize(
    'payload',
    [
        repos_payload(license=OTHER),
        # However it arrives: a field stated outright is held to the
        # same rule as one read out of GitHub's object.
        repos_payload(license_spdx_id='NOASSERTION', license_name='Other'),
    ],
    ids=['githubs-object', 'stated-outright'],
)
def test_an_unidentified_licence_keeps_its_name_and_no_spdx_id(payload):
    """GitHub's `NOASSERTION` is blanked, and its "Other" kept.

    The column is published as "SPDX licence id, or empty", and
    `NOASSERTION` is not one: stored, it would be counted beside `MIT`
    as though it were a licence. The name is what still tells "a
    licence nobody could identify" from "no licence at all".
    """
    repository = Repository.model_validate(payload)
    assert repository.license_spdx_id is None
    assert repository.license_name == 'Other'


def test_a_mirror_is_read_from_its_mirror_url():
    mirror = Repository.model_validate(repos_payload(mirror_url=MIRROR_URL))
    assert mirror.is_mirror is True
    assert Repository.model_validate(repos_payload()).is_mirror is False


def test_a_field_already_set_is_not_overwritten():
    """GitHub's object fills what is missing; a caller who states a
    field means it."""
    repository = Repository.model_validate(
        repos_payload(
            license_spdx_id='Apache-2.0', license_name='Apache License 2.0',
        ),
    )
    assert repository.license_spdx_id == 'Apache-2.0'
    assert repository.license_name == 'Apache License 2.0'


#: A ledger line as `Storage.save` wrote it until now: fields by name,
#: the licence fields left out because they were None, `is_mirror`
#: false because nothing set it, and GitHub's own keys kept beside them
#: as extras.
LEDGER_LINE = {
    'id': 1296269,
    'owner': 'octocat',
    'repo': 'Hello-World',
    'stars': 2984,
    'url': 'https://github.com/octocat/Hello-World',
    'default_branch': 'master',
    'description': 'My first repository on GitHub!',
    'topics': ['api', 'octocat'],
    'language': 'Ruby',
    'is_archived': False,
    'is_fork': False,
    'is_template': False,
    'is_mirror': False,
    'disk_usage': 108,
    'fork_count': 3047,
    'watchers_count': 2984,
    'total_releases': 0,
    'license': MIT,
    'mirror_url': MIRROR_URL,
}


@pytest.mark.parametrize(
    'line',
    [
        LEDGER_LINE,
        # `model_dump` writes the empty fields out as null, and the
        # repository cache is stored that way. A null beside GitHub's
        # object is the old code's silence, not an answer.
        {**LEDGER_LINE, 'license_spdx_id': None, 'license_name': None},
    ],
    ids=['as-storage-wrote-it', 'with-nulls-written-out'],
)
def test_a_line_written_before_the_fix_is_read_with_them(line):
    """The ledgers kept GitHub's keys, so nothing needs refetching."""
    repository = Repository.model_validate_json(json.dumps(line))
    assert repository.license_spdx_id == 'MIT'
    assert repository.license_name == 'MIT License'
    assert repository.is_mirror is True
