"""`GitHubRelease` reads the releases API as GitHub writes it.

The model calls its flags `is_prerelease` and `is_draft`, the API calls
them `prerelease` and `draft`, and `extra='ignore'` dropped the API's
spelling without a word. Both flags were always False, so a release
candidate could be picked as the latest *stable* release (#10).
"""
from datetime import datetime
from datetime import timezone

from chatsbom.models.github_release import GitHubRelease
from chatsbom.models.repository import Repository

OCTOCAT = {
    'login': 'octocat',
    'id': 583231,
    'node_id': 'MDQ6VXNlcjU4MzIzMQ==',
    'avatar_url': 'https://avatars.githubusercontent.com/u/583231?v=4',
    'gravatar_id': '',
    'url': 'https://api.github.com/users/octocat',
    'html_url': 'https://github.com/octocat',
    'type': 'User',
    'user_view_type': 'public',
    'site_admin': False,
}

#: One entry of `GET /repos/{owner}/{repo}/releases`, as GitHub sends it.
RELEASE_CANDIDATE = {
    'url': 'https://api.github.com/repos/octocat/Hello-World/releases/226046810',
    'assets_url': 'https://api.github.com/repos/octocat/Hello-World/releases/226046810/assets',
    'upload_url': 'https://uploads.github.com/repos/octocat/Hello-World/releases/226046810/assets{?name,label}',
    'html_url': 'https://github.com/octocat/Hello-World/releases/tag/v2.0.0-rc.1',
    'id': 226046810,
    'author': OCTOCAT,
    'node_id': 'RE_kwDOABPHjc4Nd4ha',
    'tag_name': 'v2.0.0-rc.1',
    'target_commitish': 'main',
    'name': 'v2.0.0 Release Candidate 1',
    'draft': False,
    'immutable': False,
    'prerelease': True,
    'created_at': '2026-06-01T09:12:44Z',
    'updated_at': '2026-06-01T10:30:05Z',
    'published_at': '2026-06-01T10:30:05Z',
    'assets': [
        {
            'url': 'https://api.github.com/repos/octocat/Hello-World/releases/assets/262626262',
            'id': 262626262,
            'node_id': 'RA_kwDOABPHjc4Pp8mW',
            'name': 'hello-world_2.0.0-rc.1_linux_amd64.tar.gz',
            'label': '',
            'uploader': OCTOCAT,
            'content_type': 'application/gzip',
            'state': 'uploaded',
            'size': 4194304,
            'digest': 'sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08',
            'download_count': 17,
            'created_at': '2026-06-01T10:29:51Z',
            'updated_at': '2026-06-01T10:29:58Z',
            'browser_download_url': 'https://github.com/octocat/Hello-World/releases/download/v2.0.0-rc.1/hello-world_2.0.0-rc.1_linux_amd64.tar.gz',
        },
    ],
    'tarball_url': 'https://api.github.com/repos/octocat/Hello-World/tarball/v2.0.0-rc.1',
    'zipball_url': 'https://api.github.com/repos/octocat/Hello-World/zipball/v2.0.0-rc.1',
    'body': "## What's Changed\n* First release candidate for 2.0.0",
    'mentions_count': 1,
}


def test_a_pre_release_is_read_as_one():
    release = GitHubRelease.model_validate(RELEASE_CANDIDATE)
    assert release.is_prerelease is True
    assert release.is_draft is False


def test_a_draft_is_read_as_one():
    """A draft has no `published_at` until it is published."""
    release = GitHubRelease.model_validate({
        **RELEASE_CANDIDATE,
        'draft': True,
        'prerelease': False,
        'published_at': None,
    })
    assert release.is_draft is True
    assert release.is_prerelease is False
    assert release.published_at is None


def test_the_rest_of_the_payload_is_read():
    release = GitHubRelease.model_validate(RELEASE_CANDIDATE)
    assert release.id == 226046810
    assert release.tag_name == 'v2.0.0-rc.1'
    assert release.name == 'v2.0.0 Release Candidate 1'
    assert release.published_at == datetime(
        2026, 6, 1, 10, 30, 5, tzinfo=timezone.utc,
    )
    assert [a['name'] for a in release.assets] == [
        'hello-world_2.0.0-rc.1_linux_amd64.tar.gz',
    ]


def test_ledgers_spell_the_flags_the_models_way():
    """`data/03-github-release/*.jsonl` and later stages store them by
    field name, so every ledger written so far says `is_prerelease`."""
    release = GitHubRelease.model_validate(
        {'tag_name': 'v1.0.0', 'is_prerelease': True, 'is_draft': True},
    )
    assert release.is_prerelease is True
    assert release.is_draft is True


def test_the_flags_survive_a_ledger_round_trip():
    """What `Storage.save` writes, `load_jsonl` reads back."""
    repository = Repository(
        id=1, owner='octocat', repo='Hello-World',
        all_releases=[GitHubRelease.model_validate(RELEASE_CANDIDATE)],
    )
    line = repository.model_dump_json(exclude_none=True)
    assert '"is_prerelease":true' in line

    reloaded = Repository.model_validate_json(line)
    assert reloaded.all_releases[0].is_prerelease is True
