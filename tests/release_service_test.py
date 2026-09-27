"""The release stage, over a fake `git ls-remote` and a fake GitHub API.

`ReleaseService` merges a repository's GitHub releases with its git tags
and picks the newest stable one; the commit stage resolves that ref and
the content stage downloads it. Nothing tested this service, and three
faults compounded in it (#10):

- every ref `ls-remote` listed became a "tag", HEAD and branches
  included, each dated by its head commit. On any project whose default
  branch had moved since its last release, which is most of them, a
  branch became the latest stable release, and the content stage then
  asked for `refs/tags/<branch>`, which does not exist;
- the API's `prerelease` and `draft` were never read, so a release
  candidate counted as stable;
- one tag without a date crashed the sort, and the repository got no
  release record at all.
"""
import json
import os
from datetime import datetime
from datetime import timezone

import pytest

from chatsbom.core.config import get_config
from chatsbom.models.repository import Repository
from chatsbom.services.git_service import GitService
from chatsbom.services.release_service import ReleaseService
from chatsbom.services.release_service import ReleaseStats

OWNER = 'octo'
REPO = 'widget'

MAIN = 'a' * 40
GH_PAGES = 'b' * 40
DEPENDABOT = 'c' * 40
PULL = 'd' * 40
V1_TAG_OBJECT = 'e' * 40
V1 = 'f' * 40
RC1 = '1' * 40
V09 = '2' * 40

#: v1.0.0 is annotated, so it is listed twice: the tag object, then
#: (`^{}`) the commit it points to. v2.0.0-rc1 is lightweight.
TAGS = [
    f'{V1_TAG_OBJECT}\trefs/tags/v1.0.0',
    f'{V1}\trefs/tags/v1.0.0^{{}}',
    f'{RC1}\trefs/tags/v2.0.0-rc1',
]
TAGS_ONLY = '\n'.join(TAGS)

#: What `git ls-remote` prints for an active project.
ACTIVE_PROJECT = '\n'.join([
    f'{MAIN}\tHEAD',
    f'{MAIN}\trefs/heads/main',
    f'{GH_PAGES}\trefs/heads/gh-pages',
    f'{DEPENDABOT}\trefs/heads/dependabot/npm_and_yarn/lodash-4.17.21',
    f'{PULL}\trefs/pull/1/head',
    *TAGS,
])

#: Branches keep moving after a release, which is how they won.
COMMIT_DATES = {
    MAIN: '2026-09-01T00:00:00Z',
    GH_PAGES: '2026-09-20T00:00:00Z',
    DEPENDABOT: '2026-09-25T00:00:00Z',
    PULL: '2026-09-26T00:00:00Z',
    V1: '2025-01-01T00:00:00Z',
    RC1: '2026-06-01T00:00:00Z',
}


def api_release(
    release_id, tag, published_at, *,
    prerelease=False, draft=False, created_at=None,
):
    """One entry of `GET /repos/{owner}/{repo}/releases`, as GitHub sends it."""
    return {
        'url': f'https://api.github.com/repos/{OWNER}/{REPO}/releases/{release_id}',
        'html_url': f'https://github.com/{OWNER}/{REPO}/releases/tag/{tag}',
        'id': release_id,
        'node_id': f'RE_kwDO{release_id:08d}',
        'tag_name': tag,
        'target_commitish': 'main',
        'name': tag,
        'draft': draft,
        'prerelease': prerelease,
        'created_at': created_at or published_at,
        'published_at': published_at,
        'assets': [],
        'tarball_url': f'https://api.github.com/repos/{OWNER}/{REPO}/tarball/{tag}',
        'zipball_url': f'https://api.github.com/repos/{OWNER}/{REPO}/zipball/{tag}',
        'body': '',
    }


RC1_RELEASE = api_release(
    2, 'v2.0.0-rc1', '2026-06-01T00:00:00Z', prerelease=True,
)
V1_RELEASE = api_release(1, 'v1.0.0', '2025-01-01T00:00:00Z')


class FakeGitHub:
    """The two API calls the release stage makes."""

    def __init__(self, releases: list[dict], dates: dict[str, str] | None = None) -> None:
        self.releases = releases
        self.dates = COMMIT_DATES if dates is None else dates
        self.release_fetches = 0
        self.date_lookups: list[str] = []

    def get_repository_releases(self, owner: str, repo: str) -> list[dict]:
        self.release_fetches += 1
        return self.releases

    def get_commit_date(self, owner: str, repo: str, sha: str) -> str | None:
        # Each of these is a `/commits/{sha}` API call.
        self.date_lookups.append(sha)
        return self.dates.get(sha)


class FakeGit:
    """`git ls-remote`, answered from a string instead of github.com."""

    def __init__(self, output: str) -> None:
        self.output = output
        self.calls = 0

    def ls_remote(self, url: str, **kwargs: object) -> str:
        self.calls += 1
        return self.output


@pytest.fixture(autouse=True)
def working_directory(tmp_path, monkeypatch):
    """The release cache lives under `.cache/` in the working directory."""
    monkeypatch.chdir(tmp_path)


def git_service(git):
    service = GitService()
    service.g = git
    return service


def collect(api, git=None, stats=None):
    """Run the release stage once on octo/widget; return the repository."""
    repository = Repository(id=1, owner=OWNER, repo=REPO)
    service = ReleaseService(api, git_service(git or FakeGit(ACTIVE_PROJECT)))
    result = service.process_repo(
        repository, stats or ReleaseStats(), 'python',
    )
    assert result is not None
    return repository


def tag_names(repository):
    return sorted(r.tag_name for r in repository.all_releases)


class TestOnlyTagsAreReleases:
    """`ls-remote` lists HEAD, branches and pull requests beside the tags."""

    def test_branches_and_head_are_not_releases(self):
        repository = collect(FakeGitHub([RC1_RELEASE]))
        assert tag_names(repository) == ['v1.0.0', 'v2.0.0-rc1']
        assert repository.total_releases == 2

    def test_a_newer_branch_does_not_become_the_latest_stable_release(self):
        repository = collect(FakeGitHub([RC1_RELEASE]))
        assert repository.latest_stable_release.tag_name == 'v1.0.0'

    def test_an_annotated_tag_is_the_commit_it_points_to(self):
        """The tag object is not a commit, so it has no commit date."""
        api = FakeGitHub([])
        repository = collect(api)
        v1 = next(r for r in repository.all_releases if r.tag_name == 'v1.0.0')
        assert v1.target_commitish == V1
        assert v1.published_at == datetime(2025, 1, 1, tzinfo=timezone.utc)
        assert V1_TAG_OBJECT not in api.date_lookups

    def test_commit_dates_are_looked_up_only_for_tags_without_a_release(self):
        """A release carries its own date; a branch is not a release."""
        api = FakeGitHub([RC1_RELEASE])
        collect(api)
        assert api.date_lookups == [V1]


class TestPreReleasesAndDraftsAreNotStable:
    """Tags only here, so that no branch can win instead."""

    def test_a_release_candidate_is_not_the_latest_stable_release(self):
        api = FakeGitHub([RC1_RELEASE, V1_RELEASE])
        repository = collect(api, FakeGit(TAGS_ONLY))
        assert repository.latest_stable_release.tag_name == 'v1.0.0'

    def test_the_release_candidate_is_still_recorded_as_one(self):
        """The `releases` table carries the flag, so it must be GitHub's."""
        api = FakeGitHub([RC1_RELEASE, V1_RELEASE])
        repository = collect(api, FakeGit(TAGS_ONLY))
        flags = {r.tag_name: r.is_prerelease for r in repository.all_releases}
        assert flags == {'v2.0.0-rc1': True, 'v1.0.0': False}

    def test_a_draft_is_not_the_latest_stable_release(self):
        """A draft has no `published_at`, so it sorts by `created_at`."""
        draft = api_release(
            3, 'v3.0.0', None, draft=True, created_at='2026-09-26T00:00:00Z',
        )
        api = FakeGitHub([draft, RC1_RELEASE, V1_RELEASE])
        repository = collect(api, FakeGit(TAGS_ONLY))
        assert repository.latest_stable_release.tag_name == 'v1.0.0'


class TestUndatedTags:
    """A tag whose commit date could not be fetched."""

    def test_an_undated_tag_does_not_crash(self):
        """GitHub's dates carry a timezone; the fallback for none did not.

        Comparing the two raised `TypeError: can't compare offset-naive
        and offset-aware datetimes`, and the repository got no release
        record at all.
        """
        api = FakeGitHub([RC1_RELEASE, V1_RELEASE], dates={})
        git = FakeGit(TAGS_ONLY + f'\n{V09}\trefs/tags/v0.9.0')
        repository = collect(api, git)
        assert api.date_lookups == [V09]
        assert repository.latest_stable_release.tag_name == 'v1.0.0'
        # Undated sorts last rather than first.
        assert repository.all_releases[-1].tag_name == 'v0.9.0'
        assert repository.all_releases[-1].published_at is None


class TestTheReleaseCache:
    """`.cache/api.github.com/repos/<owner>/<repo>/releases/index.json`."""

    def test_a_fresh_cache_is_used(self):
        api = FakeGitHub([RC1_RELEASE])
        git = FakeGit(ACTIVE_PROJECT)
        first = collect(api, git)

        stats = ReleaseStats()
        second = collect(api, git, stats)

        assert api.release_fetches == 1
        assert git.calls == 1
        assert stats.cache_hits == 1
        assert [r.model_dump() for r in second.all_releases] == [
            r.model_dump() for r in first.all_releases
        ]

    def test_a_cache_written_by_the_old_code_is_refetched(self):
        """Its `tags` held every short ref name `ls-remote` listed, and
        once written a branch cannot be told from a tag. This is the
        shape the old code wrote: no `version` at all."""
        path = get_config().paths.get_release_cache_path(OWNER, REPO)
        path.parent.mkdir(parents=True)
        path.write_text(
            json.dumps({
                'releases': [RC1_RELEASE],
                'tags': {
                    'HEAD': MAIN,
                    'main': MAIN,
                    'gh-pages': GH_PAGES,
                    'v1.0.0': V1,
                    'v2.0.0-rc1': RC1,
                },
                'updated_at': '2026-09-26T00:00:00+00:00',
            }),
        )

        api = FakeGitHub([RC1_RELEASE])
        stats = ReleaseStats()
        repository = collect(api, stats=stats)

        assert api.release_fetches == 1
        assert stats.cache_hits == 0
        assert tag_names(repository) == ['v1.0.0', 'v2.0.0-rc1']

    def test_a_cache_rewrite_cut_short_keeps_the_cache_already_there(
        self, full_disk,
    ):
        """The cache was written in place, so a full disk midway through a
        refresh replaced a good cache with a prefix of the next one (#13)."""
        api = FakeGitHub([RC1_RELEASE])
        collect(api)
        path = get_config().paths.get_release_cache_path(OWNER, REPO)
        previous = path.read_text(encoding='utf-8')
        os.utime(path, (0, 0))  # long expired, so it is fetched again

        full_disk.fill(path.parent)
        ReleaseService(api, git_service(FakeGit(ACTIVE_PROJECT))).process_repo(
            Repository(id=1, owner=OWNER, repo=REPO), ReleaseStats(), 'python',
        )

        assert api.release_fetches == 2
        assert path.read_text(encoding='utf-8') == previous
        assert sorted(p.name for p in path.parent.iterdir()) == ['index.json']


class TestTheRefsCache:
    """`.cache/api.github.com/repos/<owner>/<repo>/git/refs/index.json`,
    which the commit stage resolves refs from."""

    def test_a_write_cut_short_leaves_nothing_behind(self, tmp_path, full_disk):
        """It was already written beside the cache and renamed over it,
        but always as `index.tmp`, and a failed write left that there for
        good."""
        cache = tmp_path / 'git' / 'refs' / 'index.json'
        full_disk.fill(cache.parent)

        refs, is_cached = git_service(FakeGit(TAGS_ONLY)).get_repo_refs(
            OWNER, REPO, cache_path=cache,
        )

        assert refs['v1.0.0'] == V1
        assert is_cached is False
        assert list(cache.parent.iterdir()) == []


class TestGitServiceTags:
    """Tags for releases, kept apart from the refs used for resolution."""

    def test_only_refs_tags_are_tags(self):
        service = git_service(FakeGit(ACTIVE_PROJECT))
        tags, is_cached = service.get_repo_tags(OWNER, REPO)
        assert tags == {'v1.0.0': V1, 'v2.0.0-rc1': RC1}
        assert is_cached is False

    def test_the_refs_cache_is_read_by_full_name(self, tmp_path):
        """The commit stage's refs cache stores short names beside full ones.

        `main` there says nothing about being a branch, and `HEAD` has no
        prefix at all; only `refs/tags/...` says a ref is a tag.
        """
        cache = tmp_path / 'git' / 'refs' / 'index.json'
        git_service(FakeGit(ACTIVE_PROJECT)).get_repo_refs(
            OWNER, REPO, cache_path=cache,
        )

        offline = FakeGit('')
        tags, is_cached = git_service(offline).get_repo_tags(
            OWNER, REPO, cache_path=cache,
        )
        assert tags == {'v1.0.0': V1, 'v2.0.0-rc1': RC1}
        assert is_cached is True
        assert offline.calls == 0

    def test_a_branch_named_like_a_tag_ref_is_not_that_tag(self):
        """Git allows a branch called `refs/tags/v2.0.0-rc1`.

        Its short name is spelled exactly like the tag's full name, in the
        same dict, and branches are listed first: it posed as a tag, and
        took the real tag's place when resolving it.
        """
        listing = '\n'.join([
            f'{MAIN}\trefs/heads/main',
            f'{GH_PAGES}\trefs/heads/refs/tags/v2.0.0-rc1',
            f'{GH_PAGES}\trefs/heads/refs/tags/v9.9.9',
            *TAGS,
        ])
        service = git_service(FakeGit(listing))
        tags, _ = service.get_repo_tags(OWNER, REPO)
        assert tags == {'v1.0.0': V1, 'v2.0.0-rc1': RC1}
        assert service.resolve_ref(OWNER, REPO, 'v2.0.0-rc1')[0] == RC1

    def test_branches_still_resolve(self):
        """The commit stage resolves the default branch through `resolve_ref`."""
        service = git_service(FakeGit(ACTIVE_PROJECT))
        assert service.resolve_ref(OWNER, REPO, 'main')[0] == MAIN
        assert service.resolve_ref(OWNER, REPO, 'gh-pages')[0] == GH_PAGES
        assert service.resolve_ref(OWNER, REPO, 'v1.0.0')[0] == V1
