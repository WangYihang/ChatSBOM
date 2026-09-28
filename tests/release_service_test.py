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
from chatsbom.services.git_service import TagDate
from chatsbom.services.release_service import API_DATE_CAP
from chatsbom.services.release_service import looks_like_prerelease
from chatsbom.services.release_service import ReleaseService
from chatsbom.services.release_service import ReleaseStats
from chatsbom.services.release_service import version_key

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
        self.sent = 0

    def requests_sent(self) -> int:
        return self.sent

    def get_repository_releases(self, owner: str, repo: str) -> list[dict]:
        self.release_fetches += 1
        self.sent += 1
        return self.releases

    def get_commit_date(self, owner: str, repo: str, sha: str) -> str | None:
        # Each of these is a `/commits/{sha}` API call.
        self.date_lookups.append(sha)
        self.sent += 1
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


def git_service(git, tag_dates=None):
    """A `GitService` over `git`, whose tag fetch answers `tag_dates`.

    `tag_dates` is `{tag: TagDate}` as `get_tag_dates` returns it; None,
    the default, is a fetch that failed, so tags are dated through the
    API fallback, as every tag was before PR F of #55.
    """
    service = GitService()
    service.g = git
    service.tag_fetches = 0

    def get_tag_dates(owner, repo, url=None):
        service.tag_fetches += 1
        return tag_dates
    service.get_tag_dates = get_tag_dates
    return service


def collect(api, git=None, stats=None, tag_dates=None, service=None):
    """Run the release stage once on octo/widget; return the repository."""
    repository = Repository(id=1, owner=OWNER, repo=REPO)
    service = service or ReleaseService(
        api, git_service(git or FakeGit(ACTIVE_PROJECT), tag_dates),
    )
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


def git_dated(*names):
    """`get_tag_dates` for the named tags, dated as their commits are."""
    peeled = {'v1.0.0': V1, 'v2.0.0-rc1': RC1, 'v0.9.0': V09}
    return {
        name: TagDate(
            sha=peeled[name],
            date=COMMIT_DATES.get(peeled[name], '2024-01-01T00:00:00Z'),
        )
        for name in names
    }


class TestTagsAreDatedWithGit:
    """PR F of #55: one `/commits/{sha}` call per tag without a release
    was a mean of 47.4 per repository, about 2.8 M calls over 60 k
    repositories. Git dates them for no REST quota at all."""

    def test_git_dates_every_tag_and_the_api_dates_none(self):
        api = FakeGitHub([RC1_RELEASE])
        stats = ReleaseStats()
        repository = collect(
            api, stats=stats, tag_dates=git_dated('v1.0.0', 'v2.0.0-rc1'),
        )
        assert api.date_lookups == []
        v1 = next(r for r in repository.all_releases if r.tag_name == 'v1.0.0')
        assert v1.published_at == datetime(2025, 1, 1, tzinfo=timezone.utc)
        assert repository.latest_stable_release.tag_name == 'v1.0.0'
        # The releases page, and nothing else.
        assert stats.api_requests == 1

    def test_the_same_release_is_chosen_as_with_the_api(self):
        """Git's date is the committer date `/commits/{sha}` gave."""
        by_api = collect(FakeGitHub([]))
        by_git = collect(
            FakeGitHub([]), tag_dates=git_dated('v1.0.0', 'v2.0.0-rc1'),
        )
        assert [
            (r.tag_name, r.published_at) for r in by_git.all_releases
        ] == [(r.tag_name, r.published_at) for r in by_api.all_releases]

    def test_a_tag_that_moved_since_ls_remote_is_asked_of_the_api(self):
        """git's date is for the commit it fetched, which is not the one
        `ls-remote` listed; the listed one is dated."""
        moved = {'v1.0.0': TagDate(sha=MAIN, date='2026-09-01T00:00:00Z')}
        api = FakeGitHub([RC1_RELEASE])
        collect(api, tag_dates=moved)
        assert api.date_lookups == [V1]

    def test_a_tag_git_has_no_date_for_is_asked_of_the_api(self):
        api = FakeGitHub([RC1_RELEASE])
        collect(api, tag_dates={'v1.0.0': TagDate(sha=V1, date='')})
        assert api.date_lookups == [V1]


class TestTheApiFallbackIsCapped:
    """When the git fetch fails, `/commits/{sha}` dates at most
    `API_DATE_CAP` tags, the newest versions first."""

    def test_at_most_the_cap_newest_versions_first(self):
        tags = {f'v1.{minor}.0': f'{minor:040x}' for minor in range(30)}
        listing = '\n'.join(
            f'{sha}\trefs/tags/{name}' for name, sha in tags.items()
        )
        dates = {
            sha: f'2020-01-{1 + minor % 28:02d}T00:00:00Z'
            for minor, sha in enumerate(tags.values())
        }
        api = FakeGitHub([], dates=dates)
        stats = ReleaseStats()
        repository = collect(api, FakeGit(listing), stats=stats)

        assert len(api.date_lookups) == API_DATE_CAP
        asked = [name for name, sha in tags.items() if sha in api.date_lookups]
        # v1.29.0 down to v1.10.0: numbers, not text (text puts v1.9.0 on top).
        assert sorted(asked, key=version_key) == [
            f'v1.{m}.0' for m in range(10, 30)
        ]
        undated = [r for r in repository.all_releases if r.published_at is None]
        assert len(undated) == 30 - API_DATE_CAP
        # The releases page and the capped lookups.
        assert stats.api_requests == 1 + API_DATE_CAP

    def test_version_key_orders_numbers_as_numbers(self):
        tags = ['v1.9.0', 'v1.10.0', 'v1.10.0-rc1', 'v2.0', 'release-3']
        assert sorted(tags, key=version_key, reverse=True)[
            :2
        ] == ['v2.0', 'v1.10.0-rc1']
        assert version_key('v1.10.0') > version_key('v1.9.0')


class TestDatesAreKeptInTheCache:
    """Dated once. A fresh cache costs nothing; a refresh dates only the
    tags that are new or moved."""

    def test_a_fresh_cache_needs_no_git_and_no_api(self):
        api = FakeGitHub([RC1_RELEASE])
        git = git_service(FakeGit(ACTIVE_PROJECT), git_dated('v1.0.0'))
        service = ReleaseService(api, git)
        collect(api, service=service)
        stats = ReleaseStats()
        repository = collect(api, stats=stats, service=service)

        assert git.tag_fetches == 1
        assert api.date_lookups == []
        assert stats.api_requests == 0
        assert repository.latest_stable_release.tag_name == 'v1.0.0'

    def test_a_cache_nothing_could_date_is_not_asked_again(self):
        """An undated tag is not looked up on every pass until the cache
        is refreshed: that is what spent the quota before."""
        api = FakeGitHub([RC1_RELEASE], dates={})
        service = ReleaseService(api, git_service(FakeGit(ACTIVE_PROJECT)))
        collect(api, service=service)
        collect(api, service=service)
        assert api.date_lookups == [V1]

    def test_a_version_2_cache_without_dates_is_dated_not_refetched(self):
        """Written by the tags-only fix (deb258e) before tags were dated
        with git. Its tags are right, so only the dates are added."""
        path = get_config().paths.get_release_cache_path(OWNER, REPO)
        path.parent.mkdir(parents=True)
        path.write_text(
            json.dumps({
                'version': 2,
                'releases': [RC1_RELEASE],
                'tags': {'v1.0.0': V1, 'v2.0.0-rc1': RC1},
                'updated_at': '2026-09-26T00:00:00+00:00',
            }),
        )
        api = FakeGitHub([RC1_RELEASE])
        git = git_service(FakeGit(ACTIVE_PROJECT), git_dated('v1.0.0'))
        stats = ReleaseStats()
        repository = collect(
            api, stats=stats, service=ReleaseService(api, git),
        )

        assert api.release_fetches == 0
        assert api.date_lookups == []
        assert git.tag_fetches == 1
        assert stats.api_requests == 0
        assert repository.latest_stable_release.tag_name == 'v1.0.0'
        written = json.loads(path.read_text())
        assert written['tag_dates'] == {'v1.0.0': COMMIT_DATES[V1]}

    def test_a_refresh_dates_only_new_or_moved_tags(self):
        api = FakeGitHub([])
        first = git_service(
            FakeGit(TAGS_ONLY),
            git_dated('v1.0.0', 'v2.0.0-rc1'),
        )
        collect(api, service=ReleaseService(api, first))
        path = get_config().paths.get_release_cache_path(OWNER, REPO)
        os.utime(path, (0, 0))  # expired: releases and tags are fetched again

        unchanged = git_service(FakeGit(TAGS_ONLY), None)
        collect(api, service=ReleaseService(api, unchanged))
        assert unchanged.tag_fetches == 0
        assert api.date_lookups == []

        os.utime(path, (0, 0))
        grown = git_service(
            FakeGit(TAGS_ONLY + f'\n{V09}\trefs/tags/v0.9.0'),
            git_dated('v0.9.0'),
        )
        repository = collect(api, service=ReleaseService(api, grown))
        assert grown.tag_fetches == 1
        assert api.date_lookups == []
        assert tag_names(repository) == ['v0.9.0', 'v1.0.0', 'v2.0.0-rc1']


class TestRunCountsTheReleaseStage:
    """`chatsbom run --quota` sums the stages' own counters. The release
    stage was handed a counter of its own on every call, so none of its
    requests reached the sum and the quota never stopped a release pass."""

    def test_the_release_stage_counts_into_the_pass(self, tmp_path):
        from types import SimpleNamespace

        from chatsbom.commands.run import stage_runners
        from chatsbom.core.config import PathConfig
        from chatsbom.core.ledger import Stage

        api = FakeGitHub([RC1_RELEASE])
        service = ReleaseService(api, git_service(FakeGit(ACTIVE_PROJECT)))
        container = SimpleNamespace(
            config=SimpleNamespace(paths=PathConfig(base_data_dir=tmp_path)),
            get_release_service=lambda token: service,
        )
        stats = ReleaseStats()
        runners = stage_runners(container, 'token', release_stats=stats)

        runners[Stage.RELEASE](Repository(id=1, owner=OWNER, repo=REPO), {})

        # The releases page, and one date lookup: the git fetch failed.
        assert stats.api_requests == 2


class TestGitHubServiceCountsWhatItSends:
    """`requests_sent` is what `--quota` is counted in: requests that
    reached GitHub, per thread, cache hits excluded."""

    class Response:
        def __init__(self, payload, from_cache):
            self.status_code = 200
            self.headers = {}
            self.text = ''
            self.from_cache = from_cache
            self._payload = payload

        def json(self):
            return self._payload

    class Session:
        def __init__(self, pages, from_cache=False):
            self.pages = list(pages)
            self.from_cache = from_cache
            self.headers = {}

        def request(self, method, url, **kwargs):
            return TestGitHubServiceCountsWhatItSends.Response(
                self.pages.pop(0), self.from_cache,
            )

        get = None

    def service(self, session, cached=False):
        from chatsbom.services.github_service import GitHubService

        github = GitHubService('token')
        github.session = session
        github._is_cached = lambda *args, **kwargs: cached
        session.get = lambda url, **kwargs: session.request('GET', url)
        return github

    def test_every_page_sent_is_counted(self):
        pages = [[api_release(i, f'v{i}', None) for i in range(100)], []]
        github = self.service(self.Session(pages))
        assert len(github.get_repository_releases(OWNER, REPO)) == 100
        assert github.requests_sent() == 2

    def test_a_cache_hit_is_free(self):
        github = self.service(self.Session([[]], from_cache=True), cached=True)
        github.get_repository_releases(OWNER, REPO)
        assert github.requests_sent() == 0

    def test_counted_per_thread(self):
        import threading

        github = self.service(self.Session([[], []]))
        github.get_repository_releases(OWNER, REPO)
        other: list[int] = []
        thread = threading.Thread(
            target=lambda: other.append(github.requests_sent()),
        )
        thread.start()
        thread.join()
        assert github.requests_sent() == 1
        assert other == [0]


PRERELEASE_NAMES = [
    # SemVer-style suffixes, with and without separators and numbers
    'v7.3-rc5', 'v3.0.0-rc.1', 'v1.0.0-RC.2', 'REL_2.2-rc-1',
    'v1.0.0-alpha', '8.0.0-alpha.3020', 'v1.0.0-beta2', 'v1.0.0-Beta.1',
    'v1.0-pre1', 'v1.0-preview', 'v1.0.0-preview.3', 'v1.2.3-dev',
    'v1.0.0-canary.3', 'v1-nightly', 'v14.0.0-next.1', 'v1.0-snapshot',
    # PEP 440
    '1.2.0a1', '1.2.0b2', '1.2.0rc1', '1.0.dev0', '2.1.0.dev3',
    # Maven qualifiers
    '2.0.0-M1', '2.0.0-RC1', '2.0.0.RC1', '1.0-SNAPSHOT',
    '5.0.0.BUILD-SNAPSHOT', '0.9.7#2.13.0-M3#8',
]

STABLE_NAMES = [
    'v7.2', 'v1.0.0', '0.12.0', 'v2026.4', 'r1.12.145', 'REL-0.2',
    'v2.0.0-final', 'v4.0.0-ga', 'v1.2.3-1',
    # build metadata says nothing about the version
    'v1.0.0+build.5', 'v1.0.0+build.rc1',
    # PEP 440 post-releases are releases
    '1.0.post1', '1.0.0.post2',
    # words, not markers
    'release-3', 'stable', 'pre-commit-v1', 'v1.0-alphabet',
    'v1.0-devtools', 'go1.21.0', 'rust-1.70.0',
]


class TestPreReleaseTagNames:
    """A bare tag has no `prerelease` flag, so its name decides (owner
    decision on #67): `v7.3-rc5` is not Linux's latest stable release."""

    @pytest.mark.parametrize('tag', PRERELEASE_NAMES)
    def test_a_prerelease_name(self, tag):
        assert looks_like_prerelease(tag)

    @pytest.mark.parametrize('tag', STABLE_NAMES)
    def test_not_a_prerelease_name(self, tag):
        assert not looks_like_prerelease(tag)

    def _tags(self, *tags):
        """`ls-remote` for lightweight tags, and git dating them in order."""
        shas = {tag: f'{i + 1:040x}' for i, tag in enumerate(tags)}
        listing = '\n'.join(f'{sha}\trefs/tags/{t}' for t, sha in shas.items())
        dates = {
            t: TagDate(sha=sha, date=f'2026-0{1 + i}-01T00:00:00Z')
            for i, (t, sha) in enumerate(shas.items())
        }
        return FakeGit(listing), dates

    def test_linux_resolves_to_its_latest_non_rc_tag(self):
        git, dates = self._tags(
            'v7.1', 'v7.2-rc1', 'v7.2', 'v7.3-rc1', 'v7.3-rc5',
        )
        repository = collect(FakeGitHub([]), git, tag_dates=dates)
        assert repository.latest_stable_release.tag_name == 'v7.2'

    def test_cryptgeon_resolves_past_its_release_candidate(self):
        """v3.0.0-rc.0 and -rc.1 are bare tags newer than its last release."""
        git, dates = self._tags('v2.9.1', 'v3.0.0-rc.0', 'v3.0.0-rc.1')
        repository = collect(FakeGitHub([]), git, tag_dates=dates)
        assert repository.latest_stable_release.tag_name == 'v2.9.1'

    def test_a_github_release_marked_stable_wins_over_its_name(self):
        release = api_release(5, 'v3.0.0-rc.1', '2026-09-01T00:00:00Z')
        git, dates = self._tags('v2.9.1', 'v3.0.0-rc.1')
        repository = collect(FakeGitHub([release]), git, tag_dates=dates)
        assert repository.latest_stable_release.tag_name == 'v3.0.0-rc.1'

    def test_only_prereleases_means_the_default_branch(self):
        """No stable candidate: no latest release, so the commit stage
        resolves the default branch, as for a repository with no tags."""
        git, dates = self._tags('v1.0.0-rc1', 'v1.0.0-beta2')
        repository = collect(FakeGitHub([]), git, tag_dates=dates)
        assert repository.latest_stable_release is None
        # Still recorded, and not flagged: the name is a judgement made
        # only when choosing, not a fact GitHub stated.
        assert repository.total_releases == 2
        assert not any(r.is_prerelease for r in repository.all_releases)
