"""The release and commit decisions, kept in the store as files (#147).

`chatsbom run` kept each repository's release list and the commit its
chain resolved to only in ClickHouse's `raw_documents`, inside the
record `RecordStore` writes. The store keeps them now, as the owner
decided on #100 (Q3):

    03-github-release/<id>/<P>/release@2.json      the release decision
    03-github-release/<id>/releases/<digest>.json  a release list
    04-github-commit/<id>/<K>/commit@1.json        the commit decision

Each is written once and never over; the same content twice is one
file, and a push with the same releases writes only its decision.
"""
from __future__ import annotations

import hashlib
import json
import os
import random
import string
from datetime import datetime
from datetime import timedelta
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest

from chatsbom.core import decisions
from chatsbom.core.config import PathConfig
from chatsbom.core.decisions import Outcome
from chatsbom.core.layout import CommitKey
from chatsbom.core.layout import key_name
from chatsbom.core.layout import key_of
from chatsbom.core.layout import MAX_NAME
from chatsbom.core.layout import push_instant
from chatsbom.core.layout import push_name
from chatsbom.core.layout import push_of
from chatsbom.models.github_release import GitHubRelease

UTC = timezone.utc

PUSH = '2026-09-29T12:28:14Z'
LATER = '2026-10-03T08:15:00Z'
S1 = 'a' * 40
S2 = 'b' * 40

UPLOADER = {'login': 'octocat', 'id': 1, 'node_id': 'MDQ6', 'type': 'User'}


def asset(name: str, downloads: int = 17) -> dict[str, Any]:
    """A release asset, as GitHub sends it: sixteen fields."""
    return {
        'url': f'https://api.github.com/repos/acme/app/releases/assets/{name}',
        'id': 262626262,
        'node_id': 'RA_kwDOABPHjc4Pp8mW',
        'name': name,
        'label': '',
        'uploader': UPLOADER,
        'content_type': 'application/gzip',
        'state': 'uploaded',
        'size': 4194304,
        'digest': 'sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822c',
        'download_count': downloads,
        'created_at': '2026-06-01T10:29:51Z',
        'updated_at': '2026-06-01T10:29:58Z',
        'browser_download_url': f'https://github.com/acme/app/releases/download/{name}',
    }


def release(
    tag: str,
    published: str,
    *,
    prerelease: bool = False,
    draft: bool = False,
    assets: list[dict[str, Any]] | None = None,
    source: str = 'github_release',
) -> dict[str, Any]:
    """A release as the record holds it: `GitHubRelease`, dumped."""
    return {
        'id': abs(hash(tag)) % 10_000, 'tag_name': tag, 'name': tag,
        'published_at': published, 'target_commitish': 'main',
        'is_prerelease': prerelease, 'is_draft': draft,
        'created_at': published, 'assets': assets or [], 'source': source,
    }


V2 = release('v2.0.0', '2026-09-01T00:00:00Z', assets=[asset('app.tar.gz')])
V1 = release('v1.0.0', '2025-01-01T00:00:00Z')
RC = release('v3.0.0-rc1', '2026-09-20T00:00:00Z', prerelease=True)


def record(**fields: Any) -> dict[str, Any]:
    """A repository after the release and commit stages, as `run` hands
    it on: `Repository.model_dump(mode='json')`."""
    return {
        'id': 42, 'owner': 'acme', 'repo': 'app', 'pushed_at': PUSH,
        'default_branch': 'main', 'has_releases': True,
        'total_releases': 3, 'all_releases': [RC, V2, V1],
        'latest_stable_release': V2,
        'download_target': {
            'ref': 'v2.0.0', 'ref_type': 'release', 'commit_sha': S1,
            'commit_sha_short': S1[:7],
        },
        **fields,
    }


@pytest.fixture
def paths(tmp_path: Path) -> PathConfig:
    return PathConfig(base_data_dir=tmp_path / 'data')


def body(path: Path) -> dict[str, Any]:
    loaded = json.loads(path.read_text(encoding='utf-8'))
    assert isinstance(loaded, dict)
    return loaded


def files(root: Path) -> list[str]:
    """Every file under `root`, relative, sorted."""
    return sorted(
        str(path.relative_to(root)) for path in root.rglob('*')
        if path.is_file()
    )


# -- the names ----------------------------------------------------------


class TestAPushsName:
    """`P`, the push a release decision is for: its instant in UTC, to the
    second, as `YYYYMMDDTHHMMSSZ`, which is how `09-github-depgraph`
    names a fetch."""

    def test_it_is_the_instant_in_utc(self) -> None:
        assert push_name(PUSH) == '20260929T122814Z'
        eight_hours_east = timezone(timedelta(hours=8))
        assert push_name(
            datetime(2026, 9, 29, 20, 28, 14, tzinfo=eight_hours_east),
        ) == '20260929T122814Z'
        assert push_name('2026-09-29T12:28:14+00:00') == '20260929T122814Z'

    def test_a_time_with_no_zone_is_utc_and_the_second_is_whole(self) -> None:
        assert push_name(datetime(2026, 9, 29, 12, 28, 14, 999_999)) == (
            '20260929T122814Z'
        )

    def test_no_push_has_no_name(self) -> None:
        assert push_name(None) is None
        assert push_name('') is None
        assert push_name('yesterday') is None

    def test_it_parses_back(self) -> None:
        assert push_of('20260929T122814Z') == datetime(
            2026, 9, 29, 12, 28, 14, tzinfo=UTC,
        )
        for other in ('releases', '2026-09-29T12:28:14Z', '20260929', ''):
            assert push_of(other) is None

    def test_names_sort_as_their_instants(self) -> None:
        rng = random.Random(147)
        instants = [
            datetime(2020, 1, 1, tzinfo=UTC) + timedelta(
                seconds=rng.randrange(10 ** 9),
            )
            for _ in range(500)
        ]
        names = [str(push_name(instant)) for instant in instants]
        assert sorted(names) == [str(push_name(i)) for i in sorted(instants)]


#: Tags as git allows them, and what they are named. Every byte outside
#: `a-z 0-9 . _ - @` is `%xx`, a capital letter too.
TAGS: list[tuple[str, str]] = [
    ('v1.2.3', 'tag-v1.2.3'),
    ('V1.2.3', 'tag-%561.2.3'),
    ('release/1.4.0', 'tag-release%2f1.4.0'),
    ('@scope/pkg@1.0.0', 'tag-@scope%2fpkg@1.0.0'),
    ('v1.0.0+build.1', 'tag-v1.0.0%2bbuild.1'),
    ('spring-boot_2.0.0', 'tag-spring-boot_2.0.0'),
    ('版本1', 'tag-%e7%89%88%e6%9c%ac1'),
    ('100%', 'tag-100%25'),
    ('a b', 'tag-a%20b'),
    ('a\\b', 'tag-a%5cb'),
    ('CON', 'tag-%43%4f%4e'),
    # Git refuses a ref that ends in a dot; Windows drops one from a name.
    ('v1.', 'tag-v1%2e'),
]

#: What a name may hold after its kind, on every file system the store
#: may be kept on, and in a shell without quoting.
SAFE = set(string.ascii_lowercase + string.digits + '._-@%')


class TestAKeysName:
    """`K`, the key a commit decision is for: `tag:T`, or `head:P` when
    the push has no release, spelled `tag-<T>` and `head-<P>`."""

    def test_a_head_is_named_by_its_push(self) -> None:
        key = CommitKey.head(PUSH)
        assert str(key) == 'head:2026-09-29T12:28:14Z'
        assert key_name(key) == 'head-20260929T122814Z'

    @pytest.mark.parametrize('tag,name', TAGS, ids=[t for t, _ in TAGS])
    def test_a_tag_is_spelled_so_every_file_system_keeps_it(
        self, tag: str, name: str,
    ) -> None:
        key = CommitKey.tag(tag)
        assert str(key) == f'tag:{tag}'
        assert key_name(key) == name
        assert key_of(name) == key

    def test_tags_that_differ_only_in_case_have_two_names_where_case_is_not(
        self,
    ) -> None:
        """APFS and NTFS ignore case by default: `v1.0` and `V1.0` are two
        tags to git, and must be two directories to them."""
        lower = key_name(CommitKey.tag('v1.0'))
        upper = key_name(CommitKey.tag('V1.0'))
        assert lower.casefold() != upper.casefold()

    def test_every_name_parses_back_and_is_safe(self) -> None:
        rng = random.Random(128)
        alphabet = string.printable + 'éß版本ÄΩ/@+%'
        for _ in range(2000):
            tag = ''.join(
                rng.choice(alphabet) for _ in range(rng.randrange(1, 30))
            )
            key = CommitKey.tag(tag)
            name = key_name(key)
            assert key_of(name) == key, tag
            kind, _, rest = name.partition('-')
            assert kind == 'tag'
            assert set(rest) <= SAFE, name
            assert not name.endswith(('.', ' '))
            assert len(name.encode()) <= MAX_NAME
        for _ in range(200):
            instant = datetime(2020, 1, 1, tzinfo=UTC) + timedelta(
                seconds=rng.randrange(10 ** 9),
            )
            head = CommitKey.head(instant)
            assert key_of(key_name(head)) == head

    def test_an_overlong_tag_is_named_by_its_digest(self) -> None:
        """A name holds 255 bytes on ext4, APFS and NTFS, and 143 under
        eCryptfs. The key itself is in the file."""
        tag = 'release/' + 'x' * 300
        name = key_name(CommitKey.tag(tag))
        assert name == 'tag~' + hashlib.sha256(tag.encode()).hexdigest()
        assert key_of(name) is None

    @pytest.mark.parametrize(
        'name', [
            'tag-%4A', 'tag-V1', 'head-2026', 'head-20260929T122814',
            'releases', 'tag', 'branch-main', 'tag-%zz', 'tag-a%2',
        ],
    )
    def test_a_name_not_written_this_way_is_no_key(self, name: str) -> None:
        """One key, one name: `%4A` would be a second spelling of `J`."""
        assert key_of(name) is None


# -- writing -------------------------------------------------------------


class TestTheReleaseDecision:

    def test_it_is_filed_under_its_push(self, paths: PathConfig) -> None:
        kept = decisions.keep_release(paths, record())

        assert kept.decision is Outcome.WRITTEN
        written = paths.release_dir / '42' / '20260929T122814Z' / 'release@2.json'
        stored = body(written)
        digest = stored.pop('releases')
        assert stored == {
            'id': 42, 'key': PUSH, 'out': 'v2.0.0', 'stage': 'release',
            'sv': 2,
        }
        assert files(paths.release_dir) == [
            '42/20260929T122814Z/release@2.json',
            f'42/releases/{digest}.json',
        ]

    def test_the_list_is_named_by_its_digest(self, paths: PathConfig) -> None:
        decisions.keep_release(paths, record())

        [listing] = (paths.release_dir / '42' / 'releases').iterdir()
        data = listing.read_bytes()
        assert listing.name == hashlib.sha256(data).hexdigest() + '.json'
        assert [r['tag_name'] for r in json.loads(data)] == [
            'v3.0.0-rc1', 'v2.0.0', 'v1.0.0',
        ]

    def test_the_list_keeps_what_the_warehouse_reads_and_no_count(
        self, paths: PathConfig,
    ) -> None:
        """An asset as `db index` keeps it (`ASSET_FIELDS`), less its
        download count: that moves on every fetch, and with it the same
        releases would be a new list each time."""
        decisions.keep_release(paths, record())

        [listing] = (paths.release_dir / '42' / 'releases').iterdir()
        v2 = json.loads(listing.read_bytes())[1]
        assert v2['assets'] == [{
            'name': 'app.tar.gz', 'content_type': 'application/gzip',
            'size': 4194304,
            'digest': 'sha256:9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822c',
            'created_at': '2026-06-01T10:29:51Z',
            'browser_download_url':
                'https://github.com/acme/app/releases/download/app.tar.gz',
        }]
        assert GitHubRelease.model_validate(v2) == GitHubRelease.model_validate(
            {**V2, 'assets': v2['assets']},
        )

    def test_a_push_with_no_stable_release_decides_none(
        self, paths: PathConfig,
    ) -> None:
        decisions.keep_release(
            paths, record(all_releases=[RC], latest_stable_release=None),
        )

        written = paths.release_dir / '42' / '20260929T122814Z' / 'release@2.json'
        assert body(written)['out'] is None

    def test_a_repository_with_no_releases_has_an_empty_list(
        self, paths: PathConfig,
    ) -> None:
        kept = decisions.keep_release(
            paths, record(
                all_releases=[], latest_stable_release=None,
                has_releases=False, total_releases=0,
            ),
        )

        assert kept.decision is Outcome.WRITTEN
        [listing] = (paths.release_dir / '42' / 'releases').iterdir()
        assert json.loads(listing.read_bytes()) == []


class TestTheCommitDecision:

    def test_it_is_filed_under_its_key(self, paths: PathConfig) -> None:
        assert decisions.keep_commit(paths, record()) is Outcome.WRITTEN

        written = paths.commit_dir / '42' / 'tag-v2.0.0' / 'commit@1.json'
        assert body(written) == {
            'id': 42, 'key': 'tag:v2.0.0', 'out': S1, 'push': PUSH,
            'ref': 'v2.0.0', 'ref_type': 'release', 'stage': 'commit',
            'sv': 1,
        }
        assert files(paths.commit_dir) == ['42/tag-v2.0.0/commit@1.json']

    def test_with_no_release_it_is_the_heads_at_the_push(
        self, paths: PathConfig,
    ) -> None:
        decisions.keep_commit(
            paths, record(
                latest_stable_release=None, download_target={
                    'ref': 'main', 'ref_type': 'branch', 'commit_sha': S2,
                    'commit_sha_short': S2[:7],
                },
            ),
        )

        written = (
            paths.commit_dir / '42' / 'head-20260929T122814Z' / 'commit@1.json'
        )
        assert body(written) == {
            'id': 42, 'key': 'head:2026-09-29T12:28:14Z', 'out': S2,
            'push': PUSH, 'ref': 'main', 'ref_type': 'branch',
            'stage': 'commit', 'sv': 1,
        }

    def test_a_tag_that_was_not_found_keeps_its_key_and_says_what_was_taken(
        self, paths: PathConfig,
    ) -> None:
        """The commit stage falls back to the default branch when the tag
        is gone: the decision is still the tag's, and says so."""
        decisions.keep_commit(
            paths, record(
                download_target={
                    'ref': 'main', 'ref_type': 'branch', 'commit_sha': S2,
                    'commit_sha_short': S2[:7],
                },
            ),
        )

        written = paths.commit_dir / '42' / 'tag-v2.0.0' / 'commit@1.json'
        assert body(written)['key'] == 'tag:v2.0.0'
        assert (body(written)['ref'], body(written)['ref_type']) == (
            'main', 'branch',
        )


class TestWrittenOnce:

    def test_the_same_decision_twice_is_one_file(
        self, paths: PathConfig,
    ) -> None:
        decisions.keep_release(paths, record())
        decisions.keep_commit(paths, record())
        before = {
            name: (paths.base_data_dir / name).stat().st_mtime_ns
            for name in files(paths.base_data_dir)
        }

        again = decisions.keep_release(paths, record())

        assert again.decision is Outcome.KEPT
        assert again.releases is Outcome.KEPT
        assert decisions.keep_commit(paths, record()) is Outcome.KEPT
        assert {
            name: (paths.base_data_dir / name).stat().st_mtime_ns
            for name in files(paths.base_data_dir)
        } == before

    def test_another_release_decision_for_the_same_push_leaves_the_first(
        self, paths: PathConfig,
    ) -> None:
        decisions.keep_release(paths, record())

        later = record(latest_stable_release=V1)
        assert decisions.keep_release(paths, later).decision is (
            Outcome.CONFLICT
        )

        release_file = (
            paths.release_dir / '42' / '20260929T122814Z' / 'release@2.json'
        )
        assert body(release_file)['out'] == 'v2.0.0'

    def test_a_push_with_the_same_releases_writes_only_its_decision(
        self, paths: PathConfig,
    ) -> None:
        """The list is content-addressed: a second push that decides the
        same releases names the list already there. The download count
        moved, and the uploader's object came back reordered."""
        decisions.keep_release(paths, record())
        recounted = [
            RC,
            {**V2, 'assets': [asset('app.tar.gz', downloads=9001)]},
            V1,
        ]

        kept = decisions.keep_release(
            paths, record(pushed_at=LATER, all_releases=recounted),
        )

        assert kept.decision is Outcome.WRITTEN
        assert kept.releases is Outcome.KEPT
        pushes = sorted(
            p.name for p in (paths.release_dir / '42').iterdir()
            if p.name != 'releases'
        )
        assert pushes == ['20260929T122814Z', '20261003T081500Z']
        assert len(list((paths.release_dir / '42' / 'releases').iterdir())) == 1

    def test_a_list_about_to_be_named_again_is_touched(
        self, paths: PathConfig,
    ) -> None:
        """Its day of grace from `data prune` starts again: it is about to
        be named, and may be named by no kept decision until then."""
        decisions.keep_release(paths, record())
        [listing] = (paths.release_dir / '42' / 'releases').iterdir()
        os.utime(listing, (1000, 1000))

        kept = decisions.keep_release(paths, record(pushed_at=LATER))

        assert kept.releases is Outcome.KEPT
        assert listing.stat().st_mtime > 1000

    def test_a_new_release_is_a_new_list(self, paths: PathConfig) -> None:
        decisions.keep_release(paths, record())
        v3 = release('v3.0.0', '2026-10-02T00:00:00Z')

        kept = decisions.keep_release(
            paths, record(
                pushed_at=LATER, all_releases=[v3, RC, V2, V1],
                latest_stable_release=v3,
            ),
        )

        assert kept.releases is Outcome.WRITTEN
        assert len(list((paths.release_dir / '42' / 'releases').iterdir())) == 2

    def test_a_report_writes_nothing(self, paths: PathConfig) -> None:
        kept = decisions.keep_release(paths, record(), apply=False)

        assert (kept.decision, kept.releases) == (
            Outcome.WRITTEN, Outcome.WRITTEN,
        )
        assert decisions.keep_commit(paths, record(), apply=False) is (
            Outcome.WRITTEN
        )
        assert not paths.base_data_dir.exists()

    def test_nothing_else_is_left_beside_them(self, paths: PathConfig) -> None:
        decisions.keep_release(paths, record())
        decisions.keep_commit(paths, record())
        decisions.keep_release(paths, record())

        leftovers = [
            name for name in files(paths.base_data_dir)
            if name.split('/')[-1].startswith('.')
        ]
        assert leftovers == []


def target(commit: str, ref: str = 'v2.0.0') -> dict[str, str]:
    """A download target the commit stage resolved."""
    return {
        'ref': ref, 'ref_type': 'release' if ref.startswith('v') else 'branch',
        'commit_sha': commit, 'commit_sha_short': commit[:7],
    }


def resolved(paths: PathConfig, pushed: str, tag: str = 'v2.0.0') -> str | None:
    """The commit the store has `tag` resolved to for the push `pushed`."""
    decision = decisions.commit_decision(
        paths, 42, CommitKey.tag(tag), push_instant(pushed),
    )
    return decision.commit_sha if decision is not None else None


class TestALaterResolution:
    """A tag moved, or went and the commit stage took the default branch
    in its place: a later push's key resolves to another commit. The
    first resolution stands for the pushes before; the later one is kept
    beside it, under the push it was resolved for, and stands from that
    push on. Nothing is written over."""

    def test_it_is_kept_beside_the_first_under_its_push(
        self, paths: PathConfig,
    ) -> None:
        decisions.keep_commit(paths, record())

        later = decisions.keep_commit(
            paths, record(pushed_at=LATER, download_target=target(S2)),
        )

        assert later is Outcome.WRITTEN
        assert files(paths.commit_dir) == [
            '42/tag-v2.0.0/20261003T081500Z/commit@1.json',
            '42/tag-v2.0.0/commit@1.json',
        ]
        key = paths.commit_dir / '42' / 'tag-v2.0.0'
        assert body(key / 'commit@1.json')['out'] == S1
        assert body(key / '20261003T081500Z' / 'commit@1.json') == {
            'id': 42, 'key': 'tag:v2.0.0', 'out': S2, 'push': LATER,
            'ref': 'v2.0.0', 'ref_type': 'release', 'stage': 'commit',
            'sv': 1,
        }

    def test_a_push_reads_the_newest_resolution_at_or_before_it(
        self, paths: PathConfig,
    ) -> None:
        decisions.keep_commit(paths, record())
        decisions.keep_commit(
            paths, record(pushed_at=LATER, download_target=target(S2)),
        )

        assert resolved(paths, PUSH) == S1
        assert resolved(paths, '2026-10-01T00:00:00Z') == S1
        assert resolved(paths, LATER) == S2
        assert resolved(paths, '2027-01-01T00:00:00Z') == S2
        # Before every resolution, the first.
        assert resolved(paths, '2026-01-01T00:00:00Z') == S1

    def test_the_chain_is_the_newest_resolution(
        self, paths: PathConfig,
    ) -> None:
        """`newest`, `newest_resolved` and so prune's current scan, and
        the warehouse, read the commit the tag is at now."""
        for pushed, commit in ((PUSH, S1), (LATER, S2)):
            decisions.keep_release(paths, record(pushed_at=pushed))
            decisions.keep_commit(
                paths, record(pushed_at=pushed, download_target=target(commit)),
            )

        chain = decisions.newest(paths, 42)
        resolved_chain = decisions.newest_resolved(paths, 42)

        assert chain is not None and chain.commit is not None
        assert chain.commit.commit_sha == S2
        assert resolved_chain is not None and resolved_chain.commit is not None
        assert resolved_chain.commit.commit_sha == S2

    def test_the_same_commit_again_is_not_written(
        self, paths: PathConfig,
    ) -> None:
        decisions.keep_commit(paths, record())
        decisions.keep_commit(
            paths, record(pushed_at=LATER, download_target=target(S2)),
        )

        again = decisions.keep_commit(
            paths, record(
                pushed_at='2026-10-05T00:00:00Z', download_target=target(S2),
            ),
        )

        assert again is Outcome.KEPT
        assert len(files(paths.commit_dir)) == 2

    def test_a_tag_moved_back_is_resolved_again(
        self, paths: PathConfig,
    ) -> None:
        decisions.keep_commit(paths, record())
        decisions.keep_commit(
            paths, record(pushed_at=LATER, download_target=target(S2)),
        )

        back = decisions.keep_commit(
            paths, record(
                pushed_at='2026-10-05T00:00:00Z', download_target=target(S1),
            ),
        )

        assert back is Outcome.WRITTEN
        assert resolved(paths, '2026-10-05T00:00:00Z') == S1
        assert resolved(paths, LATER) == S2

    def test_the_default_branch_taken_for_a_missing_tag_moves_with_it(
        self, paths: PathConfig,
    ) -> None:
        """The tag is gone, and every push takes the head of the default
        branch, which moves with the push."""
        heads = {PUSH: S1, LATER: S2, '2026-10-05T00:00:00Z': 'c' * 40}
        for pushed, head in heads.items():
            decisions.keep_release(paths, record(pushed_at=pushed))
            decisions.keep_commit(
                paths, record(
                    pushed_at=pushed, download_target=target(head, 'main'),
                ),
            )

        assert {pushed: resolved(paths, pushed) for pushed in heads} == heads
        chain = decisions.newest(paths, 42)
        assert chain is not None and chain.commit is not None
        assert (chain.commit.commit_sha, chain.commit.ref) == ('c' * 40, 'main')

    def test_one_push_resolved_again_differently_takes_the_later(
        self, paths: PathConfig,
    ) -> None:
        """A walk that resolved one push twice: the second is kept, under
        that push, and a third for it is not."""
        decisions.keep_commit(paths, record())

        again = decisions.keep_commit(
            paths, record(download_target=target(S2)),
        )
        third = decisions.keep_commit(
            paths, record(download_target=target('c' * 40)),
        )

        assert (again, third) == (Outcome.WRITTEN, Outcome.CONFLICT)
        assert resolved(paths, PUSH) == S2
        assert files(paths.commit_dir) == [
            '42/tag-v2.0.0/20260929T122814Z/commit@1.json',
            '42/tag-v2.0.0/commit@1.json',
        ]

    def test_one_written_out_of_order_stands_for_its_own_push(
        self, paths: PathConfig,
    ) -> None:
        """`github commit` over an older list, or the backfill while the
        collector runs: an older push resolved after a newer one."""
        decisions.keep_commit(
            paths, record(pushed_at=LATER, download_target=target(S2)),
        )

        older = decisions.keep_commit(paths, record())

        assert older is Outcome.WRITTEN
        assert resolved(paths, PUSH) == S1
        assert resolved(paths, LATER) == S2

    def test_one_with_no_push_to_file_it_under_leaves_the_first(
        self, paths: PathConfig,
    ) -> None:
        decisions.keep_commit(paths, record())

        outcome = decisions.keep_commit(
            paths, record(pushed_at=None, download_target=target(S2)),
        )

        assert outcome is Outcome.CONFLICT
        assert files(paths.commit_dir) == ['42/tag-v2.0.0/commit@1.json']

    def test_a_report_writes_nothing(self, paths: PathConfig) -> None:
        decisions.keep_commit(paths, record())

        outcome = decisions.keep_commit(
            paths, record(pushed_at=LATER, download_target=target(S2)),
            apply=False,
        )

        assert outcome is Outcome.WRITTEN
        assert files(paths.commit_dir) == ['42/tag-v2.0.0/commit@1.json']

    def test_one_filed_under_another_push_is_not_read(
        self, paths: PathConfig,
    ) -> None:
        decisions.keep_commit(paths, record())
        decisions.keep_commit(
            paths, record(pushed_at=LATER, download_target=target(S2)),
        )
        key = paths.commit_dir / '42' / 'tag-v2.0.0'
        stray = key / '20261001T000000Z'
        stray.mkdir()
        (stray / 'commit@1.json').write_bytes(
            (key / '20261003T081500Z' / 'commit@1.json').read_bytes(),
        )

        assert resolved(paths, '2026-10-02T00:00:00Z') == S1


#: A tag git holds as bytes that are not UTF-8, as GitPython hands it
#: on: decoded with surrogateescape.
NOT_UTF8 = b'v1.0\xff'.decode('utf-8', 'surrogateescape')


class TestATagThatIsNotUtf8:
    """Git keeps a tag's name as bytes, which need not be UTF-8, and the
    release stage takes the name GitPython decodes, with surrogateescape
    where it is not. Such a tag is named by its bytes, and the files that
    say it are written all the same."""

    def test_it_is_named_by_its_bytes(self) -> None:
        key = CommitKey.tag(NOT_UTF8)

        assert key_name(key) == 'tag-v1.0%ff'
        assert key_of('tag-v1.0%ff') == key

    def test_an_overlong_one_is_named_by_its_bytes_digest(self) -> None:
        tag = NOT_UTF8 + 'x' * 200

        assert key_name(CommitKey.tag(tag)) == 'tag~' + hashlib.sha256(
            b'v1.0\xff' + b'x' * 200,
        ).hexdigest()

    def test_its_decisions_are_kept_and_read_back(
        self, paths: PathConfig,
    ) -> None:
        chosen = release(NOT_UTF8, '2026-09-01T00:00:00Z', source='git_tag')
        made = record(
            all_releases=[chosen, V1], latest_stable_release=chosen,
            download_target=target(S2, NOT_UTF8),
        )

        kept = decisions.keep_release(paths, made)
        committed = decisions.keep_commit(paths, made)

        assert (kept.decision, kept.releases, committed) == (
            Outcome.WRITTEN, Outcome.WRITTEN, Outcome.WRITTEN,
        )
        assert files(paths.commit_dir) == ['42/tag-v1.0%ff/commit@1.json']
        for name in files(paths.base_data_dir):
            (paths.base_data_dir / name).read_bytes().decode('ascii')
        chain = decisions.newest(paths, 42)
        assert chain is not None and chain.commit is not None
        assert chain.releases is not None
        assert chain.release.tag == NOT_UTF8
        assert chain.releases[0]['tag_name'] == NOT_UTF8
        assert (chain.commit.commit_sha, chain.commit.ref) == (S2, NOT_UTF8)


class TestWhatCannotBeKeyed:

    def test_no_push_is_no_release_decision(self, paths: PathConfig) -> None:
        assert decisions.keep_release(
            paths, record(pushed_at=None),
        ).decision is Outcome.UNKEYED
        assert not paths.release_dir.exists()

    def test_a_release_stage_that_did_not_run_decides_nothing(
        self, paths: PathConfig,
    ) -> None:
        """`run` walks on when the releases could not be fetched, and the
        commit stage takes the default branch: that is no decision for a
        push whose release is not known."""
        failed = record(
            all_releases=None, has_releases=None, latest_stable_release=None,
            total_releases=0,
        )
        assert decisions.keep_release(paths, failed).decision is (
            Outcome.UNKEYED
        )
        assert decisions.keep_commit(paths, failed) is Outcome.UNKEYED
        assert not paths.base_data_dir.exists()

    def test_a_head_needs_its_push(self, paths: PathConfig) -> None:
        assert decisions.keep_commit(
            paths, record(latest_stable_release=None, pushed_at=None),
        ) is Outcome.UNKEYED

    def test_an_unresolved_commit_is_no_decision(
        self, paths: PathConfig,
    ) -> None:
        assert decisions.keep_commit(
            paths, record(download_target=None),
        ) is Outcome.UNKEYED


# -- reading --------------------------------------------------------------


class TestReadingBack:

    def test_the_newest_push_is_the_current_chain(
        self, paths: PathConfig,
    ) -> None:
        decisions.keep_release(paths, record())
        decisions.keep_commit(paths, record())
        v3 = release('v3.0.0', '2026-10-02T00:00:00Z')
        newer = record(
            pushed_at=LATER, all_releases=[v3, RC, V2, V1],
            latest_stable_release=v3, download_target={
                'ref': 'v3.0.0', 'ref_type': 'release', 'commit_sha': S2,
                'commit_sha_short': S2[:7],
            },
        )
        decisions.keep_release(paths, newer)
        decisions.keep_commit(paths, newer)

        chain = decisions.newest(paths, 42)

        assert chain is not None
        assert chain.release.push == datetime(2026, 10, 3, 8, 15, tzinfo=UTC)
        assert chain.release.tag == 'v3.0.0'
        assert chain.releases is not None
        assert [r['tag_name'] for r in chain.releases] == [
            'v3.0.0', 'v3.0.0-rc1', 'v2.0.0', 'v1.0.0',
        ]
        assert chain.commit is not None
        assert (chain.commit.commit_sha, chain.commit.ref) == (S2, 'v3.0.0')

    def test_a_chain_whose_commit_is_not_decided_yet_has_none(
        self, paths: PathConfig,
    ) -> None:
        decisions.keep_release(paths, record())

        chain = decisions.newest(paths, 42)

        assert chain is not None and chain.commit is None
        assert decisions.newest(paths, 43) is None

    def test_the_newest_resolved_chain_is_the_one_the_scan_descends_from(
        self, paths: PathConfig,
    ) -> None:
        """A newer push whose commit is not decided yet: the scan in the
        store is still the older chain's."""
        decisions.keep_release(paths, record())
        decisions.keep_commit(paths, record())
        v3 = release('v3.0.0', '2026-10-02T00:00:00Z')
        decisions.keep_release(
            paths, record(
                pushed_at=LATER, all_releases=[v3, RC, V2, V1],
                latest_stable_release=v3,
            ),
        )

        resolved = decisions.newest_resolved(paths, 42)

        assert resolved is not None
        assert resolved.release.tag == 'v2.0.0'
        assert resolved.commit is not None
        assert resolved.commit.commit_sha == S1

    def test_an_empty_push_directory_is_passed_over(
        self, paths: PathConfig,
    ) -> None:
        """What a writer killed between making the directory and linking
        its file leaves."""
        decisions.keep_release(paths, record())
        (paths.release_dir / '42' / '20261003T081500Z').mkdir()

        chain = decisions.newest(paths, 42)

        assert chain is not None
        assert chain.release.tag == 'v2.0.0'

    def test_the_newest_version_this_code_knows_is_read(
        self, paths: PathConfig,
    ) -> None:
        decisions.keep_release(paths, record())
        directory = paths.release_dir / '42' / '20260929T122814Z'
        older = {
            **body(directory / 'release@2.json'), 'sv': 1, 'out': 'v1.0.0',
        }
        (directory / 'release@1.json').write_text(json.dumps(older))

        chain = decisions.newest(paths, 42)

        assert chain is not None
        assert (chain.release.version, chain.release.tag) == (2, 'v2.0.0')

    def test_a_later_versions_decision_is_not_read(
        self, paths: PathConfig,
    ) -> None:
        """What a later version of a stage decided is its own to read:
        it may mean something this code does not know."""
        decisions.keep_release(paths, record())
        decisions.keep_commit(paths, record())
        directory = paths.release_dir / '42' / '20260929T122814Z'
        later = {
            **body(directory / 'release@2.json'), 'sv': 10, 'out': 'v1.0.0',
        }
        (directory / 'release@10.json').write_text(json.dumps(later))
        key = paths.commit_dir / '42' / 'tag-v2.0.0'
        (key / 'commit@7.json').write_text(
            json.dumps({**body(key / 'commit@1.json'), 'sv': 7, 'out': S2}),
        )

        chain = decisions.newest(paths, 42)

        assert chain is not None and chain.commit is not None
        assert (chain.release.version, chain.release.tag) == (2, 'v2.0.0')
        assert (chain.commit.version, chain.commit.commit_sha) == (1, S1)

    def test_a_decision_filed_under_another_name_is_not_read(
        self, paths: PathConfig,
    ) -> None:
        decisions.keep_release(paths, record())
        stray = paths.release_dir / '42' / '20261003T081500Z'
        stray.mkdir()
        (stray / 'release@2.json').write_bytes(
            (
                paths.release_dir / '42' / '20260929T122814Z' / 'release@2.json'
            ).read_bytes(),
        )

        chain = decisions.newest(paths, 42)

        assert chain is not None
        assert push_name(chain.release.push) == '20260929T122814Z'

    def test_the_record_it_makes_is_the_one_the_stages_made(
        self, paths: PathConfig,
    ) -> None:
        """What the warehouse reads in place of the record's own: the
        releases, the latest stable one, and the download target."""
        decisions.keep_release(paths, record())
        decisions.keep_commit(paths, record())

        chain = decisions.newest(paths, 42)
        assert chain is not None
        made = decisions.as_record(chain)

        assert made['has_releases'] is True
        assert made['total_releases'] == 3
        assert [r['tag_name'] for r in made['all_releases']] == [
            'v3.0.0-rc1', 'v2.0.0', 'v1.0.0',
        ]
        assert made['latest_stable_release']['tag_name'] == 'v2.0.0'
        assert made['latest_stable_release'] == made['all_releases'][1]
        assert made['download_target'] == {
            'ref': 'v2.0.0', 'ref_type': 'release', 'commit_sha': S1,
            'commit_sha_short': S1[:7],
        }

    def test_a_tag_its_list_lacks_is_still_the_latest(
        self, paths: PathConfig,
    ) -> None:
        """A record another version of the stage made may name a latest
        release its list does not hold: the decision says the tag, and
        the tag is what is known of it."""
        decisions.keep_release(paths, record(all_releases=[V1]))

        chain = decisions.newest(paths, 42)
        assert chain is not None
        made = decisions.as_record(chain)

        assert made['latest_stable_release'] == {'tag_name': 'v2.0.0'}
        assert made['total_releases'] == 1

    def test_the_latest_stable_is_the_list_entry_chosen(self) -> None:
        """A draft may share the tag of the release chosen: the stable
        one is the one the release stage took."""
        draft = release('v2.0.0', '2026-09-02T00:00:00Z', draft=True)
        chosen = decisions.chosen(
            [GitHubRelease.model_validate(r) for r in (RC, draft, V2, V1)],
            'v2.0.0',
        )
        assert chosen is not None
        assert chosen.is_draft is False
        assert chosen.published_at == datetime(2026, 9, 1, tzinfo=UTC)
