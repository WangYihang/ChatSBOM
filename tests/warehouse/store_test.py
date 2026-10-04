"""`warehouse build` reads the store with the parsers `db index` uses.

Every scan the store holds is ingested, not only the one a record points
at: every commit's Syft document and manifests, and every fetch of the
dependency graph. Each is parsed by the code that makes ClickHouse's
rows (`DbService`), so the two engines read one store the same way
(#131).
"""
from __future__ import annotations

from datetime import date
from datetime import datetime
from typing import Any

import duckdb
import pytest

from chatsbom.core.documents import DEPGRAPH
from chatsbom.core.documents import FILE_MANIFESTS
from chatsbom.core.documents import FILES
from chatsbom.core.documents import SYFT
from chatsbom.core.edges import collect_edges
from chatsbom.models.repository import Repository
from chatsbom.services.db_service import DbService
from chatsbom.warehouse.store import MANIFEST_TOOL
from tests.warehouse.conftest import artifact
from tests.warehouse.conftest import at
from tests.warehouse.conftest import Build
from tests.warehouse.conftest import Listed
from tests.warehouse.conftest import rows
from tests.warehouse.conftest import spdx
from tests.warehouse.conftest import Store

A = 'a' * 40
B = 'b' * 40
HEAD_1 = 'e' * 40
HEAD_2 = 'f' * 40

FEB = at(2026, 2, 11, 9, 30)
SEP = at(2026, 9, 14, 10, 0)

APP = Listed(1, 'acme', 'app', stars=500, language='Java')
WEB = Listed(2, 'acme', 'web', stars=900, language='JavaScript')
GRAPHED = Listed(3, 'acme', 'graphed', stars=1200, language='Kotlin')

PACKAGE_JSON_A = '{"dependencies": {"react": "^18.2.0", "lodash": "^4.17.0"}}'
PACKAGE_JSON_B = '{"dependencies": {"react": "^18.3.0"}}'
BUILD_GRADLE = """
dependencies {
    implementation 'com.google.guava:guava:33.0.0-jre'
}
"""


@pytest.fixture
def corpus(store: Store) -> Store:
    """Three repositories of the newest snapshot. `acme/app` was scanned
    at two commits and its graph fetched twice; `acme/web` scanned at
    two commits whose manifests differ; `acme/graphed` has no record,
    only the graph kept from before every fetch was."""
    store.snapshot(
        date(2026, 9, 1), APP, WEB, GRAPHED, complete=False,
    )

    store.sbom(
        1, A, artifact('guava', '32.1.0-jre', 'java-archive'), at=FEB,
    )
    store.sbom(
        1, B,
        artifact('guava', '33.0.0-jre', 'java-archive'),
        artifact('jsr305', '3.0.2', 'java-archive', licenses=['Apache-2.0']),
        at=SEP, version='1.53.0',
    )
    store.content(1, A, {'build.gradle': BUILD_GRADLE})
    store.content(1, B, {'build.gradle': BUILD_GRADLE})
    store.record(
        1, 'acme', 'app', commit=B, ref='v2.0.0', listing='java',
        all_releases=[
            {
                'id': 11, 'tag_name': 'v1.0.0', 'published_at':
                '2026-01-10T00:00:00Z', 'prerelease': False,
            },
            {
                'id': 12, 'tag_name': 'v2.0.0-rc1', 'published_at':
                '2026-08-01T00:00:00Z', 'prerelease': True,
            },
        ],
    )
    store.graph(
        1,
        spdx(
            '2026-03-05T08:00:00Z',
            [('com.google.guava:guava', '32.1.0-jre', 'maven')],
            direct=['com.google.guava:guava'],
        ),
        fetched=at(2026, 3, 5, 8, 0, 30), head=HEAD_1,
    )
    store.graph(
        1,
        spdx(
            '2026-09-13T08:00:00Z',
            [
                ('com.google.guava:guava', '33.0.0-jre', 'maven'),
                ('com.google.code.findbugs:jsr305', '3.0.2', 'maven'),
            ],
            direct=['com.google.guava:guava'],
            edges=[(
                'com.google.guava:guava', 'com.google.code.findbugs:jsr305',
            )],
        ),
        fetched=at(2026, 9, 13, 8, 0, 30), head=HEAD_2,
    )

    npm = [
        artifact('react', '18.2.0', 'npm', licenses=['MIT']),
        artifact('lodash', '4.17.21', 'npm', licenses=['MIT']),
    ]
    store.sbom(2, A, *npm, at=FEB)
    store.sbom(2, B, *npm, at=SEP)
    store.content(2, A, {'package.json': PACKAGE_JSON_A})
    store.content(2, B, {'package.json': PACKAGE_JSON_B})
    store.record(2, 'acme', 'web', commit=B, listing='javascript')

    store.legacy_graph(
        3,
        spdx(
            '2026-02-01T00:00:00Z',
            [('left-pad', '1.3.0', 'npm')], direct=['left-pad'],
        ),
    )
    return store


def ingested(con: duckdb.DuckDBPyConnection, scan_id: int) -> list[dict[str, Any]]:
    """The scan's observations as `artifacts` rows, each with its scan's
    columns put back under their ClickHouse names."""
    found = con.execute(
        """SELECT o.repository_id, o.artifact_id, o.name, o.version, o.type,
                  o.purl, o.found_by, o.licenses, o.relationship, o.source,
                  o.version_kind, s.ref, s.commit_sha, s.observed_at
           FROM observations AS o JOIN scans AS s USING (scan_id)
           WHERE scan_id = ? ORDER BY o.position""",
        [scan_id],
    ).fetchall()
    names = (
        'repository_id', 'artifact_id', 'name', 'version', 'type', 'purl',
        'found_by', 'licenses', 'relationship', 'source', 'version_kind',
        'sbom_ref', 'sbom_commit_sha', 'observed_at',
    )
    return [dict(zip(names, row)) for row in found]


def naive(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """`db index`'s rows with their instant as the warehouse stores it:
    UTC, with no zone."""
    return [
        {
            **row,
            'observed_at': row['observed_at'].replace(tzinfo=None),
        }
        for row in rows
    ]


def scan_id(
    con: duckdb.DuckDBPyConnection,
    repository_id: int,
    source: str,
    input_key: str,
) -> int:
    (found,), = con.execute(
        'SELECT scan_id FROM scans WHERE repository_id = ? AND source = ? '
        'AND input_key = ?',
        [repository_id, source, input_key],
    ).fetchall()
    return int(found)


class TestScans:

    def test_every_commit_the_store_holds_is_a_scan(
        self, corpus: Store, built: Build,
    ) -> None:
        """Not only the commit the record names: `db index` added the
        older one when it was current, and the store still has it."""
        con = built()
        assert rows(
            con,
            'SELECT repository_id, input_key, tool, observed_at FROM scans '
            "WHERE source = 'syft' ORDER BY repository_id, observed_at",
        ) == [
            (1, A, 'syft@1.52.0', FEB.replace(tzinfo=None)),
            (1, B, 'syft@1.53.0', SEP.replace(tzinfo=None)),
            (2, A, 'syft@1.52.0', FEB.replace(tzinfo=None)),
            (2, B, 'syft@1.52.0', SEP.replace(tzinfo=None)),
        ]

    def test_a_commits_manifests_are_a_scan_of_their_own(
        self, corpus: Store, built: Build,
    ) -> None:
        """Dated as `db index` dates their rows: by the Syft document of
        the same commit."""
        con = built()
        assert rows(
            con,
            'SELECT repository_id, input_key, observed_at, observations '
            "FROM scans WHERE source = 'manifest' "
            'ORDER BY repository_id, observed_at',
        ) == [
            (1, A, FEB.replace(tzinfo=None), 1),
            (1, B, SEP.replace(tzinfo=None), 1),
            (2, A, FEB.replace(tzinfo=None), 0),
            (2, B, SEP.replace(tzinfo=None), 0),
        ]

    def test_each_fetch_of_the_graph_is_a_scan(
        self, corpus: Store, built: Build,
    ) -> None:
        """Dated by what the document states, with the branch and HEAD
        it was fetched at; the graph kept from before is one too."""
        con = built()
        assert rows(
            con,
            'SELECT repository_id, input_key, tool, observed_at, ref, '
            "commit_sha FROM scans WHERE source = 'github-depgraph' "
            'ORDER BY repository_id, observed_at',
        ) == [
            (
                1, f'20260305T080030Z-{HEAD_1}',
                'GitHub.com-Dependency-Graph',
                datetime(2026, 3, 5, 8, 0), 'main', HEAD_1,
            ),
            (
                1, f'20260913T080030Z-{HEAD_2}',
                'GitHub.com-Dependency-Graph',
                datetime(2026, 9, 13, 8, 0), 'main', HEAD_2,
            ),
            (
                3, 'legacy', 'GitHub.com-Dependency-Graph',
                datetime(2026, 2, 1, 0, 0), 'main', '',
            ),
        ]

    def test_a_commit_is_dated_by_when_the_store_first_had_it(
        self, corpus: Store, built: Build,
    ) -> None:
        """After a Syft upgrade `sbom generate` writes every stored
        root's document again, an older commit's included, in the order
        it walks them. A commit is dated by the earliest of its outputs,
        its manifests before its document, so a document written again
        moves neither its commit in time nor the current scan to it."""
        corpus.content(2, A, {'package.json': PACKAGE_JSON_A}, at=FEB)
        corpus.content(2, B, {'package.json': PACKAGE_JSON_B}, at=SEP)
        again = at(2026, 9, 28, 3, 0)
        corpus.sbom(
            2, A, artifact('react', '18.2.0', 'npm', licenses=['MIT']),
            artifact('lodash', '4.17.21', 'npm', licenses=['MIT']),
            at=again, version='1.53.0',
        )
        con = built()
        assert rows(
            con,
            'SELECT source, input_key, tool, observed_at FROM scans '
            "WHERE repository_id = 2 AND source IN ('syft', 'manifest') "
            'ORDER BY source, observed_at',
        ) == [
            ('manifest', A, MANIFEST_TOOL, FEB.replace(tzinfo=None)),
            ('manifest', B, MANIFEST_TOOL, SEP.replace(tzinfo=None)),
            ('syft', A, 'syft@1.53.0', FEB.replace(tzinfo=None)),
            ('syft', B, 'syft@1.52.0', SEP.replace(tzinfo=None)),
        ]
        assert rows(
            con,
            'SELECT input_key FROM current_scans WHERE repository_id = 2 '
            "AND source = 'syft'",
        ) == [(B,)]

    def test_a_commit_with_no_manifests_is_dated_by_its_tree(
        self, corpus: Store, built: Build,
    ) -> None:
        """A commit whose content root is empty has no manifest to date
        it by, and its document is written again after a Syft upgrade
        (#180). Its tree, which the tree stage lists once, when the
        commit is the download target, says when the store first had it:
        so the commit downloaded since stays the current one."""
        empty = 'c' * 40
        corpus.tree(2, empty, ['README.md'], at=FEB)
        corpus.content(2, empty, {})
        corpus.sbom(2, empty, at=at(2026, 9, 30, 4, 55), version='1.53.0')
        corpus.tree(2, B, ['package.json'], at=SEP)
        corpus.content(2, B, {'package.json': PACKAGE_JSON_B}, at=SEP)
        con = built()
        assert rows(
            con,
            'SELECT source, observed_at FROM scans WHERE repository_id = 2 '
            'AND input_key = ? ORDER BY source', empty,
        ) == [
            ('manifest', FEB.replace(tzinfo=None)),
            ('syft', FEB.replace(tzinfo=None)),
        ]
        assert rows(
            con,
            'SELECT source, input_key FROM current_scans '
            "WHERE repository_id = 2 AND source IN ('syft', 'manifest') "
            'ORDER BY source',
        ) == [('manifest', B), ('syft', B)]

    def test_the_ref_is_the_download_targets_where_a_record_names_it(
        self, corpus: Store, built: Build,
    ) -> None:
        """The layout names a scan by its commit; the ref is the record's,
        and only its newest commit's is still said anywhere."""
        con = built()
        assert rows(
            con,
            'SELECT input_key, ref, ref_type, commit_sha FROM scans '
            "WHERE repository_id = 1 AND source = 'syft' "
            'ORDER BY observed_at',
        ) == [(A, '', '', A), (B, 'v2.0.0', 'release', B)]


class TestTheParsersAreDbIndexs:

    def test_a_syft_scan_is_the_rows_db_index_makes_of_it(
        self, corpus: Store, built: Build,
    ) -> None:
        con = built()
        service = DbService()
        paths = corpus.paths
        # acme/web's record names the second commit, at `v1.0.0`.
        for commit, ref in ((A, ''), (B, 'v1.0.0')):
            document = FILES.get(SYFT, 2, str(paths.sbom_file(2, commit)))
            manifests = FILE_MANIFESTS.for_repository(
                2, str(paths.content_root(2, commit)),
            )
            expected, _ = service.scan_rows(
                document, manifests, 2,
                {'sbom_ref': ref, 'sbom_commit_sha': commit},
            )
            assert ingested(con, scan_id(con, 2, 'syft', commit)) == naive(
                expected,
            )

    def test_the_manifests_rows_are_db_indexs(
        self, corpus: Store, built: Build,
    ) -> None:
        con = built()
        paths = corpus.paths
        document = FILES.get(SYFT, 1, str(paths.sbom_file(1, B)))
        manifests = FILE_MANIFESTS.for_repository(
            1, str(paths.content_root(1, B)),
        )
        _, expected = DbService().scan_rows(
            document, manifests, 1,
            {'sbom_ref': 'v2.0.0', 'sbom_commit_sha': B},
        )
        assert expected
        assert ingested(con, scan_id(con, 1, 'manifest', B)) == naive(
            expected,
        )

    def test_a_graph_is_the_rows_db_index_makes_of_it(
        self, corpus: Store, built: Build,
    ) -> None:
        con = built()
        service = DbService()
        record = next(
            r for r in (
                Repository.model_validate(data)
                for data in _records(corpus)
            ) if r.id == 1
        )
        repo_row = service.parse_repository(record)
        key = f'20260913T080030Z-{HEAD_2}'
        document = FILES.get(
            DEPGRAPH, 1,
            str(corpus.paths.depgraph_dir / '1' / key / 'sbom.spdx.json'),
        )
        assert document is not None
        expected = service.parse_dependency_graph(document, 1, repo_row)
        assert ingested(con, scan_id(con, 1, DEPGRAPH, key)) == naive(
            expected,
        )

    def test_each_commit_is_judged_by_its_own_manifests(
        self, corpus: Store, built: Build,
    ) -> None:
        """`lodash` was declared at the first commit and not at the
        second, where the same document puts it."""
        con = built()
        assert rows(
            con,
            'SELECT s.input_key, o.name, o.relationship '
            'FROM observations AS o JOIN scans AS s USING (scan_id) '
            "WHERE s.repository_id = 2 AND s.source = 'syft' "
            'ORDER BY s.observed_at, o.name',
        ) == [
            (A, 'lodash', 'direct'),
            (A, 'react', 'direct'),
            (B, 'lodash', 'transitive'),
            (B, 'react', 'direct'),
        ]


def _records(store: Store) -> list[dict[str, Any]]:
    import json

    found: dict[int, dict[str, Any]] = {}
    for listing in sorted(store.paths.sbom_dir.glob('*.jsonl')):
        for line in listing.read_text(encoding='utf-8').splitlines():
            record = json.loads(line)
            found[record['id']] = record
    return list(found.values())


class TestRepositories:

    def test_every_repository_the_store_names_has_its_metadata(
        self, corpus: Store, built: Build,
    ) -> None:
        """A record's where there is one, as `db index` projects it;
        else what the ledger and the snapshot say of it."""
        con = built()
        assert rows(
            con,
            'SELECT id, owner, repo, stars, github_language, snapshot '
            'FROM repositories ORDER BY id',
        ) == [
            (1, 'acme', 'app', 100, 'Java', 'all-2026-09-01'),
            (2, 'acme', 'web', 100, 'JavaScript', 'all-2026-09-01'),
            (3, 'acme', 'graphed', 1200, 'Kotlin', 'all-2026-09-01'),
        ]

    def test_a_repository_row_is_db_indexs_projection(
        self, corpus: Store, built: Build,
    ) -> None:
        con = built()
        columns = [
            name for (name,) in con.execute(
                'SELECT column_name FROM information_schema.columns '
                "WHERE table_name = 'repositories' "
                'ORDER BY ordinal_position',
            ).fetchall()
        ]
        stored = dict(
            zip(
                columns,
                con.execute(
                    'SELECT * FROM repositories WHERE id = 2',
                ).fetchone()
                or (),
            ),
        )
        record = next(r for r in _records(corpus) if r['id'] == 2)
        projected = DbService().parse_repository(
            Repository.model_validate({
                **record, 'github_language': 'JavaScript',
                'snapshot': 'all-2026-09-01', 'stargazers_count': 100,
            }),
        )
        for column, value in stored.items():
            expected = projected[column]
            if isinstance(expected, datetime):
                expected = expected.replace(tzinfo=None)
            assert value == expected, column

    def test_each_snapshot_listing_is_history(
        self, corpus: Store, built: Build,
    ) -> None:
        """Every dated snapshot the store keeps says what the search saw
        that day: the stars, the name, the language and the push."""
        corpus.snapshot(
            date(2026, 3, 1),
            Listed(1, 'acme', 'app-old', stars=400, language='Java'),
        )
        con = built()
        assert rows(
            con,
            'SELECT id, snapshot, observed_at, repo, stars '
            'FROM repository_history ORDER BY id, observed_at',
        ) == [
            (1, 'all-2026-03-01', datetime(2026, 3, 1), 'app-old', 400),
            (1, 'all-2026-09-01', datetime(2026, 9, 1), 'app', 500),
            (2, 'all-2026-09-01', datetime(2026, 9, 1), 'web', 900),
            (3, 'all-2026-09-01', datetime(2026, 9, 1), 'graphed', 1200),
        ]

    def test_releases_are_the_records(
        self, corpus: Store, built: Build,
    ) -> None:
        con = built()
        assert rows(
            con,
            'SELECT repository_id, release_id, tag_name, is_prerelease, '
            'published_at FROM releases ORDER BY release_id',
        ) == [
            (1, 11, 'v1.0.0', False, datetime(2026, 1, 10)),
            (1, 12, 'v2.0.0-rc1', True, datetime(2026, 8, 1)),
        ]


def github_release(
    release_id: int, tag: str, published: str, *, prerelease: bool = False,
    assets: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        'id': release_id, 'tag_name': tag, 'name': tag,
        'published_at': published, 'created_at': published,
        'is_prerelease': prerelease, 'is_draft': False,
        'target_commitish': 'main', 'source': 'github_release',
        'assets': assets or [],
    }


V3 = github_release(
    31, 'v3.1.0', '2026-09-20T00:00:00Z', assets=[{
        'name': 'app.tar.gz', 'size': 10, 'download_count': 99,
        'content_type': 'application/gzip',
        'browser_download_url': 'https://example/app.tar.gz',
        'created_at': '2026-09-20T00:00:00Z',
        'uploader': {'login': 'octocat'},
    }],
)
V3_RC = github_release(
    32, 'v3.2.0-rc1', '2026-09-25T00:00:00Z', prerelease=True,
)
V30 = github_release(30, 'v3.0.0', '2026-06-01T00:00:00Z')
DECIDED = Listed(4, 'acme', 'decided', stars=2000, language='Go')


@pytest.fixture
def decided(store: Store) -> Store:
    """A repository `chatsbom run` collected: its scans and its decisions
    are in the store, and its record only in `raw_documents`, which the
    warehouse does not read."""
    store.snapshot(date(2026, 9, 1), DECIDED)
    store.sbom(4, A, artifact('cobra', '1.7.0', 'go-module'), at=FEB)
    store.sbom(4, B, artifact('cobra', '1.8.0', 'go-module'), at=SEP)
    store.decide(
        4, pushed_at='2026-06-02T00:00:00Z', releases=[V30], latest='v3.0.0',
        commit=A, ref='v3.0.0', ref_type='release',
    )
    store.decide(
        4, pushed_at='2026-09-26T00:00:00Z', releases=[V3_RC, V3, V30],
        latest='v3.1.0', commit=B, ref='v3.1.0', ref_type='release',
    )
    return store


class TestTheDecisions:
    """What a record said of the release and commit stages, read from
    their decisions (#147): the newest per repository, beside what the
    records and the ledger say."""

    def test_the_releases_are_the_newest_decisions_list(
        self, decided: Store, built: Build,
    ) -> None:
        con = built()
        assert rows(
            con,
            'SELECT tag_name, is_prerelease, published_at, release_assets '
            'FROM releases WHERE repository_id = 4 ORDER BY published_at',
        ) == [
            ('v3.0.0', False, datetime(2026, 6, 1), '[]'),
            (
                'v3.1.0', False, datetime(2026, 9, 20),
                '[{"browser_download_url": "https://example/app.tar.gz", '
                '"content_type": "application/gzip", "created_at": '
                '"2026-09-20T00:00:00Z", "name": "app.tar.gz", "size": 10}]',
            ),
            ('v3.2.0-rc1', True, datetime(2026, 9, 25), '[]'),
        ]
        assert rows(
            con,
            'SELECT has_releases, latest_release_tag, '
            'latest_release_published_at, total_releases '
            'FROM repositories WHERE id = 4',
        ) == [(True, 'v3.1.0', datetime(2026, 9, 20), 3)]

    def test_each_scan_has_the_ref_its_commit_was_resolved_from(
        self, decided: Store, built: Build,
    ) -> None:
        """An older scan's too: the commit decision that resolved to its
        commit says the ref, while the store keeps it."""
        con = built()
        assert rows(
            con,
            'SELECT input_key, ref, ref_type FROM scans '
            "WHERE repository_id = 4 AND source = 'syft' ORDER BY observed_at",
        ) == [(A, 'v3.0.0', 'release'), (B, 'v3.1.0', 'release')]

    def test_a_moved_tag_is_read_at_its_newest_resolution(
        self, decided: Store, built: Build,
    ) -> None:
        """`v3.1.0` moved to a commit scanned since, and the commit stage
        resolved it again: the current scan is that commit's, with the
        tag's ref, not the first commit the tag was resolved to."""
        moved = 'c' * 40
        decided.sbom(
            4, moved, artifact('cobra', '1.9.0', 'go-module'),
            at=at(2026, 9, 29),
        )
        decided.decide(
            4, pushed_at='2026-09-28T00:00:00Z', releases=[V3_RC, V3, V30],
            latest='v3.1.0', commit=moved, ref='v3.1.0', ref_type='release',
        )
        con = built()
        assert rows(
            con,
            'SELECT commit_sha, ref, ref_type FROM current_scans '
            "WHERE repository_id = 4 AND source = 'syft'",
        ) == [(moved, 'v3.1.0', 'release')]

    def test_the_decisions_are_read_before_the_records(
        self, decided: Store, built: Build,
    ) -> None:
        """A record the stage-major commands filed before, with the first
        push's releases and ref: the decisions are newer."""
        decided.record(
            4, 'acme', 'decided', commit=A, ref='v3.0.0', listing='go',
            all_releases=[V30],
        )
        con = built()
        assert rows(
            con,
            'SELECT count(*) FROM releases WHERE repository_id = 4',
        ) == [(3,)]
        assert rows(
            con,
            'SELECT input_key, ref FROM scans '
            "WHERE repository_id = 4 AND source = 'syft' ORDER BY observed_at",
        ) == [(A, 'v3.0.0'), (B, 'v3.1.0')]

    def test_a_push_whose_commit_is_not_decided_yet_is_not_read_yet(
        self, decided: Store, built: Build,
    ) -> None:
        """The newest push chose a release whose commit is not decided:
        the walk has not reached a scan, and `db index` reads the record
        the last walk that did landed. So does the warehouse: the
        releases and the ref of the newest push whose commit the store
        has a scan of."""
        v4 = github_release(40, 'v4.0.0', '2026-09-28T00:00:00Z')
        decided.decide(
            4, pushed_at='2026-09-28T01:00:00Z', releases=[v4, V3_RC, V3, V30],
            latest='v4.0.0',
        )
        con = built()
        assert rows(
            con,
            'SELECT latest_release_tag, total_releases FROM repositories '
            'WHERE id = 4',
        ) == [('v3.1.0', 3)]
        assert rows(
            con,
            'SELECT input_key, ref FROM scans '
            "WHERE repository_id = 4 AND source = 'syft' ORDER BY observed_at",
        ) == [(A, 'v3.0.0'), (B, 'v3.1.0')]

    def test_a_commit_decided_and_not_scanned_yet_is_not_read_yet(
        self, decided: Store, built: Build,
    ) -> None:
        """Its commit is decided, and its scan is not in the store: the
        current scan keeps its ref, and the repository what the walk
        that scanned it decided."""
        v4 = github_release(40, 'v4.0.0', '2026-09-28T00:00:00Z')
        decided.decide(
            4, pushed_at='2026-09-28T01:00:00Z', releases=[v4, V3_RC, V3, V30],
            latest='v4.0.0', commit='c' * 40, ref='v4.0.0',
            ref_type='release',
        )
        con = built()
        assert rows(
            con,
            'SELECT latest_release_tag, total_releases FROM repositories '
            'WHERE id = 4',
        ) == [('v3.1.0', 3)]
        assert rows(
            con,
            'SELECT commit_sha, ref, ref_type FROM current_scans '
            "WHERE repository_id = 4 AND source = 'syft'",
        ) == [(B, 'v3.1.0', 'release')]

    def test_a_commit_two_keys_resolved_to_has_the_newest_ones_ref(
        self, decided: Store, built: Build,
    ) -> None:
        """The release withdrawn, the push after it took the head, which
        was the release's commit: the ref is the head's, as the record
        that walk landed says."""
        decided.decide(
            4, pushed_at='2026-09-28T01:00:00Z', releases=[V30],
            latest=None, commit=B, ref='main', ref_type='branch',
        )
        con = built()
        assert rows(
            con,
            'SELECT commit_sha, ref, ref_type FROM current_scans '
            "WHERE repository_id = 4 AND source = 'syft'",
        ) == [(B, 'main', 'branch')]

    def test_a_tag_that_is_not_utf8_is_read_as_text(
        self, decided: Store, built: Build,
    ) -> None:
        """Git keeps a tag's name as bytes, which need not be UTF-8, and
        GitPython decodes one that is not with surrogateescape. The store
        keeps it as git has it; the warehouse holds text, and has U+FFFD
        for each byte that is not UTF-8, in the releases, the repository
        and the scan's ref."""
        tag = b'v9.0\xe9'.decode('utf-8', 'surrogateescape')
        v9 = github_release(90, tag, '2026-09-27T00:00:00Z')
        tagged = 'c' * 40
        decided.sbom(
            4, tagged, artifact('cobra', '1.9.0', 'go-module'),
            at=at(2026, 9, 29),
        )
        decided.decide(
            4, pushed_at='2026-09-28T00:00:00Z',
            releases=[v9, V3_RC, V3, V30], latest=tag, commit=tagged,
            ref=tag, ref_type='release',
        )
        con = built()
        text = 'v9.0�'
        assert rows(
            con,
            'SELECT tag_name, name FROM releases '
            'WHERE repository_id = 4 AND release_id = 90',
        ) == [(text, text)]
        assert rows(
            con, 'SELECT latest_release_tag FROM repositories WHERE id = 4',
        ) == [(text,)]
        assert rows(
            con,
            'SELECT commit_sha, ref FROM current_scans '
            "WHERE repository_id = 4 AND source = 'syft'",
        ) == [(tagged, text)]

    def test_with_no_scan_of_any_decided_commit_the_newest_is_read(
        self, store: Store, built: Build,
    ) -> None:
        """A repository the walk has not scanned yet: its newest
        decisions, as they stand."""
        unscanned = Listed(5, 'acme', 'unscanned', stars=10, language='Go')
        store.snapshot(date(2026, 9, 1), unscanned)
        store.decide(
            5, pushed_at='2026-06-02T00:00:00Z', releases=[V30],
            latest='v3.0.0', commit=A, ref='v3.0.0', ref_type='release',
        )
        store.decide(
            5, pushed_at='2026-09-26T00:00:00Z', releases=[V3_RC, V3, V30],
            latest='v3.1.0',
        )
        con = built()
        assert rows(
            con,
            'SELECT latest_release_tag, total_releases FROM repositories '
            'WHERE id = 5',
        ) == [('v3.1.0', 3)]

    def test_a_list_that_cannot_be_read_is_counted_and_the_record_stands(
        self, corpus: Store, built: Build,
    ) -> None:
        corpus.decide(
            1, pushed_at='2026-09-26T00:00:00Z', releases=[V30],
            latest='v3.0.0', commit=B, ref='v3.0.0', ref_type='release',
        )
        for listing in (corpus.paths.release_dir / '1' / 'releases').iterdir():
            listing.write_text('[]')
        con = built()
        assert rows(
            con,
            'SELECT tag_name FROM releases WHERE repository_id = 1 '
            'ORDER BY tag_name',
        ) == [('v1.0.0',), ('v2.0.0-rc1',)]
        assert rows(con, 'SELECT unreadable FROM build') == [(1,)]
        # The ref is the commit decision's all the same.
        assert rows(
            con,
            'SELECT ref FROM scans WHERE repository_id = 1 '
            "AND source = 'syft' AND input_key = ?", B,
        ) == [('v3.0.0',)]


class TestEdges:

    def test_the_edges_are_db_edges_counts(
        self, corpus: Store, built: Build,
    ) -> None:
        """Counted from each repository's newest graph, by `edges_in`,
        as `db edges` counts them."""
        con = built()
        counted = collect_edges(corpus.paths.depgraph_dir)
        assert rows(
            con,
            'SELECT parent, child, repositories, observed_at FROM edges '
            'ORDER BY parent, child',
        ) == sorted(
            (
                parent, child, count,
                counted.observed_at.replace(tzinfo=None),
            )
            for (parent, child), count in counted.items()
        )
        assert counted

    def test_each_repositorys_part_is_kept_beside_them(
        self, corpus: Store, built: Build,
    ) -> None:
        """The pairs each repository's newest graph shows: what `edges`
        counts, by repository, for a pass that carries the repository
        over to count it (#187)."""
        con = built()
        assert rows(
            con,
            'SELECT repository_id, parent, child FROM graph_edges '
            'ORDER BY ALL',
        ) == [
            (1, 'com.google.guava:guava', 'com.google.code.findbugs:jsr305'),
        ]
        assert rows(
            con,
            'SELECT parent, child, count(*) FROM graph_edges GROUP BY ALL '
            'ORDER BY ALL',
        ) == rows(
            con,
            'SELECT parent, child, repositories FROM edges ORDER BY ALL',
        )


class TestCorpus:

    def test_the_corpus_is_the_newest_complete_snapshot(
        self, corpus: Store, built: Build,
    ) -> None:
        """Today's is not complete until its search leaves the marker
        (`core/catalog.py`); the one before it is."""
        corpus.snapshot(date(2026, 9, 29), APP)
        con = built(date(2026, 9, 29))
        assert rows(con, 'SELECT id FROM corpus ORDER BY id') == [
            (1,), (2,), (3,),
        ]
        assert rows(con, 'SELECT corpus FROM build') == [('all-2026-09-01',)]

    def test_todays_snapshot_is_the_corpus_once_marked_complete(
        self, corpus: Store, built: Build,
    ) -> None:
        corpus.snapshot(date(2026, 9, 29), APP, complete=True)
        con = built(date(2026, 9, 29))
        assert rows(con, 'SELECT id FROM corpus ORDER BY id') == [(1,)]
        assert rows(con, 'SELECT corpus FROM build') == [('all-2026-09-29',)]

    def test_a_repository_the_snapshot_lists_is_in_it_with_nothing_collected(
        self, corpus: Store, built: Build,
    ) -> None:
        """Seeded or not, a listed repository is in the denominators:
        the snapshot is the universe."""
        corpus.snapshot(
            date(2026, 9, 2), APP, WEB, GRAPHED,
            Listed(4, 'acme', 'bare', stars=3000, language='C++'),
        )
        con = built()
        assert rows(con, 'SELECT id FROM corpus ORDER BY id') == [
            (1,), (2,), (3,), (4,),
        ]
        assert rows(
            con,
            'SELECT owner, repo, stars, github_language FROM repositories '
            'WHERE id = 4',
        ) == [('acme', 'bare', 3000, 'C++')]

    def test_a_repository_with_no_record_has_what_github_last_said_of_it(
        self, store: Store, built: Build,
    ) -> None:
        """No `07-sbom` record: its flags are the newest `02-github-repo`
        line's, as `db index` read them from `repo-metadata` (#181)."""
        store.snapshot(
            date(2026, 9, 1),
            Listed(4, 'acme', 'retired', stars=3000, language='Go'),
        )
        store.metadata(4, 'acme', 'retired', is_archived=False)
        store.metadata(
            4, 'acme', 'retired', is_archived=True,
            is_fork=True, is_template=True, is_mirror=True,
        )
        con = built()
        assert rows(
            con,
            'SELECT is_archived, is_fork, is_template, is_mirror '
            'FROM repositories WHERE id = 4',
        ) == [(True, True, True, True)]

    def test_a_repository_with_no_record_has_the_rest_of_its_metadata(
        self, store: Store, built: Build,
    ) -> None:
        """Its description, licence, topics, dates and counts too: all
        of them came from `repo-metadata` in `db index` (#181)."""
        store.snapshot(
            date(2026, 9, 1),
            Listed(4, 'acme', 'retired', stars=3000, language='Go'),
        )
        store.metadata(
            4, 'acme', 'retired', description='A retired thing',
            topics=['go', 'web'], license_spdx_id='MIT',
            license_name='MIT License', created_at='2019-06-13T15:18:08Z',
            pushed_at='2026-02-04T01:42:42Z', fork_count=263,
            watchers_count=2900, disk_usage=14543, total_releases=7,
        )
        con = built()
        assert rows(
            con,
            'SELECT description, topics, license_spdx_id, license_name, '
            'created_at, pushed_at, fork_count, watchers_count, disk_usage, '
            'total_releases FROM repositories WHERE id = 4',
        ) == [(
            'A retired thing', ['go', 'web'], 'MIT', 'MIT License',
            datetime(2019, 6, 13, 15, 18, 8), datetime(2026, 2, 4, 1, 42, 42),
            263, 2900, 14543, 7,
        )]

    def test_what_the_list_says_is_newer_than_what_github_repo_said(
        self, store: Store, built: Build,
    ) -> None:
        """The stars and the default branch are the newest snapshot's,
        as they are over a record's (`TrackedRecords._stated`)."""
        store.snapshot(
            date(2026, 9, 1),
            Listed(
                4, 'acme', 'retired', stars=3000, language='Go',
                branch='trunk',
            ),
        )
        store.metadata(
            4, 'acme', 'retired', stars=2900, default_branch='master',
        )
        con = built()
        assert rows(
            con,
            'SELECT stars, default_branch FROM repositories WHERE id = 4',
        ) == [(3000, 'trunk')]

    def test_a_repository_an_older_snapshot_listed_keeps_its_row(
        self, store: Store, built: Build,
    ) -> None:
        """Outside the corpus, and in the list: every complete snapshot's
        repositories, as the newest to list each says, as the ledger the
        old pipeline seeded from each snapshot kept them (#171). No
        ledger is read."""
        store.snapshot(
            date(2026, 3, 1), APP,
            Listed(8, 'acme', 'dropped', stars=1100, language='Go'),
        )
        store.snapshot(
            date(2026, 9, 1),
            Listed(1, 'acme', 'app', stars=1500, language='Java'),
        )
        con = built()
        assert rows(con, 'SELECT id FROM corpus ORDER BY id') == [(1,)]
        assert rows(
            con,
            'SELECT id, repo, stars, github_language, snapshot '
            'FROM repositories ORDER BY id',
        ) == [
            (1, 'app', 1500, 'Java', 'all-2026-09-01'),
            (8, 'dropped', 1100, 'Go', 'all-2026-03-01'),
        ]

    def test_without_a_snapshot_the_corpus_is_every_repository(
        self, store: Store, built: Build,
    ) -> None:
        """As ClickHouse's corpus is while no repository names one."""
        store.record(7, 'acme', 'lone')
        con = built()
        assert rows(con, 'SELECT id FROM corpus') == [(7,)]
        assert rows(con, 'SELECT corpus FROM build') == [('',)]


class TestWhatCannotBeRead:

    def test_an_unreadable_document_is_left_out_and_counted(
        self, corpus: Store, built: Build,
    ) -> None:
        """One corrupt file costs its own scan, not the repository's
        others."""
        corpus.paths.sbom_file(2, A).write_text('{"artifacts": [', 'utf-8')
        con = built()
        assert rows(
            con,
            'SELECT input_key FROM scans WHERE repository_id = 2 '
            "AND source = 'syft'",
        ) == [(B,)]
        assert rows(con, 'SELECT unreadable FROM build') == [(1,)]
