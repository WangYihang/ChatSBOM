"""Which dependency graph is current, through `db index` itself (#22).

GitHub's dependency graph describes the default branch as it was when
the document was fetched, and the document says when: its
`creationInfo.created`. Its rows were stamped with the Syft scan's
commit instead, which is a different observation: a graph fetched again
while the Syft target stayed put was a second set of rows under the same
commit, and both were current. `forget_scans` hid that for a repository
with a Syft target, by deleting every row under the commit — the older
graph's history with it. A repository with no target was never
forgotten, so each `db index` added another copy of its graph and every
one stayed current.

These run the command against a real database, documents landed in
`raw_documents` as `db raw` lands them, and ask each reader afterwards.
`current_state_test.py` asks the same of seeded rows.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from datetime import timezone
from types import SimpleNamespace
from typing import Any

import pytest
from typer.testing import CliRunner

from chatsbom.__main__ import app
from chatsbom.core.config import ChatSBOMConfig
from chatsbom.core.config import DatabaseConfig
from chatsbom.core.config import PathConfig
from chatsbom.core.documents import DEPGRAPH
from chatsbom.core.documents import SYFT
from chatsbom.core.repository import IngestionRepository
from chatsbom.core.repository import QueryRepository
from chatsbom.export.queries import QUERIES
from chatsbom.services.db_service import DbService
from tests.conftest import CLICKHOUSE_HOST
from tests.conftest import CLICKHOUSE_PORT
from tests.conftest import requires_clickhouse
from tests.current_state_test import dependants

pytestmark = requires_clickhouse

#: The Syft target, which does not move in these tests.
COMMIT = 'c' * 40

#: When `db raw` landed each copy: the file's mtime.
LANDED_BEFORE = datetime(2026, 9, 7, 4, 0, tzinfo=timezone.utc)
LANDED = datetime(2026, 9, 14, 4, 0, tzinfo=timezone.utc)

#: `creationInfo.created` of the two graphs, a week apart. The second
#: is stated in another zone, with a fraction of a second, as a
#: document may: 11:56:20.924 at +08:00 is 03:56:20 UTC, to the second.
CREATED_BEFORE = '2026-09-07T03:56:20Z'
CREATED = '2026-09-14T11:56:20.924281+08:00'
GRAPHED = datetime(2026, 9, 14, 3, 56, 20, tzinfo=timezone.utc)


def land(
    ingest: IngestionRepository,
    kind: str,
    repository_id: int,
    path: str,
    body: dict[str, Any],
    fetched_at: datetime,
) -> None:
    """One document into `raw_documents`, as `db raw --apply` writes it."""
    text = json.dumps(body)
    ingest.client.insert(
        'raw_documents',
        [[
            kind, repository_id, path,
            hashlib.sha256(text.encode('utf-8')).hexdigest(), fetched_at,
            text,
        ]],
        column_names=[
            'kind', 'repository_id', 'path', 'sha256', 'fetched_at', 'body',
        ],
    )


def record(
    repository_id: int,
    repo: str,
    commit: str | None,
) -> dict[str, Any]:
    """A repository record as `chatsbom run` keeps it."""
    data: dict[str, Any] = {
        'id': repository_id, 'owner': 'acme', 'name': repo,
        'language': 'Ruby', 'stargazers_count': 100,
        'html_url': f'https://github.com/acme/{repo}',
        # Not `main`, so the ref a graph row names is visibly this one.
        'default_branch': 'develop',
    }
    if commit:
        data['download_target'] = {
            'ref': 'v4.3.0', 'ref_type': 'release',
            'commit_sha': commit, 'commit_sha_short': commit[:7],
        }
    return data


def spdx(created: str, *packages: tuple[str, str]) -> dict[str, Any]:
    """GitHub's answer: the repository DESCRIBED, and each package one
    it DEPENDS_ON directly."""
    root = 'SPDXRef-github-acme-app-develop'
    return {
        'sbom': {
            'SPDXID': 'SPDXRef-DOCUMENT',
            'creationInfo': {
                'created': created,
                'creators': ['Tool: GitHub.com-Dependency-Graph'],
            },
            'packages': [
                {
                    'SPDXID': f'SPDXRef-gem-{name}',
                    'name': name,
                    'versionInfo': version,
                    'externalRefs': [{
                        'referenceType': 'purl',
                        'referenceLocator': f'pkg:gem/{name}',
                    }],
                }
                for name, version in packages
            ],
            'relationships': [
                {
                    'relationshipType': 'DESCRIBES',
                    'spdxElementId': 'SPDXRef-DOCUMENT',
                    'relatedSpdxElement': root,
                },
                *(
                    {
                        'relationshipType': 'DEPENDS_ON',
                        'spdxElementId': root,
                        'relatedSpdxElement': f'SPDXRef-gem-{name}',
                    }
                    for name, _ in packages
                ),
            ],
        },
    }


def syft_sbom(*packages: tuple[str, str]) -> dict[str, Any]:
    return {
        'artifacts': [
            {
                'id': f'{name}@{version}', 'name': name, 'version': version,
                'type': 'gem', 'purl': f'pkg:gem/{name}@{version}',
                'foundBy': 'ruby-gemfile-cataloger',
            }
            for name, version in packages
        ],
    }


def land_repository(
    ingest: IngestionRepository,
    repository_id: int,
    repo: str,
    commit: str | None,
) -> None:
    """The record, and a Syft SBOM at its commit when it has one."""
    land(
        ingest, 'repo', repository_id, 'data/07-sbom/ruby.jsonl',
        record(repository_id, repo, commit), LANDED_BEFORE,
    )
    if commit:
        land(
            ingest, SYFT, repository_id,
            f'data/07-sbom/ruby/acme/{repo}/v4.3.0/{commit}/sbom.json',
            syft_sbom(('mail', '2.9.1')), LANDED_BEFORE,
        )


def land_graph(
    ingest: IngestionRepository,
    repository_id: int,
    repo: str,
    document: dict[str, Any],
    landed: datetime,
) -> None:
    """A graph, at the path `github depgraph` overwrites in place."""
    land(
        ingest, DEPGRAPH, repository_id,
        f'data/09-github-depgraph/ruby/acme/{repo}/sbom.spdx.json',
        document, landed,
    )


@pytest.fixture
def index(clickhouse_db, tmp_path, monkeypatch):
    """`chatsbom db index --language ruby`, run against the test database.

    The command as written, with only its container swapped: the same
    forgets, the same ingest, OPTIMIZE, the dictionary reload and the
    rollup refresh.
    """
    config = ChatSBOMConfig(
        paths=PathConfig(base_data_dir=tmp_path / 'data'),
        _db_base=DatabaseConfig(
            host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT,
            database=clickhouse_db,
        ),
    )
    container = SimpleNamespace(
        config=config,
        get_db_service=DbService,
        get_ingestion_repository=lambda: IngestionRepository(
            config.get_db_config('admin'),
        ),
    )
    monkeypatch.setattr(
        'chatsbom.commands.db.index.get_container', lambda: container,
    )
    monkeypatch.setattr(
        'chatsbom.commands.db.index.check_clickhouse_connection',
        lambda **_: None,
    )

    def run() -> None:
        result = CliRunner().invoke(
            app, ['db', 'index', '--language', 'ruby'],
        )
        assert result.exit_code == 0, result.output

    return run


def rows_of(query: QueryRepository, sql: str) -> list[tuple[Any, ...]]:
    return sorted(tuple(row) for row in query.client.query(sql).result_rows)


def current(query: QueryRepository, repository_id: int) -> list[tuple]:
    return rows_of(
        query,
        'SELECT name, version, source FROM current_artifacts '
        f'WHERE repository_id = {repository_id}',
    )


class TestAGraphFetchedAgain:

    def test_the_newer_graph_replaces_the_older_at_the_same_scan(
        self, ingest, query, index,
    ):
        """The Syft target did not move and the graph was fetched again:
        `sidekiq` left the Gemfile in between and `rails` moved on.
        Every reader has the newer document alone, and history keeps
        the older one, which the forget keyed on the commit deleted."""
        land_repository(ingest, 1, 'app', COMMIT)
        land_graph(
            ingest, 1, 'app',
            spdx(CREATED_BEFORE, ('rails', '~> 7.0'), ('sidekiq', '~> 7.0')),
            LANDED_BEFORE,
        )
        index()
        land_graph(
            ingest, 1, 'app', spdx(CREATED, ('rails', '~> 7.1')), LANDED,
        )
        index()

        assert current(query, 1) == [
            ('mail', '2.9.1', 'syft'),
            ('rails', '~> 7.1', DEPGRAPH),
        ]
        # The CLI, the rollups, the dashboard and the export.
        assert query.get_dependents('sidekiq') == []
        assert {d.version for d in query.get_dependents('rails')} == {
            '~> 7.1',
        }
        assert ('sidekiq',) not in rows_of(
            query, 'SELECT name FROM mv_packages',
        )
        assert dependants(query, 'sidekiq') == []
        assert dependants(query, 'rails') == [(1, '~> 7.1')]
        assert 'sidekiq' not in {
            row['name'] for row in query.stream_rows(QUERIES['artifacts'])
        }
        # History is what the table is append-only for.
        assert rows_of(
            query,
            "SELECT name, month FROM mv_package_month WHERE name = 'sidekiq'",
        ) == [('sidekiq', '2026-09')]

    def test_a_repository_with_no_commit_keeps_one_graph(
        self, ingest, query, index,
    ):
        """No Syft target, so nothing was ever forgotten for it, and
        both graphs were current: the issue in full."""
        land_repository(ingest, 2, 'graph-only', None)
        land_graph(
            ingest, 2, 'graph-only', spdx(CREATED_BEFORE, ('puma', '6.0')),
            LANDED_BEFORE,
        )
        index()
        land_graph(
            ingest, 2, 'graph-only', spdx(CREATED, ('rack', '3.1')), LANDED,
        )
        index()

        assert current(query, 2) == [('rack', '3.1', DEPGRAPH)]
        assert query.get_dependent_count('puma') == 0
        assert dependants(query, 'puma') == []
        assert rows_of(query, 'SELECT name FROM mv_packages') == [('rack',)]


class TestIndexingAgain:

    def test_the_same_documents_twice_write_nothing_new(
        self, ingest, query, index,
    ):
        """With and without a Syft target. The one without had its
        graph appended again by every pass."""
        land_repository(ingest, 1, 'app', COMMIT)
        land_repository(ingest, 2, 'graph-only', None)
        for repository_id, repo in ((1, 'app'), (2, 'graph-only')):
            land_graph(
                ingest, repository_id, repo,
                spdx(CREATED, ('rails', '~> 7.1')), LANDED,
            )
        index()
        once = rows_of(query, 'SELECT * EXCEPT (updated_at) FROM artifacts')
        index()

        assert rows_of(
            query, 'SELECT * EXCEPT (updated_at) FROM artifacts',
        ) == once
        assert current(query, 1) == [
            ('mail', '2.9.1', 'syft'), ('rails', '~> 7.1', DEPGRAPH),
        ]
        assert current(query, 2) == [('rails', '~> 7.1', DEPGRAPH)]


class TestTheIdentity:

    def test_a_graph_row_and_its_repository_record_one_instant(
        self, ingest, query, index,
    ):
        """Written by one function, into two `DateTime` columns: an
        offset and a fraction in the document must not make the two
        disagree, and neither may land eight hours out."""
        land_repository(ingest, 1, 'app', COMMIT)
        land_graph(
            ingest, 1, 'app', spdx(CREATED, ('rails', '~> 7.1')), LANDED,
        )
        index()

        expected = int(GRAPHED.timestamp())
        assert rows_of(
            query,
            'SELECT toUnixTimestamp(depgraph_observed_at) '
            'FROM repositories FINAL WHERE id = 1',
        ) == [(expected,)]
        assert rows_of(
            query,
            'SELECT DISTINCT toUnixTimestamp(observed_at) FROM artifacts '
            f"WHERE source = '{DEPGRAPH}'",
        ) == [(expected,)]

    def test_a_graph_row_names_the_branch_it_describes(
        self, ingest, query, index,
    ):
        """GitHub builds the graph from the default branch, not from the
        release the Syft scan read, so the row names that branch."""
        land_repository(ingest, 1, 'app', COMMIT)
        land_graph(
            ingest, 1, 'app', spdx(CREATED, ('rails', '~> 7.1')), LANDED,
        )
        index()

        assert rows_of(
            query, 'SELECT DISTINCT source, sbom_ref FROM artifacts',
        ) == [(DEPGRAPH, 'develop'), ('syft', 'v4.3.0')]


class TestTheSbomIsTheScansOwn:
    """`RawDocuments.get` took the newest SBOM a repository ever landed.

    A document landed for an earlier commit, after the one for this
    commit, was then read as this scan and stamped with its commit. The
    landed path names the commit it was generated at
    (`.../<ref>/<sha>/sbom.json`), as #21 used for the manifests.
    """

    def test_the_sbom_read_is_the_one_for_the_records_commit(
        self, ingest, query, index,
    ):
        land_repository(ingest, 1, 'app', COMMIT)
        # January's SBOM, landed again after this commit's.
        land(
            ingest, SYFT, 1,
            f'data/07-sbom/ruby/acme/app/v4.2.0/{"a" * 40}/sbom.json',
            syft_sbom(('mail', '2.7.1'), ('left-pad', '1.3.0')), LANDED,
        )
        index()

        assert current(query, 1) == [('mail', '2.9.1', 'syft')]

    def test_a_record_with_no_commit_reads_the_newest_as_before(
        self, ingest, query, index,
    ):
        """No scan to narrow to."""
        land_repository(ingest, 2, 'graph-only', None)
        land(
            ingest, SYFT, 2,
            f'data/07-sbom/ruby/acme/graph-only/main/{"a" * 40}/sbom.json',
            syft_sbom(('mail', '2.7.1')), LANDED_BEFORE,
        )
        land(
            ingest, SYFT, 2,
            f'data/07-sbom/ruby/acme/graph-only/main/{"b" * 40}/sbom.json',
            syft_sbom(('mail', '2.9.1')), LANDED,
        )
        index()

        assert current(query, 2) == [('mail', '2.9.1', 'syft')]
