"""`db index` masters on the ledger (#55 §4.11).

It mastered on the records a finished walk files: a repository without
one never got a `repositories` row, so the ~28 k repositories a search
snapshot seeded had their dependency graphs fetched and landed and
never indexed, and every coverage ratio was measured against the
repositories that had already succeeded. Records of repositories
tracked with no language were filed under `07-sbom/index.jsonl`, which
no `--language` pass read.

These run the command against a real database, with a ledger, and
documents landed in `raw_documents` as `db raw` lands them.
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime
from datetime import timezone
from pathlib import Path
from typing import Any

import pytest

from chatsbom.core.documents import TrackedRecords
from chatsbom.core.ledger import Ledger
from chatsbom.core.ledger import resolve_names
from chatsbom.core.ledger import tracked_repositories
from chatsbom.core.repository import IngestionRepository
from chatsbom.core.repository import QueryRepository
from tests.conftest import requires_clickhouse

COMMIT = 'c' * 40
HEAD = 'd' * 40
LANDED = datetime(2026, 9, 20, 4, 0, tzinfo=timezone.utc)

BUILD = """
plugins { id 'org.springframework.boot' version '3.5.9' }
dependencies {
    api 'org.springframework.boot:spring-boot-starter-web'
}
"""


def land(
    ingest: IngestionRepository,
    kind: str,
    repository_id: int,
    path: str,
    body: Any,
) -> None:
    text = body if isinstance(body, str) else json.dumps(body)
    ingest.client.insert(
        'raw_documents',
        [[
            kind, repository_id, path,
            hashlib.sha256(text.encode('utf-8')).hexdigest(), LANDED, text,
        ]],
        column_names=[
            'kind', 'repository_id', 'path', 'sha256', 'fetched_at', 'body',
        ],
    )


def record(repository_id: int, name: str) -> dict[str, Any]:
    return {
        'id': repository_id, 'owner': 'acme', 'name': name,
        'language': 'Java', 'stargazers_count': 100,
        'default_branch': 'main',
        'download_target': {
            'ref': 'v1.0.0', 'ref_type': 'release',
            'commit_sha': COMMIT, 'commit_sha_short': COMMIT[:7],
        },
    }


def graph() -> dict[str, Any]:
    return {
        'sbom': {
            'creationInfo': {'created': '2026-09-27T10:00:00Z'},
            'packages': [{
                'SPDXID': 'SPDXRef-maven-web',
                'name': 'org.springframework.boot:spring-boot-starter-web',
                'versionInfo': '3.5.9',
                'externalRefs': [{
                    'referenceType': 'purl',
                    'referenceLocator': (
                        'pkg:maven/org.springframework.boot/'
                        'spring-boot-starter-web@3.5.9'
                    ),
                }],
            }],
            'relationships': [],
        },
    }


def ledger(path: Path) -> None:
    with Ledger(path) as kept:
        # Collected, with a record, under the Java list.
        kept.track(1, 'acme', 'scanned', 'java')
        # Seeded from a snapshot: no record, a graph and its metadata.
        kept.seed(
            2, 'acme', 'graphed', snapshot='all-2026-03-09',
            github_language='Kotlin', stars=5000, default_branch='trunk',
        )
        # Seeded, and nothing collected at all yet.
        kept.seed(
            3, 'acme', 'bare', snapshot='all-2026-03-09',
            github_language='C++', stars=1200, default_branch='master',
        )
        # Tracked with no language: its record is in `07-sbom/index.jsonl`.
        kept.seed(4, 'acme', 'unlisted', snapshot='all-2026-03-09')


@pytest.fixture
def corpus(ingest: IngestionRepository, db_command: Any) -> Path:
    paths = db_command.container.config.paths
    ledger(paths.ledger_path)
    land(ingest, 'repo', 1, 'data/07-sbom/java.jsonl', record(1, 'scanned'))
    land(
        ingest, 'syft', 1, f'07-sbom/1/{COMMIT}/sbom.json',
        {'artifacts': [{'name': 'jackson-core', 'type': 'java-archive'}]},
    )
    land(
        ingest, 'content', 1,
        f'06-github-content/1/{COMMIT}/app/build.gradle', BUILD,
    )
    land(
        ingest, 'repo-metadata', 2, 'data/02-github-repo/index.jsonl',
        {
            'id': 2, 'name': 'graphed', 'owner': {'login': 'acme'},
            'stargazers_count': 5100, 'language': 'Kotlin',
            'description': 'from the repository resource',
        },
    )
    land(
        ingest, 'github-depgraph', 2,
        f'09-github-depgraph/2/20260927T100000Z-{HEAD}/sbom.spdx.json',
        graph(),
    )
    land(ingest, 'repo', 4, 'data/07-sbom/index.jsonl', record(4, 'unlisted'))
    land(
        ingest, 'syft', 4, f'07-sbom/4/{COMMIT}/sbom.json',
        {'artifacts': [{'name': 'left-pad', 'type': 'npm'}]},
    )
    return paths.ledger_path


def rows(query: QueryRepository, sql: str) -> list[tuple[Any, ...]]:
    return sorted(tuple(r) for r in query.client.query(sql).result_rows)


@requires_clickhouse
class TestEveryTrackedRepositoryIsIndexed:

    def test_with_or_without_a_record(self, corpus, query, db_command):
        db_command('index')
        assert rows(
            query,
            'SELECT id, repo, stars, default_branch, github_language '
            'FROM repositories FINAL',
        ) == [
            # No snapshot said: the record's.
            (1, 'scanned', 100, 'main', 'Java'),
            # The repository resource, fresher than the snapshot.
            (2, 'graphed', 5100, 'trunk', 'Kotlin'),
            (3, 'bare', 1200, 'master', 'C++'),
            (4, 'unlisted', 100, 'main', 'Java'),
        ]

    def test_a_graph_without_a_scan_is_current(self, corpus, query, db_command):
        db_command('index')
        assert rows(
            query,
            'SELECT repository_id, source, name, sbom_ref, sbom_commit_sha '
            'FROM current_artifacts ORDER BY repository_id',
        ) == [
            (1, 'manifest', 'spring-boot-starter-web', 'v1.0.0', COMMIT),
            (1, 'syft', 'jackson-core', 'v1.0.0', COMMIT),
            (
                2, 'github-depgraph',
                'org.springframework.boot:spring-boot-starter-web',
                'trunk', HEAD,
            ),
            (4, 'syft', 'left-pad', 'v1.0.0', COMMIT),
        ]

    def test_the_repository_records_the_graphs_stamp(
        self, corpus, query, db_command,
    ):
        db_command('index')
        assert rows(
            query,
            'SELECT id, depgraph_ref, depgraph_commit_sha, ecosystems '
            'FROM repositories FINAL',
        ) == [
            (1, '', '', ['maven']),
            (2, 'trunk', HEAD, ['maven']),
            (3, '', '', []),
            (4, '', '', ['npm']),
        ]

    def test_indexing_again_adds_nothing(self, corpus, query, db_command):
        db_command('index')
        once = rows(
            query, 'SELECT source, count() FROM artifacts GROUP BY source',
        )
        db_command('index')
        query.client.command('OPTIMIZE TABLE artifacts FINAL')
        assert rows(
            query, 'SELECT source, count() FROM artifacts GROUP BY source',
        ) == once == [('github-depgraph', 1), ('manifest', 1), ('syft', 2)]

    def test_a_repos_file_narrows_to_those(
        self, corpus, query, db_command, tmp_path,
    ):
        wanted = tmp_path / 'repos.txt'
        wanted.write_text('acme/graphed\n# a comment\nnobody/else\n')
        result = db_command('index', '--repos-file', str(wanted))
        assert 'nobody/else' in result.output
        assert rows(query, 'SELECT id FROM repositories FINAL') == [(2,)]

    def test_the_ledger_is_not_written(self, corpus, db_command):
        before = corpus.read_bytes()
        db_command('index')
        assert corpus.read_bytes() == before


# --- the pieces, without a database ----------------------------------------

class Records:
    def __init__(self, *records: dict[str, Any]) -> None:
        self._records = records

    def records(self, limit: int | None = None):
        yield from self._records[:limit]


def test_tracked_records_fill_in_what_has_no_record(tmp_path):
    ledger(tmp_path / 'ledger.sqlite3')
    tracked = tracked_repositories(tmp_path / 'ledger.sqlite3')
    assert tracked is not None and sorted(tracked) == [1, 2, 3, 4]

    source = TrackedRecords(
        Records(record(1, 'scanned')), tracked,
        metadata=lambda ids: {2: {'id': 2, 'stargazers_count': 5100}},
    )
    found = {r['id']: r for r in source.records()}
    assert sorted(found) == [1, 2, 3, 4]
    assert 'download_target' in found[1]
    assert found[2]['stargazers_count'] == 5100
    assert found[3] == {
        'id': 3, 'owner': 'acme', 'name': 'bare',
        'html_url': 'https://github.com/acme/bare',
        'stargazers_count': 1200, 'default_branch': 'master',
        'language': 'C++', 'github_language': 'C++',
    }


def test_a_record_the_ledger_does_not_track_is_kept(tmp_path):
    """Dropping it would delete a repository from the dataset because
    the ledger was never seeded with it."""
    source = TrackedRecords(Records(record(9, 'old')), {})
    assert [r['id'] for r in source.records()] == [9]


def test_a_limit_counts_both_kinds(tmp_path):
    ledger(tmp_path / 'ledger.sqlite3')
    tracked = tracked_repositories(tmp_path / 'ledger.sqlite3')
    source = TrackedRecords(Records(record(1, 'scanned')), tracked)
    assert [r['id'] for r in source.records(2)] == [1, 2]
    assert [r['id'] for r in source.records(2)] == [1, 2], 'the same twice'


def test_names_resolve_against_the_ledger(tmp_path):
    ledger(tmp_path / 'ledger.sqlite3')
    tracked = tracked_repositories(tmp_path / 'ledger.sqlite3')
    assert tracked is not None
    assert resolve_names(tracked, ['ACME/Bare', '4', 'x/y', '', '# c']) == (
        {3, 4}, ['x/y'],
    )


def test_no_ledger_is_none(tmp_path):
    assert tracked_repositories(tmp_path / 'absent.sqlite3') is None
    assert not (tmp_path / 'absent.sqlite3').exists()


def test_an_older_ledger_reads_with_empty_snapshot_columns(tmp_path):
    import sqlite3

    path = tmp_path / 'old.sqlite3'
    db = sqlite3.connect(path)
    db.execute(
        'CREATE TABLE repository_state ('
        'repository_id INTEGER PRIMARY KEY, owner TEXT, repo TEXT)',
    )
    db.execute("INSERT INTO repository_state VALUES (7, 'o', 'r')")
    db.commit()
    db.close()

    tracked = tracked_repositories(path)
    assert tracked is not None
    assert tracked[7].github_language == '' and tracked[7].stars is None
