"""Where the dependency-graph stage keeps what it fetched, and how every
reader finds it.

Each fetch is kept for good under the repository's id, stamped with the
default branch and HEAD it was fetched at. The legacy one-file-per-
repository documents stay readable where they are.
"""
from __future__ import annotations

import json
from datetime import datetime
from datetime import timezone
from pathlib import Path

from chatsbom.core import depgraph_store
from chatsbom.core.documents import DEPGRAPH
from chatsbom.core.documents import FILES
from chatsbom.core.documents import RawDocuments
from chatsbom.core.documents import SYFT
from chatsbom.core.edges import collect_edges
from chatsbom.services.db_service import _graph_path
from chatsbom.services.db_service import DbService
from chatsbom.services.git_service import parse_symref_head

SHA = '0123456789abcdef0123456789abcdef01234567'
OTHER = 'f' * 40
WHEN = datetime(2026, 9, 28, 12, 30, 5, tzinfo=timezone.utc)
LATER = datetime(2026, 10, 28, 8, 0, 0, tzinfo=timezone.utc)


def _graph(created: str = '2026-09-28T12:30:00Z', *packages) -> dict:
    return {
        'sbom': {
            'spdxVersion': 'SPDX-2.3',
            'creationInfo': {'created': created},
            'packages': list(packages),
        },
    }


def _store(root: Path, payload=None, when=WHEN, sha=SHA, **kwargs):
    return depgraph_store.store(
        root,
        repository_id=kwargs.pop('repository_id', 42),
        owner=kwargs.pop('owner', 'halo-dev'),
        repo=kwargs.pop('repo', 'halo'),
        payload=payload if payload is not None else _graph(),
        fetched_at=when,
        ref=kwargs.pop('ref', 'main'),
        head_sha=sha,
        http_status=200,
    )


# --- the layout -------------------------------------------------------------------

def test_a_fetch_is_its_own_directory_under_the_repository_id(tmp_path):
    stored = _store(tmp_path)

    assert stored.written
    document = stored.fetch.document
    assert document == tmp_path / '42' / \
        f'20260928T123005Z-{SHA}' / 'sbom.spdx.json'
    assert json.loads(document.read_text()) == _graph()
    meta = json.loads((document.parent / 'meta.json').read_text())
    assert meta == {
        'repository_id': 42,
        'owner': 'halo-dev',
        'repo': 'halo',
        'ref': 'main',
        'commit_sha': SHA,
        'fetched_at': '2026-09-28T12:30:05+00:00',
        'http_status': 200,
        'sha256': stored.fetch.sha256,
    }


def test_the_stamp_reads_back_from_the_path_and_the_meta(tmp_path):
    document = _store(tmp_path).fetch.document

    assert depgraph_store.stamp_of_path(document) == ('main', SHA)
    assert depgraph_store.stamp_of(document.parent.name) == (WHEN, SHA)


def test_a_legacy_document_has_no_stamp(tmp_path):
    legacy = tmp_path / 'java' / 'o' / 'r' / 'sbom.spdx.json'

    assert depgraph_store.stamp_of_path(legacy) == ('', '')
    assert depgraph_store.stamp_of_path(None) == ('', '')


def test_an_unknown_head_is_named_so(tmp_path):
    stored = _store(tmp_path, sha='')

    assert stored.fetch.directory.name == '20260928T123005Z-unknown'
    assert stored.fetch.commit_sha == ''
    assert depgraph_store.stamp_of_path(stored.fetch.document) == ('main', '')


def test_a_language_directory_is_never_a_repository_directory():
    assert depgraph_store.stamp_of('sbom.spdx.json') is None
    assert not 'java'.isdigit()


# --- never overwritten ----------------------------------------------------------------

def test_a_later_fetch_is_kept_beside_the_earlier(tmp_path):
    first = _store(tmp_path)
    second = _store(tmp_path, _graph('2026-10-28T08:00:00Z'), LATER, OTHER)

    kept = depgraph_store.fetches(tmp_path, 42)
    assert [fetch.document for fetch in kept] == [
        first.fetch.document, second.fetch.document,
    ]
    assert depgraph_store.newest(tmp_path, 42) == second.fetch
    assert json.loads(first.fetch.document.read_text()) == _graph()


def test_the_same_second_twice_takes_the_next_one(tmp_path):
    first = _store(tmp_path)
    second = _store(tmp_path, _graph('2026-09-28T12:30:06Z'))

    assert first.fetch.directory != second.fetch.directory
    assert second.fetch.directory.name.startswith('20260928T123006Z-')


def test_an_identical_document_is_not_kept_twice(tmp_path):
    first = _store(tmp_path)

    again = _store(tmp_path, when=LATER, sha=OTHER)

    assert not again.written
    assert again.fetch == first.fetch
    assert len(depgraph_store.fetches(tmp_path, 42)) == 1


def test_a_document_cut_short_is_not_a_fetch(tmp_path):
    stored = _store(tmp_path)
    stored.fetch.document.write_text('{"sbom": {')

    assert depgraph_store.fetches(tmp_path, 42) == []


# --- the index, and what reads it -------------------------------------------------------

def test_every_fetch_is_logged_and_the_newest_wins(tmp_path):
    _store(tmp_path)
    second = _store(tmp_path, _graph('x'), LATER, OTHER)
    _store(tmp_path, repository_id=7, repo='other')

    lines = (tmp_path / 'index.jsonl').read_text().splitlines()
    assert len(lines) == 3
    assert depgraph_store.newest_paths(tmp_path)[42] == str(
        second.fetch.document,
    )
    assert set(depgraph_store.newest_paths(tmp_path)) == {42, 7}


def test_a_logged_document_that_is_gone_is_left_out(tmp_path):
    stored = _store(tmp_path)
    stored.fetch.document.unlink()

    assert depgraph_store.newest_paths(tmp_path) == {}


def test_db_index_prefers_the_newest_fetch_over_the_legacy_document(
    tmp_path,
):
    """The record still names the legacy document it was written with."""
    record = {'id': 42, 'depgraph_path': 'legacy/sbom.spdx.json'}
    fetched = {42: 'new/sbom.spdx.json'}
    moved = tmp_path / '9' / 'legacy' / 'sbom.spdx.json'
    moved.parent.mkdir(parents=True)
    moved.write_text('{}')

    assert _graph_path(record, 42, fetched, tmp_path) == 'new/sbom.spdx.json'
    assert _graph_path(record, 42, {}, tmp_path) == 'legacy/sbom.spdx.json'
    # With neither, the legacy graph `data migrate-layout` moved under
    # the repository's id.
    assert _graph_path({'id': 9}, 9, fetched, tmp_path) == str(moved)
    assert _graph_path({'id': 8}, 8, fetched, tmp_path) is None


# --- the stamp reaches the rows -------------------------------------------------------------

REPO_ROW = {'default_branch': 'master', 'sbom_commit_sha': 'syft' * 10}


def test_a_kept_graph_is_read_with_its_own_stamp_from_disk(tmp_path):
    document = _store(tmp_path).fetch.document

    read = FILES.get(DEPGRAPH, 42, str(document))

    assert read is not None
    assert (read.ref, read.commit_sha) == ('main', SHA)


def test_a_syft_document_is_never_stamped_so(tmp_path):
    document = _store(tmp_path).fetch.document

    read = FILES.get(SYFT, 42, str(document))

    assert read is not None and (read.ref, read.commit_sha) == ('', '')


class FakeClient:
    def __init__(self, path: str, body: dict) -> None:
        self._row = (json.dumps(body), WHEN.replace(tzinfo=None), path)

    def query(self, sql, parameters):
        assert 'path' in sql
        return type('Result', (), {'result_rows': [self._row]})()


def test_a_landed_graph_keeps_its_commit_from_the_landed_path():
    path = f'/data/09-github-depgraph/42/20260928T123005Z-{SHA}/sbom.spdx.json'

    read = RawDocuments(FakeClient(path, _graph())).get(DEPGRAPH, 42)

    assert read is not None and read.commit_sha == SHA


def test_a_landed_legacy_graph_has_no_commit():
    path = '/data/09-github-depgraph/java/o/r/sbom.spdx.json'

    read = RawDocuments(FakeClient(path, _graph())).get(DEPGRAPH, 42)

    assert read is not None and read.commit_sha == ''


def test_graph_rows_carry_the_graphs_own_ref_and_commit(tmp_path):
    """Not the Syft scan's: the graph describes the default branch when
    it was fetched."""
    document = _store(
        tmp_path,
        _graph(
            '2026-09-28T12:30:00Z',
            {
                'SPDXID': 'SPDXRef-x', 'name': 'x', 'versionInfo': '1.0',
                'externalRefs': [{
                    'referenceType': 'purl',
                    'referenceLocator': 'pkg:maven/g/x@1.0',
                }],
            },
        ),
    ).fetch.document
    read = FILES.get(DEPGRAPH, 42, str(document))
    assert read is not None

    [row] = DbService().parse_dependency_graph(read, 42, REPO_ROW)

    assert (row['sbom_ref'], row['sbom_commit_sha']) == ('main', SHA)


def test_a_legacy_graphs_rows_keep_the_old_stamp(tmp_path):
    legacy = tmp_path / 'java' / 'o' / 'r' / 'sbom.spdx.json'
    legacy.parent.mkdir(parents=True)
    legacy.write_text(
        json.dumps(
            _graph(
                '2026-01-01T00:00:00Z',
                {
                    'SPDXID': 'SPDXRef-x', 'name': 'x',
                    'externalRefs': [{
                        'referenceType': 'purl', 'referenceLocator': 'pkg:npm/x',
                    }],
                },
            ),
        ),
    )
    read = FILES.get(DEPGRAPH, 42, str(legacy))
    assert read is not None

    [row] = DbService().parse_dependency_graph(read, 42, REPO_ROW)

    assert (row['sbom_ref'], row['sbom_commit_sha']) == (
        'master', REPO_ROW['sbom_commit_sha'],
    )


# --- `db edges` reads one document per repository ------------------------------------------

def _edge_graph(created: str, parent: str, child: str) -> dict:
    return {
        'sbom': {
            'creationInfo': {'created': created},
            'packages': [
                {'SPDXID': 'p', 'name': parent},
                {'SPDXID': 'c', 'name': child},
            ],
            'relationships': [{
                'spdxElementId': 'p', 'relationshipType': 'DEPENDS_ON',
                'relatedSpdxElement': 'c',
            }],
        },
    }


def test_edges_count_each_repository_once_whatever_it_has_kept(tmp_path):
    """Every fetch is kept, and each has a `meta.json` beside it: walked
    as `*.json`, a repository fetched twice was two repositories, and
    each meta file a third document."""
    _store(tmp_path, _edge_graph('2026-09-01T00:00:00Z', 'a', 'b'))
    _store(tmp_path, _edge_graph('2026-10-01T00:00:00Z', 'a', 'b'), LATER, OTHER)
    # The same repository's legacy document, and another repository's.
    for repo, edge in (('halo', ('x', 'y')), ('other', ('a', 'b'))):
        legacy = tmp_path / 'java' / 'halo-dev' / repo / 'sbom.spdx.json'
        legacy.parent.mkdir(parents=True)
        legacy.write_text(
            json.dumps(
                _edge_graph('2026-01-01T00:00:00Z', *edge),
            ),
        )

    counts = collect_edges(tmp_path)

    assert counts.documents == 2
    assert counts[('a', 'b')] == 2
    assert ('x', 'y') not in counts, 'the legacy copy of a kept repository'


def test_current_documents_are_the_newest_fetch_and_uncovered_legacy(tmp_path):
    _store(tmp_path)
    newest = _store(tmp_path, _graph('x'), LATER, OTHER).fetch.document
    legacy = tmp_path / 'go' / 'o' / 'r' / 'sbom.spdx.json'
    legacy.parent.mkdir(parents=True)
    legacy.write_text('{}')

    assert list(depgraph_store.current_documents(tmp_path)) == [
        newest, legacy,
    ]


# --- the stamp's source ------------------------------------------------------------------------

def test_the_head_is_read_from_ls_remote_symref():
    output = f'ref: refs/heads/trunk\tHEAD\n{SHA}\tHEAD\n'

    assert parse_symref_head(output) == ('trunk', SHA)


def test_a_head_without_a_commit_is_unknown():
    assert parse_symref_head('') is None
    assert parse_symref_head('ref: refs/heads/main\tHEAD\n') is None


def test_a_detached_head_has_its_commit_and_no_branch():
    assert parse_symref_head(f'{SHA}\tHEAD\n') == ('', SHA)
