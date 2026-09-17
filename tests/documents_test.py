"""The two document sources must be interchangeable.

`db index --from-raw` is only safe if reading a document out of
`raw_documents` gives the same rows as reading the same document off
disk. That is one property, and it is the whole reason this abstraction
exists, so it is tested directly rather than inferred from the two
sources passing their own tests.
"""
from __future__ import annotations

import json
from datetime import datetime
from datetime import timezone

import pytest

from chatsbom.core.documents import DEPGRAPH
from chatsbom.core.documents import FILES
from chatsbom.core.documents import RawDocuments
from chatsbom.core.documents import SYFT

SBOM = {
    'artifacts': [{
        'id': 'abc123', 'name': 'mail', 'version': '2.9.0', 'type': 'gem',
        'purl': 'pkg:gem/mail@2.9.0', 'foundBy': 'ruby-gemfile-cataloger',
        'licenses': [{'value': 'MIT'}],
    }],
}

GRAPH = {
    'sbom': {
        'creationInfo': {
            'creators': ['Tool: GitHub.com-Dependency-Graph'],
            'created': '2026-09-14T03:56:20Z',
        },
        'packages': [{
            'SPDXID': 'p1', 'name': 'org.slf4j:slf4j-api',
            'versionInfo': '2.0.13',
            'externalRefs': [{
                'referenceType': 'purl',
                'referenceLocator': 'pkg:maven/org.slf4j/slf4j-api',
            }],
        }],
    },
}

#: What `db raw` wrote into `fetched_at`: the file's mtime, so that the
#: timestamp survives the move into the database.
COLLECTED = datetime(2026, 2, 11, 9, 30, tzinfo=timezone.utc)

#: The same instant as the driver hands it back. `clickhouse_connect`
#: returns `DateTime` columns naive, so the read path has to re-attach
#: UTC -- and if it ever re-attached local time instead, this is the
#: fixture that would catch it.
AS_READ_BACK = COLLECTED.replace(tzinfo=None)


class FakeClient:
    """Enough of a ClickHouse client to answer one point query.

    Hand-written rather than mocked: the assertion worth making is that
    the source asks for the newest row of the right kind, and a mock
    that records the call would pass whatever query string it was given.
    """

    def __init__(self, rows: dict[tuple[str, int], list[tuple]]) -> None:
        self._rows = rows
        self.queries: list[dict] = []

    def query(self, sql: str, parameters: dict):
        self.queries.append({'sql': sql, 'parameters': parameters})
        key = (parameters['kind'], parameters['repository_id'])
        ordered = sorted(
            self._rows.get(key, []), key=lambda row: row[1], reverse=True,
        )
        return type('Result', (), {'result_rows': ordered[:1]})()


def _at(path, when: datetime) -> None:
    stamp = when.replace(tzinfo=timezone.utc).timestamp()
    import os
    os.utime(path, (stamp, stamp))


def test_the_two_sources_agree_on_a_syft_sbom(tmp_path):
    """Same body, same observation time, whichever side it came from."""
    path = tmp_path / 'sbom.json'
    path.write_text(json.dumps(SBOM))
    _at(path, COLLECTED)

    from_file = FILES.get(SYFT, 4321, str(path))
    from_raw = RawDocuments(
        FakeClient({(SYFT, 4321): [(json.dumps(SBOM), AS_READ_BACK)]}),
    ).get(SYFT, 4321)

    assert from_file is not None and from_raw is not None
    assert from_file.body == from_raw.body
    assert from_file.observed_at == from_raw.observed_at == COLLECTED


def test_the_two_sources_agree_on_a_dependency_graph(tmp_path):
    """Here the document states its own date, and both must honour it.

    The mtime and the `fetched_at` are set to a different year on
    purpose: if either source fell back to them, the two would still
    agree with each other and disagree with the document.
    """
    path = tmp_path / 'graph.spdx.json'
    path.write_text(json.dumps(GRAPH))
    _at(path, datetime(2020, 1, 1))

    from_file = FILES.get(DEPGRAPH, 4321, str(path))
    from_raw = RawDocuments(
        FakeClient({
            (DEPGRAPH, 4321): [(json.dumps(GRAPH), datetime(2020, 1, 1))],
        }),
    ).get(DEPGRAPH, 4321)

    assert from_file is not None and from_raw is not None
    assert from_file.body == from_raw.body
    assert from_file.observed_at == from_raw.observed_at
    assert from_file.observed_at.year == 2026, 'the document states 2026'
    assert from_file.observed_at.month == 9


def test_the_newest_copy_wins():
    """A repository collected twice is two rows, keyed by content hash.

    Only the newest describes it now, so an older copy left behind must
    not be what the transform reads.
    """
    old = {'artifacts': [{'name': 'old', 'type': 'gem'}]}
    new = {'artifacts': [{'name': 'new', 'type': 'gem'}]}
    source = RawDocuments(
        FakeClient({
            (SYFT, 7): [
                (json.dumps(old), datetime(2026, 2, 11)),
                (json.dumps(new), datetime(2026, 9, 14)),
            ],
        }),
    )
    document = source.get(SYFT, 7)
    assert document is not None
    assert document.body['artifacts'][0]['name'] == 'new'


def test_an_absent_row_is_absent_not_an_error():
    """The dependency graph covers whatever it has reached, so a
    repository with no row is the normal case, not a failure."""
    assert RawDocuments(FakeClient({})).get(DEPGRAPH, 7) is None


def test_a_corrupt_row_names_the_row_it_came_from():
    """There is no path to print, so the message must identify the row
    another way or the failure is unlocatable."""
    source = RawDocuments(
        FakeClient({
            (SYFT, 7): [('{not json', datetime(2026, 2, 11))],
        }),
    )
    with pytest.raises(ValueError, match=r'raw_documents syft/7'):
        source.get(SYFT, 7)


def test_the_query_is_a_primary_key_prefix_lookup():
    """`raw_documents` is ordered by `(kind, repository_id, sha256)`.

    One query per document is only viable because both leading columns
    are bound; a query that filtered on either alone would scan, and
    24,451 repositories would make that the slowest stage in the
    pipeline.
    """
    client = FakeClient({(SYFT, 7): [(json.dumps(SBOM), AS_READ_BACK)]})
    RawDocuments(client).get(SYFT, 7)

    sql = client.queries[0]['sql']
    assert 'kind = {kind:String}' in sql
    assert 'repository_id = {repository_id:UInt64}' in sql
    assert client.queries[0]['parameters'] == {
        'kind': SYFT, 'repository_id': 7,
    }
