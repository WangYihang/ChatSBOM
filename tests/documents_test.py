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


#: A repository's manifests as `db raw` stores them: `path` is the full
#: path on disk, under `<content_dir>/<language>/<owner>/<repo>/<ref>/<sha>`.
CONTENT_ROOT = 'data/06-github-content/ruby/mikel/mail/v3.2.0/abc123'

GEMFILE = "source 'https://rubygems.org'\ngem 'mail'\n"
GEMSPEC = """
Gem::Specification.new do |s|
  s.add_dependency 'mini_mime'
end
"""


class FakeManifestClient:
    """Answers the one query `RawManifests` makes."""

    def __init__(self, rows):
        self._rows = rows
        self.queries = []

    def query(self, sql, parameters):
        self.queries.append({'sql': sql, 'parameters': parameters})
        key = (parameters['kind'], parameters['repository_id'])
        return type('Result', (), {'result_rows': self._rows.get(key, [])})()


def test_both_manifest_sources_declare_the_same_set(tmp_path):
    """The property `--from-raw` rests on.

    A different declared set means different direct/transitive labels,
    which is the one thing in this table a reader cannot check.
    """
    from chatsbom.core.documents import CONTENT
    from chatsbom.core.documents import FileManifests
    from chatsbom.core.documents import RawManifests
    from chatsbom.core.manifest import relationships_from
    from chatsbom.models.language import Language

    root = tmp_path / 'ruby' / 'mikel' / 'mail' / 'v3.2.0' / 'abc123'
    root.mkdir(parents=True)
    (root / 'Gemfile').write_text(GEMFILE)
    (root / 'mail.gemspec').write_text(GEMSPEC)

    from_file = FileManifests().for_repository(4321, str(root))
    from_raw = RawManifests(
        FakeManifestClient({
            (CONTENT, 4321): [
                (f'{CONTENT_ROOT}/Gemfile', GEMFILE),
                (f'{CONTENT_ROOT}/mail.gemspec', GEMSPEC),
            ],
        }),
        'data/06-github-content',
    ).for_repository(4321)

    assert sorted(from_file) == sorted(from_raw)

    file_deps = relationships_from(from_file, Language.RUBY)
    raw_deps = relationships_from(from_raw, Language.RUBY)
    assert file_deps.names == raw_deps.names
    assert file_deps.sources == raw_deps.sources
    assert file_deps.relationship_of('mail') == 'direct'


def test_a_nested_manifest_keeps_its_directory(tmp_path):
    """812 of the 46,433 stored files are nested — a monorepo declares
    dependencies in more than one place, and reducing them all to a
    basename would make `sources` claim two manifests were one."""
    from chatsbom.core.documents import CONTENT
    from chatsbom.core.documents import RawManifests

    read = RawManifests(
        FakeManifestClient({
            (CONTENT, 7): [
                (f'{CONTENT_ROOT}/Gemfile', GEMFILE),
                (f'{CONTENT_ROOT}/engines/api/Gemfile', GEMFILE),
            ],
        }),
        'data/06-github-content',
    ).for_repository(7)

    assert sorted(path for path, _ in read) == [
        'Gemfile', 'engines/api/Gemfile',
    ]


def test_a_path_outside_the_content_dir_keeps_its_basename(tmp_path):
    """A row whose path does not sit where expected still names a
    manifest, and the parser only needs the filename to pick a reader.
    Dropping it would lose a repository's whole declared set."""
    from chatsbom.core.documents import CONTENT
    from chatsbom.core.documents import RawManifests

    read = RawManifests(
        FakeManifestClient({(CONTENT, 7): [('/elsewhere/Gemfile', GEMFILE)]}),
        'data/06-github-content',
    ).for_repository(7)
    assert read == [('Gemfile', GEMFILE)]


def test_a_repository_with_nothing_stored_declares_nothing(tmp_path):
    """Not an error: its dependencies stay `unknown`, which is the
    honest answer when no manifest was ever downloaded."""
    from chatsbom.core.documents import FileManifests
    from chatsbom.core.documents import RawManifests

    assert RawManifests(FakeManifestClient({})).for_repository(7) == []
    assert FileManifests().for_repository(7, None) == []
    assert FileManifests().for_repository(7, str(tmp_path / 'nope')) == []


def test_the_manifest_query_is_a_primary_key_prefix_lookup():
    """46,433 manifests across 28,075 repositories: a query that did not
    bind both leading columns would scan the table per repository."""
    from chatsbom.core.documents import CONTENT
    from chatsbom.core.documents import RawManifests

    client = FakeManifestClient({})
    RawManifests(client).for_repository(7)
    sql = client.queries[0]['sql']
    assert 'kind = {kind:String}' in sql
    assert 'repository_id = {repository_id:UInt64}' in sql
    assert client.queries[0]['parameters'] == {
        'kind': CONTENT, 'repository_id': 7,
    }


def _both_sources(tmp_path, name, landed_as, content):
    """One manifest read off disk and out of `raw_documents`.

    `content` is the file's bytes. `db raw` lands a manifest as
    `bytes.decode('utf-8', 'replace')`, so that is what the table holds.
    """
    from chatsbom.core.documents import CONTENT
    from chatsbom.core.documents import FileManifests
    from chatsbom.core.documents import RawManifests

    root = tmp_path / 'ruby' / 'mikel' / 'mail' / 'v3.2.0' / 'abc123'
    root.mkdir(parents=True, exist_ok=True)
    (root / name).write_bytes(content)
    rows = {(CONTENT, 4321): [
        (f'{CONTENT_ROOT}/{name}', content.decode('utf-8', 'replace')),
        *landed_as,
    ]}
    for other, text in landed_as:
        (root / other.rsplit('/', 1)[-1]).write_text(text)
    return {
        'file': FileManifests().for_repository(4321, str(root)),
        'raw': RawManifests(
            FakeManifestClient(rows), 'data/06-github-content',
        ).for_repository(4321),
    }


def test_an_unreadable_manifest_is_incomplete_from_either_source(tmp_path):
    """#15: a manifest nobody could read may declare anything, so what
    no other manifest declares is `unknown`, not `transitive`.

    Off disk these bytes do not decode. Landed, they arrive as U+FFFD,
    which parsed as they stand would have made the Gemfile complete and
    every other name `transitive` -- a different verdict for the same
    file, depending only on where it was read from.
    """
    from chatsbom.core.manifest import relationships_from
    from chatsbom.models.language import Language

    sources = _both_sources(
        tmp_path, 'Gemfile',
        [(f'{CONTENT_ROOT}/mail.gemspec', GEMSPEC)],
        b"source 'https://rubygems.org'\ngem 'rails'\n\x80\x81\n",
    )
    for source, read in sources.items():
        deps = relationships_from(read, Language.RUBY)
        assert deps.relationship_of('mini_mime') == 'direct', source
        assert deps.relationship_of('rack') == 'unknown', source
        assert deps.incomplete == ('Gemfile',), source
        assert deps.sources == ('mail.gemspec',), source


def test_a_byte_order_mark_is_dropped_from_either_source(tmp_path):
    """Windows editors write one, and json.loads rejects it. Off disk
    `_decoded` slices it off; landed, it is U+FEFF at the front of the
    text, and has to go the same way."""
    import codecs

    from chatsbom.core.manifest import relationships_from
    from chatsbom.models.language import Language

    sources = _both_sources(
        tmp_path, 'package.json', [],
        codecs.BOM_UTF8 + b'{"dependencies": {"react": "^18"}}',
    )
    for source, read in sources.items():
        deps = relationships_from(read, Language.JAVASCRIPT)
        assert deps.relationship_of('react') == 'direct', source
        assert deps.incomplete == (), source


def test_an_oversized_manifest_is_incomplete_from_either_source(
    tmp_path, monkeypatch,
):
    """`db raw` lands a manifest whatever its size; it is judged against
    the same cap as the file."""
    from chatsbom.core.manifest import relationships_from
    from chatsbom.models.language import Language

    monkeypatch.setattr('chatsbom.core.manifest.MAX_MANIFEST_BYTES', 64)
    sources = _both_sources(
        tmp_path, 'package.json', [],
        b'{"dependencies": {"express": "^4"}, "description": "'
        + b'x' * 64 + b'"}',
    )
    for source, read in sources.items():
        deps = relationships_from(read, Language.JAVASCRIPT)
        assert deps.incomplete == ('package.json',), source
        assert deps.relationship_of('express') == 'unknown', source


def test_a_utf16_manifest_landed_by_db_raw_is_unknown_not_misread():
    """`db raw` decoded it as UTF-8 with replacement, which leaves
    U+FFFD and NULs where the text was. What it declared cannot be read
    back out of that, so it counts as unread, and a name only it
    declares is `unknown` -- not `transitive`, which parsing the remains
    would have made it. Off disk, `_decoded` honours its mark and reads
    it; `db raw` would have to land it decoded the same way for the two
    to agree."""
    import codecs

    from chatsbom.core.documents import CONTENT
    from chatsbom.core.documents import RawManifests
    from chatsbom.core.manifest import relationships_from
    from chatsbom.models.language import Language

    utf16 = codecs.BOM_UTF16_LE + 'requests==2.31.0\n'.encode('utf-16-le')
    landed = utf16.decode('utf-8', 'replace')
    read = RawManifests(
        FakeManifestClient({(CONTENT, 7): [
            (f'{CONTENT_ROOT}/requirements.txt', landed),
            (f'{CONTENT_ROOT}/requirements-dev.txt', 'flask==3.0\n'),
        ]}),
        'data/06-github-content',
    ).for_repository(7)

    deps = relationships_from(read, Language.PYTHON)
    assert deps.relationship_of('flask') == 'direct'
    assert deps.relationship_of('requests') == 'unknown'
    assert deps.incomplete == ('requirements.txt',)


#: The record as a stage ledger stores it, and the fresher API response
#: that overlays it. Two different documents about the same repository.
LEDGER_RECORD = {
    'id': 4321, 'owner': 'mikel', 'name': 'mail', 'language': 'Ruby',
    'stars': 4034, 'sbom_path': 'data/07-sbom/ruby/mikel/mail/sbom.json',
    'download_target': {
        'ref': 'v3.2.0', 'ref_type': 'release',
        'commit_sha': 'abc123', 'commit_sha_short': 'abc123',
    },
}

FRESH_METADATA = {
    'id': 4321, 'owner': 'mikel', 'name': 'mail', 'language': 'Ruby',
    'stars': 4197, 'pushed_at': '2026-09-11T22:49:12Z',
    # Not a field that goes stale, and must not be carried over: it
    # describes *this* SBOM and has to keep pointing at the commit that
    # was actually scanned.
    'sbom_path': 'somewhere/else.json',
}


class FakeRecordClient:
    """Answers `RawRecords`' two queries.

    Applies the `suffix` filter itself, because the real query does it
    in SQL: filtering after the fetch meant transferring 5.16 GiB of
    stored records per language, and a double that ignored the
    parameter would let that regress silently.
    """

    def __init__(self, rows):
        # rows: kind -> [(repository_id, path, body_dict)]
        self._rows = rows
        self.queries = []

    def query(self, sql, parameters):
        self.queries.append({'sql': sql, 'parameters': parameters})
        suffix = parameters.get('suffix') or ''
        out = [
            (rid, path, json.dumps(body))
            for rid, path, body in self._rows.get(parameters['kind'], [])
            if not suffix or str(path).endswith(suffix)
        ]
        return type('Result', (), {'result_rows': out})()


def _raw_records(**kinds):
    from chatsbom.core.documents import RawRecords
    return RawRecords(FakeRecordClient(kinds))


def test_both_record_sources_apply_the_metadata_overlay(tmp_path):
    """A refresh has to reach the row, whichever source supplied it."""
    from chatsbom.core.documents import LedgerRecords
    from chatsbom.core.documents import REPO
    from chatsbom.core.documents import REPO_METADATA

    sbom_list = tmp_path / 'ruby.jsonl'
    sbom_list.write_text(json.dumps(LEDGER_RECORD) + '\n')
    metadata = tmp_path / 'meta-ruby.jsonl'
    metadata.write_text(json.dumps(FRESH_METADATA) + '\n')

    from_ledger = list(LedgerRecords(sbom_list, metadata).records('ruby'))
    from_raw = list(
        _raw_records(
            **{
                REPO: [(4321, 'data/07-sbom/ruby.jsonl', LEDGER_RECORD)],
                REPO_METADATA: [(4321, 'data/02-github-repo/ruby.jsonl', FRESH_METADATA)],
            },
        ).records('ruby'),
    )

    assert len(from_ledger) == len(from_raw) == 1
    assert from_ledger[0]['stars'] == from_raw[0]['stars'] == 4197
    assert from_ledger[0]['sbom_path'] == from_raw[0]['sbom_path'], (
        'the overlay must not overwrite which commit was scanned'
    )
    assert from_ledger[0]['sbom_path'].endswith('sbom.json')


def test_the_overlay_follows_the_ledger_not_the_api_language():
    """`github/choosealicense.com` is a Jekyll site: GitHub reports it
    as HTML, and it sits in the Ruby corpus.

    Reading the language off the record dropped its overlay, and the
    transform served January's 4,034 stars instead of September's
    4,197. Which language a repository belongs to is the pipeline's
    judgement, recorded in which ledger it was written to.
    """
    from chatsbom.core.documents import REPO
    from chatsbom.core.documents import REPO_METADATA

    jekyll = {**FRESH_METADATA, 'language': 'HTML'}
    records = list(
        _raw_records(
            **{
                REPO: [(4321, 'data/07-sbom/ruby.jsonl', LEDGER_RECORD)],
                REPO_METADATA: [(4321, 'data/02-github-repo/ruby.jsonl', jekyll)],
            },
        ).records('ruby'),
    )

    assert len(records) == 1, 'the record is in the ruby ledger'
    assert records[0]['stars'] == 4197, 'and its overlay applies'


def test_a_record_from_another_language_is_not_returned():
    """The per-language iteration is what bounds a pass, so a row from
    `python.jsonl` must not appear when indexing ruby."""
    from chatsbom.core.documents import REPO

    records = list(
        _raw_records(
            **{
                REPO: [
                    (4321, 'data/07-sbom/ruby.jsonl', LEDGER_RECORD),
                    (
                        9999, 'data/07-sbom/python.jsonl',
                        {**LEDGER_RECORD, 'id': 9999},
                    ),
                ],
            },
        ).records('ruby'),
    )
    assert [r['id'] for r in records] == [4321]


def test_the_language_filter_reaches_the_query():
    """Not applied in Python afterwards: that transfers every `repo`
    row for every language, 5.16 GiB of records nine times over, and
    the first version of this did not finish."""
    from chatsbom.core.documents import RawRecords
    from chatsbom.core.documents import REPO

    client = FakeRecordClient({REPO: []})
    list(RawRecords(client).records('ruby'))
    assert client.queries[0]['parameters']['suffix'] == '/ruby.jsonl'
    assert 'endsWith(path' in client.queries[0]['sql']


def test_only_the_newest_copy_of_a_record_is_used():
    """A repository collected twice is two rows distinguished by content
    hash, and only the latest describes it now."""
    from chatsbom.core.documents import REPO

    client = FakeRecordClient({
        REPO: [(4321, 'data/07-sbom/ruby.jsonl', LEDGER_RECORD)],
    })
    from chatsbom.core.documents import RawRecords
    list(RawRecords(client).records('ruby'))
    sql = client.queries[0]['sql']
    assert 'ORDER BY fetched_at DESC' in sql
    assert 'LIMIT 1 BY repository_id' in sql


def test_a_limit_stops_the_stream():
    """`--limit` has to narrow the source, not just the ingest: it also
    decides which scans get dropped before re-ingesting."""
    from chatsbom.core.documents import REPO

    source = _raw_records(
        **{
            REPO: [
                (i, 'data/07-sbom/ruby.jsonl', {**LEDGER_RECORD, 'id': i})
                for i in range(1, 6)
            ],
        },
    )
    assert len(list(source.records('ruby', limit=2))) == 2


def test_an_unreadable_record_does_not_lose_the_rest():
    """One corrupt row is not a reason to drop a language."""
    from chatsbom.core.documents import RawRecords

    class Broken(FakeRecordClient):
        def query(self, sql, parameters):
            return type(
                'Result', (), {
                    'result_rows': [
                        (1, 'data/07-sbom/ruby.jsonl', '{not json'),
                        (
                            2, 'data/07-sbom/ruby.jsonl', json.dumps(
                                {**LEDGER_RECORD, 'id': 2},
                            ),
                        ),
                    ],
                },
            )()

    records = list(RawRecords(Broken({})).records('ruby'))
    assert [r['id'] for r in records] == [2]
