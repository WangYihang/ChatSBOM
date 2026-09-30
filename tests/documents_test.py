"""The store's documents, manifests and records, read as the warehouse
reads them (`core/documents.py`).

The documents and records could be read out of ClickHouse's
`raw_documents` too, and most of what was here held the two sources to
agreeing. That went with the server (#153); what is left is the files,
and what reading them must not get wrong.
"""
from __future__ import annotations

import codecs
import json

from chatsbom.core.documents import FILE_MANIFESTS
from chatsbom.core.documents import FileManifests
from chatsbom.core.documents import FILES
from chatsbom.core.documents import LedgerRecords
from chatsbom.core.documents import SYFT
from chatsbom.core.manifest import relationships_from

SBOM = {
    'artifacts': [{
        'id': 'abc123', 'name': 'mail', 'version': '2.9.0', 'type': 'gem',
        'purl': 'pkg:gem/mail@2.9.0', 'foundBy': 'ruby-gemfile-cataloger',
        'licenses': [{'value': 'MIT'}],
    }],
}

GEMFILE = "source 'https://rubygems.org'\ngem 'mail'\n"
GEMSPEC = """
Gem::Specification.new do |s|
  s.add_dependency 'mini_mime'
end
"""


def test_the_recorded_path_is_already_the_scans(tmp_path):
    """A scan's document is at its own commit's path, so there is
    nothing to narrow; a path is not checked against the commit."""
    path = tmp_path / 'sbom.json'
    path.write_text(json.dumps(SBOM))
    assert FILES.get(SYFT, 7, str(path), commit_sha='b' * 40) is not None


def test_manifests_are_the_named_commits_alone(tmp_path):
    """One commit's directory is read, and only that.

    `content` writes a directory per commit, and a scan is judged by the
    manifests of its own. An older commit's directory beside it is not
    part of this scan's declared set.
    """
    repository = tmp_path / '4321'
    january = repository / ('a' * 40)
    january.mkdir(parents=True)
    (january / 'mail.gemspec').write_text(GEMSPEC)
    september = repository / ('b' * 40)
    september.mkdir(parents=True)
    (september / 'Gemfile').write_text(GEMFILE)

    assert FileManifests().for_repository(4321, str(september)) == [
        ('Gemfile', GEMFILE),
    ]


def test_a_repository_with_nothing_stored_declares_nothing(tmp_path):
    """Not an error: its dependencies stay `unknown`, which is the
    honest answer when no manifest was ever downloaded."""
    assert FileManifests().for_repository(7, None) == []
    assert FileManifests().for_repository(7, str(tmp_path / 'nope')) == []


def manifests(tmp_path, name, content, *others):
    """A commit's manifests, `name` holding `content`'s bytes beside
    `others`, `(name, text)`, read as the warehouse reads them."""
    root = tmp_path / '4321' / ('c' * 40)
    root.mkdir(parents=True, exist_ok=True)
    (root / name).write_bytes(content)
    for other, text in others:
        (root / other).write_text(text)
    return FILE_MANIFESTS.for_repository(4321, str(root))


def test_an_unreadable_manifest_is_incomplete(tmp_path):
    """#15: a manifest nobody could read may declare anything, so what
    no other manifest declares is `unknown`, not `transitive`."""
    read = manifests(
        tmp_path, 'Gemfile',
        b"source 'https://rubygems.org'\ngem 'rails'\n\x80\x81\n",
        ('mail.gemspec', GEMSPEC),
    )
    deps = relationships_from(read)['gem']
    assert deps.relationship_of('mini_mime') == 'direct'
    assert deps.relationship_of('rack') == 'unknown'
    assert deps.incomplete == ('Gemfile',)
    assert deps.sources == ('mail.gemspec',)


def test_a_byte_order_mark_is_dropped(tmp_path):
    """Windows editors write one, and json.loads rejects it."""
    read = manifests(
        tmp_path, 'package.json',
        codecs.BOM_UTF8 + b'{"dependencies": {"react": "^18"}}',
    )
    deps = relationships_from(read)['npm']
    assert deps.relationship_of('react') == 'direct'
    assert deps.incomplete == ()


def test_an_oversized_manifest_is_incomplete(tmp_path, monkeypatch):
    monkeypatch.setattr('chatsbom.core.manifest.MAX_MANIFEST_BYTES', 64)
    read = manifests(
        tmp_path, 'package.json',
        b'{"dependencies": {"express": "^4"}, "description": "'
        + b'x' * 64 + b'"}',
    )
    deps = relationships_from(read)['npm']
    assert deps.incomplete == ('package.json',)
    assert deps.relationship_of('express') == 'unknown'


def test_a_utf16_manifest_is_read_by_its_mark(tmp_path):
    """`_decoded` honours the mark, so what it declares is read."""
    read = manifests(
        tmp_path, 'requirements.txt',
        codecs.BOM_UTF16_LE + 'requests==2.31.0\n'.encode('utf-16-le'),
        ('requirements-dev.txt', 'flask==3.0\n'),
    )
    deps = relationships_from(read)['pypi']
    assert deps.relationship_of('flask') == 'direct'
    assert deps.relationship_of('requests') == 'direct'
    assert deps.incomplete == ()


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


def test_the_records_carry_the_metadata_overlay(tmp_path):
    """A refresh has to reach the row."""
    sbom_list = tmp_path / 'ruby.jsonl'
    sbom_list.write_text(json.dumps(LEDGER_RECORD) + '\n')
    metadata = tmp_path / 'meta-ruby.jsonl'
    metadata.write_text(json.dumps(FRESH_METADATA) + '\n')

    [record] = LedgerRecords(sbom_list, metadata).records()

    assert record['stars'] == 4197
    assert record['sbom_path'] == LEDGER_RECORD['sbom_path'], (
        'the overlay must not overwrite which commit was scanned'
    )


def test_every_record_is_read_whatever_list_it_was_filed_under(tmp_path):
    """No list is read by language any more (#55): a repository tracked
    with no language had its record filed under `07-sbom/index.jsonl`,
    and is read like any other; one listed twice is its newest line."""
    ruby = tmp_path / 'ruby.jsonl'
    ruby.write_text(
        json.dumps({**LEDGER_RECORD, 'stars': 1}) + '\n'
        + json.dumps(LEDGER_RECORD) + '\n',
    )
    unlisted = tmp_path / 'index.jsonl'
    unlisted.write_text(json.dumps({**LEDGER_RECORD, 'id': 9}) + '\n')

    records = list(LedgerRecords([ruby, unlisted]).records())

    assert [(r['id'], r['stars']) for r in records] == [
        (4321, 4034), (9, 4034),
    ]
