"""The parsers of the store's documents: what their rows say, and where
it comes from (`services/db_service.py`).

They filled ClickHouse through `db index`, and fill the warehouse now
(#131, #153): the rows are the warehouse's (`warehouse/schema_test.py`
holds the columns to its tables), and so is what this holds them to,
SBOM provenance, dates and licences among it. A repository's record is
read as the warehouse reads it, through `LedgerRecords` with the
metadata overlay (`warehouse/store.py`).
"""
import json

import pytest

from chatsbom.core.documents import _fresh_metadata
from chatsbom.core.documents import DEPGRAPH
from chatsbom.core.documents import Document
from chatsbom.core.documents import FILES
from chatsbom.core.documents import LedgerRecords
from chatsbom.core.documents import SYFT
from chatsbom.core.manifest import resolve_relationships
from chatsbom.models.repository import Repository
from chatsbom.services.db_service import DbService
from chatsbom.warehouse import schema
from tests.repository_model_test import APACHE
from tests.repository_model_test import MIRROR_URL
from tests.repository_model_test import MIT
from tests.repository_model_test import OTHER

FULL_SHA = '8a79c788a54745c467cf6a1a9d438c9c91881001'


def record_of(listing, metadata=None):
    """The repository a JSONL list holds, as the warehouse reads it: the
    newest line, with the metadata overlay's fresher fields."""
    [record] = LedgerRecords(listing, metadata).records()
    return Repository.model_validate(record)


def syft(path):
    """The SBOM at `path`, read the way the warehouse reads it.

    The parsers take a document rather than a path, so the tests go
    through the same `DocumentSource` the command does -- a fixture that
    hand-built a Document would stop covering the reading.
    """
    return FILES.get(SYFT, 1, str(path))


def graph(path):
    """The dependency-graph document at `path`."""
    return FILES.get(DEPGRAPH, 1, str(path))


def make_repo(**overrides):
    data = {
        'id': 4321,
        'owner': 'discourse',
        'name': 'discourse',
        'stargazers_count': 46265,
        'html_url': 'https://github.com/discourse/discourse',
        'language': 'ruby',
        'default_branch': 'main',
        'download_target': {
            'ref': 'v3.2.0',
            'ref_type': 'release',
            'commit_sha': FULL_SHA,
            'commit_sha_short': FULL_SHA[:7],
        },
    }
    data.update(overrides)
    return Repository.model_validate(data)


@pytest.fixture
def service():
    return DbService()


# --- column contract -------------------------------------------------------

def test_parse_repository_returns_column_keyed_mapping(service):
    """Every column of the warehouse's `repositories`, and the ones that
    point at a scan, which are the scans' there."""
    row = service.parse_repository(make_repo())
    assert set(row) == (
        set(schema.REPOSITORIES.column_names) | set(schema.SCAN_POINTERS)
    )
    assert row['owner'] == 'discourse'
    assert row['stars'] == 46265


def test_repository_sbom_provenance_from_download_target(service):
    row = service.parse_repository(make_repo())
    assert row['sbom_ref'] == 'v3.2.0'
    assert row['sbom_ref_type'] == 'release'
    assert row['sbom_commit_sha'] == FULL_SHA
    assert row['sbom_commit_sha_short'] == FULL_SHA[:7]


# --- the off-by-one regression --------------------------------------------

def test_artifacts_inherit_full_sha_and_real_ref(service, tmp_path):
    """Artifacts used to get sbom_ref_type and the 7-char sha instead."""
    sbom = tmp_path / 'sbom.json'
    sbom.write_text(
        json.dumps({
            'artifacts': [{
                'id': 'abc123',
                'name': 'mail',
                'version': '2.9.0',
                'type': 'gem',
                'purl': 'pkg:gem/mail@2.9.0',
                'foundBy': 'ruby-gemfile-cataloger',
                'licenses': [{'value': 'MIT'}],
            }],
        }),
    )

    repo_row = service.parse_repository(make_repo())
    artifacts = service.parse_artifacts(
        syft(sbom), repo_id=4321, repo_row=repo_row,
    )

    assert len(artifacts) == 1
    art = artifacts[0]
    assert art['sbom_ref'] == 'v3.2.0', 'must be the ref, not the ref type'
    assert art['sbom_commit_sha'] == FULL_SHA, 'must be the full sha'
    assert art['name'] == 'mail'
    assert art['licenses'] == ['MIT']


def test_parse_artifacts_normalises_license_shapes(service, tmp_path):
    sbom = tmp_path / 'sbom.json'
    sbom.write_text(
        json.dumps({
            'artifacts': [{
                'name': 'x', 'version': '1', 'type': 'gem',
                'licenses': [
                    {'value': 'MIT'},
                    {'spdxExpression': 'Apache-2.0'},
                    {'name': 'BSD-3-Clause'},
                    'ISC',
                    {},
                ],
            }],
        }),
    )
    row = service.parse_artifacts(
        syft(sbom), 1, service.parse_repository(make_repo()),
    )[0]
    assert row['licenses'] == ['MIT', 'Apache-2.0', 'BSD-3-Clause', 'ISC']


def test_a_missing_document_is_absent_not_empty(tmp_path):
    """The source answers None, and the caller counts that as skipped.

    A repository with no SBOM and a repository whose SBOM produced no
    packages are different facts, and returning `[]` for both made them
    the same one.
    """
    assert syft(tmp_path / 'nope.json') is None
    assert FILES.get(SYFT, 1, None) is None, 'no path recorded at all'


def test_an_unreadable_document_names_itself(tmp_path):
    """Corrupt is worth failing on -- absent is not.

    The message carries the path because "unreadable sbom" with no
    subject has cost real debugging time.
    """
    bad = tmp_path / 'sbom.json'
    bad.write_text('{not json')
    with pytest.raises(ValueError, match=r'unreadable syft .*sbom\.json'):
        syft(bad)


def test_a_document_that_is_not_an_object_is_unreadable(tmp_path):
    """`json.load` accepts a bare list, and `.get` on it raises
    AttributeError two layers down from the file that caused it."""
    odd = tmp_path / 'sbom.json'
    odd.write_text('[]')
    with pytest.raises(ValueError, match='not an object'):
        syft(odd)


# --- stats accuracy --------------------------------------------------------


# --- dependency relationship ----------------------------------------------

def test_artifacts_are_marked_direct_or_transitive(service, tmp_path):
    content = tmp_path / 'content'
    content.mkdir()
    (content / 'Gemfile').write_text("gem 'mail'\n")

    sbom = tmp_path / 'sbom.json'
    sbom.write_text(
        json.dumps({
            'artifacts': [
                {'name': 'mail', 'version': '2.9.0', 'type': 'gem'},
                {'name': 'mini_mime', 'version': '1.1', 'type': 'gem'},
            ],
        }),
    )

    deps = resolve_relationships(content)
    rows = service.parse_artifacts(
        syft(sbom), 1, service.parse_repository(make_repo()),
        direct_deps=deps,
    )
    by_name = {r['name']: r['relationship'] for r in rows}
    assert by_name == {'mail': 'direct', 'mini_mime': 'transitive'}


def test_artifacts_default_to_unknown_relationship(service, tmp_path):
    sbom = tmp_path / 'sbom.json'
    sbom.write_text(
        json.dumps(
            {'artifacts': [{'name': 'mail', 'type': 'gem'}]},
        ),
    )
    rows = service.parse_artifacts(
        syft(sbom), 1, service.parse_repository(make_repo()),
    )
    assert rows[0]['relationship'] == 'unknown'


# --- the repository list must not shrink ----------------------------------


class TestObservedAt:
    """When a dependency was *collected*, not when it was indexed.

    `observed_at` defaulted to `now()`. Everything downstream reads it
    as the collection date — the export comments the column "when *we*
    last looked", the dashboard heads it "SCANNED" — so a
    `db index --rebuild` restamped all 19,361,638 rows with the moment
    it ran. Measured against the documents on disk: the syft SBOMs were
    collected 2026-02-11 and the dependency graphs 2026-09-14, and the
    table claimed 2026-09-14 for every row. Six million of them were
    seven months old and said they were hours old.
    """

    SYFT = {
        'artifacts': [{
            'id': 'a1', 'name': 'mail', 'version': '2.9.0', 'type': 'gem',
            'purl': 'pkg:gem/mail@2.9.0', 'foundBy': 'ruby-gemfile-cataloger',
            'licenses': [{'value': 'MIT'}],
        }],
    }

    @staticmethod
    def _at(path, when):
        """Set a file's mtime, which is all a syft document offers."""
        import os
        stamp = when.timestamp()
        os.utime(path, (stamp, stamp))

    def test_a_syft_document_is_dated_by_its_mtime(self, service, tmp_path):
        """Syft writes no timestamp at all — its `descriptor` names the
        tool and version and nothing else — so the file's mtime is the
        only evidence of when the scan happened."""
        from datetime import datetime
        from datetime import timezone

        sbom = tmp_path / 'sbom.json'
        sbom.write_text(json.dumps(self.SYFT))
        february = datetime(2026, 2, 11, 9, 30, tzinfo=timezone.utc)
        self._at(sbom, february)

        repo_row = service.parse_repository(make_repo())
        rows = service.parse_artifacts(
            syft(sbom), repo_id=4321, repo_row=repo_row,
        )

        assert rows[0]['observed_at'].date() == february.date()

    def test_a_rebuild_does_not_change_when_it_was_observed(
        self, service, tmp_path,
    ):
        """The property that matters. Parsing the same document twice
        must give the same answer, however far apart the two runs are —
        otherwise re-indexing silently ages the whole corpus forward."""
        from datetime import datetime
        from datetime import timezone

        sbom = tmp_path / 'sbom.json'
        sbom.write_text(json.dumps(self.SYFT))
        self._at(sbom, datetime(2026, 2, 11, 9, 30, tzinfo=timezone.utc))
        repo_row = service.parse_repository(make_repo())

        once = service.parse_artifacts(syft(sbom), 4321, repo_row)
        twice = service.parse_artifacts(syft(sbom), 4321, repo_row)
        first = once[0]['observed_at']
        assert first == twice[0]['observed_at']
        # And not today, which is what `now()` would have given.
        assert first.year == 2026 and first.month == 2

    def test_a_dependency_graph_is_dated_by_what_it_states(
        self, service, tmp_path,
    ):
        """GitHub's SPDX carries `creationInfo.created`, which is the
        graph's own view of when it was produced — better evidence than
        anything on this side of the wire, including the mtime."""
        from datetime import datetime
        from datetime import timezone

        doc = tmp_path / 'sbom.spdx.json'
        doc.write_text(
            json.dumps({
                'sbom': {
                    # Deliberately not today: with `now()` in place, a
                    # fixture dated today passes the assertion by
                    # coincidence, and the test sits green against the bug
                    # it exists for. GitHub's real value on these documents
                    # was 2026-09-14T03:56:20Z.
                    'creationInfo': {
                        'creators': ['Tool: GitHub.com-Dependency-Graph'],
                        'created': '2026-03-07T03:56:20Z',
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
            }),
        )
        # An mtime that disagrees, to prove which one wins.
        self._at(doc, datetime(2020, 1, 1, tzinfo=timezone.utc))

        repo_row = service.parse_repository(make_repo())
        rows = service.parse_dependency_graph(
            graph(doc), 4321, repo_row,
        )

        assert rows, 'the document should still have produced rows'
        for row in rows:
            assert row['observed_at'].year == 2026
            assert row['observed_at'].month == 3
            assert row['observed_at'].day == 7

    def test_an_unparsable_timestamp_falls_back_rather_than_raising(
        self, service, tmp_path,
    ):
        """A document that cannot be dated is still a document worth
        ingesting, so the failure must fall through to the mtime."""
        from datetime import datetime
        from datetime import timezone

        doc = tmp_path / 'sbom.spdx.json'
        doc.write_text(
            json.dumps({
                'sbom': {
                    'creationInfo': {'created': 'not a timestamp'},
                    'packages': [{
                        'SPDXID': 'p1', 'name': 'org.slf4j:slf4j-api',
                        'versionInfo': '2.0.13',
                        'externalRefs': [{
                            'referenceType': 'purl',
                            'referenceLocator': 'pkg:maven/org.slf4j/slf4j-api',
                        }],
                    }],
                },
            }),
        )
        self._at(doc, datetime(2026, 5, 4, tzinfo=timezone.utc))

        repo_row = service.parse_repository(make_repo())
        rows = service.parse_dependency_graph(
            graph(doc), 4321, repo_row,
        )
        assert rows
        assert rows[0]['observed_at'].month == 5

    def test_the_document_decides_when_it_was_observed(self, service):
        """Whatever the document says it was observed at is what lands:
        the warehouse dates a commit's scan by when the store first had
        it, and hands the parser the document so dated
        (`store._first_had`)."""
        from datetime import datetime

        stated = datetime(2025, 7, 1, 12, 0)
        rows = service.parse_artifacts(
            Document(body=self.SYFT, observed_at=stated, origin='test'),
            4321,
            service.parse_repository(make_repo()),
        )
        assert rows[0]['observed_at'] == stated


class TestFreshMetadataOverlay:
    """The SBOM ledger carries the repository metadata as it was when the
    SBOM was generated, and `github repo` refreshes it in a list of its
    own, which is overlaid on the record: `LedgerRecords`, as the
    warehouse reads it.

    Without the overlay a metadata refresh was invisible to the index,
    ClickHouse's then. Measured: the
    ledger knew 722 repositories had been pushed in September while
    `repositories.pushed_at` still topped out at 2026-02-09, and
    `rails/rails` sat at 58,182 stars against a refreshed 58,751.
    """

    @staticmethod
    def _metadata(tmp_path, **fields):
        index = tmp_path / 'ruby.jsonl'
        index.write_text(json.dumps({'id': 4321, **fields}) + '\n')
        return index

    def test_fresher_stars_reach_the_row(self, service, tmp_path):
        index = self._metadata(tmp_path, stars=58751)
        assert _fresh_metadata(index)[4321]['stars'] == 58751

    def test_the_overlay_reaches_the_row(self, service, tmp_path):
        """The loader working is not the same as the overlay being
        applied — the first version of this suite tested only the
        loader, so disabling the overlay at the call site passed every
        test. This reads the list as the warehouse does, and projects
        the record."""
        stale = make_repo(id=4321, stargazers_count=58182).model_dump(
            mode='json',
        )
        listing = tmp_path / 'ruby.jsonl'
        listing.write_text(json.dumps(stale) + '\n')

        metadata = tmp_path / 'meta.jsonl'
        metadata.write_text(json.dumps({'id': 4321, 'stars': 58751}) + '\n')

        row = service.parse_repository(record_of(listing, metadata))
        assert row['stars'] == 58751, 'the overlay was not applied'

    def test_without_an_overlay_the_ledger_value_stands(
        self, service, tmp_path,
    ):
        stale = make_repo(id=4321, stargazers_count=58182).model_dump(
            mode='json',
        )
        listing = tmp_path / 'ruby.jsonl'
        listing.write_text(json.dumps(stale) + '\n')

        assert service.parse_repository(record_of(listing))['stars'] == 58182

    def test_it_carries_only_fields_that_go_stale(self, service, tmp_path):
        """A blanket merge would also overwrite `sbom_commit_sha`, which
        describes *this* SBOM and must keep pointing at the commit that
        was actually scanned. A fresh `pushed_at` beside a stale
        `sbom_commit_sha` is the truth, and the panel says so.
        """
        index = self._metadata(
            tmp_path,
            stars=1,
            sbom_commit_sha='deadbeef',
            sbom_path='/somewhere/else',
        )
        carried = _fresh_metadata(index)[4321]
        assert 'stars' in carried
        assert 'sbom_commit_sha' not in carried
        assert 'sbom_path' not in carried

    def test_no_index_means_no_overlay(self, service, tmp_path):
        assert _fresh_metadata(None) == {}
        assert _fresh_metadata(tmp_path / 'absent.jsonl') == {}

    def test_one_bad_line_does_not_lose_the_rest(self, service, tmp_path):
        index = tmp_path / 'x.jsonl'
        index.write_text(
            '{"id": 1, "stars": 5}\nnot json\n{"id": 2, "stars": 6}\n',
        )
        fresh = _fresh_metadata(index)
        assert sorted(fresh) == [1, 2]

    def test_a_record_without_an_id_is_skipped(self, service, tmp_path):
        """There is nothing to key it by, and guessing would attach one
        repository's stars to another."""
        index = tmp_path / 'x.jsonl'
        index.write_text('{"stars": 5}\n{"id": "not-an-int", "stars": 6}\n')
        assert _fresh_metadata(index) == {}


class TestRepositoryLicence:
    """The licence and the mirror flag, from ledger line to row (#11).

    Neither was ever set: the licence was empty on every row, in
    ClickHouse and so in Parquet and D1, and `is_mirror` false, because
    the model never read GitHub's `license` object or its `mirror_url`.
    Every line written so far keeps both as extras, though, so indexing
    again is enough to recover them — nothing needs refetching.
    """

    @staticmethod
    def _stored(**fields):
        """A ledger line as `Storage.save` wrote it before #11: the
        licence fields left out as None, `is_mirror` false, and
        GitHub's own keys kept beside them."""
        return {
            **make_repo().model_dump(mode='json', exclude_none=True),
            **fields,
        }

    @staticmethod
    def _index(service, tmp_path, line, metadata=None):
        """The `repositories` row of one ledger line, overlaid with
        `metadata` if given, as the warehouse reads and projects it."""
        listing = tmp_path / 'ruby.jsonl'
        listing.write_text(json.dumps(line) + '\n')

        index = None
        if metadata is not None:
            index = tmp_path / 'meta.jsonl'
            index.write_text(json.dumps({'id': line['id'], **metadata}) + '\n')

        return service.parse_repository(record_of(listing, index))

    def test_a_line_carrying_only_githubs_object_is_indexed_with_it(
        self, service, tmp_path,
    ):
        line = self._stored(license=MIT)
        assert 'license_spdx_id' not in line, 'as every line written so far'

        row = self._index(service, tmp_path, line)
        assert row['license_spdx_id'] == 'MIT'
        assert row['license_name'] == 'MIT License'

    def test_an_unidentified_licence_is_indexed_by_its_name_alone(
        self, service, tmp_path,
    ):
        """`NOASSERTION` is not an SPDX licence id, so the column says
        empty, as it is published to; "Other" is what tells it from a
        repository with no licence at all."""
        row = self._index(service, tmp_path, self._stored(license=OTHER))
        assert row['license_spdx_id'] == ''
        assert row['license_name'] == 'Other'

    def test_a_mirror_is_indexed_as_one(self, service, tmp_path):
        line = self._stored(mirror_url=MIRROR_URL)
        assert line['is_mirror'] is False, 'as every line written so far'

        row = self._index(service, tmp_path, line)
        assert row['is_mirror'] is True

    def test_a_fresher_licence_in_the_metadata_ledger_wins(
        self, service, tmp_path,
    ):
        """The warehouse overlays what `github repo` refreshed, and reads
        that ledger as raw JSON, not through the model. Written before
        #11, it carries only GitHub's object, so the overlay has to read
        the licence out of it as the model does."""
        row = self._index(
            service, tmp_path, self._stored(license=MIT),
            metadata={'license': APACHE},
        )
        assert row['license_spdx_id'] == 'Apache-2.0'
        assert row['license_name'] == 'Apache License 2.0'

    def test_an_older_licence_does_not_come_back_through_the_overlay(
        self, service, tmp_path,
    ):
        """Relicensed, since the SBOM was generated, to something GitHub
        cannot identify.

        A line written after #11 leaves the empty SPDX id out. The
        overlay has to say it is empty, or the SBOM ledger's `MIT`
        stands; and it has to bring GitHub's newer object along, or the
        model refills the empty field from the ledger's older one.
        """
        line = self._stored(
            license=MIT, license_spdx_id='MIT', license_name='MIT License',
        )
        row = self._index(
            service, tmp_path, line,
            metadata={'license': OTHER, 'license_name': 'Other'},
        )
        assert (row['license_spdx_id'], row['license_name']) == ('', 'Other')


def _graph_document(created: str) -> dict:
    return {
        'sbom': {
            'creationInfo': {'created': created},
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


class TestTheGraphRows:
    """What a dependency-graph row, and its repository, record of it."""

    def test_a_repository_records_the_graph_it_was_indexed_with(
        self, service, tmp_path,
    ):
        """In one function for both, so the row and the repository hold
        the same instant: aware UTC, in whole seconds."""
        from datetime import datetime
        from datetime import timezone

        doc = tmp_path / 'sbom.spdx.json'
        doc.write_text(
            json.dumps(_graph_document('2026-09-14T11:56:20.924281+08:00')),
        )
        document = graph(doc)
        repo_row = service.parse_repository(make_repo(), graph=document)
        rows = service.parse_dependency_graph(document, 4321, repo_row)

        instant = datetime(2026, 9, 14, 3, 56, 20, tzinfo=timezone.utc)
        assert repo_row['depgraph_observed_at'] == instant
        assert [row['observed_at'] for row in rows] == [instant]

    def test_a_repository_without_a_graph_records_none(self, service):
        """The unset date, as every absent date here is. Not the
        column's default, which marks a row written before the column
        existed."""
        from chatsbom.core.instants import UNSET

        row = service.parse_repository(make_repo())
        assert row['depgraph_observed_at'] == UNSET

    def test_a_graph_row_names_the_default_branch(self, service, tmp_path):
        """GitHub builds the graph from the default branch. The release
        the Syft scan read is not what it describes."""
        doc = tmp_path / 'sbom.spdx.json'
        doc.write_text(json.dumps(_graph_document('2026-09-14T00:00:00Z')))
        repo_row = service.parse_repository(
            make_repo(default_branch='develop'),
        )
        [row] = service.parse_dependency_graph(graph(doc), 4321, repo_row)
        assert row['sbom_ref'] == 'develop'
        assert repo_row['sbom_ref'] == 'v3.2.0', 'the Syft scan keeps its ref'
