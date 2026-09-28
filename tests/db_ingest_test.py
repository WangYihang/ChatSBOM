"""Ingestion tests: column contracts, SBOM provenance, and stats accuracy."""
import json

import pytest

from chatsbom.core.documents import _fresh_metadata
from chatsbom.core.documents import DEPGRAPH
from chatsbom.core.documents import Document
from chatsbom.core.documents import FILES
from chatsbom.core.documents import LedgerRecords
from chatsbom.core.documents import SYFT
from chatsbom.core.manifest import resolve_relationships
from chatsbom.core.schema import ARTIFACTS
from chatsbom.core.schema import REPOSITORIES
from chatsbom.export.queries import QUERIES
from chatsbom.models.repository import Repository
from chatsbom.services.db_service import DbService
from tests.conftest import requires_clickhouse
from tests.repository_model_test import APACHE
from tests.repository_model_test import MIRROR_URL
from tests.repository_model_test import MIT
from tests.repository_model_test import OTHER


FULL_SHA = '8a79c788a54745c467cf6a1a9d438c9c91881001'


def ledger_records(listing, metadata=None):
    """The records in a JSONL ledger, as `db index` reads them.

    `ingest_from_list` takes a source rather than a path now — the point
    of the change — so the tests go through the same `LedgerRecords` the
    command uses when `--from-raw` is off.
    """
    return LedgerRecords(listing, metadata)


def ingest(service, listing, repo_db, metadata=None, **kw):
    """`ingest_from_list` against a ledger on disk."""
    return service.ingest_from_list(
        ledger_records(listing, metadata), repo_db, **kw,
    )


def syft(path):
    """The SBOM at `path`, read the way `db index` reads it.

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


class FakeIngestionRepository:
    """Records inserts so tests can assert on what would reach ClickHouse."""

    def __init__(self):
        self.batches: list[tuple[str, list, list]] = []

    def insert_batch(self, table, data, columns):
        self.batches.append((table, data, columns))

    def rows_for(self, table: str) -> list[dict]:
        """Re-key recorded rows by column name."""
        out: list[dict] = []
        for name, data, columns in self.batches:
            if name != table:
                continue
            out.extend(dict(zip(columns, row)) for row in data)
        return out


# --- column contract -------------------------------------------------------

def test_parse_repository_returns_column_keyed_mapping(service):
    row = service.parse_repository(make_repo())
    assert set(row) == set(REPOSITORIES.columns)
    assert row['owner'] == 'discourse'
    assert row['stars'] == 46265


def test_parse_repository_output_projects_cleanly(service):
    """The mapping must satisfy the table contract exactly."""
    REPOSITORIES.row(service.parse_repository(make_repo()))


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
    ARTIFACTS.row(art)


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

def _write_list(tmp_path, sbom_path, count=1):
    repo = make_repo().model_dump(mode='json')
    repo['sbom_path'] = str(sbom_path)
    p = tmp_path / 'list.jsonl'
    with open(p, 'w') as f:
        for i in range(count):
            repo['id'] = 4321 + i
            f.write(json.dumps(repo) + '\n')
    return p


def test_artifact_count_is_not_doubled(service, tmp_path):
    sbom = tmp_path / 'sbom.json'
    sbom.write_text(
        json.dumps({
            'artifacts': [
                {'name': 'a', 'version': '1', 'type': 'gem'},
                {'name': 'b', 'version': '2', 'type': 'gem'},
                {'name': 'c', 'version': '3', 'type': 'gem'},
            ],
        }),
    )
    fake = FakeIngestionRepository()
    stats = ingest(service, _write_list(tmp_path, sbom), fake)

    assert stats.repos == 1
    assert stats.artifacts == 3, 'was reported as 6'
    assert stats.failed == 0
    assert len(fake.rows_for('artifacts')) == 3


def test_repository_without_sbom_path_is_skipped_not_failed(service, tmp_path):
    repo = make_repo().model_dump(mode='json')
    repo.pop('sbom_path', None)
    p = tmp_path / 'list.jsonl'
    p.write_text(json.dumps(repo) + '\n')

    fake = FakeIngestionRepository()
    stats = ingest(service, p, fake)

    assert stats.repos == 1
    assert stats.artifacts == 0
    assert stats.skipped == 1
    assert stats.failed == 0


def test_ingest_honours_limit(service, tmp_path):
    sbom = tmp_path / 'sbom.json'
    sbom.write_text(json.dumps({'artifacts': []}))
    fake = FakeIngestionRepository()
    stats = ingest(
        service, _write_list(tmp_path, sbom, count=10), fake, limit=3,
    )
    assert stats.repos == 3


def test_progress_callback_fires_once_per_repository(service, tmp_path):
    sbom = tmp_path / 'sbom.json'
    sbom.write_text(json.dumps({'artifacts': [{'name': 'a', 'type': 'gem'}]}))
    seen = []
    ingest(
        service,
        _write_list(tmp_path, sbom, count=4),
        FakeIngestionRepository(),
        progress_callback=lambda: seen.append(1),
    )
    assert len(seen) == 4


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


def test_ingest_classifies_relationships_from_local_content(service, tmp_path):
    content = tmp_path / 'content'
    content.mkdir()
    (content / 'Gemfile').write_text("gem 'mail'\n")

    sbom = tmp_path / 'sbom.json'
    sbom.write_text(
        json.dumps({
            'artifacts': [
                {'name': 'mail', 'type': 'gem'},
                {'name': 'mini_mime', 'type': 'gem'},
            ],
        }),
    )

    repo = make_repo().model_dump(mode='json')
    repo['sbom_path'] = str(sbom)
    repo['local_content_path'] = str(content)
    listing = tmp_path / 'list.jsonl'
    listing.write_text(json.dumps(repo) + '\n')

    fake = FakeIngestionRepository()
    ingest(service, listing, fake)

    rows = {r['name']: r['relationship'] for r in fake.rows_for('artifacts')}
    assert rows == {'mail': 'direct', 'mini_mime': 'transitive'}


def test_ingest_without_local_content_leaves_relationship_unknown(service, tmp_path):
    sbom = tmp_path / 'sbom.json'
    sbom.write_text(
        json.dumps(
            {'artifacts': [{'name': 'mail', 'type': 'gem'}]},
        ),
    )
    repo = make_repo().model_dump(mode='json')
    repo['sbom_path'] = str(sbom)
    repo.pop('local_content_path', None)
    listing = tmp_path / 'list.jsonl'
    listing.write_text(json.dumps(repo) + '\n')

    fake = FakeIngestionRepository()
    ingest(service, listing, fake)
    assert fake.rows_for('artifacts')[0]['relationship'] == 'unknown'


def test_ingest_unknown_language_does_not_break_classification(service, tmp_path):
    sbom = tmp_path / 'sbom.json'
    sbom.write_text(json.dumps({'artifacts': [{'name': 'x', 'type': 'gem'}]}))
    content = tmp_path / 'content'
    content.mkdir()

    repo = make_repo(language='Brainfuck').model_dump(mode='json')
    repo['sbom_path'] = str(sbom)
    repo['local_content_path'] = str(content)
    listing = tmp_path / 'list.jsonl'
    listing.write_text(json.dumps(repo) + '\n')

    fake = FakeIngestionRepository()
    stats = ingest(service, listing, fake)
    assert stats.failed == 0
    assert fake.rows_for('artifacts')[0]['relationship'] == 'unknown'


# --- the repository list must not shrink ----------------------------------

def test_the_sbom_ledger_decides_which_repositories_are_ingested(
    service, tmp_path,
):
    """A partial depgraph ledger must not shrink the corpus.

    `db index` preferred the depgraph ledger, described in a comment as a
    superset. `github depgraph --limit 120` makes it a *subset*: Java went
    from 1,215 indexed repositories to 87, silently, because the shorter
    ledger became the input list.
    """
    sbom = tmp_path / 'sbom.json'
    sbom.write_text(json.dumps({'artifacts': [{'name': 'a', 'type': 'gem'}]}))

    def record(repo_id: int) -> dict:
        row = make_repo(id=repo_id).model_dump(mode='json')
        row['sbom_path'] = str(sbom)
        return row

    full = tmp_path / 'sbom.jsonl'
    full.write_text('\n'.join(json.dumps(record(i)) for i in range(1, 6)))

    # Only one repository has a graph kept.
    kept = tmp_path / '09-github-depgraph' / '1' / 'legacy'
    kept.mkdir(parents=True)
    (kept / 'sbom.spdx.json').write_text(json.dumps({'sbom': {}}))

    fake = FakeIngestionRepository()
    stats = ingest(
        service, full, fake, depgraph_root=tmp_path / '09-github-depgraph',
    )

    assert stats.repos == 5, 'every repository in the SBOM ledger'


def test_depgraph_documents_are_attached_where_present(service, tmp_path):
    sbom = tmp_path / 'sbom.json'
    sbom.write_text(json.dumps({'artifacts': [{'name': 'a', 'type': 'gem'}]}))

    depgraph = tmp_path / 'dg.json'
    depgraph.write_text(
        json.dumps({
            'sbom': {
                'packages': [{
                    'SPDXID': 'p1', 'name': 'org.x:y',
                    'externalRefs': [{
                        'referenceType': 'purl',
                        'referenceLocator': 'pkg:maven/org.x/y',
                    }],
                }],
            },
        }),
    )

    covered = make_repo(id=1).model_dump(mode='json')
    covered['sbom_path'] = str(sbom)
    covered['depgraph_path'] = str(depgraph)

    uncovered = make_repo(id=2).model_dump(mode='json')
    uncovered['sbom_path'] = str(sbom)

    full = tmp_path / 'sbom.jsonl'
    full.write_text(f'{json.dumps(covered)}\n{json.dumps(uncovered)}\n')

    fake = FakeIngestionRepository()
    stats = ingest(service, full, fake)

    assert stats.repos == 2
    sources = {r['source'] for r in fake.rows_for('artifacts')}
    assert sources == {'syft', 'github-depgraph'}


def test_a_missing_depgraph_store_is_not_an_error(service, tmp_path):
    sbom = tmp_path / 'sbom.json'
    sbom.write_text(json.dumps({'artifacts': []}))
    row = make_repo().model_dump(mode='json')
    row['sbom_path'] = str(sbom)
    listing = tmp_path / 'l.jsonl'
    listing.write_text(json.dumps(row) + '\n')

    stats = ingest(
        service, listing, FakeIngestionRepository(),
        depgraph_root=tmp_path / 'absent',
    )
    assert stats.repos == 1


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
        from datetime import datetime, timezone

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
        from datetime import datetime, timezone

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
        from datetime import datetime, timezone

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
        from datetime import datetime, timezone

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
        """Whatever the document says it was observed at is what lands.

        This is what lets `raw_documents` stand in for the files: the
        row there carries `fetched_at`, copied from the file's mtime, so
        a document read from the database is dated the same as the same
        document read from disk.
        """
        from datetime import datetime

        stated = datetime(2025, 7, 1, 12, 0)
        rows = service.parse_artifacts(
            Document(body=self.SYFT, observed_at=stated, origin='test'),
            4321,
            service.parse_repository(make_repo()),
        )
        assert rows[0]['observed_at'] == stated


class TestFreshMetadataOverlay:
    """`db index` reads the SBOM ledger, which carries the repository
    metadata as it was when the SBOM was generated.

    So a metadata refresh was invisible to the database. Measured: the
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

    def test_ingest_applies_the_overlay(self, service, tmp_path):
        """The loader working is not the same as the overlay being
        applied — the first version of this suite tested only the
        loader, so disabling the overlay at the call site passed every
        test. This exercises `ingest_from_list` end to end.
        """
        sbom = tmp_path / 'sbom.json'
        sbom.write_text(json.dumps({'artifacts': []}))

        stale = make_repo(id=4321, stargazers_count=58182).model_dump(
            mode='json',
        )
        stale['sbom_path'] = str(sbom)
        listing = tmp_path / 'ruby.jsonl'
        listing.write_text(json.dumps(stale) + '\n')

        metadata = tmp_path / 'meta.jsonl'
        metadata.write_text(json.dumps({'id': 4321, 'stars': 58751}) + '\n')

        fake = FakeIngestionRepository()
        ingest(service, listing, fake, metadata=metadata)
        rows = fake.rows_for('repositories')
        assert rows, 'the repository row should have been written'
        assert rows[0]['stars'] == 58751, 'the overlay was not applied'

    def test_ingest_without_an_overlay_keeps_the_ledger_value(
        self, service, tmp_path,
    ):
        sbom = tmp_path / 'sbom.json'
        sbom.write_text(json.dumps({'artifacts': []}))
        stale = make_repo(id=4321, stargazers_count=58182).model_dump(
            mode='json',
        )
        stale['sbom_path'] = str(sbom)
        listing = tmp_path / 'ruby.jsonl'
        listing.write_text(json.dumps(stale) + '\n')

        fake = FakeIngestionRepository()
        ingest(service, listing, fake)
        assert fake.rows_for('repositories')[0]['stars'] == 58182

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
    def _index(service, tmp_path, line, metadata=None, repo_db=None):
        """Index one ledger line, overlaid with `metadata` if given.

        Into `repo_db` when one is given; otherwise into a fake, and
        the `repositories` row it received is returned.
        """
        sbom = tmp_path / 'sbom.json'
        sbom.write_text(json.dumps({'artifacts': []}))
        listing = tmp_path / 'ruby.jsonl'
        listing.write_text(json.dumps({**line, 'sbom_path': str(sbom)}) + '\n')

        index = None
        if metadata is not None:
            index = tmp_path / 'meta.jsonl'
            index.write_text(json.dumps({'id': line['id'], **metadata}) + '\n')

        target = FakeIngestionRepository() if repo_db is None else repo_db
        stats = ingest(service, listing, target, metadata=index)
        assert stats.failed == 0
        if repo_db is not None:
            return None
        (row,) = target.rows_for('repositories')
        return row

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
        """`db index` overlays what `github repo` refreshed, and reads
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

    @requires_clickhouse
    def test_the_licence_reaches_what_the_exports_publish(
        self, service, tmp_path, ingest, query,
    ):
        """Parquet and D1 both publish `REPOSITORIES_QUERY`, which is
        where an empty licence was seen: this follows one ledger line
        through a real ClickHouse to that query's rows."""
        self._index(
            service, tmp_path, self._stored(license=MIT), repo_db=ingest,
        )
        rows = list(query.stream_rows(QUERIES['repositories']))
        assert [row['license_spdx_id'] for row in rows] == ['MIT']


def _recording_repository():
    """An IngestionRepository whose client records the SQL it is handed.

    Built without `__init__` so no connection is opened: what is being
    asserted is the statement, and a live client would make these tests
    need a database to check a string.
    """
    from chatsbom.core.repository import IngestionRepository

    class Recorder:
        def __init__(self):
            self.commands: list[str] = []

        def command(self, sql):
            self.commands.append(sql)

    recorder = Recorder()
    repo = IngestionRepository.__new__(IngestionRepository)
    repo._client = recorder
    return recorder, repo


class TestReIngestingIsNotAppending:
    """`artifacts` is append-only, and a re-ingest is not new data.

    A row there is an *observation* — this package, at this version, in
    this repository, as seen in this scan — so nothing deduplicates the
    table: a repository re-scanned at a new commit must keep its old
    rows, or "how long did projects take to move off mail 2.7" becomes
    unanswerable.

    The gap that leaves is re-reading the *same* documents. Measured:
    `db index --language python` appended 687,000 duplicate rows, and
    the refusal message for `--rebuild --language` recommended that
    command as the way to refresh one language.
    """

    def test_the_scans_a_ledger_will_write_are_read_from_it(self, tmp_path):
        """The pairs to drop come from the ledger, not from the database.

        Asking the database which scans it holds would answer with what
        is already there, which is the wrong set: the rows to drop are
        the ones about to be written.
        """
        ledger = tmp_path / 'list.jsonl'
        record = make_repo().model_dump(mode='json')
        with ledger.open('w') as handle:
            handle.write(json.dumps(record) + '\n')
            record['id'] = 9999
            handle.write(json.dumps(record) + '\n')

        scans = DbService.scans_in(ledger_records(ledger))
        assert scans == [(4321, FULL_SHA), (9999, FULL_SHA)]

    def test_a_limit_narrows_the_scans_too(self, tmp_path):
        """Otherwise `--limit 3` would drop the whole corpus's rows and
        refill three of them — the shape of the bug it is meant to
        avoid."""
        ledger = tmp_path / 'list.jsonl'
        record = make_repo().model_dump(mode='json')
        with ledger.open('w') as handle:
            for index in range(5):
                record['id'] = 100 + index
                handle.write(json.dumps(record) + '\n')

        assert len(
            DbService.scans_in(
                ledger_records(ledger), limit=2,
            ),
        ) == 2

    def test_a_record_with_no_commit_sha_is_left_alone(self, tmp_path):
        """An empty sha in the predicate would match every row whose
        scan is unknown, in every repository."""
        ledger = tmp_path / 'list.jsonl'
        record = make_repo().model_dump(mode='json')
        record['download_target'] = None
        ledger.write_text(json.dumps(record) + '\n')

        assert DbService.scans_in(ledger_records(ledger)) == []


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


class TestTheGraphsAboutToBeWritten:
    """`graphs_in`: the graph's counterpart to `scans_in` (#22).

    A graph row is keyed by its document, which the document itself
    names: `creationInfo.created`. So the pre-pass asks the document
    source when each graph it would read was produced, rather than the
    ledger, which knows only where the file is.
    """

    @staticmethod
    def _ledger(tmp_path, records):
        ledger = tmp_path / 'list.jsonl'
        ledger.write_text(''.join(json.dumps(r) + '\n' for r in records))
        return ledger_records(ledger)

    def test_each_graph_is_named_by_when_it_was_produced(self, tmp_path):
        from datetime import datetime, timezone

        graph_path = tmp_path / 'sbom.spdx.json'
        graph_path.write_text(
            json.dumps(_graph_document('2026-09-14T11:56:20.9+08:00')),
        )
        covered = make_repo(id=1).model_dump(mode='json')
        covered['depgraph_path'] = str(graph_path)
        uncovered = make_repo(id=2).model_dump(mode='json')

        graphs = DbService.graphs_in(
            self._ledger(tmp_path, [covered, uncovered]), FILES,
        )
        assert graphs == [
            (1, datetime(2026, 9, 14, 3, 56, 20, tzinfo=timezone.utc)),
        ]

    def test_the_depgraph_store_is_read_as_the_ingest_reads_it(
        self, tmp_path,
    ):
        """A record without a `depgraph_path` of its own takes the graph
        kept under its id, in the ingest; so here too."""
        root = tmp_path / '09-github-depgraph'
        graph_path = root / '1' / 'legacy' / 'sbom.spdx.json'
        graph_path.parent.mkdir(parents=True)
        graph_path.write_text(
            json.dumps(
                _graph_document('2026-09-14T00:00:00Z'),
            ),
        )
        record = make_repo(id=1).model_dump(mode='json')

        graphs = DbService.graphs_in(
            self._ledger(tmp_path, [record]), FILES, depgraph_root=root,
        )
        assert [repository_id for repository_id, _ in graphs] == [1]

    def test_a_limit_narrows_the_graphs_too(self, tmp_path):
        graph_path = tmp_path / 'sbom.spdx.json'
        graph_path.write_text(
            json.dumps(
                _graph_document('2026-09-14T00:00:00Z'),
            ),
        )
        records = []
        for repository_id in range(1, 6):
            record = make_repo(id=repository_id).model_dump(mode='json')
            record['depgraph_path'] = str(graph_path)
            records.append(record)

        graphs = DbService.graphs_in(
            self._ledger(tmp_path, records), FILES, limit=2,
        )
        assert [repository_id for repository_id, _ in graphs] == [1, 2]

    def test_an_unreadable_graph_is_not_forgotten(self, tmp_path):
        """The ingest fails that record, and writes nothing for it, so
        there is nothing to make room for; and one bad file must not
        stop the forgetting of the rest."""
        bad = tmp_path / 'bad.spdx.json'
        bad.write_text('{not json')
        good = tmp_path / 'good.spdx.json'
        good.write_text(json.dumps(_graph_document('2026-09-14T00:00:00Z')))
        records = []
        for repository_id, path in ((1, bad), (2, good)):
            record = make_repo(id=repository_id).model_dump(mode='json')
            record['depgraph_path'] = str(path)
            records.append(record)

        graphs = DbService.graphs_in(
            self._ledger(tmp_path, records), FILES,
        )
        assert [repository_id for repository_id, _ in graphs] == [2]


class TestTheGraphRows:
    """What a dependency-graph row, and its repository, record of it."""

    def test_a_repository_records_the_graph_it_was_indexed_with(
        self, service, tmp_path,
    ):
        """In one function for both, so the row and the repository hold
        the same instant: aware UTC, in whole seconds."""
        from datetime import datetime, timezone

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
        REPOSITORIES.row(repo_row)
        ARTIFACTS.row(rows[0])

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


class TestForgettingAScan:
    """What `forget_scans` names, and what it leaves."""

    def test_it_names_the_scan_not_the_repository(self):
        """Deleting by repository would discard the history the table
        exists to keep."""
        recorder, repo = _recording_repository()
        repo.forget_scans([(4321, FULL_SHA)])

        sql = recorder.commands[0]
        assert '(repository_id, sbom_commit_sha) IN' in sql
        assert FULL_SHA in sql
        assert 'repository_id IN' not in sql, 'must not delete by id alone'

    def test_it_leaves_the_dependency_graphs(self):
        """A graph row carries the scan's commit and is not part of the
        scan: it is its own document, forgotten by `forget_graphs`.
        Keyed on the commit alone this deleted every graph under it."""
        recorder, repo = _recording_repository()
        repo.forget_scans([(4321, FULL_SHA)])
        assert "source IN ('syft', 'manifest')" in recorder.commands[0]
        assert 'github-depgraph' not in recorder.commands[0]

    def test_nothing_is_deleted_for_an_empty_list(self):
        recorder, repo = _recording_repository()
        assert repo.forget_scans([]) == 0
        assert recorder.commands == []

    def test_the_predicate_is_chunked(self):
        """24,451 pairs in one statement is a query ClickHouse parses
        for longer than it spends deleting."""
        recorder, repo = _recording_repository()
        repo.forget_scans([(index, FULL_SHA) for index in range(1200)])
        assert len(recorder.commands) == 3, '1200 pairs at 500 per statement'

    def test_a_quote_in_a_sha_cannot_end_the_predicate(self):
        """These are hex from the GitHub API, so nothing should need
        escaping — which is the argument for doing it, since the value
        that is not a sha is the one that matters."""
        recorder, repo = _recording_repository()
        repo.forget_scans([(1, "abc' OR 1=1 --")])
        assert 'OR 1=1' in recorder.commands[0], 'kept, as data'
        assert "\\'" in recorder.commands[0], 'and escaped'


class TestForgettingAGraph:
    """What `forget_graphs` names, and what it leaves (#22)."""

    def test_it_names_the_document_not_the_repository(self):
        """By the instant the document states, as its rows carry it, and
        only among graph rows: another document of the same repository
        is history, and a Syft row is another observation."""
        from datetime import datetime, timezone

        recorder, repo = _recording_repository()
        repo.forget_graphs([
            (4321, datetime(2026, 9, 14, 3, 56, 20, tzinfo=timezone.utc)),
        ])

        [sql] = recorder.commands
        assert "source = 'github-depgraph'" in sql
        assert '(repository_id, toUnixTimestamp(observed_at)) IN' in sql
        assert '(4321, 1789358180)' in sql
        assert 'repository_id IN' not in sql, 'must not delete by id alone'

    def test_an_instant_is_the_same_in_any_zone(self):
        """Seconds since the epoch, so neither side's zone can move it:
        the eight-hour shift `instants.py` fixed came from exactly that."""
        from datetime import datetime, timedelta, timezone

        recorder, repo = _recording_repository()
        repo.forget_graphs([
            (1, datetime(2026, 9, 14, 11, 56, 20, tzinfo=timezone(timedelta(hours=8)))),
        ])
        assert '(1, 1789358180)' in recorder.commands[0]

    def test_nothing_is_deleted_for_an_empty_list(self):
        recorder, repo = _recording_repository()
        assert repo.forget_graphs([]) == 0
        assert recorder.commands == []

    def test_the_predicate_is_chunked(self):
        from datetime import datetime, timezone

        recorder, repo = _recording_repository()
        instant = datetime(2026, 9, 14, tzinfo=timezone.utc)
        repo.forget_graphs([(index, instant) for index in range(1200)])
        assert len(recorder.commands) == 3, '1200 pairs at 500 per statement'
