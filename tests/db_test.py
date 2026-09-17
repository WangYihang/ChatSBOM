from chatsbom.models.repository import Repository
from chatsbom.services.db_service import DbService


class TestDbService:
    """Tests for DbService row projection.

    Assertions address columns by name; the previous positional form
    (repo_row[6]) is what let an index shift corrupt two columns silently.
    """

    def test_iso_timestamps_become_naive_datetimes(self):
        service = DbService()
        repo = Repository.model_validate({
            'id': 12345,
            'owner': 'owner',
            'name': 'repo',
            'stargazers_count': 100,
            'created_at': '2024-06-03T23:37:33Z',
        })
        row = service.parse_repository(repo)

        created = row['created_at']
        assert (created.year, created.month, created.day) == (2024, 6, 3)
        assert created.tzinfo is None, 'ClickHouse DateTime takes naive values'

    def test_null_description_handled(self):
        service = DbService()
        repo = Repository.model_validate({
            'id': 123,
            'owner': 'owner',
            'name': 'repo',
            'description': None,
        })
        assert service.parse_repository(repo)['description'] == ''

    def test_missing_dates_fall_back_to_epoch(self):
        service = DbService()
        repo = Repository.model_validate({
            'id': 123, 'owner': 'owner', 'name': 'repo',
        })
        row = service.parse_repository(repo)
        assert row['created_at'].year == 1970
        assert row['pushed_at'].year == 1970
        assert row['latest_release_published_at'].year == 1970

    def test_repository_without_download_target_gets_empty_provenance(self):
        service = DbService()
        repo = Repository.model_validate({
            'id': 123, 'owner': 'owner', 'name': 'repo',
        })
        row = service.parse_repository(repo)
        assert row['sbom_ref'] == ''
        assert row['sbom_ref_type'] == ''
        assert row['sbom_commit_sha'] == ''
        assert row['sbom_commit_sha_short'] == ''

    def test_releases_are_projected_with_assets_as_json(self):
        service = DbService()
        repo = Repository.model_validate({
            'id': 123, 'owner': 'owner', 'name': 'repo',
            'all_releases': [{
                'id': 9,
                'tag_name': 'v1.0.0',
                'published_at': '2025-01-02T03:04:05Z',
                'assets': [{'name': 'x.tar.gz'}],
            }],
        })
        rows = service.parse_releases(repo)
        assert len(rows) == 1
        assert rows[0]['tag_name'] == 'v1.0.0'
        assert rows[0]['release_assets'] == '[{"name": "x.tar.gz"}]'

    def test_no_releases_yields_no_rows(self):
        service = DbService()
        repo = Repository.model_validate({
            'id': 123, 'owner': 'owner', 'name': 'repo',
        })
        assert service.parse_releases(repo) == []


class TestReleaseAssetsAreTrimmed:
    """`release_assets` was the largest column in the database, unread.

    No query, no rollup and no panel touches it, and it held 5.00 GiB
    uncompressed against about 90 MiB for every other column in
    `releases` combined — plus 9.7 GiB of the `*.jsonl` ledgers on
    disk. A single asset averaged 1,555 bytes, of which `uploader` was
    a complete GitHub user object.

    Trimmed rather than dropped: "which releases ship a binary, how
    large, does it carry a checksum" is a fair question of a
    supply-chain dataset, and this project has twice paid for
    discarding what it had not yet needed. Measured: 1,555 -> 301
    bytes, 81% smaller.
    """

    def test_it_keeps_what_a_question_could_need(self) -> None:
        from chatsbom.services.db_service import _trimmed_assets
        asset = {
            'name': 'jekyll.deb', 'content_type': 'application/x-deb',
            'size': 4096, 'download_count': 12,
            'browser_download_url': 'https://example/x.deb',
            'created_at': '2015-06-29T23:50:34Z',
            'digest': 'sha256:abc',
        }
        kept = _trimmed_assets([asset])[0]
        assert kept == asset

    def test_it_drops_the_uploader_and_the_api_plumbing(self) -> None:
        """`uploader` alone is a whole user object, and `url`,
        `node_id`, `id`, `state`, `label` and `updated_at` answer
        nothing this dataset is about."""
        from chatsbom.services.db_service import _trimmed_assets
        kept = _trimmed_assets([{
            'name': 'x.deb',
            'uploader': {'login': 'someone', 'id': 1, 'node_id': 'MDQ6'},
            'url': 'https://api.github.com/...',
            'node_id': 'MDEy', 'id': 675010, 'state': 'uploaded',
            'label': None, 'updated_at': '2015-06-29T23:52:58Z',
        }])[0]
        assert kept == {'name': 'x.deb'}

    def test_it_keeps_the_checksum_though_most_rows_lack_one(self) -> None:
        """`digest` is on 11.2% of assets. A field present on a tenth
        of rows is still the answer to "can I verify this download"."""
        from chatsbom.services.db_service import ASSET_FIELDS
        assert 'digest' in ASSET_FIELDS

    def test_an_odd_shape_does_not_stop_an_ingest(self) -> None:
        """This runs over 28,000 repositories; one malformed release is
        not a reason to lose the rest."""
        from chatsbom.services.db_service import _trimmed_assets
        assert _trimmed_assets(None) == []
        assert _trimmed_assets('not a list') == []
        assert _trimmed_assets([None, 42, {'name': 'x'}]) == [{'name': 'x'}]

    def test_the_ingest_uses_it(self) -> None:
        """A helper nothing calls is the same as no helper."""
        import inspect
        from chatsbom.services.db_service import DbService
        source = inspect.getsource(DbService)
        assert '_trimmed_assets(r.assets)' in source
        assert 'json.dumps(r.assets)' not in source
