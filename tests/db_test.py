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
