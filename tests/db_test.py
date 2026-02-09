from chatsbom.models.repository import Repository
from chatsbom.services.db_service import DbService


class TestDbService:
    """Tests for DbService."""

    def test_parse_iso_time_logic(self):
        """Test parsing ISO time logic via _parse_repository."""
        service = DbService()
        repo = Repository.model_validate({
            'id': 12345,
            'full_name': 'owner/repo',
            'stargazers_count': 100,
            'created_at': '2024-06-03T23:37:33Z',
        })
        parsed = service._parse_repository(repo)
        assert parsed is not None
        # created_at is index 7 in repo_row
        dt = parsed.repo_row[7]
        assert dt.year == 2024
        assert dt.month == 6
        assert dt.day == 3

    def test_repo_without_slash(self):
        """Test full_name without owner/repo format."""
        service = DbService()
        repo = Repository.model_validate({
            'id': 123,
            'full_name': 'single-name',
        })
        parsed = service._parse_repository(repo)
        assert parsed is not None
        assert parsed.repo_row[1] == ''  # owner empty
        assert parsed.repo_row[2] == 'single-name'  # repo

    def test_null_description_handled(self):
        """Test null description is converted to empty string."""
        service = DbService()
        repo = Repository.model_validate({
            'id': 123,
            'full_name': 'owner/repo',
            'description': None,
        })
        parsed = service._parse_repository(repo)
        # description should be empty string (index 6)
        assert parsed.repo_row[6] == ''
