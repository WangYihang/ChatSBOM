from chatsbom.models.repository import Repository
from chatsbom.services.db_service import DbService


class TestDbService:
    """Tests for DbService."""

    def test_parse_iso_time_logic(self):
        """Test parsing ISO time logic via _parse_repository."""
        service = DbService()
        repo = Repository.model_validate({
            'id': 12345,
            'owner': 'owner',
            'name': 'repo',
            'stargazers_count': 100,
            'created_at': '2024-06-03T23:37:33Z',
        })
        parsed = service._parse_repository(repo)
        assert parsed is not None
        # created_at is index 6 in repo_row
        dt = parsed.repo_row[6]
        assert dt.year == 2024
        assert dt.month == 6
        assert dt.day == 3

    def test_null_description_handled(self):
        """Test null description is converted to empty string."""
        service = DbService()
        repo = Repository.model_validate({
            'id': 123,
            'owner': 'owner',
            'name': 'repo',
            'description': None,
        })
        parsed = service._parse_repository(repo)
        # description should be empty string (index 5)
        assert parsed.repo_row[5] == ''
