import json

from chatsbom.services.indexer_service import IndexerService


class TestIndexerService:
    """Tests for IndexerService."""

    def test_parse_iso_time_logic(self):
        """Test parsing ISO time logic via parse_repository_line (indirectly)."""
        service = IndexerService()
        # Accessing private method for testing purposes if necessary,
        # but let's test public behavior.
        line = json.dumps({
            'id': 12345,
            'full_name': 'owner/repo',
            'stargazers_count': 100,
            'created_at': '2024-06-03T23:37:33Z',
        })
        parsed = service.parse_repository_line(line, 'python')
        assert parsed is not None
        # created_at is index 7 in repo_row
        dt = parsed.repo_row[7]
        assert dt.year == 2024
        assert dt.month == 6
        assert dt.day == 3

    def test_invalid_json_returns_none(self):
        """Test invalid JSON returns None."""
        service = IndexerService()
        parsed = service.parse_repository_line('not valid json', 'python')
        assert parsed is None

    def test_missing_full_name_returns_none(self):
        """Test missing full_name returns None."""
        service = IndexerService()
        line = json.dumps({'id': 123})
        parsed = service.parse_repository_line(line, 'python')
        assert parsed is None

    def test_repo_without_slash(self):
        """Test full_name without owner/repo format."""
        service = IndexerService()
        line = json.dumps({
            'id': 123,
            'full_name': 'single-name',
        })
        parsed = service.parse_repository_line(line, 'go')
        assert parsed is not None
        assert parsed.repo_row[1] == ''  # owner empty
        assert parsed.repo_row[2] == 'single-name'  # repo

    def test_null_description_handled(self):
        """Test null description is converted to empty string."""
        service = IndexerService()
        line = json.dumps({
            'id': 123,
            'full_name': 'owner/repo',
            'description': None,
        })
        parsed = service.parse_repository_line(line, 'python')
        # description should be empty string (index 6)
        assert parsed.repo_row[6] == ''
