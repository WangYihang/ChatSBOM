import json
from datetime import datetime

from chatsbom.commands.index import parse_iso_time
from chatsbom.commands.index import parse_repo_line


class TestParseIsoTime:
    """Tests for parse_iso_time function."""

    def test_valid_iso_time_with_z(self):
        """Test parsing ISO time with Z suffix."""
        result = parse_iso_time('2024-06-03T23:37:33Z')
        assert result.year == 2024
        assert result.month == 6
        assert result.day == 3

    def test_valid_iso_time_with_offset(self):
        """Test parsing ISO time with timezone offset."""
        result = parse_iso_time('2024-01-15T10:30:00+00:00')
        assert result.year == 2024
        assert result.month == 1

    def test_none_returns_epoch(self):
        """Test None input returns epoch."""
        result = parse_iso_time(None)
        assert result == datetime(1970, 1, 1)

    def test_empty_string_returns_epoch(self):
        """Test empty string returns epoch."""
        result = parse_iso_time('')
        assert result == datetime(1970, 1, 1)

    def test_invalid_format_returns_epoch(self):
        """Test invalid format returns epoch."""
        result = parse_iso_time('not-a-date')
        assert result == datetime(1970, 1, 1)


class TestParseRepoLine:
    """Tests for parse_repo_line function."""

    def test_valid_repo_line(self):
        """Test parsing a valid JSONL line."""
        line = json.dumps({
            'id': 12345,
            'full_name': 'owner/repo',
            'url': 'https://github.com/owner/repo',
            'stars': 100,
            'description': 'A test repo',
            'created_at': '2024-01-01T00:00:00Z',
            'topics': ['python', 'testing'],
        })
        repo_row, meta_context = parse_repo_line(line, 'python')

        assert repo_row is not None
        assert repo_row[0] == 12345  # id
        assert repo_row[1] == 'owner'  # owner
        assert repo_row[2] == 'repo'  # repo
        assert repo_row[3] == 'owner/repo'  # full_name
        assert repo_row[5] == 100  # stars
        assert meta_context['owner'] == 'owner'
        assert meta_context['language'] == 'python'

    def test_invalid_json_returns_none(self):
        """Test invalid JSON returns None."""
        repo_row, meta_context = parse_repo_line('not valid json', 'python')
        assert repo_row is None
        assert meta_context is None

    def test_missing_full_name_returns_none(self):
        """Test missing full_name returns None."""
        line = json.dumps({'id': 123})
        repo_row, meta_context = parse_repo_line(line, 'python')
        assert repo_row is None
        assert meta_context is None

    def test_repo_without_slash(self):
        """Test full_name without owner/repo format."""
        line = json.dumps({
            'id': 123,
            'full_name': 'single-name',
        })
        repo_row, meta_context = parse_repo_line(line, 'go')
        assert repo_row is not None
        assert repo_row[1] == ''  # owner empty
        assert repo_row[2] == 'single-name'  # repo

    def test_null_description_handled(self):
        """Test null description is converted to empty string."""
        line = json.dumps({
            'id': 123,
            'full_name': 'owner/repo',
            'description': None,
        })
        repo_row, _ = parse_repo_line(line, 'python')
        # description should be empty string (index 6)
        assert repo_row[6] == ''
