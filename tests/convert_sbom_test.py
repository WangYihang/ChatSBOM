from sbom_insight.commands.convert_sbom import ConvertResult
from sbom_insight.commands.convert_sbom import find_project_dirs
from sbom_insight.models.language import Language


class TestFindProjectDirs:
    """Tests for find_project_dirs function."""

    def test_empty_dir_returns_empty(self, tmp_path):
        """Test empty directory returns empty list."""
        result = find_project_dirs(tmp_path)
        assert result == []

    def test_nonexistent_dir_returns_empty(self, tmp_path):
        """Test non-existent directory returns empty list."""
        result = find_project_dirs(tmp_path / 'nonexistent')
        assert result == []

    def test_finds_project_dirs(self, tmp_path):
        """Test finds project directories in correct structure."""
        # Create structure: base/go/owner/repo/branch/
        project_dir = tmp_path / 'go' / 'owner' / 'repo' / 'main'
        project_dir.mkdir(parents=True)
        (project_dir / 'go.mod').touch()

        result = find_project_dirs(tmp_path)
        assert len(result) == 1
        assert result[0] == project_dir

    def test_finds_multiple_projects(self, tmp_path):
        """Test finds multiple project directories."""
        # Create multiple projects
        (tmp_path / 'go' / 'owner1' / 'repo1' / 'main').mkdir(parents=True)
        (tmp_path / 'go' / 'owner2' / 'repo2' / 'master').mkdir(parents=True)
        (tmp_path / 'python' / 'owner3' / 'repo3' / 'main').mkdir(parents=True)

        result = find_project_dirs(tmp_path)
        assert len(result) == 3

    def test_filter_by_language(self, tmp_path):
        """Test filtering by language."""
        (tmp_path / 'go' / 'owner1' / 'repo1' / 'main').mkdir(parents=True)
        (tmp_path / 'python' / 'owner2' / 'repo2' / 'main').mkdir(parents=True)

        result = find_project_dirs(tmp_path, Language.GO)
        assert len(result) == 1
        assert 'go' in str(result[0])

    def test_ignores_files_at_wrong_level(self, tmp_path):
        """Test ignores files at language/owner level."""
        (tmp_path / 'go').mkdir()
        (tmp_path / 'go' / 'some_file.txt').touch()

        result = find_project_dirs(tmp_path)
        assert result == []


class TestConvertResult:
    """Tests for ConvertResult dataclass."""

    def test_default_values(self):
        """Test default values are set correctly."""
        result = ConvertResult(project_path='/test', status_msg='ok')
        assert result.converted == 0
        assert result.skipped == 0
        assert result.failed == 0

    def test_can_update_counts(self):
        """Test counts can be updated."""
        result = ConvertResult(project_path='/test', status_msg='ok')
        result.converted += 1
        result.skipped += 2
        assert result.converted == 1
        assert result.skipped == 2
