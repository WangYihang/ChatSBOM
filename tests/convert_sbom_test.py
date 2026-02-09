from pathlib import Path

from chatsbom.models.language import Language
from chatsbom.services.converter_service import ConversionResult
from chatsbom.services.converter_service import ConverterService


class TestConverterService:
    """Tests for ConverterService."""

    def test_empty_dir_returns_empty(self, tmp_path):
        """Test empty directory returns empty list."""
        service = ConverterService(base_dir=str(tmp_path))
        result = service.find_projects()
        assert result == []

    def test_nonexistent_dir_returns_empty(self, tmp_path):
        """Test non-existent directory returns empty list."""
        service = ConverterService(base_dir=str(tmp_path / 'nonexistent'))
        result = service.find_projects()
        assert result == []

    def test_finds_project_dirs(self, tmp_path):
        """Test finds project directories in correct structure."""
        # Create structure: base/go/owner/repo/ref/sha/metadata.json
        project_dir = tmp_path / 'go' / 'owner' / 'repo' / 'main' / 'deadbeef'
        project_dir.mkdir(parents=True)
        (project_dir / 'metadata.json').touch()

        service = ConverterService(base_dir=str(tmp_path))
        result = service.find_projects()
        assert len(result) == 1
        assert result[0] == project_dir

    def test_finds_multiple_projects(self, tmp_path):
        """Test finds multiple project directories."""
        # Create multiple projects
        (tmp_path / 'go' / 'owner1' / 'repo1' / 'main' / 'sha1').mkdir(parents=True)
        (
            tmp_path / 'go' / 'owner1' / 'repo1' /
            'main' / 'sha1' / 'metadata.json'
        ).touch()
        (
            tmp_path / 'go' / 'owner2' / 'repo2' /
            'master' / 'sha2'
        ).mkdir(parents=True)
        (
            tmp_path / 'go' / 'owner2' / 'repo2' /
            'master' / 'sha2' / 'metadata.json'
        ).touch()
        (
            tmp_path / 'python' / 'owner3' / 'repo3' /
            'main' / 'sha3'
        ).mkdir(parents=True)
        (
            tmp_path / 'python' / 'owner3' / 'repo3' /
            'main' / 'sha3' / 'metadata.json'
        ).touch()

        service = ConverterService(base_dir=str(tmp_path))
        result = service.find_projects()
        assert len(result) == 3

    def test_filter_by_language(self, tmp_path):
        """Test filtering by language."""
        (tmp_path / 'go' / 'owner1' / 'repo1' / 'main' / 'sha1').mkdir(parents=True)
        (
            tmp_path / 'go' / 'owner1' / 'repo1' /
            'main' / 'sha1' / 'metadata.json'
        ).touch()
        (
            tmp_path / 'python' / 'owner2' / 'repo2' /
            'main' / 'sha2'
        ).mkdir(parents=True)
        (
            tmp_path / 'python' / 'owner2' / 'repo2' /
            'main' / 'sha2' / 'metadata.json'
        ).touch()

        service = ConverterService(base_dir=str(tmp_path))
        result = service.find_projects(Language.GO)
        assert len(result) == 1
        assert 'go' in str(result[0])


class TestConversionResult:
    """Tests for ConversionResult dataclass."""

    def test_default_values(self):
        """Test default values are set correctly."""
        result = ConversionResult(project_path=Path('/test'))
        assert result.is_converted is False
        assert result.is_skipped is False
        assert result.is_failed is False
