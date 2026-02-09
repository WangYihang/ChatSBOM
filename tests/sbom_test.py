from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from chatsbom.services.sbom_service import SbomService
from chatsbom.services.sbom_service import SbomStats


@pytest.fixture
def sbom_service(tmp_path):
    with patch('chatsbom.services.sbom_service.get_config') as mock_config:
        # Mock paths
        mock_config.return_value.paths.content_dir = tmp_path / '05-github-content'
        mock_config.return_value.paths.sbom_dir = tmp_path / '06-sbom'
        service = SbomService()
        return service


def test_sbom_service_process_repo_missing_path(sbom_service):
    """Test skipped if local_content_path is missing."""
    stats = SbomStats()
    repo_dict = {'full_name': 'owner/repo'}
    result = sbom_service.process_repo(repo_dict, stats, 'python')
    assert result is None
    assert stats.skipped == 1


@patch('subprocess.run')
def test_sbom_service_process_repo_success(mock_run, sbom_service, tmp_path):
    """Test successful SBOM generation."""
    # Setup mock content path
    content_dir = tmp_path / '05-github-content' / \
        'python' / 'owner' / 'repo' / 'main' / 'sha123'
    content_dir.mkdir(parents=True)
    (content_dir / 'requirements.txt').touch()

    repo_dict = {
        'full_name': 'owner/repo',
        'local_content_path': str(content_dir),
    }

    # Mock syft output
    mock_run.return_value = MagicMock(stdout='{"sbom": "data"}', check=True)

    stats = SbomStats()
    result = sbom_service.process_repo(repo_dict, stats, 'python')

    assert result is not None
    assert 'sbom_path' in result
    assert stats.generated == 1

    # Check output file exists in 06-sbom
    sbom_file = tmp_path / '06-sbom' / 'python' / \
        'owner' / 'repo' / 'main' / 'sha123' / 'sbom.json'
    assert sbom_file.exists()
    assert sbom_file.read_text() == '{"sbom": "data"}'


class TestSbomStats:
    """Tests for SbomStats dataclass."""

    def test_default_values(self):
        """Test default values are set correctly."""
        stats = SbomStats()
        assert stats.generated == 0
        assert stats.skipped == 0
        assert stats.failed == 0
