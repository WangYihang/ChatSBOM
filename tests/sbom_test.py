import json
import shutil
from unittest.mock import MagicMock
from unittest.mock import patch

import pytest

from chatsbom.services.sbom_service import SbomService
from chatsbom.services.sbom_service import SbomStats


@pytest.fixture
def sbom_service(tmp_path):
    # The service stops the command when syft is missing. In a fixture
    # that read as a broken test rather than a missing tool; CI installs
    # syft, so there the skip fails the run (tests/conftest.py).
    if shutil.which('syft') is None:
        pytest.skip('syft is not installed')
    with patch('chatsbom.services.sbom_service.get_config') as mock_config:
        # Mock paths
        mock_config.return_value.paths.content_dir = tmp_path / '06-github-content'
        mock_config.return_value.paths.sbom_dir = tmp_path / '07-sbom'
        # .cache/syft/<syft-version>/<repository_id>/<hash>.json
        mock_config.return_value.paths.get_sbom_cache_path.side_effect = \
            lambda rid, h, v=None: (
                tmp_path / '.cache' / 'syft' / (v or 'unknown') /
                str(rid) / f'{h}.json'
            )
        service = SbomService()
        return service


def test_sbom_service_process_repo_missing_path(sbom_service):
    """Test skipped if local_content_path is missing."""
    stats = SbomStats()
    repo_dict = {'owner': 'owner', 'repo': 'repo'}
    result = sbom_service.process_repo(repo_dict, stats)
    assert result is None
    assert stats.skipped == 1


@patch('subprocess.run')
def test_sbom_service_process_repo_success(mock_run, sbom_service, tmp_path):
    """Test successful SBOM generation."""
    # Setup mock content path
    content_dir = tmp_path / '06-github-content' / '42' / 'sha123'
    content_dir.mkdir(parents=True)
    (content_dir / 'requirements.txt').write_text('some content')

    repo_dict = {
        'owner': 'owner',
        'repo': 'repo',
        'local_content_path': str(content_dir),
    }

    # Mock syft output
    mock_run.return_value = MagicMock(stdout='{"sbom": "data"}', check=True)

    stats = SbomStats()
    result = sbom_service.process_repo(repo_dict, stats)

    assert result is not None
    assert 'sbom_path' in result
    assert stats.generated == 1
    assert stats.cache_hits == 0

    # Check output file exists in 07-sbom
    sbom_file = tmp_path / '07-sbom' / '42' / 'sha123' / 'sbom.json'
    assert sbom_file.exists()
    assert sbom_file.read_text() == '{"sbom": "data"}'

    # Check it was saved to cache
    # Hash of "requirements.txt" with "some content"
    content_hash = sbom_service._calculate_dir_hash(content_dir)
    version = sbom_service.syft_version or 'unknown'
    cache_file = tmp_path / '.cache' / 'syft' / version / \
        '42' / f'{content_hash}.json'
    assert cache_file.exists()
    assert cache_file.read_text() == '{"sbom": "data"}'


@patch('subprocess.run')
def test_sbom_service_process_repo_cache_hit(mock_run, sbom_service, tmp_path):
    """Test SBOM generation hits global cache."""
    # Setup mock content path
    content_dir = tmp_path / '06-github-content' / '42' / 'sha123'
    content_dir.mkdir(parents=True)
    (content_dir / 'requirements.txt').write_text('cached content')

    # Pre-populate cache
    content_hash = sbom_service._calculate_dir_hash(content_dir)
    version = sbom_service.syft_version or 'unknown'
    cache_dir = tmp_path / '.cache' / 'syft' / version / '42'
    cache_dir.mkdir(parents=True)
    cache_file = cache_dir / f'{content_hash}.json'
    # A Syft document: an entry without Syft's top-level keys is not one,
    # and is a miss.
    cached = '{"artifacts": [], "source": {}, "descriptor": {"name": "syft"}}'
    cache_file.write_text(cached)

    repo_dict = {
        'owner': 'owner',
        'repo': 'repo',
        'local_content_path': str(content_dir),
    }

    stats = SbomStats()
    result = sbom_service.process_repo(repo_dict, stats)

    assert result is not None
    assert stats.generated == 1
    assert stats.cache_hits == 1
    # syft should NOT be called
    mock_run.assert_not_called()

    # Check output file was copied from cache
    sbom_file = tmp_path / '07-sbom' / '42' / 'sha123' / 'sbom.json'
    assert sbom_file.exists()
    assert sbom_file.read_text() == cached


def test_what_syft_writes_is_current_for_it(sbom_service, tmp_path):
    """A stored SBOM is current only while the Syft now running wrote it:
    the version its descriptor records, against the one `syft version`
    reports. Were the two ever to differ, every SBOM would be
    regenerated on every run, so the real Syft is asked, the image's in
    CI (workflows_test).

    And its descriptor must lie well inside the end that is read for it,
    at most half of it: past it, every check reads up to a MiB more. An
    upgrade that eats the margin fails here, and moves the window."""
    from chatsbom.services.sbom_service import DESCRIPTOR_WINDOW
    from chatsbom.services.sbom_service import recorded_syft_version

    content_dir = tmp_path / '06-github-content' / '42' / 'sha123'
    content_dir.mkdir(parents=True)
    (content_dir / 'requirements.txt').write_text('requests==2.31.0\n')
    record = {
        'owner': 'owner', 'repo': 'repo',
        'local_content_path': str(content_dir),
    }

    stats = SbomStats()
    assert sbom_service.process_repo(dict(record), stats) is not None
    assert sbom_service.process_repo(dict(record), stats) is not None

    assert (stats.generated, stats.skipped) == (1, 1)
    sbom_file = tmp_path / '07-sbom' / '42' / 'sha123' / 'sbom.json'
    assert sbom_service.syft_version is not None
    assert recorded_syft_version(sbom_file) == sbom_service.syft_version
    written = sbom_file.read_bytes()
    from_end = len(written) - written.rindex(b'"descriptor"')
    assert 2 * from_end <= DESCRIPTOR_WINDOW, from_end


#: A Dart project as `pub get` leaves it: its manifest and its lockfile.
PUBSPEC = """\
name: demo
environment:
  sdk: ">=3.0.0 <4.0.0"
dependencies:
  http: ^1.2.0
"""
PUBSPEC_LOCK = """\
packages:
  http:
    dependency: "direct main"
    description:
      name: http
      sha256: "b9c29a161230ee03d3ccf545097fccd9b87a5264228c5d348202e0f0c28f9010"
      url: "https://pub.dev"
    source: hosted
    version: "1.2.2"
sdks:
  dart: ">=3.4.0 <4.0.0"
"""


def test_a_dart_projects_packages_are_in_the_pub_ecosystem(
    sbom_service, tmp_path,
):
    """Syft types a Dart package `dart-pub`, where the dependency graph
    and discovery say `pub`. Read as it was typed, a Dart repository's
    Syft rows were an ecosystem of their own in the rollups, and none
    at all among the repository's `ecosystems` (#120). The real Syft is
    asked, as in the test above: the name is its to change."""
    from chatsbom.core.ecosystems import artifact_ecosystem
    from chatsbom.services.db_service import ecosystems_of

    content_dir = tmp_path / '06-github-content' / '43' / 'sha456'
    content_dir.mkdir(parents=True)
    (content_dir / 'pubspec.yaml').write_text(PUBSPEC)
    (content_dir / 'pubspec.lock').write_text(PUBSPEC_LOCK)
    record = {
        'owner': 'owner', 'repo': 'dart',
        'local_content_path': str(content_dir),
    }

    assert sbom_service.process_repo(record, SbomStats()) is not None

    sbom_file = tmp_path / '07-sbom' / '43' / 'sha456' / 'sbom.json'
    artifacts = json.loads(sbom_file.read_text())['artifacts']
    assert 'http' in {artifact['name'] for artifact in artifacts}
    assert {
        artifact_ecosystem(artifact['type'], artifact['purl'])
        for artifact in artifacts
    } == {'pub'}
    assert ecosystems_of(artifacts) == ['pub']


class TestSbomStats:
    """Tests for SbomStats dataclass."""

    def test_default_values(self):
        """Test default values are set correctly."""
        stats = SbomStats()
        assert stats.generated == 0
        assert stats.skipped == 0
        assert stats.failed == 0
