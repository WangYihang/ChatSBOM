"""What the real Syft writes, as the collector runs it, in its pool
(`collector/syftpool.py`).

The Syft on PATH, the image's in CI (workflows_test), is asked: whether
what it writes is current for it is a question of what it says it is,
and what the warehouse reads of a document is its to change. These
asked the old pipeline's `SbomService`, which ran Syft for `sbom
generate`, and went with it (#171).
"""
import asyncio
import json
import shutil

import pytest

from chatsbom.collector.syftpool import DEFAULT_MEMORY
from chatsbom.collector.syftpool import SyftPool
from chatsbom.collector.syftpool import SyftSettings
from chatsbom.services.sbom_service import DESCRIPTOR_WINDOW
from chatsbom.services.sbom_service import recorded_syft_version
from chatsbom.services.sbom_service import staleness


@pytest.fixture
def syft() -> SyftPool:
    """The collector's pool, of the Syft on PATH. Skipped without one,
    where an error in a fixture read as a broken test rather than a
    missing tool; CI installs syft, so there the skip fails the run
    (tests/conftest.py)."""
    found = shutil.which('syft')
    if found is None:
        pytest.skip('syft is not installed')
    return SyftPool(
        SyftSettings(
            slots=1, timeout=120, memory=DEFAULT_MEMORY, command=found,
        ),
    )


def test_what_syft_writes_is_current_for_it(syft, tmp_path):
    """A stored SBOM is current only while the Syft now running wrote it:
    the version its descriptor records, against the one `syft version`
    reports. Were the two ever to differ, every SBOM would be
    regenerated on every walk, so the real Syft is asked.

    And its descriptor must lie well inside the end that is read for it,
    at most half of it: past it, every check reads up to a MiB more. An
    upgrade that eats the margin fails here, and moves the window."""
    content = tmp_path / 'content'
    content.mkdir()
    (content / 'requirements.txt').write_text('requests==2.31.0\n')
    stored = tmp_path / 'sbom.json'
    stored.write_bytes(asyncio.run(syft.scan(content)))

    version = asyncio.run(syft.version())
    assert version is not None
    assert recorded_syft_version(stored) == version
    assert staleness(stored, content, syft_version=version) is None
    written = stored.read_bytes()
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


def test_a_dart_projects_packages_are_in_the_pub_ecosystem(syft, tmp_path):
    """Syft types a Dart package `dart-pub`, where the dependency graph
    and discovery say `pub`. Read as it was typed, a Dart repository's
    Syft rows were an ecosystem of their own in the rollups, and none
    at all among the repository's `ecosystems` (#120). The real Syft is
    asked, as in the test above: the name is its to change."""
    from chatsbom.core.ecosystems import artifact_ecosystem
    from chatsbom.services.db_service import ecosystems_of

    content = tmp_path / 'content'
    content.mkdir()
    (content / 'pubspec.yaml').write_text(PUBSPEC)
    (content / 'pubspec.lock').write_text(PUBSPEC_LOCK)

    artifacts = json.loads(asyncio.run(syft.scan(content)))['artifacts']
    assert 'http' in {artifact['name'] for artifact in artifacts}
    assert {
        artifact_ecosystem(artifact['type'], artifact['purl'])
        for artifact in artifacts
    } == {'pub'}
    assert ecosystems_of(artifacts) == ['pub']
