import json
import os
from pathlib import Path

import pytest
from structlog.testing import capture_logs

FIXTURES = Path(__file__).parent / 'fixtures'

#: What each Syft wrote itself: `syft dir:project -o json`, run beside a
#: `project/` holding a requirements.txt of `requests==2.31.0` alone.
REAL = {
    '1.41.2': FIXTURES / 'syft-1.41.2.json',
    '1.52.0': FIXTURES / 'syft-1.52.0.json',
}


def _bytes_read(monkeypatch) -> list[int]:
    """How many bytes each read returned, from here on, of every file
    opened through `Path.open`."""
    reads: list[int] = []
    real_open = Path.open

    class Counting:
        def __init__(self, handle):
            self._handle = handle

        def __enter__(self):
            return self

        def __exit__(self, *exc_info):
            self._handle.close()

        def __getattr__(self, name):
            return getattr(self._handle, name)

        def read(self, *args):
            data = self._handle.read(*args)
            reads.append(len(data))
            return data

    def counting_open(self, *args, **kwargs):
        return Counting(real_open(self, *args, **kwargs))

    monkeypatch.setattr(Path, 'open', counting_open)
    return reads


def _sparse(path: Path, head: bytes, hole: int, end: bytes) -> None:
    """`head`, then `hole` bytes of NULs that take no disk, then `end`."""
    with path.open('wb') as handle:
        handle.write(head)
        handle.seek(len(head) + hole)
        handle.write(end)


class TestAZeroByteSbomIsNotDone:
    """An interrupted write used to poison a repository permanently.

    `sbom generate` skipped when the output path existed, and a
    zero-byte file exists. So a truncated syft write meant every later
    run skipped that repository, and `db index` failed it with
    `unreadable sbom ... Expecting value: line 1 column 1 (char 0)`.
    Two repositories in the corpus sat like that —
    `btmills/geopattern` and `layerJS/layerJS` — and only `--force`
    over all 5,834 JavaScript repositories would have recovered them.
    """

    def test_an_empty_file_does_not_count_as_generated(self, tmp_path) -> None:
        from chatsbom.services.sbom_service import _is_usable_sbom
        empty = tmp_path / 'sbom.json'
        empty.touch()
        assert empty.exists()
        assert not _is_usable_sbom(empty)

    def test_a_written_file_counts(self, tmp_path) -> None:
        from chatsbom.services.sbom_service import _is_usable_sbom
        written = tmp_path / 'sbom.json'
        written.write_text('{"artifacts": []}')
        assert _is_usable_sbom(written)

    def test_a_missing_file_does_not_count(self, tmp_path) -> None:
        from chatsbom.services.sbom_service import _is_usable_sbom
        assert not _is_usable_sbom(tmp_path / 'absent.json')

    def test_a_file_cut_short_does_not_count(self, tmp_path) -> None:
        """A kill or a full disk midway through the write left a prefix.
        It is not empty, so it passed, and it is not JSON, so `db index`
        failed it on every run (#13)."""
        from chatsbom.services.sbom_service import _is_usable_sbom
        from tests.fake_upstream_test import cut_short
        from tests.fake_upstream_test import syft_document
        stored = tmp_path / 'sbom.json'
        stored.write_text(cut_short(syft_document()))
        assert not _is_usable_sbom(stored)

    def test_a_whole_syft_document_counts(self, tmp_path) -> None:
        from chatsbom.services.sbom_service import _is_usable_sbom
        from tests.fake_upstream_test import syft_document
        stored = tmp_path / 'sbom.json'
        stored.write_text(syft_document())
        assert _is_usable_sbom(stored)

    def test_an_unreadable_path_does_not_raise(self, tmp_path) -> None:
        """This runs once per repository inside a worker; an OSError
        here would abort the language rather than skip one row."""
        from chatsbom.services.sbom_service import _is_usable_sbom
        assert not _is_usable_sbom(tmp_path)  # a directory, not a file


class TestTheOuterSkipSeesUnusableSboms:
    """`process_repo`'s check alone was unreachable.

    `generate.py` skipped on a ledger entry *before* calling
    `process_repo`, so an entry pointing at a zero-byte SBOM
    short-circuited the very check meant to catch it. The outer skip is
    now the same check, `is_current_sbom`, which also asks whether the
    content changed since.
    """

    @staticmethod
    def _project(tmp_path):
        project = tmp_path / 'content'
        project.mkdir()
        (project / 'package.json').write_text('{}')
        return project

    def test_a_zero_byte_sbom_is_not_current(self, tmp_path) -> None:
        from chatsbom.services.sbom_service import is_current_sbom
        from tests.fake_upstream_test import SYFT_VERSION
        project = self._project(tmp_path)
        empty = tmp_path / 'empty.json'
        empty.touch()
        assert not is_current_sbom(empty, project, syft_version=SYFT_VERSION)

    def test_an_sbom_cut_short_is_not_current(self, tmp_path) -> None:
        from chatsbom.services.sbom_service import is_current_sbom
        from tests.fake_upstream_test import cut_short
        from tests.fake_upstream_test import syft_document
        from tests.fake_upstream_test import SYFT_VERSION
        project = self._project(tmp_path)
        cut = tmp_path / 'cut.json'
        cut.write_text(cut_short(syft_document()))
        assert not is_current_sbom(cut, project, syft_version=SYFT_VERSION)

    def test_a_whole_sbom_newer_than_its_content_is_current(
        self, tmp_path,
    ) -> None:
        from chatsbom.services.sbom_service import is_current_sbom
        from tests.fake_upstream_test import syft_document
        from tests.fake_upstream_test import SYFT_VERSION
        project = self._project(tmp_path)
        good = tmp_path / 'good.json'
        good.write_text(syft_document())
        assert is_current_sbom(good, project, syft_version=SYFT_VERSION)

    def test_content_newer_than_the_sbom_makes_it_stale(
        self, tmp_path,
    ) -> None:
        from chatsbom.services.sbom_service import is_current_sbom
        from tests.fake_upstream_test import syft_document
        from tests.fake_upstream_test import SYFT_VERSION
        project = self._project(tmp_path)
        good = tmp_path / 'good.json'
        good.write_text(syft_document())
        os.utime(good, (1_000_000, 1_000_000))
        assert not is_current_sbom(good, project, syft_version=SYFT_VERSION)


class TestTheSyftVersionAStoredSbomRecords:
    """`recorded_syft_version`: which Syft wrote a stored SBOM, as its
    descriptor says, read from the end of the file."""

    @pytest.mark.parametrize('version', sorted(REAL))
    def test_each_syfts_own_document_is_read(self, version) -> None:
        from chatsbom.services.sbom_service import recorded_syft_version
        assert recorded_syft_version(REAL[version]) == version

    @pytest.mark.parametrize('version', sorted(REAL))
    def test_indented_it_is_read_as_well(self, tmp_path, version) -> None:
        """As `SYFT_FORMAT_PRETTY` has Syft write it: spaced, over many
        lines, and the descriptor further from the end."""
        from chatsbom.services.sbom_service import recorded_syft_version
        stored = tmp_path / 'sbom.json'
        document = json.loads(REAL[version].read_text(encoding='utf-8'))
        stored.write_text(json.dumps(document, indent=2) + '\n')
        assert recorded_syft_version(stored) == version

    @pytest.mark.parametrize(
        'descriptor',
        [
            None,
            {'name': 'syft'},
            {'name': 'syft', 'version': ''},
            {'name': 'syft', 'version': 1.52},
            {'name': 'grype', 'version': '1.52.0'},
            'syft 1.52.0',
        ],
        ids=[
            'no-descriptor', 'no-version', 'empty-version',
            'not-a-string', 'another-tool', 'not-an-object',
        ],
    )
    def test_none_where_no_syft_version_is_recorded(
        self, tmp_path, descriptor,
    ) -> None:
        from chatsbom.services.sbom_service import recorded_syft_version
        document: dict = {'artifacts': []}
        if descriptor is not None:
            document['descriptor'] = descriptor
        stored = tmp_path / 'sbom.json'
        stored.write_text(json.dumps(document) + '\n')
        assert recorded_syft_version(stored) is None

    def test_none_where_there_is_nothing_to_read(self, tmp_path) -> None:
        """Missing, a directory, empty, or cut inside the descriptor.
        None rather than an error, which would stop the whole scan to
        spare one root a regeneration."""
        from chatsbom.services.sbom_service import recorded_syft_version
        empty = tmp_path / 'empty.json'
        empty.touch()
        cut = tmp_path / 'cut.json'
        cut.write_bytes(REAL['1.52.0'].read_bytes()[:-1000])
        for path in (tmp_path / 'absent.json', tmp_path, empty, cut):
            assert recorded_syft_version(path) is None, path

    def test_the_last_descriptor_is_the_one_read(self, tmp_path) -> None:
        """Syft writes its descriptor after everything the scan found, and
        only the schema after that. What a project's files say can reach
        an artifact's metadata, but never past the descriptor."""
        from chatsbom.services.sbom_service import recorded_syft_version
        from tests.fake_upstream_test import syft_document
        from tests.fake_upstream_test import SYFT_VERSION
        document = json.loads(syft_document())
        document['artifacts'][0]['metadata'] = {
            'descriptor': {'name': 'syft', 'version': '0.0.1'},
        }
        stored = tmp_path / 'sbom.json'
        stored.write_text(json.dumps(document, separators=(',', ':')) + '\n')
        assert recorded_syft_version(stored) == SYFT_VERSION

    def test_only_the_end_is_read(self, tmp_path, monkeypatch) -> None:
        """This is asked of every stored SBOM before anything is scanned:
        tens of thousands of them, 16 GB. So the rest is neither parsed
        nor read. Here it is 64 MiB of NULs, which is no JSON at all, and
        one window's worth is read."""
        from chatsbom.services.sbom_service import DESCRIPTOR_WINDOW
        from chatsbom.services.sbom_service import recorded_syft_version
        whole = REAL['1.52.0'].read_bytes()
        end = whole[whole.index(b',"source"'):]
        stored = tmp_path / 'sbom.json'
        _sparse(stored, b'{"artifacts":[', 64 << 20, end)
        reads = _bytes_read(monkeypatch)

        assert recorded_syft_version(stored) == '1.52.0'
        assert 0 < sum(reads) <= DESCRIPTOR_WINDOW

    def test_a_descriptor_past_the_window_is_found_on_a_second_look(
        self, tmp_path,
    ) -> None:
        """A later Syft whose configuration outgrew the window. Were a miss
        taken for another Syft's SBOM, every SBOM that Syft wrote would be
        regenerated on every run."""
        from chatsbom.services.sbom_service import DESCRIPTOR_WINDOW
        from chatsbom.services.sbom_service import recorded_syft_version
        from tests.fake_upstream_test import syft_document
        from tests.fake_upstream_test import SYFT_VERSION
        document = json.loads(syft_document())
        document['descriptor']['configuration'] = {
            'padding': 'x' * (4 * DESCRIPTOR_WINDOW),
        }
        stored = tmp_path / 'sbom.json'
        stored.write_text(json.dumps(document, separators=(',', ':')) + '\n')
        assert recorded_syft_version(stored) == SYFT_VERSION

    def test_one_that_records_none_is_not_read_whole(
        self, tmp_path, monkeypatch,
    ) -> None:
        """The second look is bounded too: a document that names no Syft
        is not read to its start to find that out."""
        from chatsbom.services.sbom_service import DESCRIPTOR_WINDOW
        from chatsbom.services.sbom_service import DESCRIPTOR_WINDOW_LIMIT
        from chatsbom.services.sbom_service import recorded_syft_version
        stored = tmp_path / 'sbom.json'
        _sparse(stored, b'{"artifacts":[', 64 << 20, b'],"schema":{}}\n')
        reads = _bytes_read(monkeypatch)

        assert recorded_syft_version(stored) is None
        assert sum(reads) <= DESCRIPTOR_WINDOW + DESCRIPTOR_WINDOW_LIMIT


class TestAnSbomAnotherSyftWroteIsNotCurrent:
    """A stored SBOM is current only while the Syft now running wrote it,
    so an upgrade regenerates each one, once. The version it is judged by
    is the one it records itself."""

    @staticmethod
    def _stored(tmp_path, document: str) -> tuple[Path, Path]:
        """A content root, and an SBOM of it written after it."""
        project = tmp_path / 'content'
        project.mkdir()
        (project / 'package.json').write_text('{}')
        stored = tmp_path / 'sbom.json'
        stored.write_text(document)
        return project, stored

    def test_one_another_version_wrote_is_not_current(self, tmp_path) -> None:
        from chatsbom.services.sbom_service import is_current_sbom
        from chatsbom.services.sbom_service import Stale
        from chatsbom.services.sbom_service import staleness
        from tests.fake_upstream_test import syft_document
        from tests.fake_upstream_test import SYFT_VERSION
        project, stored = self._stored(
            tmp_path, syft_document(version='1.41.2'),
        )
        assert not is_current_sbom(stored, project, syft_version=SYFT_VERSION)
        assert staleness(
            stored, project, syft_version=SYFT_VERSION,
        ) is Stale.ANOTHER_SYFT

    def test_one_the_running_version_wrote_is_current(self, tmp_path) -> None:
        from chatsbom.services.sbom_service import is_current_sbom
        from chatsbom.services.sbom_service import staleness
        from tests.fake_upstream_test import syft_document
        from tests.fake_upstream_test import SYFT_VERSION
        project, stored = self._stored(tmp_path, syft_document())
        assert is_current_sbom(stored, project, syft_version=SYFT_VERSION)
        assert staleness(stored, project, syft_version=SYFT_VERSION) is None

    def test_a_later_version_is_another_one_too(self, tmp_path) -> None:
        """Going back to an older Syft regenerates as well: whichever way
        it moved, the corpus ends up one Syft's."""
        from chatsbom.services.sbom_service import is_current_sbom
        from tests.fake_upstream_test import syft_document
        project, stored = self._stored(tmp_path, syft_document())
        assert not is_current_sbom(stored, project, syft_version='1.41.2')

    def test_a_whole_document_that_records_none_is_not_current(
        self, tmp_path,
    ) -> None:
        from chatsbom.services.sbom_service import Stale
        from chatsbom.services.sbom_service import staleness
        from tests.fake_upstream_test import SYFT_VERSION
        project, stored = self._stored(tmp_path, '{"artifacts": []}\n')
        assert staleness(
            stored, project, syft_version=SYFT_VERSION,
        ) is Stale.ANOTHER_SYFT

    @pytest.mark.parametrize('version', sorted(REAL))
    def test_each_syfts_own_document(self, tmp_path, version) -> None:
        from chatsbom.services.sbom_service import is_current_sbom
        project, stored = self._stored(
            tmp_path, REAL[version].read_text(encoding='utf-8'),
        )
        [other] = set(REAL) - {version}
        assert is_current_sbom(stored, project, syft_version=version)
        assert not is_current_sbom(stored, project, syft_version=other)

    def test_the_version_is_asked_before_the_content_root_is_walked(
        self, tmp_path, monkeypatch,
    ) -> None:
        """Cheapest first. The walk lists every directory under the content
        root and stats every file; the version is one read at the end of
        a file whose ends were just read. A root regenerated for its Syft
        is not walked at all."""
        from chatsbom.services import sbom_service
        from tests.fake_upstream_test import syft_document
        from tests.fake_upstream_test import SYFT_VERSION
        walked: list[Path | None] = []

        def newest_mtime(root: Path | None) -> float:
            walked.append(root)
            return 0.0

        monkeypatch.setattr(sbom_service, '_newest_mtime', newest_mtime)
        project, stored = self._stored(
            tmp_path, syft_document(version='1.41.2'),
        )
        assert sbom_service.staleness(
            stored, project, syft_version=SYFT_VERSION,
        ) is sbom_service.Stale.ANOTHER_SYFT
        assert walked == []

    def test_newer_content_is_still_a_reason(self, tmp_path) -> None:
        from chatsbom.services.sbom_service import Stale
        from chatsbom.services.sbom_service import staleness
        from tests.fake_upstream_test import syft_document
        from tests.fake_upstream_test import SYFT_VERSION
        project, stored = self._stored(tmp_path, syft_document())
        os.utime(stored, (1_000_000, 1_000_000))
        assert staleness(
            stored, project, syft_version=SYFT_VERSION,
        ) is Stale.INPUT_CHANGED

    def test_with_the_running_version_unknown_times_alone_decide(
        self, tmp_path,
    ) -> None:
        """Judged against no version, every SBOM would be regenerated, and
        judged against none again on the next run."""
        from chatsbom.services.sbom_service import is_current_sbom
        from tests.fake_upstream_test import syft_document
        project, stored = self._stored(
            tmp_path, syft_document(version='1.41.2'),
        )
        assert is_current_sbom(stored, project, syft_version=None)
        os.utime(stored, (1_000_000, 1_000_000))
        assert not is_current_sbom(stored, project, syft_version=None)


class TestAnUnknownSyftVersion:
    """`syft version` failed, or said nothing that reads as a version: the
    times alone decide, and a warning says so, once however many roots
    and callers ask."""

    @pytest.fixture
    def unknown(self, monkeypatch):
        from chatsbom.services import sbom_service
        monkeypatch.setattr(sbom_service, 'get_syft_version', lambda: None)
        sbom_service._warn_syft_version_unknown.cache_clear()
        yield
        sbom_service._warn_syft_version_unknown.cache_clear()

    def test_it_is_said_once(self, unknown) -> None:
        from chatsbom.services.sbom_service import running_syft_version
        with capture_logs() as logs:
            assert running_syft_version() is None
            assert running_syft_version() is None
        warned = [e for e in logs if e['log_level'] == 'warning']
        assert len(warned) == 1, warned

    def test_a_known_version_is_not_warned_about(self, monkeypatch) -> None:
        from chatsbom.services import sbom_service
        monkeypatch.setattr(sbom_service, 'get_syft_version', lambda: '1.52.0')
        with capture_logs() as logs:
            assert sbom_service.running_syft_version() == '1.52.0'
        assert [e for e in logs if e['log_level'] == 'warning'] == []
