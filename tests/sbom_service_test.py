import pytest


@pytest.fixture
def syft(tmp_path, monkeypatch, no_database):
    """A faked Syft (`sbom_generate_test.FakeSyft`), in a fresh working
    directory: the real service and paths, and no real Syft."""
    from chatsbom.core.container import Container
    from chatsbom.services import sbom_service
    from tests.sbom_generate_test import FakeSyft
    from tests.sbom_generate_test import SYFT_VERSION

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(Container, '_instance', None)
    monkeypatch.setattr(sbom_service, 'check_syft_installed', lambda: True)
    monkeypatch.setattr(sbom_service, 'get_syft_version', lambda: SYFT_VERSION)
    fake = FakeSyft()
    monkeypatch.setattr(sbom_service.subprocess, 'run', fake)
    return fake


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
        from tests.sbom_generate_test import cut_short
        from tests.sbom_generate_test import syft_document
        stored = tmp_path / 'sbom.json'
        stored.write_text(cut_short(syft_document()))
        assert not _is_usable_sbom(stored)

    def test_a_whole_syft_document_counts(self, tmp_path) -> None:
        from chatsbom.services.sbom_service import _is_usable_sbom
        from tests.sbom_generate_test import syft_document
        stored = tmp_path / 'sbom.json'
        stored.write_text(syft_document())
        assert _is_usable_sbom(stored)

    def test_an_unreadable_path_does_not_raise(self, tmp_path) -> None:
        """This runs once per repository inside a worker; an OSError
        here would abort the language rather than skip one row."""
        from chatsbom.services.sbom_service import _is_usable_sbom
        assert not _is_usable_sbom(tmp_path)  # a directory, not a file

    def test_the_skip_uses_it(self, syft) -> None:
        """A helper nothing calls is the same as no helper: the service
        scans again over a zero-byte SBOM, and skips a whole one."""
        from chatsbom.services.sbom_service import SbomService
        from chatsbom.services.sbom_service import SbomStats
        from tests.sbom_generate_test import _downloaded
        from tests.sbom_generate_test import _generated
        from tests.sbom_generate_test import _project
        from tests.sbom_generate_test import _sbom
        from tests.sbom_generate_test import syft_document
        _downloaded('a', 'b')
        _generated('a', '')
        _generated('b', syft_document('b'))
        service, stats = SbomService(), SbomStats()

        for name in ('a', 'b'):
            service.process_repo(
                {
                    'owner': 'o', 'repo': name,
                    'local_content_path': str(_project(name)),
                },
                stats,
            )

        assert syft.scanned == ['a']
        assert (stats.generated, stats.skipped) == (1, 1)
        assert _sbom('a').read_text() == syft_document('a')


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
        project = self._project(tmp_path)
        empty = tmp_path / 'empty.json'
        empty.touch()
        assert not is_current_sbom(empty, project)

    def test_an_sbom_cut_short_is_not_current(self, tmp_path) -> None:
        from chatsbom.services.sbom_service import is_current_sbom
        from tests.sbom_generate_test import cut_short
        from tests.sbom_generate_test import syft_document
        project = self._project(tmp_path)
        cut = tmp_path / 'cut.json'
        cut.write_text(cut_short(syft_document()))
        assert not is_current_sbom(cut, project)

    def test_a_whole_sbom_newer_than_its_content_is_current(
        self, tmp_path,
    ) -> None:
        from chatsbom.services.sbom_service import is_current_sbom
        from tests.sbom_generate_test import syft_document
        project = self._project(tmp_path)
        good = tmp_path / 'good.json'
        good.write_text(syft_document())
        assert is_current_sbom(good, project)

    def test_content_newer_than_the_sbom_makes_it_stale(
        self, tmp_path,
    ) -> None:
        import os

        from chatsbom.services.sbom_service import is_current_sbom
        from tests.sbom_generate_test import syft_document
        project = self._project(tmp_path)
        good = tmp_path / 'good.json'
        good.write_text(syft_document())
        os.utime(good, (1_000_000, 1_000_000))
        assert not is_current_sbom(good, project)

    def test_the_gate_consults_it(self, syft) -> None:
        """A check nothing calls is the same as no check: `sbom generate`
        scans the root whose SBOM is zero bytes, and only that one."""
        from tests.sbom_generate_test import _downloaded
        from tests.sbom_generate_test import _generated
        from tests.sbom_generate_test import generate
        from tests.sbom_generate_test import syft_document
        _downloaded('a', 'b')
        _generated('a', '')
        _generated('b', syft_document('b'))

        result = generate()

        assert result.exit_code == 0, result.output
        assert syft.scanned == ['a']
