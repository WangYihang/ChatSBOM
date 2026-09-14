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

    def test_an_unreadable_path_does_not_raise(self, tmp_path) -> None:
        """This runs once per repository inside a worker; an OSError
        here would abort the language rather than skip one row."""
        from chatsbom.services.sbom_service import _is_usable_sbom
        assert not _is_usable_sbom(tmp_path)  # a directory, not a file

    def test_the_skip_uses_it(self) -> None:
        """A helper nothing calls is the same as no helper."""
        import inspect
        from chatsbom.services.sbom_service import SbomService
        source = inspect.getsource(SbomService.process_repo)
        assert '_is_usable_sbom(output_file)' in source
        assert 'force and output_file.exists()' not in source
