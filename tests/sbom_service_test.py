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


class TestTheOuterSkipSeesUnusableSboms:
    """`process_repo`'s check alone was unreachable.

    `generate.py` skips on `repo.id in storage.visited_ids` *before*
    calling `process_repo`, so a ledger entry pointing at a zero-byte
    SBOM short-circuited the very check meant to catch it. The fix
    committed first was therefore a no-op for the two repositories it
    was written for — found by trying to recover them, not by reading
    the diff.
    """

    def test_unusable_ids_finds_a_zero_byte_entry(self, tmp_path) -> None:
        import json
        from chatsbom.commands.sbom.generate import _unusable_ids
        empty = tmp_path / 'empty.json'
        empty.touch()
        good = tmp_path / 'good.json'
        good.write_text('{"artifacts": []}')
        ledger = tmp_path / 'javascript.jsonl'
        ledger.write_text(
            json.dumps({'id': 1, 'sbom_path': str(empty)}) + '\n'
            + json.dumps({'id': 2, 'sbom_path': str(good)}) + '\n',
        )
        assert _unusable_ids(ledger) == {1}

    def test_a_missing_ledger_is_not_an_error(self, tmp_path) -> None:
        from chatsbom.commands.sbom.generate import _unusable_ids
        assert _unusable_ids(tmp_path / 'absent.jsonl') == set()

    def test_a_malformed_line_does_not_lose_the_rest(self, tmp_path) -> None:
        """These ledgers are appended to by a long-running collector."""
        import json
        from chatsbom.commands.sbom.generate import _unusable_ids
        empty = tmp_path / 'empty.json'
        empty.touch()
        ledger = tmp_path / 'ledger.jsonl'
        ledger.write_text(
            'not json\n'
            + json.dumps({'id': 7, 'sbom_path': str(empty)}) + '\n',
        )
        assert _unusable_ids(ledger) == {7}

    def test_the_gate_consults_it(self) -> None:
        """A set nothing reads is the same as no set."""
        import inspect
        from chatsbom.commands.sbom import generate
        source = inspect.getsource(generate.main)
        assert 'repo.id not in unusable' in source
