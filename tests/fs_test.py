"""Files are replaced whole or not at all, and a cut-short one is noticed.

Every collection stage skips work whose output already exists: SBOMs,
Syft cache entries, manifests, depgraph documents and trees. Each was
written with `open(path, 'w')`, which empties the file first and fills
it after. A kill, a full disk or an exception in between left a prefix,
and every later run trusted it. `db index` then failed that repository
on every run with "unreadable sbom" (#13).
"""
import errno
import stat
from pathlib import Path

import pytest

from chatsbom.core import fs
from chatsbom.core.fs import atomic_write_bytes
from chatsbom.core.fs import atomic_write_text
from chatsbom.core.fs import looks_like_whole_json_object
from chatsbom.core.fs import write_once


def _left_in(directory: Path) -> list[str]:
    return sorted(path.name for path in directory.iterdir())


def _fail_fsync(error: BaseException):
    def fsync(fd: int) -> None:
        raise error
    return fsync


class TestAWriteThatSucceeds:

    def test_the_file_holds_what_was_written(self, tmp_path):
        target = tmp_path / 'sbom.json'
        atomic_write_text(target, '{"artifacts": []}\n')
        assert target.read_text(encoding='utf-8') == '{"artifacts": []}\n'

        atomic_write_bytes(target, b'{}')
        assert target.read_bytes() == b'{}'

    def test_text_is_written_as_utf8(self, tmp_path):
        """Depgraph documents are dumped with `ensure_ascii=False`."""
        target = tmp_path / 'sbom.spdx.json'
        atomic_write_text(target, '{"name": "café"}')
        assert target.read_bytes() == '{"name": "café"}'.encode()

    def test_nothing_else_is_left_beside_it(self, tmp_path):
        atomic_write_text(tmp_path / 'go.mod', 'module example.com/m\n')
        assert _left_in(tmp_path) == ['go.mod']

    def test_the_parent_directory_is_created(self, tmp_path):
        target = tmp_path / '07-sbom' / 'python' / 'o' / 'r' / 'sbom.json'
        atomic_write_text(target, '{}')
        assert target.read_text(encoding='utf-8') == '{}'

    def test_a_new_file_gets_the_permissions_open_would_give_it(self, tmp_path):
        """Not `mkstemp`'s 0600. `sbom lock` mounts the content tree into
        a container that runs as `nobody` when the collector is root, and
        a manifest that only its owner can read is invisible there."""
        plain = tmp_path / 'plain'
        plain.write_text('x')
        atomic = tmp_path / 'atomic'
        atomic_write_text(atomic, 'x')
        assert (
            stat.S_IMODE(atomic.stat().st_mode)
            == stat.S_IMODE(plain.stat().st_mode)
        )


class TestAWriteThatFailsMidway:

    def test_a_full_disk_keeps_the_previous_content(self, tmp_path, full_disk):
        target = tmp_path / 'sbom.json'
        target.write_text('{"previous": true}')
        full_disk.fill(tmp_path)

        with pytest.raises(OSError) as raised:
            atomic_write_text(target, '{"artifacts": []}' * 100)

        assert raised.value.errno == errno.ENOSPC
        assert target.read_text() == '{"previous": true}'
        assert _left_in(tmp_path) == ['sbom.json']

    def test_a_full_disk_leaves_no_file_where_there_was_none(
        self, tmp_path, full_disk,
    ):
        full_disk.fill(tmp_path)

        with pytest.raises(OSError):
            atomic_write_bytes(tmp_path / 'go.sum', b'x' * 4096)

        assert _left_in(tmp_path) == []

    def test_a_failed_fsync_keeps_the_previous_content(
        self, tmp_path, monkeypatch,
    ):
        """Everything written, and then the disk fails to keep it.

        Replacing the file before its contents are known to be on disk
        would let a crash leave a new name over empty blocks.
        """
        target = tmp_path / 'index.json'
        target.write_text('{"previous": true}')
        monkeypatch.setattr(
            fs.os, 'fsync', _fail_fsync(OSError(errno.EIO, 'I/O error')),
        )

        with pytest.raises(OSError):
            atomic_write_text(target, '{"next": true}')

        assert target.read_text() == '{"previous": true}'
        assert _left_in(tmp_path) == ['index.json']

    def test_an_interrupt_is_cleaned_up_after_too(self, tmp_path, monkeypatch):
        """Ctrl-C is not an `Exception`; the temporary file goes all the same."""
        monkeypatch.setattr(fs.os, 'fsync', _fail_fsync(KeyboardInterrupt()))

        with pytest.raises(KeyboardInterrupt):
            atomic_write_text(tmp_path / 'tree.txt', 'README.md\n')

        assert _left_in(tmp_path) == []

    def test_it_writes_beside_the_target_under_a_name_no_glob_matches(
        self, tmp_path, full_disk,
    ):
        """Beside it, so the rename is within one filesystem and atomic.
        Never `*.json` or `*.jsonl`: `db raw` and `queue backfill` glob
        for ledgers by that, and `db edges` for depgraph documents."""
        full_disk.fill(tmp_path)

        with pytest.raises(OSError):
            atomic_write_text(tmp_path / 'java.jsonl', '{"id": 1}\n')

        [written] = full_disk.failed
        assert written.parent == tmp_path
        assert written.name != 'java.jsonl'
        assert not written.name.endswith(('.json', '.jsonl'))


class TestWriteOnce:
    """A file the store keeps for good: the release and commit decisions
    and the release lists (#147). Written whole, as `atomic_write_bytes`
    writes, and never over a file that is there."""

    def test_a_new_file_is_written_whole(self, tmp_path):
        target = tmp_path / '20260929T122814Z' / 'release@2.json'

        assert write_once(target, b'{"out": "v1.2.3"}\n') is True

        assert target.read_bytes() == b'{"out": "v1.2.3"}\n'
        assert _left_in(target.parent) == ['release@2.json']

    def test_a_file_that_is_there_is_left_as_it_is(self, tmp_path):
        target = tmp_path / 'commit@1.json'
        target.write_bytes(b'{"out": "first"}')

        assert write_once(target, b'{"out": "second"}') is False

        assert target.read_bytes() == b'{"out": "first"}'
        assert _left_in(tmp_path) == ['commit@1.json']

    def test_a_writer_that_loses_the_race_leaves_the_first_file(
        self, tmp_path, monkeypatch,
    ):
        """Another writer puts its file in place between this one's look
        and its link: a rename would replace that file, a link cannot."""
        target = tmp_path / 'release@2.json'
        real_link = fs.os.link

        def link(source, destination):
            Path(destination).write_bytes(b'{"out": "theirs"}')
            real_link(source, destination)

        monkeypatch.setattr(fs.os, 'link', link)

        assert write_once(target, b'{"out": "ours"}') is False

        assert target.read_bytes() == b'{"out": "theirs"}'
        assert _left_in(tmp_path) == ['release@2.json']

    def test_without_hard_links_it_is_renamed_into_place(
        self, tmp_path, monkeypatch,
    ):
        """A file system with no hard links (FAT, some network shares)
        refuses the link: the file is still written, where none is."""
        def refuse(source, destination):
            raise OSError(errno.EPERM, 'Operation not permitted')

        monkeypatch.setattr(fs.os, 'link', refuse)
        target = tmp_path / 'release@2.json'

        assert write_once(target, b'{}') is True
        assert target.read_bytes() == b'{}'
        assert write_once(target, b'{"other": 1}') is False
        assert target.read_bytes() == b'{}'
        assert _left_in(tmp_path) == ['release@2.json']

    def test_a_full_disk_leaves_nothing(self, tmp_path, full_disk):
        full_disk.fill(tmp_path)

        with pytest.raises(OSError):
            write_once(tmp_path / 'release@2.json', b'x' * 4096)

        assert _left_in(tmp_path) == []


class TestLooksLikeWholeJsonObject:
    """What `sbom generate` and `github depgraph` ask of a stored document
    before trusting it, once per repository, every run."""

    @pytest.mark.parametrize(
        'content',
        [
            '{}',
            '{"artifacts":[],"schema":{"version":"16.1.2"}}\n',
            '\n  {\n    "sbom": {}\n  }\n\n',
        ],
        ids=['smallest', 'as-syft-writes-it', 'pretty-printed'],
    )
    def test_a_whole_object_passes(self, tmp_path, content):
        path = tmp_path / 'sbom.json'
        path.write_text(content)
        assert looks_like_whole_json_object(path)

    @pytest.mark.parametrize(
        'content',
        [
            '',
            '{"artifacts":[{"id":"8f1b","name":"requests","purl":"pkg:py',
            '\0' * 4096,
            '{"artifacts":[]}' + '\0' * 512,
            '[]',
        ],
        ids=['empty', 'cut-short', 'nul-filled', 'nul-padded', 'not-an-object'],
    )
    def test_what_an_interrupted_write_leaves_does_not(self, tmp_path, content):
        """Empty, a prefix, or blocks of NULs: a crash can leave any of
        these, depending on the filesystem and on when it came."""
        path = tmp_path / 'sbom.json'
        path.write_text(content)
        assert not looks_like_whole_json_object(path)

    def test_a_missing_file_does_not(self, tmp_path):
        assert not looks_like_whole_json_object(tmp_path / 'absent.json')

    def test_a_directory_does_not_and_does_not_raise(self, tmp_path):
        """It reports 4096 bytes on Linux."""
        assert not looks_like_whole_json_object(tmp_path)
