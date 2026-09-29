"""Writing a file so that no reader sees it half written, and telling
when one was.

Every collection stage skips work whose output already exists, and that
output was written with `open(path, 'w')`, which empties the file first
and fills it after. A kill, a full disk or an exception in between left
a prefix, and every later run trusted it: an SBOM that `db index` then
failed on every run, a cache entry copied out as an SBOM, a manifest
that Syft scanned as it was.
"""
import contextlib
import errno
import os
import stat
import uuid
from pathlib import Path

#: Bytes read from each end of a file by `looks_like_whole_json_object`:
#: enough to see past the trailing newline and indentation that anything
#: here writes.
_EDGE = 64


def temporary_beside(path: Path) -> Path:
    """A name to write `path`'s content under before it takes its own.

    It starts with a dot and ends in `.tmp`, never `.json`, `.jsonl` or
    `.parquet`: `db raw` and `queue backfill` glob a stage's directory
    for `*.jsonl` ledgers, `db edges` globs for `*.json` documents, and
    the Parquet export's directory is one a person chose. It is unique
    per call, so two writers never share one, and it names no file
    anyone else would keep.
    """
    return path.with_name(f'.{path.name}.{uuid.uuid4().hex}.tmp')


def atomic_write_bytes(path: Path, data: bytes) -> None:
    """Replace `path` with `data` in one step.

    A reader sees the old file or the new one, never part of either. The
    data goes to a temporary file beside the target and is flushed and
    fsynced, and only then renamed over it. On any failure, Ctrl-C
    included, the temporary file is removed and the target is left as it
    was. Parent directories are created.

    The temporary file is named by `temporary_beside`.

    It is created by `open(..., 'x')` rather than `mkstemp`, so it gets
    the permissions any other file here gets. `mkstemp` makes 0600, and
    `sbom lock` reads the content tree from a container that runs as
    `nobody` when the collector is root.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = temporary_beside(path)
    handle = open(temporary, 'xb')
    try:
        with handle:
            handle.write(data)
            handle.flush()
            # On disk before it is renamed into place: otherwise a crash
            # can leave the new name pointing at blocks never written.
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        with contextlib.suppress(OSError):
            temporary.unlink()
        raise
    _sync_directory(path.parent)


def atomic_write_text(path: Path, text: str) -> None:
    """`atomic_write_bytes` for text, which is UTF-8 everywhere here."""
    atomic_write_bytes(path, text.encode('utf-8'))


#: What a file system with no hard links says when asked for one.
_NO_LINKS = frozenset({
    errno.EPERM, errno.EOPNOTSUPP, errno.ENOTSUP, errno.ENOSYS,
    errno.EMLINK,
})


def write_once(path: Path, data: bytes) -> bool:
    """Put `data` at `path` unless a file is there, and say whether it did.

    For what the store keeps for good, the release and commit decisions
    and the release lists (#147): whole or not at all, as
    `atomic_write_bytes` writes, and never over a file that is there. So
    it is linked into place rather than renamed: a rename replaces a
    file another writer put there meanwhile, and a link fails instead,
    leaving the first writer's. False when a file was there, which is
    left as it was.

    A file system with no hard links (FAT, some network shares) gets a
    rename, after a look that no file is there: the one case where two
    writers of one name at one moment could leave the second's.
    """
    if path.exists():
        return False
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = temporary_beside(path)
    handle = open(temporary, 'xb')
    try:
        with handle:
            handle.write(data)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            return False
        except OSError as error:
            if error.errno not in _NO_LINKS:
                raise
            if path.exists():
                return False
            os.replace(temporary, path)
    finally:
        with contextlib.suppress(OSError):
            temporary.unlink()
    _sync_directory(path.parent)
    return True


def _sync_directory(directory: Path) -> None:
    """Make the rename durable as well, where the platform allows it.

    The data is on disk once the file is fsynced, but the name pointing
    at it lives in the directory, and a crash before that reaches the
    disk can bring back the old name. This is best effort: the rename
    has already happened, and failing here would report a file that is
    in place as not written. Windows cannot open a directory at all.
    """
    try:
        fd = os.open(directory, os.O_RDONLY | getattr(os, 'O_DIRECTORY', 0))
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def looks_like_whole_json_object(path: Path) -> bool:
    """Whether `path` is a regular file holding what looks like one whole
    JSON object: `{` first and `}` last, ignoring whitespace.

    A write cut short keeps its start and loses its end, and a crash can
    also leave an empty file or blocks of NULs. None of those pass. Only
    a few bytes at each end are read, so this is cheap enough to ask of
    every stored document on every run. What it cannot see is a cut that
    lands just after a `}` inside the document; readers that parse the
    file still catch that.

    A regular file only: a directory reports 4096 bytes on Linux, and
    opening a FIFO would block.
    """
    try:
        status = path.stat()
        if not stat.S_ISREG(status.st_mode) or status.st_size == 0:
            return False
        # Unbuffered, or the first read would fill a whole buffer.
        with path.open('rb', buffering=0) as handle:
            head = handle.read(_EDGE)
            handle.seek(-min(status.st_size, _EDGE), os.SEEK_END)
            tail = handle.read()
    except OSError:
        return False
    return head.lstrip()[:1] == b'{' and tail.rstrip()[-1:] == b'}'


def is_whole_tree(path: Path) -> bool:
    """Whether a stored tree (`tree.txt`) was written to the end.

    Every path is written with a newline after it, so a file cut short
    mid-path does not end in one. The old in-place write was buffered, so
    one killed before its first flush left an empty file. That counts as
    cut short too: a commit with no files at all is rare enough that
    listing it again each run costs less than trusting what a crash left.

    Only the last byte is read, since this runs for every repository in
    the ledger.
    """
    try:
        with path.open('rb') as handle:
            handle.seek(-1, os.SEEK_END)
            return handle.read(1) == b'\n'
    except OSError:
        # Missing, a directory, or empty (seeking before the start fails).
        return False
