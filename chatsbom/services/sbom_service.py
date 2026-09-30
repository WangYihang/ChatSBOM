"""What the SBOM stage asks of an SBOM, apart from running Syft: the
Syft cache's key and its entries, the directory Syft scans, with the
lockfiles `sbom lock` generated merged in, and whether a stored SBOM is
current.

The old pipeline's `sbom generate` ran Syft from here too
(`SbomService`); the collector runs it in its pool
(`collector/syftpool.py`), and the service went with that pipeline
(#171).
"""
import contextlib
import hashlib
import json
import shutil
import stat
import tempfile
from collections.abc import Iterator
from enum import Enum
from pathlib import Path

import structlog

from chatsbom.core.discovery import MANIFEST_NAMES
from chatsbom.core.discovery import MANIFEST_SUFFIXES
from chatsbom.core.fs import looks_like_whole_json_object
from chatsbom.core.sandbox import recipes_for

logger = structlog.get_logger('sbom_service')

#: Seconds a scan may run before it is killed and its repository counted
#: as failed. Without a limit, one hung scan held its worker for good.
DEFAULT_SYFT_TIMEOUT = 600

#: Top-level keys of every `syft -o json` document. A cache entry without
#: them is not one, whatever else it parses as.
SYFT_DOCUMENT_KEYS = frozenset({'artifacts', 'source', 'descriptor'})

#: Bytes read from the end of a stored SBOM for the Syft version its
#: descriptor records (`recorded_syft_version`): over three times as far
#: as the descriptor was found from the end.
DESCRIPTOR_WINDOW = 16 * 1024

#: The most read for it, when the descriptor is not in the window.
DESCRIPTOR_WINDOW_LIMIT = 1024 * 1024

#: The descriptor's key, as Syft writes it.
_DESCRIPTOR_KEY = b'"descriptor"'

#: Parses the descriptor alone, where it begins, and not what follows.
_DECODER = json.JSONDecoder()


_HASH_CHUNK = 1 << 16


def _reads_contents(relative_path: str, name: str) -> bool:
    """Whether a file's *contents* decide what Syft reports: a name of
    the one manifest registry (`core/discovery.py`), which is also what
    the content stage downloads. Everything else contributes only its
    path and size, since Syft reads packages from manifests and
    lockfiles — not from source."""
    return name in MANIFEST_NAMES or relative_path.endswith(MANIFEST_SUFFIXES)


def content_fingerprint(directory: Path) -> str:
    """Stable digest of a project tree, for keying the SBOM cache.

    Hashing every byte of every file meant reading the whole tree twice:
    once here and once by Syft. Since Syft derives packages from manifests
    and lockfiles, only those are read in full; other files contribute
    their path and size, which still catches additions, removals, renames
    and edits that change length.
    """
    if not directory.is_dir():
        raise OSError(f"not a directory: {directory}")

    hasher = hashlib.sha256()

    for path in sorted(p for p in directory.rglob('*') if p.is_file()):
        relative = path.relative_to(directory).as_posix()
        hasher.update(relative.encode('utf-8'))
        hasher.update(b'\0')

        if _reads_contents(relative, path.name):
            with open(path, 'rb') as f:
                while chunk := f.read(_HASH_CHUNK):
                    hasher.update(chunk)
        else:
            hasher.update(str(path.stat().st_size).encode('ascii'))
        hasher.update(b'\n')

    return hasher.hexdigest()


def _is_usable_sbom(path: Path) -> bool:
    """Whether an existing output file is whole: the first thing asked of
    it before it can be skipped over (`staleness`).

    Size alone was not enough. A write killed midway, or cut off by a
    full disk, leaves a prefix: not empty, so it passed, and not JSON, so
    `db index` failed that repository on every run. So the file must also
    look like one whole JSON object, `{` first and `}` last.

    The ends rather than a full parse, because this is asked of every
    stored SBOM the collector's walk of its universe comes to
    (`collector/due.py`): tens of thousands of files, 16 GB in all, and a
    parse would read every byte of them to decide what to skip. Parsing only the small ones would make the cost
    depend on the corpus and still leave the large ones to this check.
    Reading a few bytes at each end costs next to nothing. The Syft
    version asked about next is read from the end for the same reason
    (`recorded_syft_version`).

    The price is a cut that lands just after a `}` inside the document.
    On a real 275 KB Syft document that is 1.1% of byte offsets, and one
    of its 67 page-aligned ones. `warehouse build` parses every document
    and names any such file; delete it, and the collector regenerates
    it.
    SBOMs are written atomically now, so only files from before that can
    be cut.

    A regular file only, because a directory reports 4096 bytes on Linux
    and would otherwise read as a finished SBOM — caught by the test for
    it rather than in the field.
    """
    return looks_like_whole_json_object(path)


def _cached_sbom(path: Path) -> bytes | None:
    """The Syft document cached at `path`, or None to scan afresh.

    Parsed in full, unlike `_is_usable_sbom`: a hit is read whole anyway
    to be copied out, and it stands in for a scan, so it has to be one.
    An entry that is empty, cut short or not a Syft document is logged
    and deleted, and the scan that follows writes a whole one in its
    place. Used whenever it existed, a zero-byte entry was copied out as
    the SBOM and counted as generated on every run.

    An entry that cannot be read at all is left alone: that says nothing
    about what is in it.
    """
    try:
        raw = path.read_bytes()
    except FileNotFoundError:
        return None
    except OSError as error:
        logger.warning(
            'Unreadable Syft cache entry', path=str(path), error=str(error),
        )
        return None

    try:
        document = json.loads(raw)
    except ValueError:
        document = None
    if isinstance(document, dict) and SYFT_DOCUMENT_KEYS <= document.keys():
        return raw

    logger.warning(
        'Discarding unusable Syft cache entry', path=str(path), size=len(raw),
    )
    with contextlib.suppress(OSError):
        path.unlink()
    return None


def _files_under(root: Path) -> list[str]:
    """Every regular file under `root`, as a repository path."""
    return sorted(
        path.relative_to(root).as_posix()
        for path in root.rglob('*') if path.is_file()
    )


def _lockfiles_to_merge(
    lock_dir: Path | None, project_dir: Path,
) -> tuple[tuple[Path, str], ...]:
    """The lockfiles from `sbom lock` that a scan of `project_dir` takes,
    each with the repository path it goes to.

    Per directory, as `sbom lock` resolved them (`recipes_for`): a
    generated `composer.lock` for `backend/` goes to `backend/`, beside
    the `composer.json` it was resolved from, and never to the root.

    Only directories that hold the recipe's manifest and ship none of
    its lockfiles, only names the recipe produces, and only regular
    files (see `LockRecipe.generated_in`). Every file in the directory
    used to be copied over the project, links followed, although the
    resolver that wrote them ran project-controlled code.

    Never a name the project already has: its own lockfile is what it
    pins. `sbom lock` used to resolve such projects as well, and the
    copies it left pin whatever the registry offered that day.
    Reproduced: a committed `composer.lock` pinning x/y 1.0.0 was
    scanned as the 1.9.3 of the resolved copy.

    An ecosystem without a recipe takes nothing: what the withdrawn
    Maven and PyPI recipes left is nothing Syft reads.
    """
    if lock_dir is None or not lock_dir.is_dir():
        return ()
    merged: list[tuple[Path, str]] = []
    for target in recipes_for(_files_under(project_dir)):
        shipped = target.recipe.shipped_by(target.within(project_dir))
        for lock in target.recipe.generated_in(target.within(lock_dir)):
            if lock.name in shipped:
                logger.info(
                    'Ships a lockfile; the generated one is not merged',
                    project=str(project_dir), file=lock.name,
                )
                continue
            relative = (
                f'{target.directory}/{lock.name}' if target.directory
                else lock.name
            )
            merged.append((lock, relative))
    return tuple(merged)


@contextlib.contextmanager
def scan_directory(
    project_dir: Path, lock_dir: Path | None, *, repo: str = '',
) -> Iterator[Path]:
    """The directory Syft scans for a content root: the root itself, or,
    where `sbom lock` generated lockfiles for it, a copy of it with them
    merged in (`_lockfiles_to_merge`), removed after.

    A lockfile we resolved ourselves makes the project scannable where
    it shipped none. Syft is pointed at the merged tree, and the
    lockfile is part of the fingerprint, so that the cache does not
    serve the scan from before it. A project that ships its own is
    scanned as it is.
    """
    locks = _lockfiles_to_merge(lock_dir, project_dir)
    if not locks:
        yield project_dir
        return
    with tempfile.TemporaryDirectory(prefix='chatsbom-scan-') as merged:
        scan_dir = Path(merged) / 'project'
        shutil.copytree(project_dir, scan_dir)
        for lock, relative in locks:
            shutil.copy2(lock, scan_dir.joinpath(*relative.split('/')))
        logger.info(
            'Scanning with generated lockfile',
            repo=repo, locks=[relative for _, relative in locks],
        )
        yield scan_dir


def _newest_mtime(root: Path | None) -> float:
    """The newest modification time of a file under `root`, or 0."""
    if root is None or not root.is_dir():
        return 0.0
    newest = 0.0
    for path in root.rglob('*'):
        try:
            if path.is_file():
                newest = max(newest, path.stat().st_mtime)
        except OSError:
            continue
    return newest


def recorded_syft_version(path: Path) -> str | None:
    """The version of the Syft that wrote the SBOM stored at `path`, as
    its descriptor records it; None if it records none, or none can be
    read.

    Read from the end rather than parsed, for the reason
    `_is_usable_sbom` gives: it is asked of every stored SBOM the
    collector's walk comes to. Syft writes its top-level keys in one
    order (artifacts, artifactRelationships, files, source, distro,
    descriptor, schema), and only the schema follows the descriptor,
    whose configuration block is most of it. So the descriptor lies a
    fixed distance from the end, whatever was scanned. Both Syfts, run
    as `sbom generate` ran them over documents of 5.9 KB to 1.06 MB (a
    requirements.txt; go.sum; yarn.lock; Gemfile.lock; uv.lock with a
    package-lock.json; all of them in one root), began its key 4,438
    bytes from the end on 1.41.2 and 4,948 on 1.52.0, every time. The
    environment moves it a little: the configuration names the home
    directory twice (a 200-character one moved it 412 bytes) and
    GOPROXY once, and Syft's indented output (`SYFT_FORMAT_PRETTY`) put
    it 6,418 bytes out on 1.52.0.

    So the last `DESCRIPTOR_WINDOW` bytes are read, over three times the
    furthest of those. Where the key is not among them, the last
    `DESCRIPTOR_WINDOW_LIMIT` bytes are, before the version is given up
    on: a later Syft whose configuration outgrew the window would
    otherwise have every SBOM it writes taken for another Syft's, and
    regenerated, on every run. The limit keeps a document that names no
    Syft at all from being read whole to find that out. And
    tests/sbom_test.py fails on a Syft that puts its descriptor past
    half the window, so that an upgrade moves the window with it rather
    than a second read becoming the rule.

    The last descriptor key is the one read: what a project's files say
    can reach an artifact's metadata, but only Syft's configuration and
    the schema come after its descriptor. Its object is parsed rather
    than matched, so the order of its keys and the spacing are Syft's to
    change. A regular file only, as for `looks_like_whole_json_object`:
    opening a FIFO would block.
    """
    tail, at = b'', -1
    try:
        status = path.stat()
        if not stat.S_ISREG(status.st_mode):
            return None
        # Unbuffered, or the first read would fill a whole buffer.
        with path.open('rb', buffering=0) as handle:
            for window in (DESCRIPTOR_WINDOW, DESCRIPTOR_WINDOW_LIMIT):
                start = max(0, status.st_size - window)
                handle.seek(start)
                tail = handle.read(status.st_size - start)
                at = tail.rfind(_DESCRIPTOR_KEY)
                if at >= 0 or start == 0:
                    break
    except OSError:
        return None
    if at < 0:
        return None
    return _syft_version_after(tail[at + len(_DESCRIPTOR_KEY):])


def _syft_version_after(rest: bytes) -> str | None:
    """The version in the Syft descriptor whose key `rest` follows, or
    None: not an object, another tool's, cut short, or no version."""
    try:
        text = rest.decode('utf-8').lstrip()
        if not text.startswith(':'):
            return None
        descriptor, _ = _DECODER.raw_decode(text[1:].lstrip())
    except ValueError:
        return None
    if not isinstance(descriptor, dict) or descriptor.get('name') != 'syft':
        return None
    version = descriptor.get('version')
    return version if isinstance(version, str) and version else None


class Stale(Enum):
    """Why a stored SBOM is not current (`staleness`)."""

    #: Missing, empty or cut short (`_is_usable_sbom`).
    UNUSABLE = 'unusable'
    #: Whole, but not written by the Syft now running: another version
    #: wrote it, or it does not say which (`recorded_syft_version`).
    ANOTHER_SYFT = 'another-syft'
    #: Whole and this Syft's, but older than a file it was generated from.
    INPUT_CHANGED = 'input-changed'


def staleness(
    output_file: Path,
    project_dir: Path,
    lock_dir: Path | None = None,
    *,
    syft_version: str | None,
) -> Stale | None:
    """Why the SBOM stored at `output_file` is not current, or None while
    it is: whole, written by the Syft now running (`syft_version`), and
    newer than every file it was generated from.

    The version, because the same files scanned by two versions are two
    different SBOMs: 1.52.0 leaves out yarn.lock's dev-only packages
    (138 rows to 70) and reads bun.lock, where 1.41.2 did neither.
    Skipped whichever Syft wrote them, the SBOMs an upgrade found kept
    the old one until their content changed, while each new root got
    the new one, and the corpus mixed the two. So an upgrade regenerates
    each stored SBOM once, as the collector comes to its repository
    (`collector/due.py`), and what that writes records the new version.
    Any other version is another, an older one too. It is the version
    the SBOM records itself (`recorded_syft_version`), and a whole
    document that records none is not current either. The Syft cache is
    keyed by version for the same reason (`get_sbom_cache_path`).

    With the running version unknown (None), the times alone decide.
    Judged against no version, every SBOM would be regenerated by a Syft
    that cannot say what it is, and judged against none again on the
    next walk: the whole corpus, for as long as `syft version` fails.

    The content stage adds manifests to a content root that already has
    an SBOM: the same commit, more of its files. Skipping on the SBOM's
    existence alone kept the root-only scan for good. Times rather than
    a recorded fingerprint, so an SBOM written before this check whose
    inputs have not changed since is still current. Files are written
    by rename, which gives each a fresh time, and never touched again.

    Asked cheapest first: the ends of the SBOM (`_is_usable_sbom`), its
    end again for the version, and only then the walk of the content
    root and the generated lockfiles, which lists every directory under
    them and stats every file. Measured warm, per root: 7 µs, 26 µs, and
    22 µs for a root of one file but 225 to 250 µs for one of 13 files
    in 7 directories. So a root regenerated for its Syft is never
    walked, and the version adds about 0.7 s to a pre-scan of 28,000
    current roots.
    """
    if not _is_usable_sbom(output_file):
        return Stale.UNUSABLE
    if (
        syft_version is not None
        and recorded_syft_version(output_file) != syft_version
    ):
        return Stale.ANOTHER_SYFT
    try:
        written = output_file.stat().st_mtime
    except OSError:
        return Stale.UNUSABLE
    newest = max(_newest_mtime(project_dir), _newest_mtime(lock_dir))
    return Stale.INPUT_CHANGED if newest > written else None
