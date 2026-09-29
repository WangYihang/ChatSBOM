import contextlib
import hashlib
import json
import shutil
import stat
import subprocess
import tempfile
import time
from dataclasses import dataclass
from enum import Enum
from functools import cache
from pathlib import Path

import structlog

from chatsbom.core.config import get_config
from chatsbom.core.discovery import MANIFEST_NAMES
from chatsbom.core.discovery import MANIFEST_SUFFIXES
from chatsbom.core.fs import atomic_write_bytes
from chatsbom.core.fs import atomic_write_text
from chatsbom.core.fs import looks_like_whole_json_object
from chatsbom.core.sandbox import recipes_for
from chatsbom.core.stats import BaseStats
from chatsbom.core.syft import check_syft_installed
from chatsbom.core.syft import get_syft_version

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


@dataclass
class SbomStats(BaseStats):
    generated: int = 0
    processing_time: float = 0.0

    def inc_generated(self, elapsed: float = 0.0):
        with self._lock:
            self.generated += 1
            self.processing_time += elapsed

    def inc_skipped(self, elapsed: float = 0.0):
        with self._lock:
            self.skipped += 1
            self.processing_time += elapsed

    def inc_failed(self, elapsed: float = 0.0):
        with self._lock:
            self.failed += 1
            self.processing_time += elapsed


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
    it before it can be skipped over (`is_current_sbom`).

    Size alone was not enough. A write killed midway, or cut off by a
    full disk, leaves a prefix: not empty, so it passed, and not JSON, so
    `db index` failed that repository on every run. So the file must also
    look like one whole JSON object, `{` first and `}` last.

    The ends rather than a full parse, because `sbom generate` asks this
    of every stored SBOM before it scans anything: tens of thousands of
    files, 16 GB in all, and a parse would read every byte of them to
    decide what to skip. Parsing only the small ones would make the cost
    depend on the corpus and still leave the large ones to this check.
    Reading a few bytes at each end costs next to nothing. The Syft
    version asked about next is read from the end for the same reason
    (`recorded_syft_version`).

    The price is a cut that lands just after a `}` inside the document.
    On a real 275 KB Syft document that is 1.1% of byte offsets, and one
    of its 67 page-aligned ones. `db index` parses every document and
    names any such file; delete it, and the next run regenerates it.
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
    `_is_usable_sbom` gives: `sbom generate` asks this of every stored
    SBOM before it scans anything. Syft writes its top-level keys in one
    order (artifacts, artifactRelationships, files, source, distro,
    descriptor, schema), and only the schema follows the descriptor,
    whose configuration block is most of it. So the descriptor lies a
    fixed distance from the end, whatever was scanned. Both Syfts, run
    as `sbom generate` runs them over documents of 5.9 KB to 1.06 MB (a
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


def running_syft_version() -> str | None:
    """The version of the Syft a scan would run, which a stored SBOM has
    to record to be current (`is_current_sbom`); None, with a warning
    the first time, if it cannot be told.

    Asked by `sbom generate` before its scan and by the service that
    runs it (`SbomService`); the warning is given once between them.
    """
    version = get_syft_version()
    if version is None:
        _warn_syft_version_unknown()
    return version


@cache
def _warn_syft_version_unknown() -> None:
    """Say, once a process, why no stored SBOM is regenerated for its
    Syft."""
    logger.warning(
        'Syft version unknown: stored SBOMs are judged by their times alone',
    )


def staleness(
    output_file: Path,
    project_dir: Path,
    lock_dir: Path | None = None,
    *,
    syft_version: str | None,
) -> Stale | None:
    """Why the SBOM stored at `output_file` is not current, or None while
    it is (`is_current_sbom`).

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


def is_current_sbom(
    output_file: Path,
    project_dir: Path,
    lock_dir: Path | None = None,
    *,
    syft_version: str | None,
) -> bool:
    """Whether a stored SBOM can be skipped: whole, written by the Syft
    now running (`syft_version`), and newer than every file it was
    generated from. `staleness` says which of them it is not.

    The version, because the same files scanned by two versions are two
    different SBOMs: 1.52.0 leaves out yarn.lock's dev-only packages
    (138 rows to 70) and reads bun.lock, where 1.41.2 did neither.
    Skipped whichever Syft wrote them, the SBOMs an upgrade found kept
    the old one until their content changed, while each new root got
    the new one, and the corpus mixed the two. So an upgrade regenerates
    each stored SBOM once: in the next `sbom generate`, and in
    `chatsbom run` for each repository it walks. What that writes
    records the new version, and the run after skips it. Any other
    version is another, an older one too. It is the version the SBOM
    records itself (`recorded_syft_version`), and a whole document that
    records none is not current either. The Syft cache is keyed by
    version for the same reason (`get_sbom_cache_path`).

    With the running version unknown (None), the times alone decide, as
    they did before, and `running_syft_version` says so. Judged against
    no version, every SBOM would be regenerated by a Syft that cannot
    say what it is, and judged against none again on the next run: the
    whole corpus, on every run, for as long as `syft version` fails.

    The content stage adds manifests to a content root that already has
    an SBOM: the same commit, more of its files. Skipping on the SBOM's
    existence alone kept the root-only scan for good. Times rather than
    a recorded fingerprint, so an SBOM written before this check whose
    inputs have not changed since is still skipped. Files are written by
    rename, which gives each a fresh time, and never touched again.
    """
    return staleness(
        output_file, project_dir, lock_dir, syft_version=syft_version,
    ) is None


class SbomService:
    """Service for generating SBOMs from raw content using Syft."""

    def __init__(self):
        check_syft_installed()
        self.config = get_config()
        self.syft_version = running_syft_version()
        logger.info('Syft detected', version=self.syft_version or 'unknown')

    def _calculate_dir_hash(self, directory: Path) -> str:
        """Cache key for a downloaded project tree."""
        return content_fingerprint(directory)

    def process_repo(
        self,
        repo_dict: dict,
        stats: SbomStats,
        force: bool = False,
        generated_lock_dir: Path | None = None,
        syft_timeout: float = DEFAULT_SYFT_TIMEOUT,
    ) -> dict | None:
        """
        Generate SBOM for a single repository based on local content.
        Expects 'local_content_path' in repo_dict.
        """
        local_path_str = repo_dict.get('local_content_path')
        if not local_path_str:
            stats.inc_skipped()
            logger.warning(
                'Missing local_content_path',
                repo=f"{repo_dict.get('owner')}/{repo_dict.get('repo')}",
            )
            return None

        project_dir = Path(local_path_str)
        if not project_dir.exists():
            logger.warning(f"Content path missing: {project_dir}")
            stats.inc_failed()
            return None

        # Determine output path: data/07-sbom/<repository_id>/<sha>/sbom.json,
        # mirroring the content root it is generated from.
        try:
            rel_path = project_dir.relative_to(self.config.paths.content_dir)
            output_dir = self.config.paths.sbom_dir / rel_path
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / 'sbom.json'
        except ValueError:
            logger.error(f"Invalid content path structure: {project_dir}")
            stats.inc_failed()
            return None

        # Skip if the SBOM at the target path is current: *usable*,
        # written by this Syft, and newer than its content
        # (`is_current_sbom`).
        #
        # `exists()` alone counted a zero-byte file as done, so an
        # interrupted syft write poisoned that repository permanently:
        # every later run skipped it, and `db index` failed it with
        # `unreadable sbom ... Expecting value: line 1 column 1`. Two
        # repositories sat like that across the whole corpus —
        # `btmills/geopattern` and `layerJS/layerJS` — and only
        # `--force` over the entire language would have recovered
        # them.
        if not force:
            stale = staleness(
                output_file, project_dir, generated_lock_dir,
                syft_version=self.syft_version,
            )
            if stale is None:
                stats.inc_skipped()
                repo_dict['sbom_path'] = str(output_file)
                logger.info(
                    'SYFT Command', command='SKIP', path=str(
                        output_file,
                    ), elapsed='0.000s', _style='dim',
                )
                return repo_dict
            if stale is Stale.ANOTHER_SYFT:
                # Every stored SBOM, after an upgrade: each says why.
                written_by = recorded_syft_version(output_file)
                logger.info(
                    'SBOM written by another Syft',
                    path=str(output_file),
                    written_by=written_by or 'unknown',
                    running=self.syft_version,
                )

        # A lockfile we resolved ourselves (see `sbom lock`) makes the
        # project scannable where it shipped none. Syft is pointed at a
        # merged tree, and the lockfile is part of the fingerprint so the
        # cache does not serve the pre-lockfile result. A project that
        # ships its own is scanned as it is.
        scan_dir = project_dir
        merged: tempfile.TemporaryDirectory | None = None
        locks = _lockfiles_to_merge(generated_lock_dir, project_dir)
        if locks:
            merged = tempfile.TemporaryDirectory(prefix='chatsbom-scan-')
            scan_dir = Path(merged.name) / 'project'
            shutil.copytree(project_dir, scan_dir)
            for lock, relative in locks:
                shutil.copy2(lock, scan_dir.joinpath(*relative.split('/')))
            logger.info(
                'Scanning with generated lockfile',
                repo=f"{repo_dict.get('owner')}/{repo_dict.get('repo')}",
                locks=[relative for _, relative in locks],
            )

        try:
            return self._run_syft(
                repo_dict, stats, scan_dir, output_file, rel_path, force,
                syft_timeout,
            )
        finally:
            if merged is not None:
                merged.cleanup()

    def _run_syft(
        self,
        repo_dict: dict,
        stats: SbomStats,
        project_dir: Path,
        output_file: Path,
        rel_path: Path,
        force: bool,
        syft_timeout: float,
    ) -> dict | None:
        # Global Cache Check
        content_hash = self._calculate_dir_hash(project_dir)

        # The repository is the content root's first part:
        # <repository_id>/<sha>. The ref is not part of the key: the
        # content hash already identifies the input.
        parts = rel_path.parts
        repository_id = (
            int(parts[0]) if parts and parts[0].isdigit()
            else int(repo_dict.get('id') or 0)
        )

        cache_path = self.config.paths.get_sbom_cache_path(
            repository_id, content_hash, self.syft_version,
        )

        cached = None if force else _cached_sbom(cache_path)
        if cached is not None:
            try:
                atomic_write_bytes(output_file, cached)

                stats.inc_cache_hits()
                stats.inc_generated()  # It's still a generated SBOM for this repo
                repo_dict['sbom_path'] = str(output_file)
                logger.info(
                    'SYFT Command',
                    command='CACHE',
                    hash=content_hash,
                    path=str(output_file),
                    _style='dim',
                )
                return repo_dict
            except Exception as e:
                logger.warning(f"Failed to use global cache: {e}")

        # Run Syft
        command = ['syft', f"dir:{project_dir.absolute()}", '-o', 'json']

        start_time = time.time()
        try:
            process = subprocess.run(
                command, capture_output=True, text=True, check=True,
                timeout=syft_timeout,
            )
            elapsed = time.time() - start_time

            # Both written whole or not at all. Written in place, a kill
            # or a full disk midway left a prefix, which the next run
            # took for a finished SBOM or a cache hit.
            atomic_write_text(output_file, process.stdout)

            try:
                atomic_write_text(cache_path, process.stdout)
            except Exception as e:
                logger.warning(f"Failed to save to global cache: {e}")

            stats.inc_generated(elapsed)
            repo_dict['sbom_path'] = str(output_file)

            logger.info(
                'SYFT Command',
                command=' '.join(command),
                path=str(output_file),
                returncode=process.returncode,
                size=len(process.stdout),
                elapsed=f"{elapsed:.3f}s",
            )
            return repo_dict

        except subprocess.TimeoutExpired:
            # `subprocess.run` has killed the scan by now. One repository
            # fails, and its worker moves on to the next.
            elapsed = time.time() - start_time
            stats.inc_failed(elapsed)
            logger.error(
                'SYFT Command Timed Out',
                command=' '.join(command),
                timeout=f"{syft_timeout}s",
                elapsed=f"{elapsed:.3f}s",
                _style='bold red',
            )
            return None
        except subprocess.CalledProcessError as e:
            elapsed = time.time() - start_time
            stats.inc_failed(elapsed)
            logger.error(
                'SYFT Command Failed',
                command=' '.join(command),
                returncode=e.returncode,
                error_output=e.stderr,
                elapsed=f"{elapsed:.3f}s",
                _style='bold red',
            )
            return None
        except Exception as e:
            stats.inc_failed()
            logger.error(
                'Error generating SBOM',
                error=str(e),
                _style='bold red',
            )
            return None
