import contextlib
import hashlib
import json
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import structlog

from chatsbom.core.config import get_config
from chatsbom.core.fs import atomic_write_bytes
from chatsbom.core.fs import atomic_write_text
from chatsbom.core.fs import looks_like_whole_json_object
from chatsbom.core.sandbox import lock_recipe_for
from chatsbom.core.stats import BaseStats
from chatsbom.core.syft import check_syft_installed
from chatsbom.core.syft import get_syft_version
from chatsbom.models.language import Language

logger = structlog.get_logger('sbom_service')

#: Seconds a scan may run before it is killed and its repository counted
#: as failed. Without a limit, one hung scan held its worker for good.
DEFAULT_SYFT_TIMEOUT = 600

#: Top-level keys of every `syft -o json` document. A cache entry without
#: them is not one, whatever else it parses as.
SYFT_DOCUMENT_KEYS = frozenset({'artifacts', 'source', 'descriptor'})


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


#: Files whose *contents* decide what Syft reports. Everything else
#: contributes only its path and size, since Syft reads packages from
#: manifests and lockfiles — not from source.
MANIFEST_NAMES = frozenset({
    'Gemfile', 'Gemfile.lock', 'gems.locked',
    'package.json', 'package-lock.json', 'yarn.lock',
    'pnpm-lock.yaml', 'npm-shrinkwrap.json', 'bun.lock', 'bun.lockb',
    'go.mod', 'go.sum', 'vendor/modules.txt',
    'Gopkg.toml', 'Gopkg.lock', 'glide.yaml', 'glide.lock',
    'Cargo.toml', 'Cargo.lock',
    'pyproject.toml', 'poetry.lock', 'uv.lock', 'Pipfile', 'Pipfile.lock',
    'setup.py', 'setup.cfg', 'pdm.lock',
    'composer.json', 'composer.lock',
    'pom.xml', 'build.gradle', 'build.gradle.kts', 'gradle.lockfile',
    'mix.exs', 'mix.lock', 'pubspec.yaml', 'pubspec.lock',
    'Package.swift', 'Package.resolved', 'Podfile', 'Podfile.lock',
    'conanfile.txt', 'conan.lock', 'vcpkg.json',
    'DESCRIPTION', 'renv.lock', 'cabal.project.freeze', 'stack.yaml.lock',
})

MANIFEST_SUFFIXES = ('.gemspec', 'requirements.txt', '.csproj', '.fsproj')

_HASH_CHUNK = 1 << 16


def _reads_contents(relative_path: str, name: str) -> bool:
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
    """Whether an existing output file can be skipped over.

    Size alone was not enough. A write killed midway, or cut off by a
    full disk, leaves a prefix: not empty, so it passed, and not JSON, so
    `db index` failed that repository on every run. So the file must also
    look like one whole JSON object, `{` first and `}` last.

    The ends rather than a full parse, because `sbom generate` asks this
    of every SBOM in a language's ledger before it scans anything: tens
    of thousands of files, 16 GB in all, and a parse would read every
    byte of them to decide what to skip. Parsing only the small ones
    would make the cost depend on the corpus and still leave the large
    ones to this check. Reading a few bytes at each end costs next to
    nothing.

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


def _lockfiles_to_merge(
    lock_dir: Path | None, project_dir: Path, language: str,
) -> tuple[Path, ...]:
    """The lockfiles from `sbom lock` that a scan of `project_dir` takes.

    Only names the language's recipe produces, and only regular files
    (see `LockRecipe.generated_in`). Every file in the directory used to
    be copied over the project, links followed, although the resolver
    that wrote them ran project-controlled code.

    Never a name the project already has: its own lockfile is what it
    pins. `sbom lock` used to resolve such projects as well, and the
    copies it left pin whatever the registry offered that day.
    Reproduced: a committed `composer.lock` pinning x/y 1.0.0 was
    scanned as the 1.9.3 of the resolved copy.

    A language without a recipe takes nothing: what the withdrawn Java
    and Python recipes left is nothing Syft reads.
    """
    if lock_dir is None or not lock_dir.is_dir():
        return ()
    try:
        recipe = lock_recipe_for(Language(language))
    except ValueError:
        return ()

    shipped = recipe.shipped_by(project_dir)
    generated = recipe.generated_in(lock_dir)
    kept = [lock.name for lock in generated if lock.name in shipped]
    if kept:
        logger.info(
            'Ships a lockfile; the generated one is not merged',
            project=str(project_dir), files=kept,
        )
    return tuple(lock for lock in generated if lock.name not in shipped)


class SbomService:
    """Service for generating SBOMs from raw content using Syft."""

    def __init__(self):
        check_syft_installed()
        self.config = get_config()
        self.syft_version = get_syft_version()
        logger.info('Syft detected', version=self.syft_version or 'unknown')

    def _calculate_dir_hash(self, directory: Path) -> str:
        """Cache key for a downloaded project tree."""
        return content_fingerprint(directory)

    def process_repo(
        self,
        repo_dict: dict,
        stats: SbomStats,
        language: str,
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

        # Skip if a *usable* SBOM exists at the target path.
        #
        # `exists()` alone counted a zero-byte file as done, so an
        # interrupted syft write poisoned that repository permanently:
        # every later run skipped it, and `db index` failed it with
        # `unreadable sbom ... Expecting value: line 1 column 1`. Two
        # repositories sat like that across the whole corpus —
        # `btmills/geopattern` and `layerJS/layerJS` — and only
        # `--force` over the entire language would have recovered
        # them.
        if not force and _is_usable_sbom(output_file):
            stats.inc_skipped()
            repo_dict['sbom_path'] = str(output_file)
            logger.info(
                'SYFT Command', command='SKIP', path=str(
                    output_file,
                ), elapsed='0.000s', _style='dim',
            )
            return repo_dict

        # A lockfile we resolved ourselves (see `sbom lock`) makes the
        # project scannable where it shipped none. Syft is pointed at a
        # merged tree, and the lockfile is part of the fingerprint so the
        # cache does not serve the pre-lockfile result. A project that
        # ships its own is scanned as it is.
        scan_dir = project_dir
        merged: tempfile.TemporaryDirectory | None = None
        locks = _lockfiles_to_merge(generated_lock_dir, project_dir, language)
        if locks:
            merged = tempfile.TemporaryDirectory(prefix='chatsbom-scan-')
            scan_dir = Path(merged.name) / 'project'
            shutil.copytree(project_dir, scan_dir)
            for lock in locks:
                shutil.copy2(lock, scan_dir / lock.name)
            logger.info(
                'Scanning with generated lockfile',
                repo=f"{repo_dict.get('owner')}/{repo_dict.get('repo')}",
                locks=[p.name for p in locks],
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
