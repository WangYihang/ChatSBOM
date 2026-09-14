import hashlib
import shutil
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path

import structlog

from chatsbom.core.config import get_config
from chatsbom.core.stats import BaseStats
from chatsbom.core.syft import check_syft_installed
from chatsbom.core.syft import get_syft_version

logger = structlog.get_logger('sbom_service')


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

        # Determine output path: data/07-sbom/<lang>/<owner>/<repo>/<ref>/<sha>/sbom.json
        try:
            rel_path = project_dir.relative_to(self.config.paths.content_dir)
            output_dir = self.config.paths.sbom_dir / rel_path
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / 'sbom.json'
        except ValueError:
            logger.error(f"Invalid content path structure: {project_dir}")
            stats.inc_failed()
            return None

        # Skip if exists at the target path
        if not force and output_file.exists():
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
        # cache does not serve the pre-lockfile result.
        scan_dir = project_dir
        merged: tempfile.TemporaryDirectory | None = None
        if generated_lock_dir and generated_lock_dir.is_dir():
            locks = [p for p in generated_lock_dir.iterdir() if p.is_file()]
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
    ) -> dict | None:
        # Global Cache Check
        content_hash = self._calculate_dir_hash(project_dir)

        # Extract metadata from rel_path: <lang>/<owner>/<repo>/<ref>/<sha>
        parts = rel_path.parts
        owner = parts[1] if len(
            parts,
        ) > 1 else repo_dict.get('owner', 'unknown')
        repo_name = parts[2] if len(
            parts,
        ) > 2 else repo_dict.get('repo', 'unknown')
        ref = parts[3] if len(parts) > 3 else 'unknown'

        cache_path = self.config.paths.get_sbom_cache_path(
            owner, repo_name, ref, content_hash, self.syft_version,
        )

        if not force and cache_path.exists():
            try:
                # Copy from cache to output file
                with open(cache_path, encoding='utf-8') as f_in:
                    content = f_in.read()
                with open(output_file, 'w', encoding='utf-8') as f_out:
                    f_out.write(content)

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
            )
            elapsed = time.time() - start_time

            # Save to output file
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(process.stdout)

            # Save to global cache
            try:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                with open(cache_path, 'w', encoding='utf-8') as f:
                    f.write(process.stdout)
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
