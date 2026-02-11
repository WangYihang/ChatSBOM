import hashlib
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import structlog

from chatsbom.core.config import get_config
from chatsbom.core.stats import BaseStats
from chatsbom.core.syft import check_syft_installed

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


class SbomService:
    """Service for generating SBOMs from raw content using Syft."""

    def __init__(self):
        check_syft_installed()
        self.config = get_config()

    def _calculate_dir_hash(self, directory: Path) -> str:
        """
        Calculates a SHA256 hash of all files in the directory.
        Sorts filenames to ensure consistent hashing.
        """
        hasher = hashlib.sha256()
        # Get all files and sort them for consistent hashing
        files = sorted([f for f in directory.rglob('*') if f.is_file()])

        for file_path in files:
            # Update hash with relative path to ensure structure is captured
            rel_path = file_path.relative_to(directory)
            hasher.update(str(rel_path).encode())

            # Update hash with file content
            with open(file_path, 'rb') as f:
                while chunk := f.read(8192):
                    hasher.update(chunk)

        return hasher.hexdigest()

    def process_repo(self, repo_dict: dict, stats: SbomStats, language: str, force: bool = False) -> dict | None:
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

        # Global Cache Check
        content_hash = self._calculate_dir_hash(project_dir)
        cache_path = self.config.paths.get_sbom_cache_path(content_hash)

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
