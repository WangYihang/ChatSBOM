import subprocess
import time
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from threading import Lock

import structlog

from chatsbom.core.config import get_config

logger = structlog.get_logger('sbom_service')


@dataclass
class SbomStats:
    total: int = 0
    generated: int = 0
    skipped: int = 0
    failed: int = 0
    elapsed_time: float = 0.0
    _lock: Lock = field(default_factory=Lock)

    def inc_generated(self, elapsed: float = 0.0):
        with self._lock:
            self.generated += 1
            self.elapsed_time += elapsed

    def inc_skipped(self, elapsed: float = 0.0):
        with self._lock:
            self.skipped += 1
            self.elapsed_time += elapsed

    def inc_failed(self, elapsed: float = 0.0):
        with self._lock:
            self.failed += 1
            self.elapsed_time += elapsed


class SbomService:
    """Service for generating SBOMs from raw content using Syft."""

    def __init__(self):
        self.config = get_config()

    def process_repo(self, repo_dict: dict, stats: SbomStats, language: str) -> dict | None:
        """
        Generate SBOM for a single repository based on local content.
        Expects 'local_content_path' in repo_dict.
        """
        local_path_str = repo_dict.get('local_content_path')
        if not local_path_str:
            stats.inc_skipped()
            return None

        project_dir = Path(local_path_str)
        if not project_dir.exists():
            logger.warning(f"Content path missing: {project_dir}")
            stats.inc_failed()
            return None

        # Determine output path: data/06-sbom/<lang>/<owner>/<repo>/<ref>/<sha>/sbom.json
        # We can reconstruct it from the content path structure or use the repo dict
        # Structure of content path: .../05-github-content/<lang>/<owner>/<repo>/<ref>/<sha>
        try:
            rel_path = project_dir.relative_to(self.config.paths.content_dir)
            output_dir = self.config.paths.sbom_dir / rel_path
            output_dir.mkdir(parents=True, exist_ok=True)
            output_file = output_dir / 'sbom.json'
        except ValueError:
            logger.error(f"Invalid content path structure: {project_dir}")
            stats.inc_failed()
            return None

        # Skip if exists
        if output_file.exists():
            stats.inc_skipped()
            repo_dict['sbom_path'] = str(output_file)
            logger.info(
                'SYFT Command', command='SKIP', path=str(
                    output_file,
                ), elapsed='0.000s', _style='dim',
            )
            return repo_dict

        # Run Syft
        command = ['syft', f"dir:{project_dir.absolute()}", '-o', 'json']

        start_time = time.time()
        try:
            process = subprocess.run(
                command, capture_output=True, text=True, check=True,
            )
            elapsed = time.time() - start_time

            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(process.stdout)

            stats.inc_generated(elapsed)
            repo_dict['sbom_path'] = str(output_file)

            logger.info(
                'SYFT Command',
                command=' '.join(command),
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
