import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

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
            stats.skipped += 1
            return None

        project_dir = Path(local_path_str)
        if not project_dir.exists():
            logger.warning(f"Content path missing: {project_dir}")
            stats.failed += 1
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
            stats.failed += 1
            return None

        # Skip if exists
        if output_file.exists():
            stats.skipped += 1
            repo_dict['sbom_path'] = str(output_file)
            return repo_dict

        # Run Syft
        command = ['syft', f"dir:{project_dir.absolute()}", '-o', 'json']

        try:
            start_time = time.time()
            process = subprocess.run(
                command, capture_output=True, text=True, check=True,
            )
            elapsed = time.time() - start_time
            stats.elapsed_time += elapsed

            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(process.stdout)

            stats.generated += 1
            repo_dict['sbom_path'] = str(output_file)

            logger.debug(
                'SBOM Generated',
                project=repo_dict.get('full_name'),
                size=len(process.stdout),
                elapsed=f"{elapsed:.2f}s",
            )
            return repo_dict

        except subprocess.CalledProcessError as e:
            stats.failed += 1
            logger.error(f"Syft failed for {project_dir}: {e.stderr}")
            return None
        except Exception as e:
            stats.failed += 1
            logger.error(f"Error generating SBOM for {project_dir}: {e}")
            return None
