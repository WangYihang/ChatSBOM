import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import structlog

from chatsbom.models.language import Language

logger = structlog.get_logger('converter_service')


@dataclass
class ConversionResult:
    project_path: Path
    is_converted: bool = False
    is_skipped: bool = False
    is_failed: bool = False
    error_message: str | None = None
    output_size: int = 0
    elapsed_time: float = 0.0


class ConverterService:
    """Service for converting project manifests to SBOMs using Syft."""

    def __init__(self, base_dir: str = 'data/sbom'):
        self.base_dir = Path(base_dir)

    def find_projects(self, language: Language | None = None) -> list[Path]:
        """Finds project leaf directories where manifest files are stored."""
        if not self.base_dir.exists():
            return []

        # Projects are identified by directories containing metadata.json
        # Structure: base_dir / lang / owner / repo / ref / commit_sha

        # Actually, it's easier to just look for metadata.json and return its parent
        projects = []
        pattern = '**/*/metadata.json'
        for metadata_path in self.base_dir.glob(pattern):
            if language and metadata_path.relative_to(self.base_dir).parts[0] != language.value:
                continue
            projects.append(metadata_path.parent)

        return sorted(projects)

    def convert_to_sbom(self, project_dir: Path, output_format: str = 'json', overwrite: bool = False) -> ConversionResult:
        """Runs Syft on a project directory to generate an SBOM."""
        output_file = project_dir / 'sbom.json'
        result = ConversionResult(project_path=project_dir)

        if output_file.exists() and not overwrite:
            result.is_skipped = True
            return result

        # Command: syft dir:<path> -o <format>
        command = [
            'syft', f"dir:{project_dir.absolute()}", '-o', output_format,
        ]

        try:
            start_time = time.time()
            process = subprocess.run(
                command, capture_output=True, text=True, check=True,
            )
            result.elapsed_time = time.time() - start_time

            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(process.stdout)

            result.is_converted = True
            result.output_size = len(process.stdout)

            logger.debug(
                'SBOM Generated', project=str(
                    project_dir,
                ), size=result.output_size,
            )

        except subprocess.CalledProcessError as e:
            result.is_failed = True
            result.error_message = e.stderr
            logger.error(
                'Syft execution failed',
                project=str(project_dir), error=e.stderr,
            )
        except Exception as e:
            result.is_failed = True
            result.error_message = str(e)
            logger.error(
                'Unexpected error during conversion',
                project=str(project_dir), error=str(e),
            )

        return result
