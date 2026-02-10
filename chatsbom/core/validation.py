"""Path validation utilities for ChatSBOM."""
import json
from pathlib import Path


class ValidationError(Exception):
    """Validation error."""


def validate_repo_list_file(file_path: Path) -> bool:
    """
    Validate repository list file format.

    Expected format: JSONL with required fields:
    - id: int
    - owner: str
    - name: str
    - stargazers_count: int
    - html_url: str
    - created_at: str

    Returns:
        True if valid

    Raises:
        ValidationError if invalid
    """
    if not file_path.exists():
        raise ValidationError(f"File does not exist: {file_path}")

    if not file_path.is_file():
        raise ValidationError(f"Not a file: {file_path}")

    if file_path.stat().st_size == 0:
        raise ValidationError(f"File is empty: {file_path}")

    # Check first line
    with open(file_path) as f:
        first_line = f.readline().strip()
        if not first_line:
            raise ValidationError(f"File is empty: {file_path}")

        try:
            data = json.loads(first_line)
        except json.JSONDecodeError as e:
            raise ValidationError(f"Invalid JSON in {file_path}: {e}")

        required_fields = [
            'id', 'owner', 'name',
            'stargazers_count', 'html_url', 'created_at',
        ]
        missing_fields = [
            field for field in required_fields if field not in data
        ]
        if missing_fields:
            raise ValidationError(
                f"Missing required fields in {file_path}: {missing_fields}",
            )

    return True


def validate_download_structure(data_dir: Path, language: str) -> bool:
    """
    Validate download directory structure.

    Expected structure:
    data/sbom/{language}/{owner}/{repo}/{branch}/[files]

    Returns:
        True if valid

    Raises:
        ValidationError if invalid
    """
    lang_dir = data_dir / language
    if not lang_dir.exists():
        raise ValidationError(f"Language directory does not exist: {lang_dir}")

    if not lang_dir.is_dir():
        raise ValidationError(f"Not a directory: {lang_dir}")

    # Check if there are any subdirectories (owners)
    subdirs = [d for d in lang_dir.iterdir() if d.is_dir()]
    if not subdirs:
        raise ValidationError(f"No owner directories found in: {lang_dir}")

    return True


def validate_sbom_file(sbom_path: Path) -> bool:
    """
    Validate SBOM file format.

    Expected: Valid JSON with 'artifacts' or 'components' field

    Returns:
        True if valid

    Raises:
        ValidationError if invalid
    """
    if not sbom_path.exists():
        raise ValidationError(f"SBOM file does not exist: {sbom_path}")

    if not sbom_path.is_file():
        raise ValidationError(f"Not a file: {sbom_path}")

    if sbom_path.stat().st_size == 0:
        raise ValidationError(f"SBOM file is empty: {sbom_path}")

    try:
        with open(sbom_path) as f:
            data = json.load(f)
    except json.JSONDecodeError as e:
        raise ValidationError(f"Invalid JSON in {sbom_path}: {e}")

    # Check for expected fields (syft format)
    if 'artifacts' not in data and 'components' not in data:
        raise ValidationError(
            f"SBOM file missing 'artifacts' or 'components' field: {sbom_path}",
        )

    return True


def count_repo_list_entries(file_path: Path) -> int:
    """Count number of entries in a repository list file."""
    if not file_path.exists():
        return 0

    count = 0
    with open(file_path) as f:
        for line in f:
            if line.strip():
                count += 1
    return count


def count_sbom_files(data_dir: Path, language: str | None = None) -> int:
    """Count number of SBOM files in the data directory."""
    if language:
        search_dir = data_dir / language
    else:
        search_dir = data_dir

    if not search_dir.exists():
        return 0

    return len(list(search_dir.rglob('sbom.json')))
