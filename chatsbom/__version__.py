"""Version information for ChatSBOM."""
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version


def get_version() -> str:
    """
    Get version from installed package metadata.

    Returns:
        Version string (e.g., "0.2.6")
    """
    try:
        return version('chatsbom')
    except PackageNotFoundError:
        return '0.0.0-dev'


__version__ = get_version()
