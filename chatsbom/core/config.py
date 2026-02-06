"""Configuration management for ChatSBOM."""
import os
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path


@dataclass
class PathConfig:
    """File path configuration."""

    # Base directories
    data_dir: Path = field(default_factory=lambda: Path('data'))
    output_dir: Path = field(default_factory=lambda: Path('.'))

    # File naming conventions
    sbom_filename: str = 'sbom.json'
    repo_list_pattern: str = '{language}.jsonl'

    def get_repo_list_path(self, language: str) -> Path:
        """Get the path for repository list file."""
        return self.output_dir / self.repo_list_pattern.format(language=language)

    def get_language_data_dir(self, language: str) -> Path:
        """Get the data directory for a specific language."""
        return self.data_dir / language

    def get_project_dir(self, language: str, owner: str, repo: str, branch: str = 'main') -> Path:
        """Get the project directory path."""
        return self.data_dir / language / owner / repo / branch

    def get_sbom_path(self, project_dir: Path) -> Path:
        """Get the SBOM file path for a project."""
        return project_dir / self.sbom_filename

    def find_all_sbom_files(self, language: str | None = None) -> list[Path]:
        """Find all SBOM files in the data directory."""
        if language:
            search_dir = self.get_language_data_dir(language)
        else:
            search_dir = self.data_dir

        if not search_dir.exists():
            return []

        return list(search_dir.rglob(self.sbom_filename))


@dataclass
class DatabaseConfig:
    """Database connection configuration."""

    host: str = field(
        default_factory=lambda: os.getenv(
            'CLICKHOUSE_HOST', 'localhost',
        ),
    )
    port: int = field(
        default_factory=lambda: int(
            os.getenv('CLICKHOUSE_PORT', '8123'),
        ),
    )
    user: str = field(
        default_factory=lambda: os.getenv(
            'CLICKHOUSE_USER', 'guest',
        ),
    )
    password: str = field(
        default_factory=lambda: os.getenv(
            'CLICKHOUSE_PASSWORD', 'guest',
        ),
    )
    database: str = field(
        default_factory=lambda: os.getenv('CLICKHOUSE_DB', 'sbom'),
    )

    # Table names
    repositories_table: str = 'repositories'
    artifacts_table: str = 'artifacts'

    def get_connection_params(self) -> dict:
        """Get connection parameters as a dictionary."""
        return {
            'host': self.host,
            'port': self.port,
            'username': self.user,
            'password': self.password,
            'database': self.database,
        }


@dataclass
class GitHubConfig:
    """GitHub API configuration."""

    token: str | None = field(
        default_factory=lambda: os.getenv('GITHUB_TOKEN'),
    )
    api_base_url: str = 'https://api.github.com'
    default_delay: float = 2.0
    default_min_stars: int = 1000


@dataclass
class ChatSBOMConfig:
    """Main configuration for ChatSBOM."""

    paths: PathConfig = field(default_factory=PathConfig)
    database: DatabaseConfig = field(default_factory=DatabaseConfig)
    github: GitHubConfig = field(default_factory=GitHubConfig)

    @classmethod
    def load(cls) -> 'ChatSBOMConfig':
        """Load configuration from environment variables."""
        return cls()


# Global config instance
_config: ChatSBOMConfig | None = None


def get_config() -> ChatSBOMConfig:
    """Get the global configuration instance."""
    global _config
    if _config is None:
        _config = ChatSBOMConfig.load()
    return _config


def reset_config() -> None:
    """Reset the global configuration (useful for testing)."""
    global _config
    _config = None
