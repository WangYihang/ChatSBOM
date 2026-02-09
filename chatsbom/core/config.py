"""Configuration management for ChatSBOM."""
import os
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Literal


@dataclass
class PathConfig:
    """File path configuration."""
    data_dir: Path = field(default_factory=lambda: Path('data/sbom'))
    output_dir: Path = field(default_factory=lambda: Path('.'))
    github_data_dir: Path = field(default_factory=lambda: Path('data/github'))
    sbom_filename: str = 'sbom.json'

    def get_repo_list_path(self, language: str, operation: str = 'collect') -> Path:
        if operation == 'enrich':
            return self.github_data_dir / 'enrich' / f'{language}.jsonl'
        return self.github_data_dir / 'collect' / f'{language}.jsonl'

    def get_language_data_dir(self, language: str) -> Path:
        return self.data_dir / language


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
    user: str = 'guest'
    password: str = 'guest'
    database: str = field(
        default_factory=lambda: os.getenv(
            'CLICKHOUSE_DB', 'chatsbom',
        ),
    )

    # Table names
    repositories_table: str = 'repositories'
    artifacts_table: str = 'artifacts'

    def get_connection_params(self) -> dict:
        return {
            'host': self.host,
            'port': self.port,
            'username': self.user,
            'password': self.password,
            'database': self.database,
        }


@dataclass
class GitHubConfig:
    token: str | None = field(
        default_factory=lambda: os.getenv('GITHUB_TOKEN'),
    )
    api_base_url: str = 'https://api.github.com'
    default_delay: float = 2.0
    default_min_stars: int = 1000


@dataclass
class ChatSBOMConfig:
    paths: PathConfig = field(default_factory=PathConfig)
    github: GitHubConfig = field(default_factory=GitHubConfig)

    # Base DB config (defaults to env vars)
    _db_base: DatabaseConfig = field(default_factory=DatabaseConfig)

    def get_db_config(self, role: Literal['admin', 'guest'] = 'guest') -> DatabaseConfig:
        """Get database configuration for a specific role."""
        config = DatabaseConfig(
            host=self._db_base.host,
            port=self._db_base.port,
            database=self._db_base.database,
        )
        if role == 'admin':
            config.user = os.getenv('CLICKHOUSE_ADMIN_USER', 'admin')
            config.password = os.getenv('CLICKHOUSE_ADMIN_PASSWORD', 'admin')
        else:
            config.user = os.getenv('CLICKHOUSE_GUEST_USER', 'guest')
            config.password = os.getenv('CLICKHOUSE_GUEST_PASSWORD', 'guest')
        return config

    @classmethod
    def load(cls) -> 'ChatSBOMConfig':
        return cls()


_config: ChatSBOMConfig | None = None


def get_config() -> ChatSBOMConfig:
    global _config
    if _config is None:
        _config = ChatSBOMConfig.load()
    return _config
