"""Configuration management for ChatSBOM."""
import os
from dataclasses import dataclass
from dataclasses import field
from pathlib import Path
from typing import Literal


@dataclass
class PathConfig:
    """File path configuration with numbered pipeline stages."""
    base_data_dir: Path = field(default_factory=lambda: Path('data'))

    @property
    def search_dir(self) -> Path:
        return self.base_data_dir / '01-github-search'

    @property
    def repo_dir(self) -> Path:
        return self.base_data_dir / '02-github-repo'

    @property
    def release_dir(self) -> Path:
        return self.base_data_dir / '03-github-release'

    @property
    def commit_dir(self) -> Path:
        return self.base_data_dir / '04-github-commit'

    @property
    def tree_dir(self) -> Path:
        return self.base_data_dir / '05-github-tree'

    @property
    def content_dir(self) -> Path:
        return self.base_data_dir / '06-github-content'

    @property
    def sbom_dir(self) -> Path:
        return self.base_data_dir / '07-sbom'

    @property
    def framework_repos_dir(self) -> Path:
        return Path('.repositories')

    @property
    def cache_dir(self) -> Path:
        """Root directory for application-level cache (not requests-cache)."""
        return Path('.cache')

    # Cache directories - Mirroring GitHub API Structure
    # Pattern: .cache/api.github.com/repos/<owner>/<repo>/...

    def get_repo_cache_path(self, owner: str, repo: str) -> Path:
        """Cache path for GET /repos/{owner}/{repo}"""
        return self.cache_dir / 'api.github.com' / 'repos' / owner / repo / 'index.json'

    def get_release_cache_path(self, owner: str, repo: str) -> Path:
        """Cache path for GET /repos/{owner}/{repo}/releases"""
        return self.cache_dir / 'api.github.com' / 'repos' / owner / repo / 'releases' / 'index.json'

    def get_git_refs_cache_path(self, owner: str, repo: str) -> Path:
        """Cache path for GET /repos/{owner}/{repo}/git/refs"""
        return self.cache_dir / 'api.github.com' / 'repos' / owner / repo / 'git' / 'refs' / 'index.json'

    def get_tree_cache_path(self, owner: str, repo: str, ref: str, sha: str) -> Path:
        """Cache path for file tree (ls-tree) data."""
        return self.cache_dir / 'git-tree' / owner / repo / ref / sha / 'tree.txt'

    def get_sbom_cache_path(self, owner: str, repo: str, ref: str, content_hash: str) -> Path:
        """Cache path for Syft SBOM output based on repo and content hash."""
        return self.cache_dir / 'syft' / owner / repo / ref / f'{content_hash}.json'

    # List files (The "Ledgers")
    def get_search_list_path(self, language: str) -> Path:
        return self.search_dir / f'{language}.jsonl'

    def get_repo_list_path(self, language: str) -> Path:
        return self.repo_dir / f'{language}.jsonl'

    def get_release_list_path(self, language: str) -> Path:
        return self.release_dir / f'{language}.jsonl'

    def get_commit_list_path(self, language: str) -> Path:
        return self.commit_dir / f'{language}.jsonl'

    def get_content_list_path(self, language: str) -> Path:
        return self.content_dir / f'{language}.jsonl'

    def get_sbom_list_path(self, language: str) -> Path:
        return self.sbom_dir / f'{language}.jsonl'

    def get_tree_list_path(self, language: str) -> Path:
        return self.tree_dir / f'{language}.jsonl'

    def get_tree_file_path(self, language: str, owner: str, repo: str, ref: str, sha: str) -> Path:
        """Path for storing the file tree JSON for a specific commit."""
        return self.tree_dir / language / owner / repo / ref / sha / 'tree.txt'


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

    def __repr__(self) -> str:
        return (
            f"DatabaseConfig(host={self.host!r}, port={self.port!r}, "
            f"user={self.user!r}, password='*****', database={self.database!r}, "
            f"repositories_table={self.repositories_table!r}, artifacts_table={self.artifacts_table!r})"
        )

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
    cache_ttl: int = 60 * 60 * 24 * 7  # 7 days in seconds

    def __repr__(self) -> str:
        return (
            f"GitHubConfig(token='*****', api_base_url={self.api_base_url!r}, "
            f"default_delay={self.default_delay!r}, default_min_stars={self.default_min_stars!r}, "
            f"cache_ttl={self.cache_ttl!r})"
        )


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
