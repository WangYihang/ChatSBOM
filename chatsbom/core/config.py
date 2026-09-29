"""Configuration management for ChatSBOM."""
import os
from dataclasses import dataclass
from dataclasses import field
from datetime import date
from pathlib import Path
from typing import Literal

import dotenv


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
    def ledger_path(self) -> Path:
        """Per-repository collection state, for continuous operation."""
        return self.base_data_dir / 'ledger.sqlite3'

    @property
    def generated_lock_dir(self) -> Path:
        """Lockfiles we resolved ourselves, for projects that ship none."""
        return self.base_data_dir / '10-generated-lock'

    @property
    def depgraph_dir(self) -> Path:
        """GitHub's own dependency graph, a second SBOM source."""
        return self.base_data_dir / '09-github-depgraph'

    @property
    def global_repos_dir(self) -> Path:
        """Global cache of full git repositories."""
        return Path('~/.repositories').expanduser()

    @property
    def workspaces_dir(self) -> Path:
        """Temporary workspaces for version-specific analysis."""
        return Path('.workspaces')

    @property
    def framework_repos_dir(self) -> Path:
        return self.workspaces_dir

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

    def get_tree_cache_path(self, repository_id: int, sha: str) -> Path:
        """Cache path for file tree (ls-tree) data.

        Keyed by repository id and commit, like the stage directories:
        the ref is metadata of the download target, and two refs at one
        commit are one tree.
        """
        return self.cache_dir / 'git-tree' / str(int(repository_id)) / sha / 'tree.txt'

    def get_readme_cache_path(self, owner: str, repo: str, ref: str = 'default', sha: str = 'default') -> Path:
        """Cache path for GitHub README content."""
        return self.cache_dir / 'github-readme' / owner / repo / ref / sha / 'readme.md'

    def get_sbom_cache_path(
        self,
        repository_id: int,
        content_hash: str,
        syft_version: str | None = None,
    ) -> Path:
        """Cache path for Syft SBOM output.

        The Syft version is part of the key: the same content scanned by
        two versions yields two different SBOMs, and without this an
        upgrade would silently serve stale results from the old one.
        The ref is not: the content hash already identifies the input.
        """
        version = syft_version or 'unknown'
        return (
            self.cache_dir / 'syft' / version /
            str(int(repository_id)) / f'{content_hash}.json'
        )

    def get_classify_cache_path(self, owner: str, repo: str, model: str) -> Path:
        """Cache path for LLM classification results."""
        # Split model name for nesting
        model_parts = model.replace(':', '/').split('/')
        return self.cache_dir / 'github-classify' / Path(*model_parts) / owner / repo / 'index.json'

    def search_snapshot(self, day: date) -> Path:
        """An unfiltered search, as it stood on `day`: the repository list
        `queue track --snapshot` seeds the ledger from (design #55, §4.14).

        One file per day, never appended to by a later refresh: the
        search's resume stops at once on a file whose fewest stars
        already reach the minimum, so a refresh into an old snapshot
        would add nothing.
        """
        return self.search_dir / f'all-{day:%Y-%m-%d}.jsonl'

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

    def get_depgraph_list_path(self, language: str) -> Path:
        return self.depgraph_dir / f'{language}.jsonl'

    def get_tree_list_path(self, language: str) -> Path:
        return self.tree_dir / f'{language}.jsonl'

    # Stage artefacts, keyed by repository id and commit (#55, owner
    # decision D3). An id does not move when a repository is renamed or
    # transferred, and the ref is metadata of the download target: two
    # refs at one commit are one scan.
    #
    #     <stage>/<repository_id>/<sha>/...
    #
    # `core/layout.py` maps the language-keyed layout this replaced onto
    # these, for `data migrate-layout` and for records written before it.

    def tree_file(self, repository_id: int, sha: str) -> Path:
        """The file tree of one commit, one path per line."""
        return self.tree_dir / str(int(repository_id)) / sha / 'tree.txt'

    def discovery_file(self, repository_id: int, sha: str) -> Path:
        """`manifests.json`: which of the tree's manifests the content
        stage selected, what it fetched, and what it left out and why.
        Beside the tree it was read from, not in the content root, which
        `db raw` lands file by file as manifests."""
        return (
            self.tree_dir / str(int(repository_id)) / sha / 'manifests.json'
        )

    def content_root(self, repository_id: int, sha: str) -> Path:
        """The manifests downloaded for one commit, at their own paths."""
        return self.content_dir / str(int(repository_id)) / sha

    def sbom_file(self, repository_id: int, sha: str) -> Path:
        """The Syft document of one commit's content root."""
        return self.sbom_dir / str(int(repository_id)) / sha / 'sbom.json'

    def generated_lock_path(self, repository_id: int, sha: str) -> Path:
        """Directory holding the lockfiles generated for one commit."""
        return self.generated_lock_dir / str(int(repository_id)) / sha

    def legacy_depgraph_file(self, repository_id: int) -> Path:
        """The one dependency graph kept per repository before every fetch
        was: read, never written. The depgraph stage keeps each fetch
        beside it instead; see `core/depgraph_store.py`."""
        return (
            self.depgraph_dir / str(int(repository_id)) / 'legacy' /
            'sbom.spdx.json'
        )


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


def _github_token() -> str | None:
    """GITHUB_TOKEN, without the whitespace around it.

    A token read from a file ends with the file's line ending when what
    read it kept it, a carriage return or a newline, and a header holding
    one is refused (#113). The commands that take `--token` clean what
    they are given the same way (`core/github.py`).
    """
    token = os.getenv('GITHUB_TOKEN')
    return token.strip() if token is not None else None


@dataclass
class GitHubConfig:
    token: str | None = field(default_factory=_github_token)
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


def load_env_file() -> Path | None:
    """Load the `.env` nearest the working directory into the environment.

    Nearest as git finds `.git`: the working directory, or its closest
    parent that has one. So a project's settings follow the project,
    whether chatsbom runs from a checkout or from a pip, pipx or uvx
    install. A variable already in the environment is never replaced —
    an `export`, or a secret a service manager injects, is a decision;
    the file is a default.

    Returns the file loaded, or None when there is none.
    """
    found = dotenv.find_dotenv(usecwd=True)
    if not found:
        return None
    dotenv.load_dotenv(found, override=False)
    return Path(found)


_config: ChatSBOMConfig | None = None


def get_config() -> ChatSBOMConfig:
    global _config
    if _config is None:
        _config = ChatSBOMConfig.load()
    return _config
