"""Dependency Injection Container."""
from typing import Optional

from chatsbom.core.config import ChatSBOMConfig
from chatsbom.core.config import get_config
from chatsbom.core.repository import IngestionRepository
from chatsbom.core.repository import QueryRepository
from chatsbom.services.commit_service import CommitService
from chatsbom.services.content_service import ContentService
from chatsbom.services.db_service import DbService
from chatsbom.services.github_service import GitHubService
from chatsbom.services.release_service import ReleaseService
from chatsbom.services.repo_service import RepoService
from chatsbom.services.sbom_service import SbomService
from chatsbom.services.search_service import SearchService


class Container:
    """Simple DI Container to manage service lifecycles."""

    _instance: Optional['Container'] = None

    def __init__(self) -> None:
        self.config: ChatSBOMConfig = get_config()
        self._github_service: GitHubService | None = None
        self._repo_service: RepoService | None = None
        self._release_service: ReleaseService | None = None
        self._commit_service: CommitService | None = None
        self._content_service: ContentService | None = None
        self._sbom_service: SbomService | None = None
        self._db_service: DbService | None = None

    @classmethod
    def get_instance(cls) -> 'Container':
        if cls._instance is None:
            cls._instance = Container()
        return cls._instance

    # -- Repositories --

    def get_ingestion_repository(self) -> IngestionRepository:
        """Get Write-Access Repository (Admin)."""
        db_config = self.config.get_db_config(role='admin')
        return IngestionRepository(db_config)

    def get_query_repository(self) -> QueryRepository:
        """Get Read-Only Repository (Guest)."""
        db_config = self.config.get_db_config(role='guest')
        return QueryRepository(db_config)

    # -- Services (Singletons) --

    def get_github_service(self, token: str | None = None) -> GitHubService:
        """Get GitHub Service. Token is required for first init if not in env."""
        if not self._github_service:
            api_token = token or self.config.github.token
            if not api_token:
                raise ValueError('GitHub Token is required')
            self._github_service = GitHubService(api_token)
        return self._github_service

    def get_repo_service(self, token: str | None = None) -> RepoService:
        if not self._repo_service:
            gh = self.get_github_service(token)
            self._repo_service = RepoService(gh)
        return self._repo_service

    def get_release_service(self, token: str | None = None) -> ReleaseService:
        if not self._release_service:
            gh = self.get_github_service(token)
            self._release_service = ReleaseService(gh)
        return self._release_service

    def get_commit_service(self, token: str | None = None) -> CommitService:
        if not self._commit_service:
            gh = self.get_github_service(token)
            self._commit_service = CommitService(gh)
        return self._commit_service

    def get_content_service(self, token: str | None = None) -> ContentService:
        if not self._content_service:
            token = token or self.config.github.token
            self._content_service = ContentService(token)
        return self._content_service

    def get_sbom_service(self) -> SbomService:
        if not self._sbom_service:
            self._sbom_service = SbomService()
        return self._sbom_service

    def get_db_service(self) -> DbService:
        if not self._db_service:
            self._db_service = DbService()
        return self._db_service

    def create_search_service(self, lang: str, min_stars: int, output_path: str, token: str | None = None) -> SearchService:
        """Factory for SearchService (stateful)."""
        gh = self.get_github_service(token)
        return SearchService(gh, lang, min_stars, output_path)

# Global Accessor


def get_container() -> Container:
    return Container.get_instance()
