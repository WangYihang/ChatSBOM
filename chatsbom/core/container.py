"""Dependency Injection Container."""
from typing import Optional

from chatsbom.core.config import ChatSBOMConfig
from chatsbom.core.config import get_config
from chatsbom.core.repository import IngestionRepository
from chatsbom.core.repository import QueryRepository
from chatsbom.services.collector_service import RepositoryCollectorService
from chatsbom.services.enrichment_service import EnrichmentService
from chatsbom.services.github_service import GitHubService
from chatsbom.services.indexer_service import IndexerService


class Container:
    """Simple DI Container to manage service lifecycles."""

    _instance: Optional['Container'] = None

    def __init__(self) -> None:
        self.config: ChatSBOMConfig = get_config()
        self._github_service: GitHubService | None = None
        self._enrichment_service: EnrichmentService | None = None
        self._indexer_service: IndexerService | None = None

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

    def get_enrichment_service(self, token: str | None = None) -> EnrichmentService:
        if not self._enrichment_service:
            gh = self.get_github_service(token)
            self._enrichment_service = EnrichmentService(gh)
        return self._enrichment_service

    def get_indexer_service(self) -> IndexerService:
        if not self._indexer_service:
            self._indexer_service = IndexerService()
        return self._indexer_service

    def create_collector_service(self, lang: str, min_stars: int, output_path: str, token: str | None = None) -> RepositoryCollectorService:
        """Factory for Collector (not singleton as it holds state per run)."""
        gh = self.get_github_service(token)
        return RepositoryCollectorService(gh, lang, min_stars, output_path)

# Global Accessor


def get_container() -> Container:
    return Container.get_instance()
