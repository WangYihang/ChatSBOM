"""Data access layer for ChatSBOM database operations."""
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from chatsbom.core.config import DatabaseConfig
from chatsbom.core.schema import ARTIFACTS_DDL
from chatsbom.core.schema import REPOSITORIES_DDL


class SBOMRepository:
    """Repository pattern for SBOM database operations."""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._client: Client | None = None

    @property
    def client(self) -> Client:
        """Get or create database client."""
        if self._client is None:
            self._client = clickhouse_connect.get_client(
                **self.config.get_connection_params(),
            )
        return self._client

    def close(self) -> None:
        """Close database connection."""
        if self._client is not None:
            self._client.close()
            self._client = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    # Schema management
    def create_tables(self) -> None:
        """Create database tables if they don't exist."""
        self.client.command(REPOSITORIES_DDL)
        self.client.command(ARTIFACTS_DDL)

    def drop_tables(self) -> None:
        """Drop all tables."""
        self.client.command(
            f'DROP TABLE IF EXISTS {self.config.artifacts_table}',
        )
        self.client.command(
            f'DROP TABLE IF EXISTS {self.config.repositories_table}',
        )

    # Repository operations
    def insert_repositories(self, data: list[list[Any]], columns: list[str]) -> None:
        """Insert repository data in batch."""
        if not data:
            return
        self.client.insert(
            self.config.repositories_table,
            data,
            column_names=columns,
        )

    def get_repository_count(self) -> int:
        """Get total number of repositories."""
        result = self.client.query(
            f'SELECT count() FROM {self.config.repositories_table}',
        )
        return result.result_rows[0][0]

    def get_repositories_by_language(self) -> list[tuple[str, int]]:
        """Get repository count grouped by language."""
        query = f"""
        SELECT language, count() as cnt
        FROM {self.config.repositories_table}
        GROUP BY language
        ORDER BY cnt DESC
        """
        result = self.client.query(query)
        return result.result_rows

    def get_top_repositories(self, limit: int = 10, language: str | None = None) -> list[dict]:
        """Get top repositories by stars."""
        where_clause = f"WHERE language = '{language}'" if language else ''
        query = f"""
        SELECT full_name, stars, language, description
        FROM {self.config.repositories_table}
        {where_clause}
        ORDER BY stars DESC
        LIMIT {limit}
        """
        result = self.client.query(query)
        return [
            {
                'full_name': row[0],
                'stars': row[1],
                'language': row[2],
                'description': row[3],
            }
            for row in result.result_rows
        ]

    # Artifact operations
    def insert_artifacts(self, data: list[list[Any]], columns: list[str]) -> None:
        """Insert artifact data in batch."""
        if not data:
            return
        self.client.insert(
            self.config.artifacts_table,
            data,
            column_names=columns,
        )

    def get_artifact_count(self) -> int:
        """Get total number of artifacts."""
        result = self.client.query(
            f'SELECT count() FROM {self.config.artifacts_table}',
        )
        return result.result_rows[0][0]

    def search_dependencies(
        self,
        library_name: str,
        language: str | None = None,
        limit: int = 50,
    ) -> list[dict]:
        """Search for repositories that depend on a specific library."""
        # First, find matching artifacts
        lang_filter = f"AND r.language = '{language}'" if language else ''
        pattern = f"%{library_name}%"

        query = f"""
        SELECT DISTINCT
            r.full_name,
            r.stars,
            r.language,
            a.name,
            a.version
        FROM {self.config.artifacts_table} a
        JOIN {self.config.repositories_table} r ON a.repository_id = r.id
        WHERE a.name LIKE %(pattern)s
        {lang_filter}
        ORDER BY r.stars DESC
        LIMIT {limit}
        """

        result = self.client.query(query, parameters={'pattern': pattern})
        return [
            {
                'full_name': row[0],
                'stars': row[1],
                'language': row[2],
                'artifact_name': row[3],
                'version': row[4],
            }
            for row in result.result_rows
        ]

    def get_top_dependencies(self, limit: int = 20, language: str | None = None) -> list[dict]:
        """Get most popular dependencies."""
        lang_filter = ''
        if language:
            lang_filter = f"""
            WHERE repository_id IN (
                SELECT id FROM {self.config.repositories_table}
                WHERE language = '{language}'
            )
            """

        query = f"""
        SELECT
            name,
            count(DISTINCT repository_id) as repo_count
        FROM {self.config.artifacts_table}
        {lang_filter}
        GROUP BY name
        ORDER BY repo_count DESC
        LIMIT {limit}
        """

        result = self.client.query(query)
        return [
            {
                'name': row[0],
                'repo_count': row[1],
            }
            for row in result.result_rows
        ]

    def get_framework_stats(self, framework_packages: list[str]) -> list[dict]:
        """Get statistics for specific framework packages."""
        packages_str = "', '".join(framework_packages)
        query = f"""
        SELECT
            a.name,
            count(DISTINCT a.repository_id) as repo_count,
            sum(r.stars) as total_stars
        FROM {self.config.artifacts_table} a
        JOIN {self.config.repositories_table} r ON a.repository_id = r.id
        WHERE a.name IN ('{packages_str}')
        GROUP BY a.name
        ORDER BY repo_count DESC
        """

        result = self.client.query(query)
        return [
            {
                'name': row[0],
                'repo_count': row[1],
                'total_stars': row[2],
            }
            for row in result.result_rows
        ]
