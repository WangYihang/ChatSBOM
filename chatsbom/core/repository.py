"""Data access layer for ChatSBOM database operations."""
from collections.abc import Generator
from collections.abc import Iterator
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
    def insert_repositories(
        self,
        data: Iterator[list[Any]] | list[list[Any]],
        columns: list[str],
        batch_size: int = 1000,
    ) -> None:
        """
        Insert repository data in batches.

        Args:
            data: Iterator or list of rows to insert
            columns: Column names
            batch_size: Number of rows per batch
        """
        if isinstance(data, list):
            # If it's already a list, insert directly
            if not data:
                return
            self.client.insert(
                self.config.repositories_table,
                data,
                column_names=columns,
            )
        else:
            # If it's an iterator, batch the inserts
            batch = []
            for row in data:
                batch.append(row)
                if len(batch) >= batch_size:
                    self.client.insert(
                        self.config.repositories_table,
                        batch,
                        column_names=columns,
                    )
                    batch = []
            # Insert remaining rows
            if batch:
                self.client.insert(
                    self.config.repositories_table,
                    batch,
                    column_names=columns,
                )

    def get_repository_count(self) -> int:
        """Get total number of repositories."""
        result = self.client.query(
            f'SELECT count() FROM {self.config.repositories_table} FINAL',
        )
        return result.result_rows[0][0]

    def get_repositories_by_language(self) -> Generator[tuple[str, int], None, None]:
        """
        Get repository count grouped by language.

        Yields:
            Tuple of (language, count)
        """
        query = f"""
        SELECT language, count() as cnt
        FROM {self.config.repositories_table} FINAL
        GROUP BY language
        ORDER BY cnt DESC
        """
        result = self.client.query(query)
        for row in result.result_rows:
            yield (row[0], row[1])

    def get_top_repositories(
        self,
        limit: int = 10,
        language: str | None = None,
    ) -> Generator[dict, None, None]:
        """
        Get top repositories by stars.

        Args:
            limit: Maximum number of repositories to return
            language: Filter by language (optional)

        Yields:
            Dictionary with repository information
        """
        where_clause = f"WHERE language = '{language}'" if language else ''
        query = f"""
        SELECT full_name, stars, language, description
        FROM {self.config.repositories_table} FINAL
        {where_clause}
        ORDER BY stars DESC
        LIMIT {limit}
        """
        result = self.client.query(query)
        for row in result.result_rows:
            yield {
                'full_name': row[0],
                'stars': row[1],
                'language': row[2],
                'description': row[3],
            }

    # Artifact operations
    def insert_artifacts(
        self,
        data: Iterator[list[Any]] | list[list[Any]],
        columns: list[str],
        batch_size: int = 1000,
    ) -> None:
        """
        Insert artifact data in batches.

        Args:
            data: Iterator or list of rows to insert
            columns: Column names
            batch_size: Number of rows per batch
        """
        if isinstance(data, list):
            # If it's already a list, insert directly
            if not data:
                return
            self.client.insert(
                self.config.artifacts_table,
                data,
                column_names=columns,
            )
        else:
            # If it's an iterator, batch the inserts
            batch = []
            for row in data:
                batch.append(row)
                if len(batch) >= batch_size:
                    self.client.insert(
                        self.config.artifacts_table,
                        batch,
                        column_names=columns,
                    )
                    batch = []
            # Insert remaining rows
            if batch:
                self.client.insert(
                    self.config.artifacts_table,
                    batch,
                    column_names=columns,
                )

    def get_artifact_count(self) -> int:
        """Get total number of artifacts."""
        result = self.client.query(
            f'SELECT count() FROM {self.config.artifacts_table} FINAL',
        )
        return result.result_rows[0][0]

    def search_dependencies(
        self,
        library_name: str,
        language: str | None = None,
        limit: int = 50,
    ) -> Generator[dict, None, None]:
        """
        Search for repositories that depend on a specific library.

        Args:
            library_name: Name of the library to search for
            language: Filter by language (optional)
            limit: Maximum number of results

        Yields:
            Dictionary with dependency information
        """
        lang_filter = f"AND r.language = '{language}'" if language else ''
        pattern = f"%{library_name}%"

        query = f"""
        SELECT DISTINCT
            r.full_name,
            r.stars,
            r.language,
            a.name,
            a.version
        FROM {self.config.artifacts_table} AS a FINAL
        JOIN {self.config.repositories_table} AS r FINAL ON a.repository_id = r.id
        WHERE a.name LIKE %(pattern)s
        {lang_filter}
        ORDER BY r.stars DESC
        LIMIT {limit}
        """

        result = self.client.query(query, parameters={'pattern': pattern})
        for row in result.result_rows:
            yield {
                'full_name': row[0],
                'stars': row[1],
                'language': row[2],
                'artifact_name': row[3],
                'version': row[4],
            }

    def get_top_dependencies(
        self,
        limit: int = 20,
        language: str | None = None,
    ) -> Generator[dict, None, None]:
        """
        Get most popular dependencies.

        Args:
            limit: Maximum number of dependencies to return
            language: Filter by language (optional)

        Yields:
            Dictionary with dependency statistics
        """
        lang_filter = ''
        if language:
            lang_filter = f"""
            WHERE repository_id IN (
                SELECT id FROM {self.config.repositories_table} FINAL
                WHERE language = '{language}'
            )
            """

        query = f"""
        SELECT
            name,
            count(DISTINCT repository_id) as repo_count
        FROM {self.config.artifacts_table} FINAL
        {lang_filter}
        GROUP BY name
        ORDER BY repo_count DESC
        LIMIT {limit}
        """

        result = self.client.query(query)
        for row in result.result_rows:
            yield {
                'name': row[0],
                'repo_count': row[1],
            }

    def get_framework_stats(
        self,
        framework_packages: list[str],
    ) -> Generator[dict, None, None]:
        """
        Get statistics for specific framework packages.

        Args:
            framework_packages: List of package names to query

        Yields:
            Dictionary with framework statistics
        """
        packages_str = "', '".join(framework_packages)
        query = f"""
        SELECT
            a.name,
            count(DISTINCT a.repository_id) as repo_count,
            sum(r.stars) as total_stars
        FROM {self.config.artifacts_table} FINAL a
        JOIN {self.config.repositories_table} FINAL r ON a.repository_id = r.id
        WHERE a.name IN ('{packages_str}')
        GROUP BY a.name
        ORDER BY repo_count DESC
        """

        result = self.client.query(query)
        for row in result.result_rows:
            yield {
                'name': row[0],
                'repo_count': row[1],
                'total_stars': row[2],
            }
