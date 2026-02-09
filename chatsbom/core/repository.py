"""Data access layer implementing CQRS (Command Query Responsibility Segregation)."""
from abc import ABC
from collections.abc import Generator
from typing import Any

import clickhouse_connect
from clickhouse_connect.driver.client import Client

from chatsbom.core.config import DatabaseConfig
from chatsbom.core.schema import ARTIFACTS_DDL
from chatsbom.core.schema import RELEASES_DDL
from chatsbom.core.schema import REPOSITORIES_DDL


class BaseRepository(ABC):
    """Abstract base repository handling connection lifecycle."""

    def __init__(self, config: DatabaseConfig):
        self.config = config
        self._client: Client | None = None

    @property
    def client(self) -> Client:
        if self._client is None:
            self._client = clickhouse_connect.get_client(
                **self.config.get_connection_params(),
            )
        return self._client

    def close(self) -> None:
        if self._client is not None:
            self._client.close()
            self._client = None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()


class IngestionRepository(BaseRepository):
    """Write-only repository for Admin operations (Collect, Enrich, Index)."""

    def ensure_schema(self) -> None:
        """Idempotent schema creation."""
        self.client.command(
            f"CREATE DATABASE IF NOT EXISTS {self.config.database}",
        )
        self.client.command(REPOSITORIES_DDL)
        self.client.command(ARTIFACTS_DDL)
        self.client.command(RELEASES_DDL)

    def reset_schema(self) -> None:
        """Drop and recreate schema (Destructive)."""
        self.client.command(
            f'DROP TABLE IF EXISTS {self.config.artifacts_table}',
        )
        self.client.command('DROP TABLE IF EXISTS releases')
        self.client.command(
            f'DROP TABLE IF EXISTS {self.config.repositories_table}',
        )
        self.ensure_schema()

    def insert_batch(self, table: str, data: list[list[Any]], columns: list[str]) -> None:
        """Generic batch insert."""
        if not data:
            return
        self.client.insert(table, data, column_names=columns)


class QueryRepository(BaseRepository):
    """Read-only repository for Guest operations (Query, Chat, Status)."""

    def get_stats(self) -> dict[str, int]:
        """Get high-level database statistics."""
        # Using separate queries for safety and simplicity
        repos = self.client.query(
            f'SELECT count() FROM {self.config.repositories_table} FINAL',
        ).result_rows[0][0]
        artifacts = self.client.query(
            f'SELECT count() FROM {self.config.artifacts_table} FINAL',
        ).result_rows[0][0]
        return {'repositories': repos, 'artifacts': artifacts}

    def get_language_stats(self) -> Generator[tuple[str, int], None, None]:
        query = f"""
        SELECT language, count() as cnt
        FROM {self.config.repositories_table} FINAL
        GROUP BY language
        ORDER BY cnt DESC
        """
        for row in self.client.query(query).result_rows:
            yield (row[0], row[1])

    def get_top_dependencies(self, limit: int = 20) -> Generator[dict, None, None]:
        query = f"""
        SELECT name, count(DISTINCT repository_id) as repo_count
        FROM {self.config.artifacts_table} FINAL
        GROUP BY name
        ORDER BY repo_count DESC
        LIMIT {limit}
        """
        for row in self.client.query(query).result_rows:
            yield {'name': row[0], 'repo_count': row[1]}

    def get_dependency_type_distribution(self) -> Generator[tuple[str, int], None, None]:
        query = f"""
        SELECT type, count() as cnt
        FROM {self.config.artifacts_table} FINAL
        GROUP BY type
        ORDER BY cnt DESC
        """
        for row in self.client.query(query).result_rows:
            yield (row[0], row[1])

    def search_library_candidates(self, pattern: str, language: str | None = None, limit: int = 20) -> list[tuple[str, int]]:
        lang_filter = 'AND r.language = {language:String}' if language else ''
        query = f"""
        SELECT a.name, count() as cnt
        FROM {self.config.artifacts_table} AS a FINAL
        JOIN {self.config.repositories_table} AS r FINAL ON a.repository_id = r.id
        WHERE a.name ILIKE {{pattern:String}} {lang_filter}
        GROUP BY a.name
        ORDER BY cnt DESC
        LIMIT {{limit:UInt32}}
        """
        params = {'pattern': f"%{pattern}%", 'limit': limit}
        if language:
            params['language'] = language

        return self.client.query(query, parameters=params).result_rows

    def get_dependent_count(self, library_name: str, language: str | None = None) -> int:
        lang_filter = 'AND r.language = {language:String}' if language else ''
        query = f"""
        SELECT count()
        FROM {self.config.artifacts_table} AS a FINAL
        JOIN {self.config.repositories_table} AS r FINAL ON a.repository_id = r.id
        WHERE a.name = {{library:String}} {lang_filter}
        """
        params = {'library': library_name}
        if language:
            params['language'] = language
        return self.client.query(query, parameters=params).result_rows[0][0]

    def get_dependents(self, library_name: str, language: str | None = None, limit: int = 50) -> list[tuple[str, str, int, str, str]]:
        lang_filter = 'AND r.language = {language:String}' if language else ''
        query = f"""
        SELECT r.owner, r.repo, r.stars, a.version, r.url
        FROM {self.config.artifacts_table} AS a FINAL
        JOIN {self.config.repositories_table} AS r FINAL ON a.repository_id = r.id
        WHERE a.name = {{library:String}} {lang_filter}
        ORDER BY r.stars DESC
        LIMIT {{limit:UInt32}}
        """
        params = {'library': library_name, 'limit': limit}
        if language:
            params['language'] = language
        return self.client.query(query, parameters=params).result_rows

    def get_framework_usage(self, language: str, packages: list[str]) -> int:
        query = f"""
        SELECT count(DISTINCT r.id)
        FROM {self.config.repositories_table} AS r FINAL
        JOIN {self.config.artifacts_table} AS a FINAL ON r.id = a.repository_id
        WHERE r.language = {{lang:String}} AND a.name IN {{pkgs:Array(String)}}
        """
        return self.client.query(query, parameters={'lang': language, 'pkgs': packages}).result_rows[0][0]

    def get_top_projects_by_framework(self, language: str, packages: list[str], limit: int = 3) -> list[tuple[str, int, str]]:
        query = f"""
        SELECT DISTINCT r.full_name, r.stars, r.url
        FROM {self.config.repositories_table} AS r FINAL
        JOIN {self.config.artifacts_table} AS a FINAL ON r.id = a.repository_id
        WHERE r.language = {{lang:String}} AND a.name IN {{pkgs:Array(String)}}
        ORDER BY r.stars DESC
        LIMIT {{limit:UInt32}}
        """
        return self.client.query(query, parameters={'lang': language, 'pkgs': packages, 'limit': limit}).result_rows
