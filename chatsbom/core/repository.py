"""Data access layer implementing CQRS (Command Query Responsibility Segregation).

Two design notes that the SQL below depends on:

No query joins against `artifacts` with `FINAL`. Deduplication comes from
the join condition instead: an artifact belongs to the current scan only
if its `sbom_commit_sha` matches the one recorded on its repository, so
superseded scans drop out without a merge pass over millions of rows.
`FINAL` appears only on `repositories` — tens of thousands of rows — and
in `get_stats`, where a raw `count()` would report un-merged duplicates.

Counts are always `count(DISTINCT repository_id)`. A repository can
contribute several artifact rows for one package — two catalogers finding
it, or a package appearing at several versions — and counting rows made
"how many projects use X" overstate itself.
"""
from abc import ABC
from collections.abc import Iterator
from datetime import datetime
from typing import Any
from typing import Self

import clickhouse_connect
import structlog
from clickhouse_connect.driver.client import Client

from chatsbom.core.config import DatabaseConfig
from chatsbom.core.schema import ARTIFACTS
from chatsbom.core.schema import ddl_column_definitions
from chatsbom.core.schema import RELEASES
from chatsbom.core.schema import REPOSITORIES
from chatsbom.core.schema import TABLE_DDL
from chatsbom.models.query import AdoptionPoint
from chatsbom.models.query import DatabaseStats
from chatsbom.models.query import Dependent
from chatsbom.models.query import LanguageCount
from chatsbom.models.query import LibraryCandidate
from chatsbom.models.query import PackagePopularity
from chatsbom.models.query import Row
from chatsbom.models.query import row_mapper
from chatsbom.models.query import VersionObservation
from chatsbom.models.relationship import DIRECT

logger = structlog.get_logger('repository')

Parameters = dict[str, Any]


class BaseRepository(ABC):
    """Abstract base repository handling connection lifecycle."""

    def __init__(self, config: DatabaseConfig) -> None:
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

    def __enter__(self) -> Self:
        return self

    def __exit__(self, exc_type: object, exc_val: object, exc_tb: object) -> None:
        self.close()


class IngestionRepository(BaseRepository):
    """Write-only repository for Admin operations (Collect, Enrich, Index)."""

    def ensure_schema(self) -> None:
        """Idempotent schema creation."""
        try:
            bootstrap = clickhouse_connect.get_client(
                host=self.config.host,
                port=self.config.port,
                username=self.config.user,
                password=self.config.password,
                database='default',
            )
        except Exception:
            # The target database may already exist and be reachable even
            # when `default` is not; fall through to the DDL below.
            pass
        else:
            with bootstrap:
                bootstrap.command(
                    f"CREATE DATABASE IF NOT EXISTS {self.config.database}",
                )

        for table, ddl in TABLE_DDL:
            self.client.command(ddl)
            self._reconcile_columns(table, ddl)

    def _reconcile_columns(self, table: str, ddl: str) -> None:
        """Add columns the DDL declares but the existing table lacks.

        `CREATE TABLE IF NOT EXISTS` does nothing when the table exists,
        so a database created before a column was declared kept working
        until an insert failed inside the driver with "Unrecognized
        column". Migration is additive and metadata-only in ClickHouse for
        a defaulted column, so it is safe to run on every startup; columns
        the DDL no longer mentions are left alone rather than dropped.
        """
        existing = {
            str(row[0])
            for row in self.client.query(
                'SELECT name FROM system.columns '
                'WHERE database = {db:String} AND table = {table:String}',
                parameters={'db': self.config.database, 'table': table},
            ).result_rows
        }
        if not existing:
            return

        for column, definition in ddl_column_definitions(ddl).items():
            if column in existing:
                continue
            self.client.command(
                f'ALTER TABLE {table} ADD COLUMN IF NOT EXISTS '
                f'{column} {definition}',
            )
            logger.info(
                'Schema migrated', table=table, added_column=column,
            )

    def reset_schema(self) -> None:
        """Drop and recreate schema (Destructive)."""
        for table in (ARTIFACTS, RELEASES, REPOSITORIES):
            self.client.command(f'DROP TABLE IF EXISTS {table.name}')
        self.ensure_schema()

    def insert_batch(
        self,
        table: str,
        data: list[list[Any]],
        columns: list[str],
    ) -> None:
        """Generic batch insert."""
        if not data:
            return
        self.client.insert(table, data, column_names=columns)

    def rebuild_table(self, table: str) -> None:
        """Drop one table and recreate it from the current DDL.

        Needed when a schema change makes existing rows unreachable
        rather than merely incomplete. The off-by-one fix did exactly
        that: 6.1M artifact rows carry a 7-character `sbom_commit_sha`,
        so the scan-matching join excludes them and no amount of
        re-ingestion replaces them.
        """
        managed = {name for name, _ in TABLE_DDL}
        if table not in managed:
            raise ValueError(
                f"{table!r} is not a managed table; "
                f"expected one of {', '.join(sorted(managed))}",
            )

        ddl = next(d for name, d in TABLE_DDL if name == table)
        self.client.command(f'DROP TABLE IF EXISTS {table}')
        self.client.command(ddl)
        logger.info('Table rebuilt', table=table)

    def optimize(self) -> None:
        """Collapse superseded ReplacingMergeTree rows.

        Run after ingestion so reads need no `FINAL` on the large tables.
        `artifacts` is a plain MergeTree — nothing to collapse there, so
        it is merged for read efficiency but not deduplicated.
        """
        for table in (REPOSITORIES, RELEASES):
            self.client.command(f'OPTIMIZE TABLE {table.name} FINAL')
        self.client.command(f'OPTIMIZE TABLE {ARTIFACTS.name}')


# Current repositories, deduplicated once so joins do not need FINAL.
_CURRENT_REPOS = f"""
SELECT id, owner, repo, stars, url, language, sbom_commit_sha
FROM {REPOSITORIES.name} FINAL
"""

# An artifact belongs to the current scan of its repository.
_ON_CURRENT_SCAN = (
    'a.repository_id = r.id AND a.sbom_commit_sha = r.sbom_commit_sha'
)


class QueryRepository(BaseRepository):
    """Read-only repository for Guest operations (Query, Chat, Status)."""

    def _rows(self, sql: str, parameters: Parameters | None = None) -> list[Row]:
        """Run a query and return rows keyed by column name."""
        result = self.client.query(sql, parameters=parameters or {})
        return list(result.named_results())

    def stream_rows(
        self,
        sql: str,
        parameters: Parameters | None = None,
    ) -> Iterator[Row]:
        """Stream a result set block by block, keyed by column name.

        For exports large enough that materialising every row at once is
        the wrong shape.
        """
        with self.client.query_row_block_stream(
            sql, parameters=parameters or {},
        ) as stream:
            columns: list[str] | None = None
            for block in stream:
                if columns is None:
                    columns = list(stream.source.column_names)
                for row in block:
                    yield dict(zip(columns, row))

    @staticmethod
    def _filters(
        language: str | None,
        direct_only: bool,
    ) -> tuple[str, str, Parameters]:
        """Optional predicates, as (repo_clause, artifact_clause, params)."""
        params: Parameters = {}
        repo_clause = ''
        artifact_clause = ''
        if language:
            repo_clause = 'WHERE lower(language) = {language:String}'
            params['language'] = language.lower()
        if direct_only:
            artifact_clause = 'AND a.relationship = {relationship:String}'
            params['relationship'] = DIRECT
        return repo_clause, artifact_clause, params

    # -- statistics ---------------------------------------------------------

    def get_stats(self) -> DatabaseStats:
        """High-level row counts per table."""
        sql = f"""
        SELECT
            (SELECT count() FROM {REPOSITORIES.name} FINAL) AS repositories,
            (SELECT count() FROM {ARTIFACTS.name}) AS artifacts,
            (SELECT count() FROM {RELEASES.name} FINAL) AS releases
        """
        return DatabaseStats.from_row(self._rows(sql)[0])

    def get_language_stats(self) -> list[LanguageCount]:
        sql = f"""
        SELECT language, count() AS repository_count
        FROM ({_CURRENT_REPOS})
        GROUP BY language
        ORDER BY repository_count DESC, language ASC
        """
        return row_mapper(LanguageCount)(self._rows(sql))

    def get_top_packages(
        self,
        limit: int = 20,
        language: str | None = None,
    ) -> list[PackagePopularity]:
        """Most depended-upon packages, split by direct vs transitive.

        The split matters: without it the ranking is dominated by npm
        micro-packages that no project ever asks for by name.
        """
        repo_clause, _, params = self._filters(language, direct_only=False)
        params['limit'] = limit
        sql = f"""
        SELECT
            a.name AS name,
            count(DISTINCT a.repository_id) AS repository_count,
            count(DISTINCT if(a.relationship = '{DIRECT}', a.repository_id, NULL))
                AS direct_count
        FROM {ARTIFACTS.name} AS a
        INNER JOIN ({_CURRENT_REPOS} {repo_clause}) AS r
            ON {_ON_CURRENT_SCAN}
        GROUP BY a.name
        ORDER BY repository_count DESC, name ASC
        LIMIT {{limit:UInt32}}
        """
        return row_mapper(PackagePopularity)(self._rows(sql, params))

    def get_dependency_type_distribution(self) -> Iterator[tuple[str, int]]:
        sql = f"""
        SELECT type, count(DISTINCT repository_id) AS repository_count
        FROM {ARTIFACTS.name}
        GROUP BY type
        ORDER BY repository_count DESC
        """
        for row in self._rows(sql):
            yield (str(row['type']), int(row['repository_count']))

    # -- history ------------------------------------------------------------

    def get_version_history(
        self,
        library_name: str,
        since: datetime | None = None,
        limit: int = 500,
    ) -> list[VersionObservation]:
        """Every version of a package, with when it was observed.

        Reads the whole append-only table rather than the current scan:
        this is the question a snapshot cannot answer.
        """
        params: Parameters = {'library': library_name, 'limit': limit}
        window = ''
        if since is not None:
            window = 'AND a.observed_at >= {since:DateTime}'
            params['since'] = since

        sql = f"""
        SELECT
            a.version AS version,
            count(DISTINCT a.repository_id) AS repository_count,
            min(a.observed_at) AS observed_at
        FROM {ARTIFACTS.name} AS a
        WHERE a.name = {{library:String}} AND a.version != '' {window}
        GROUP BY a.version
        ORDER BY observed_at ASC, version ASC
        LIMIT {{limit:UInt32}}
        """
        return row_mapper(VersionObservation)(self._rows(sql, params))

    def get_adoption_over_time(
        self,
        library_name: str,
        since: datetime | None = None,
    ) -> list[AdoptionPoint]:
        """Monthly repository counts for a package.

        The partition key is `toYYYYMM(observed_at)`, so a bounded window
        prunes whole partitions rather than scanning.
        """
        params: Parameters = {'library': library_name}
        window = ''
        if since is not None:
            window = 'AND a.observed_at >= {since:DateTime}'
            params['since'] = since

        sql = f"""
        SELECT
            formatDateTime(a.observed_at, '%Y-%m') AS month,
            count(DISTINCT a.repository_id) AS repository_count,
            count(DISTINCT if(a.relationship = '{DIRECT}', a.repository_id, NULL))
                AS direct_count
        FROM {ARTIFACTS.name} AS a
        WHERE a.name = {{library:String}} {window}
        GROUP BY month
        ORDER BY month ASC
        """
        return row_mapper(AdoptionPoint)(self._rows(sql, params))

    # -- library lookup -----------------------------------------------------

    def search_library_candidates(
        self,
        pattern: str,
        language: str | None = None,
        limit: int = 20,
    ) -> list[LibraryCandidate]:
        """Package names matching `pattern`, ranked by how many repos use them."""
        repo_clause, _, params = self._filters(language, direct_only=False)
        params.update({'pattern': f"%{pattern}%", 'limit': limit})
        sql = f"""
        SELECT a.name AS name, count(DISTINCT a.repository_id) AS repository_count
        FROM {ARTIFACTS.name} AS a
        INNER JOIN ({_CURRENT_REPOS} {repo_clause}) AS r
            ON {_ON_CURRENT_SCAN}
        WHERE a.name ILIKE {{pattern:String}}
        GROUP BY a.name
        ORDER BY repository_count DESC, name ASC
        LIMIT {{limit:UInt32}}
        """
        return row_mapper(LibraryCandidate)(self._rows(sql, params))

    def get_dependent_count(
        self,
        library_name: str,
        language: str | None = None,
        direct_only: bool = False,
    ) -> int:
        """How many repositories depend on `library_name`."""
        repo_clause, artifact_clause, params = self._filters(
            language, direct_only,
        )
        params['library'] = library_name
        sql = f"""
        SELECT count(DISTINCT a.repository_id) AS repository_count
        FROM {ARTIFACTS.name} AS a
        INNER JOIN ({_CURRENT_REPOS} {repo_clause}) AS r
            ON {_ON_CURRENT_SCAN}
        WHERE a.name = {{library:String}} {artifact_clause}
        """
        return int(self._rows(sql, params)[0]['repository_count'])

    def get_dependents(
        self,
        library_name: str,
        language: str | None = None,
        limit: int = 50,
        direct_only: bool = False,
    ) -> list[Dependent]:
        """Repositories depending on `library_name`, most starred first.

        `LIMIT 1 BY r.id` keeps one row per repository when a package is
        catalogued more than once in the same scan.
        """
        repo_clause, artifact_clause, params = self._filters(
            language, direct_only,
        )
        params.update({'library': library_name, 'limit': limit})
        sql = f"""
        SELECT
            r.owner AS owner,
            r.repo AS repo,
            r.stars AS stars,
            a.version AS version,
            r.url AS url,
            a.relationship AS relationship
        FROM {ARTIFACTS.name} AS a
        INNER JOIN ({_CURRENT_REPOS} {repo_clause}) AS r
            ON {_ON_CURRENT_SCAN}
        WHERE a.name = {{library:String}} {artifact_clause}
        ORDER BY r.stars DESC, r.owner ASC, r.repo ASC
        LIMIT 1 BY r.id
        LIMIT {{limit:UInt32}}
        """
        return row_mapper(Dependent)(self._rows(sql, params))

    # -- framework lookup ---------------------------------------------------

    def get_framework_usage(
        self,
        language: str,
        packages: list[str],
        direct_only: bool = False,
    ) -> int:
        if not packages:
            return 0
        _, artifact_clause, params = self._filters(None, direct_only)
        params.update({'lang': language.lower(), 'pkgs': packages})
        sql = f"""
        SELECT count(DISTINCT a.repository_id) AS repository_count
        FROM {ARTIFACTS.name} AS a
        INNER JOIN (
            {_CURRENT_REPOS} WHERE lower(language) = {{lang:String}}
        ) AS r ON {_ON_CURRENT_SCAN}
        WHERE a.name IN {{pkgs:Array(String)}} {artifact_clause}
        """
        return int(self._rows(sql, params)[0]['repository_count'])

    def get_top_projects_by_framework(
        self,
        language: str,
        packages: list[str],
        limit: int = 3,
    ) -> list[Dependent]:
        if not packages:
            return []
        params: Parameters = {
            'lang': language.lower(), 'pkgs': packages, 'limit': limit,
        }
        sql = f"""
        SELECT
            r.owner AS owner,
            r.repo AS repo,
            r.stars AS stars,
            a.version AS version,
            r.url AS url,
            a.relationship AS relationship
        FROM {ARTIFACTS.name} AS a
        INNER JOIN (
            {_CURRENT_REPOS} WHERE lower(language) = {{lang:String}}
        ) AS r ON {_ON_CURRENT_SCAN}
        WHERE a.name IN {{pkgs:Array(String)}}
        ORDER BY r.stars DESC, r.owner ASC, r.repo ASC
        LIMIT 1 BY r.id
        LIMIT {{limit:UInt32}}
        """
        return row_mapper(Dependent)(self._rows(sql, params))

    def get_repository_frameworks(
        self,
        repository_id: int,
        framework_map: dict[str, list[str]],
    ) -> list[tuple[str, str]]:
        """Frameworks used by one repository, as (framework, version)."""
        return self.get_frameworks_for_repositories(
            [repository_id], framework_map,
        ).get(repository_id, [])

    def get_frameworks_for_repositories(
        self,
        repository_ids: list[int],
        framework_map: dict[str, list[str]],
    ) -> dict[int, list[tuple[str, str]]]:
        """Batched form of `get_repository_frameworks`.

        Classifying thousands of repositories one query at a time was the
        N+1 in `github classify`.
        """
        package_to_framework = {
            package: framework
            for framework, packages in framework_map.items()
            for package in packages
            if package
        }
        if not repository_ids or not package_to_framework:
            return {}

        sql = f"""
        SELECT repository_id, name, version
        FROM {ARTIFACTS.name}
        WHERE repository_id IN {{repo_ids:Array(UInt64)}}
          AND name IN {{pkgs:Array(String)}}
        """
        params: Parameters = {
            'repo_ids': repository_ids,
            'pkgs': list(package_to_framework),
        }

        found: dict[int, list[tuple[str, str]]] = {}
        for row in self._rows(sql, params):
            framework = package_to_framework.get(str(row['name']))
            if not framework:
                continue
            repo_id = int(row['repository_id'])
            version = '' if row['version'] is None else str(row['version'])
            found.setdefault(repo_id, []).append((framework, version))

        for entries in found.values():
            entries.sort()
        return found
